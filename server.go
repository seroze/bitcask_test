package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

/**
what api's should this server expose

get(key)
put(key, val, expiry) // let's have expiry field as well
del(key)

**/

type Record struct {
	crc       uint32
	expiry    int64
	keySize   int64
	key       string
	valueSize int64
	value     string
}

type Response struct {
	Message string `json:"message"`
}

var storage Storage

type Storage interface {
	Get(key string) (string, bool)
	Set(key string, value string, expiry int64) error
	Delete(key string) error
}

//------------------------ BitcaskStorage ------------------------

type BitcaskStorage struct {
	fileStore *FileStore       // fileStore
	index     map[string]int64 //index
	wal       *os.File         //write ahead log
	mu        sync.RWMutex
}

func (bs *BitcaskStorage) Get(key string) (string, bool) {
	// first find file offset from index
	offset, exists := bs.index[key]
	if !exists {
		fmt.Println("offset is missing in index")
		return "", false
	}

	val, expiry, err := bs.fileStore.Get(offset)
	if err != nil {
		// could be checksum mismatch
		fmt.Println("Somethings wrong with content %w", err)
		return "", false
	}

	currTime := time.Now().Unix()
	if currTime > expiry {
		fmt.Println("Key is expired")
		return "", false
	}

	return val, true
}

func (bs *BitcaskStorage) Set(key string, val string, expiry int64) error {

	offset, err := bs.fileStore.Set(key, val, expiry)
	if err != nil {
		fmt.Println("Problem storing the key-value pair")
		return err
	}
	bs.index[key] = offset
	return nil
}

func (bs *BitcaskStorage) Delete(key string) error {
	expiry := time.Now().Unix() - 100 // past even t
	offset, err := bs.fileStore.Set(key, "", expiry)
	if err != nil {
		fmt.Println("Problem storing the key-value pair")
		return err
	}
	bs.index[key] = offset
	return nil
}

func (bs *BitcaskStorage) Compact() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()

	fmt.Println("Starting compaction process...")

	// Define a new compacted file
	tempFileName := "bitcask_db_compact.file"
	tempFile, err := os.OpenFile(tempFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create compaction file: %w", err)
	}
	defer tempFile.Close()

	newIndex := make(map[string]int64)

	// Iterate through index and copy valid records
	for key, offset := range bs.index {
		val, expiry, err := bs.fileStore.Get(offset)
		if err != nil || time.Now().Unix() > expiry {
			// Skip expired or corrupted records
			continue
		}

		newOffset, err := bs.fileStore.appendToFile(tempFile, key, val, expiry)
		if err != nil {
			return fmt.Errorf("failed to write compacted data: %w", err)
		}

		newIndex[key] = newOffset
	}

	// Close the old file
	bs.fileStore.dataFile.Close()

	// Replace old database file with compacted file
	err = os.Rename(tempFileName, "bitcask_db.file")
	if err != nil {
		return fmt.Errorf("failed to replace old database file: %w", err)
	}

	// Reopen the new compacted file
	bs.fileStore.dataFile, err = os.OpenFile("bitcask_db.file", os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to reopen compacted database file: %w", err)
	}

	// Update the index with new offsets
	bs.index = newIndex

	fmt.Println("Compaction completed successfully!")
	return nil
}

func startCompactionRoutine(bs *BitcaskStorage, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func() {
		for range ticker.C {
			err := bs.Compact()
			if err != nil {
				fmt.Printf("Compaction failed: %v\n", err)
			}
		}
	}()
}

//------------------------ File Store ------------------------

type FileStore struct {
	dataFile *os.File
	mu       sync.RWMutex
}

func NewFileStore(dataFileName string) (*FileStore, error) {

	file, err := os.OpenFile(dataFileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	return &FileStore{
		dataFile: file,
	}, nil
}

func (fs *FileStore) appendToFile(file *os.File, key string, value string, expiry int64) (int64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Encode the key-value record in Bitcask format
	record := encodeRecord(key, value, expiry)

	// Get the current offset before writing
	offset, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return -1, fmt.Errorf("failed to seek end of file: %w", err)
	}

	// Write the record to file
	_, err = file.Write(record)
	if err != nil {
		return -1, fmt.Errorf("failed to write record: %w", err)
	}

	// Ensure the data is physically written to disk
	err = file.Sync()
	if err != nil {
		return -1, fmt.Errorf("failed to sync file: %w", err)
	}

	// Return the offset where this record was written
	return offset, nil
}

func (fs *FileStore) Set(key string, value string, expiry int64) (int64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Get the current offset before writing
	offset, err := fs.dataFile.Seek(0, os.SEEK_END)
	if err != nil {
		return -1, fmt.Errorf("failed to seek end of file: %w", err)
	}

	// Construct the record format
	// crc(1 byte)+key-size(64 byte)+key+value-size(64 byte)+value
	record := encodeRecord(key, value, expiry)

	// Write to file
	_, err = fs.dataFile.Write(record)
	if err != nil {
		return -1, fmt.Errorf("failed to write data: %w", err)
	}

	//return the offset file for indexing
	return offset, nil
}

// encodeRecord
func encodeRecord(key string, val string, expiry int64) []byte {
	// Calculate sizes
	keySize := int64(len(key))
	valSize := int64(len(val))

	// Calculate total record length
	recordLen := 4 + 8 + 8 + 8 + keySize + valSize
	record := make([]byte, recordLen)

	// Encode expiry (8 bytes)
	binary.LittleEndian.PutUint64(record[4:12], uint64(expiry))

	// Encode key size (8 bytes)
	binary.LittleEndian.PutUint64(record[12:20], uint64(keySize))

	// Encode value size (8 bytes)
	binary.LittleEndian.PutUint64(record[20:28], uint64(valSize))

	// Copy key bytes
	copy(record[28:28+keySize], key)

	// Copy value bytes
	copy(record[28+keySize:], val)

	// Compute checksum (excluding first 4 bytes, where checksum is stored)
	checksum := crc32.ChecksumIEEE(record[4:])
	binary.LittleEndian.PutUint32(record[:4], checksum) // Store checksum at the beginning

	return record
}

// Close closes the file.
func (fs *FileStore) Close() error {
	return fs.dataFile.Close()
}

func (fs *FileStore) Get(offset int64) (string, int64, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Seek to the stored offset
	_, err := fs.dataFile.Seek(offset, os.SEEK_SET)

	if err != nil {
		return "", 0, fmt.Errorf("failed to seek file %w", err)
	}

	record, err := decodeRecord(fs.dataFile)
	if err != nil {
		return "", 0, fmt.Errorf("failed to decode record: %w", err)
	}

	currentTime := time.Now().Unix()
	fmt.Println(record, " record ", currentTime)

	// Check if the key has expired
	if currentTime > record.expiry {
		return "", 0, fmt.Errorf("key has expired")
	}

	return record.value, record.expiry, nil
}

func decodeRecord(file *os.File) (*Record, error) {

	header := make([]byte, 4+8+8+8)
	// loads the file contents to byte slice till it's full
	_, err := file.Read(header)
	if err != nil {
		return nil, err
	}

	// Extract checksum
	storedChecksum := binary.LittleEndian.Uint32(header[:4])

	expiry := int64(binary.LittleEndian.Uint64(header[4:12]))
	keySize := int64(binary.LittleEndian.Uint64(header[12:20]))
	valueSize := int64(binary.LittleEndian.Uint64(header[20:28]))

	// Read the key
	keyBytes := make([]byte, keySize)
	_, err = file.Read(keyBytes)
	if err != nil {
		return nil, err
	}
	key := string(keyBytes)

	// Read the val
	valBytes := make([]byte, valueSize)
	_, err = file.Read(valBytes)
	if err != nil {
		return nil, err
	}
	val := string(valBytes)

	// Recalculate checksum for validation
	fullRecord := append(header[4:], keyBytes...)
	fullRecord = append(fullRecord, valBytes...)
	calculatedChecksum := crc32.ChecksumIEEE(fullRecord)

	// Validate checksum
	if storedChecksum != calculatedChecksum {
		return nil, errors.New("checksum mismatch: data may be corrupted")
	}

	// Construct and return the decoded record
	return &Record{
		crc:       storedChecksum,
		expiry:    expiry,
		keySize:   keySize,
		key:       key,
		valueSize: valueSize,
		value:     val,
	}, nil
}

// --------------------------------------------------------------------------------

func NewBitcaskStorage(walFilename string, dbFilename string) (*BitcaskStorage, error) {

	walFile, err := os.OpenFile(walFilename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	fileStore, err := NewFileStore(dbFilename)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize data file store: %w", err)
	}

	return &BitcaskStorage{
		fileStore: fileStore, // Assuming FileStore has its own init
		index:     make(map[string]int64),
		wal:       walFile,
	}, nil
}

//------------------------ InMemoryStorage ------------------------

type InMemoryStorage struct {
	data map[string]string
	mu   sync.RWMutex
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{data: make(map[string]string)}
}

func (s *InMemoryStorage) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, exists := s.data[key]

	if !exists {
		return "", false
	}

	// extract the expiry time and val individually
	parts := strings.Split(val, ":")
	expiry, _ := strconv.Atoi(parts[0])
	currentTime := time.Now().Unix() // 64 bit int
	if currentTime > int64(expiry) {
		return "", false
	}
	return val, true
}

func (s *InMemoryStorage) Set(key string, val string, expiry int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	finalVal := fmt.Sprintf("%d:%s", expiry, val)
	fmt.Println(finalVal)
	s.data[key] = finalVal
	fmt.Println(s.data)
	return nil
}

func (s *InMemoryStorage) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
	return nil
}

func HelloHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	response := Response{Message: "hello world"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func deleteKeyValHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Recevied setKeyVal request")
	if r.Method != http.MethodDelete {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	storage.Delete(key)
	response := Response{Message: "KeyValue pair is deleted successfully"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func setKeyValHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Recevied setKeyVal request")
	if r.Method != http.MethodPut {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	value := r.URL.Query().Get("value")
	if value == "" {
		http.Error(w, "Missing value parameter", http.StatusBadRequest)
		return
	}

	expiry := r.URL.Query().Get("expiry")
	if expiry == "" {
		http.Error(w, "Missing expiry parameter", http.StatusBadRequest)
		return
	}

	expiryInt, err := strconv.Atoi(expiry)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	fmt.Println(key, value, expiry)
	storage.Set(key, value, int64(expiryInt))
	response := Response{Message: "KeyValue pair is stored successfully"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func getKeyHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Received getKey request")
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "Missing key parameter", http.StatusBadRequest)
		return
	}

	val, exists := storage.Get(key)
	if !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}

	response := Response{Message: val}
	fmt.Println("Response for getKey, ", val)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// --------------------------------------------------------------------------------

func main() {
	fmt.Println("Main started")

	// storage = NewInMemoryStorage()
	var err error
	storage, err = NewBitcaskStorage("bitcask_wal.file", "bitcask_db.file")
	startCompactionRoutine(storage.(*BitcaskStorage), 1*time.Minute)

	if err != nil {
		fmt.Println("Problem initializing bitcask storage")
		os.Exit(1)
	}

	// Create an HTTP server
	server := &http.Server{Addr: ":8080"}

	http.HandleFunc("/rpc/hello", HelloHandler)
	http.HandleFunc("/rpc/set", setKeyValHandler)
	http.HandleFunc("/rpc/get", getKeyHandler)
	http.HandleFunc("/rpc/delete", deleteKeyValHandler)

	// Channel to listen for OS signals
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Run the HTTP server in a goroutine
	go func() {
		fmt.Println("Server is running on port 8080")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("ListenAndServe Error: %v\n", err)
		}
	}()

	// Wait for a shutdown signal
	<-stop
	fmt.Println("Shutting down server...")

	// Create a context with timeout to allow graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Shut down HTTP server
	if err := server.Shutdown(ctx); err != nil {
		fmt.Printf("Server Shutdown Error: %v\n", err)
	}

	// Close storage files
	if bitcaskStorage, ok := storage.(*BitcaskStorage); ok {
		fmt.Println("Closing storage files...")
		bitcaskStorage.fileStore.Close()
		bitcaskStorage.wal.Close()
	}

	fmt.Println("Shutdown complete")
}
