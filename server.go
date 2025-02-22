package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

/**
what api's should this server expose

get(key)
put(key, val, expiry) // let's have expiry field as well
del(key)

**/

type Record struct {
	crc       int8
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
	recordLen := 1 + 8 + 8 + 8 + keySize + valSize
	record := make([]byte, recordLen)

	// Setting a dummy CRC for now (1 byte)
	record[0] = 0

	// Encode expiry (8 bytes)
	binary.LittleEndian.PutUint64(record[1:9], uint64(expiry))

	// Encode key size (8 bytes)
	binary.LittleEndian.PutUint64(record[9:17], uint64(keySize))

	// Encode value size (8 bytes)
	binary.LittleEndian.PutUint64(record[17:25], uint64(valSize))

	// Copy key bytes
	copy(record[25:25+keySize], key)

	// Copy value bytes
	copy(record[25+keySize:], val)

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

	header := make([]byte, 1+8+8+8)
	// loads the file contents to byte slice till it's full
	_, err := file.Read(header)
	if err != nil {
		return nil, err
	}

	// Extract fields from the header
	crc := int8(header[0])
	expiry := int64(binary.LittleEndian.Uint64(header[1:9]))
	keySize := int64(binary.LittleEndian.Uint64(header[9:17]))
	valueSize := int64(binary.LittleEndian.Uint64(header[17:25]))

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

	// Construct and return the decoded record
	return &Record{
		crc:       crc,
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
	if err != nil {
		fmt.Println("Problem initializing bitcask storage")
	}

	http.HandleFunc("/rpc/hello", HelloHandler)
	http.HandleFunc("/rpc/set", setKeyValHandler)
	http.HandleFunc("/rpc/get", getKeyHandler)
	http.HandleFunc("/rpc/delete", deleteKeyValHandler)

	http.ListenAndServe(":8080", nil)
}
