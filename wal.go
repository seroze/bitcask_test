package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"
)

type WalRecord struct {
	CRC       uint32
	Timestamp int64
	KeySize   int64
	Key       string
	ValueSize int64
	Value     string
	OpType    byte // 0x01 for SET, 0x02 for DELETE
}

type WAL interface {
	Write(record WalRecord) error
	ReadAll() ([]WalRecord, error)
	Sync() error
}

// NewWAL initializes a new Write-Ahead Log (WAL)
func NewWAL(walFilePath string) (WAL, error) {
	file, err := os.OpenFile(walFilePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return &FileWAL{file: file}, nil
}

type FileWAL struct {
	file *os.File
	mu   sync.Mutex
}

func NewFileWAL(walFileName string) (*FileWAL, error) {
	file, err := os.OpenFile(walFileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &FileWAL{file: file}, nil
}

func (w *FileWAL) Write(record WalRecord) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	data := encodeWalRecord(record)

	_, err := w.file.Write(data)
	if err != nil {
		return err
	}

	return w.file.Sync() // Ensure durability
}

// ReadAll reads all WAL records using decodeRecord
func (w *FileWAL) ReadAll() ([]WalRecord, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	var records []WalRecord

	// Seek to the beginning of the file
	_, err := w.file.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	for {
		record, err := w.decodeWalRecord()
		if err == io.EOF {
			break // End of file reached
		}
		if err != nil {
			return nil, err
		}
		records = append(records, *record)
	}

	return records, nil
}

func (w *FileWAL) Sync() error {
	return w.file.Sync()
}

func (w *FileWAL) Close() error {
	return w.file.Close()
}

func encodeWalRecord(record WalRecord) []byte {
	buf := new(bytes.Buffer)

	// Write fields in binary format
	binary.Write(buf, binary.LittleEndian, record.CRC)
	binary.Write(buf, binary.LittleEndian, record.Timestamp)
	binary.Write(buf, binary.LittleEndian, record.KeySize)
	buf.WriteString(record.Key)
	binary.Write(buf, binary.LittleEndian, record.ValueSize)
	buf.WriteString(record.Value)
	binary.Write(buf, binary.LittleEndian, record.OpType)

	return buf.Bytes()
}

// decodeRecord reads a single WalRecord from the file
func (w *FileWAL) decodeWalRecord() (*WalRecord, error) {
	var record WalRecord

	// Read CRC
	err := binary.Read(w.file, binary.LittleEndian, &record.CRC)
	if err != nil {
		return nil, err
	}

	// Read Timestamp
	err = binary.Read(w.file, binary.LittleEndian, &record.Timestamp)
	if err != nil {
		return nil, err
	}

	// Read KeySize
	err = binary.Read(w.file, binary.LittleEndian, &record.KeySize)
	if err != nil {
		return nil, err
	}

	// Read Key
	key := make([]byte, record.KeySize)
	_, err = io.ReadFull(w.file, key)
	if err != nil {
		return nil, err
	}
	record.Key = string(key)

	// Read ValueSize
	err = binary.Read(w.file, binary.LittleEndian, &record.ValueSize)
	if err != nil {
		return nil, err
	}

	// Read Value
	value := make([]byte, record.ValueSize)
	_, err = io.ReadFull(w.file, value)
	if err != nil {
		return nil, err
	}
	record.Value = string(value)

	// Read OpType
	err = binary.Read(w.file, binary.LittleEndian, &record.OpType)
	if err != nil {
		return nil, err
	}

	return &record, nil
}
