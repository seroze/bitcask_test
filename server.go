package main

import (
	"encoding/json"
	"fmt"
	"net/http"
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

[] First write an in-memory version of it which supports all the above apis
then we will think of how to make it fault-tolerant

Can you write a plan of how this should be done

- bitcask works by storing key,value pairs in file
- we have an in-memory index which points to the position from which the contents
  are present
  	- crc(1 byte)+key-size(64 byte)+key+value-size(64 byte)+value
-

*/

type Row struct {
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

	// extract the expiry time and val individually
	parts := strings.Split(val, ":")
	expiry, _ := strconv.Atoi(parts[0])
	currentTime := time.Now().Unix() // 64 bit int
	if currentTime > int64(expiry) {
		response := Response{Message: "No value found"}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}
	val = parts[1]
	response := Response{Message: val}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func main() {
	fmt.Println("Main started")

	storage = NewInMemoryStorage()

	http.HandleFunc("/rpc/hello", HelloHandler)
	http.HandleFunc("/rpc/set", setKeyValHandler)
	http.HandleFunc("/rpc/get", getKeyHandler)
	http.HandleFunc("/rpc/delete", deleteKeyValHandler)

	http.ListenAndServe(":8080", nil)
}
