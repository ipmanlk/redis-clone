/*
This file contains the basic implementation of an append-only file for persistent
storage. It ensures data durability by appending commands to a file and syncing
it to disk. The file is synced every second to minimize data loss in case of
a crash. For a detailed description of the AOF persistence mode, refer to the
Redis documentation:

https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/
*/

package main

import (
	"bufio"
	"io"
	"os"
	"sync"
	"time"
)

type Aof struct {
	file *os.File
	rd   *bufio.Reader
	mu   sync.Mutex
}

// NewAof creates a new Aof instance and starts a goroutine to sync the file to disk every second.
func NewAof(path string) (*Aof, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	aof := &Aof{
		file: f,
		rd:   bufio.NewReader(f),
	}

	// Start a goroutine to sync AOF to disk every second
	go aof.periodicSync()

	return aof, nil
}

// periodicSync syncs the AOF file to disk every second.
func (aof *Aof) periodicSync() {
	for {
		time.Sleep(time.Second)

		aof.mu.Lock()
		aof.file.Sync()
		aof.mu.Unlock()
	}
}

// Close closes the AOF file.
func (aof *Aof) Close() error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	return aof.file.Close()
}

// Write writes a RESP value to the AOF file.
func (aof *Aof) Write(value Value) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	_, err := aof.file.Write(value.Marshal())
	if err != nil {
		return err
	}

	return nil
}

// Read reads all RESP values from the AOF file and applies the provided function to each value.
func (aof *Aof) Read(fn func(value Value)) error {
	aof.mu.Lock()
	defer aof.mu.Unlock()

	aof.file.Seek(0, io.SeekStart)

	reader := NewResp(aof.file)

	for {
		value, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		fn(value)
	}

	return nil
}
