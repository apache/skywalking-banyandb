// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.  See the License for the specific
// language governing permissions and limitations under the License.

package flightrecorder

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"unsafe"

	"github.com/apache/skywalking-banyandb/fodc/internal/poller"
)

const (
	// DefaultBufferSize is the default number of snapshots to store
	DefaultBufferSize = 1000
	
	// HeaderSize is the size of the file header in bytes
	HeaderSize = 64
	
	// MagicNumber identifies the flight recorder file format
	MagicNumber = uint32(0x464C5243) // "FLRC"
	
	// Version is the file format version
	Version = uint32(1)
)

// Header represents the file header
type Header struct {
	Magic      uint32  // Magic number
	Version    uint32  // Format version
	BufferSize uint32  // Number of slots in buffer
	WriteIndex uint32  // Current write index (circular)
	Count      uint32  // Total number of entries written
	_          [40]byte // Padding to 64 bytes
}

// FlightRecorder implements a circular buffer using memory-mapped files
// to persist metrics data across crashes
type FlightRecorder struct {
	file       *os.File
	header     *Header
	data       []byte
	bufferSize uint32
	slotSize   uint32
	mu         sync.RWMutex
	path       string
}

// NewFlightRecorder creates a new FlightRecorder with the specified buffer size
func NewFlightRecorder(path string, bufferSize uint32) (*FlightRecorder, error) {
	if bufferSize == 0 {
		bufferSize = DefaultBufferSize
	}
	
	// Calculate slot size: estimate max snapshot size (1MB per snapshot)
	// This can be adjusted based on actual needs
	slotSize := uint32(1024 * 1024) // 1MB per slot
	
	// Total file size: header + (bufferSize * slotSize)
	fileSize := int64(HeaderSize) + int64(bufferSize)*int64(slotSize)
	
	// Open or create the file
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open flight recorder file: %w", err)
	}
	
	// Get file info to check if it exists and has data
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	
	// If file is new or smaller than expected, resize it
	if info.Size() < fileSize {
		if err := file.Truncate(fileSize); err != nil {
			file.Close()
			return nil, fmt.Errorf("failed to resize file: %w", err)
		}
	}
	
	// Memory map the file
	data, err := mmapFile(file, int(fileSize))
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to memory map file: %w", err)
	}
	
	// Read or initialize header
	header := (*Header)(unsafe.Pointer(&data[0]))
	
	if header.Magic != MagicNumber {
		// Initialize new header
		header.Magic = MagicNumber
		header.Version = Version
		header.BufferSize = bufferSize
		header.WriteIndex = 0
		header.Count = 0
		// Sync header immediately
		if err := msync(data[:HeaderSize]); err != nil {
			munmap(data)
			file.Close()
			return nil, fmt.Errorf("failed to sync header: %w", err)
		}
	} else if header.Version != Version {
		munmap(data)
		file.Close()
		return nil, fmt.Errorf("unsupported file format version: %d", header.Version)
	}
	
	fr := &FlightRecorder{
		file:       file,
		header:     header,
		data:       data,
		bufferSize: bufferSize,
		slotSize:   slotSize,
		path:       path,
	}
	
	return fr, nil
}

// Record stores a metrics snapshot in the circular buffer
func (fr *FlightRecorder) Record(snapshot poller.MetricsSnapshot) error {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	
	// Serialize snapshot to JSON
	jsonData, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("failed to serialize snapshot: %w", err)
	}
	
	// Check if data fits in slot
	if uint32(len(jsonData)) > fr.slotSize-8 { // -8 for size header
		return fmt.Errorf("snapshot too large: %d bytes (max %d)", len(jsonData), fr.slotSize-8)
	}
	
	// Calculate slot offset
	slotOffset := HeaderSize + int64(fr.header.WriteIndex)*int64(fr.slotSize)
	
	// Write size (4 bytes) + data
	binary.LittleEndian.PutUint32(fr.data[slotOffset:], uint32(len(jsonData)))
	copy(fr.data[slotOffset+4:], jsonData)
	
	// Update header
	fr.header.WriteIndex = (fr.header.WriteIndex + 1) % fr.bufferSize
	fr.header.Count++
	
	// Sync the slot and header
	if err := msync(fr.data[slotOffset : slotOffset+int64(fr.slotSize)]); err != nil {
		return fmt.Errorf("failed to sync slot: %w", err)
	}
	if err := msync(fr.data[:HeaderSize]); err != nil {
		return fmt.Errorf("failed to sync header: %w", err)
	}
	
	return nil
}

// ReadAll reads all available snapshots from the buffer
func (fr *FlightRecorder) ReadAll() ([]poller.MetricsSnapshot, error) {
	fr.mu.RLock()
	defer fr.mu.RUnlock()
	
	var snapshots []poller.MetricsSnapshot
	
	if fr.header.Count == 0 {
		return snapshots, nil
	}
	
	// Determine how many entries to read
	var count uint32
	var startIndex uint32
	
	if fr.header.Count < fr.bufferSize {
		// Buffer not full, read from beginning
		count = fr.header.Count
		startIndex = 0
	} else {
		// Buffer is full, read from writeIndex (oldest) to writeIndex-1 (newest)
		count = fr.bufferSize
		startIndex = fr.header.WriteIndex
	}
	
	// Read entries in chronological order
	for i := uint32(0); i < count; i++ {
		index := (startIndex + i) % fr.bufferSize
		slotOffset := HeaderSize + int64(index)*int64(fr.slotSize)
		
		// Read size
		size := binary.LittleEndian.Uint32(fr.data[slotOffset:])
		if size == 0 || size > fr.slotSize-4 {
			continue // Skip empty or corrupted slots
		}
		
		// Read and deserialize data
		jsonData := fr.data[slotOffset+4 : slotOffset+4+int64(size)]
		var snapshot poller.MetricsSnapshot
		if err := json.Unmarshal(jsonData, &snapshot); err != nil {
			continue // Skip corrupted entries
		}
		
		snapshots = append(snapshots, snapshot)
	}
	
	return snapshots, nil
}

// ReadRecent reads the N most recent snapshots
func (fr *FlightRecorder) ReadRecent(n uint32) ([]poller.MetricsSnapshot, error) {
	all, err := fr.ReadAll()
	if err != nil {
		return nil, err
	}
	
	if uint32(len(all)) <= n {
		return all, nil
	}
	
	// Return last N entries
	return all[len(all)-int(n):], nil
}

// GetStats returns statistics about the flight recorder
func (fr *FlightRecorder) GetStats() (totalCount uint32, bufferSize uint32, writeIndex uint32) {
	fr.mu.RLock()
	defer fr.mu.RUnlock()
	
	return fr.header.Count, fr.bufferSize, fr.header.WriteIndex
}

// Close closes the flight recorder and unmaps memory
func (fr *FlightRecorder) Close() error {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	
	if fr.data != nil {
		// Sync all data to disk before unmapping
		if err := msync(fr.data); err != nil {
			// Log error but continue
			_ = err
		}
		if err := munmap(fr.data); err != nil {
			// Log error but continue
			_ = err
		}
		fr.data = nil
	}
	
	if fr.file != nil {
		// Sync file before closing to ensure data is persisted
		if err := fr.file.Sync(); err != nil {
			// Log error but continue
			_ = err
		}
		if err := fr.file.Close(); err != nil {
			return err
		}
		fr.file = nil
	}
	
	return nil
}

// Recover attempts to recover data from a flight recorder file
func Recover(path string) ([]poller.MetricsSnapshot, error) {
	fr, err := NewFlightRecorder(path, DefaultBufferSize)
	if err != nil {
		return nil, err
	}
	defer fr.Close()
	
	return fr.ReadAll()
}

