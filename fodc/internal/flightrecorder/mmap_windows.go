//go:build windows
// +build windows

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
	"fmt"
	"os"
	"sync"
)

var (
	windowsFileMutex sync.Mutex
	windowsFileMap   = make(map[*os.File][]byte)
)

func mmapFile(file *os.File, size int) ([]byte, error) {
	// On Windows, we use a file-backed approach
	// Read entire file into memory
	windowsFileMutex.Lock()
	defer windowsFileMutex.Unlock()
	
	data := make([]byte, size)
	n, err := file.ReadAt(data, 0)
	if err != nil && err != os.ErrUnexpectedEOF {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}
	if n < size {
		// Zero out the rest
		for i := n; i < size; i++ {
			data[i] = 0
		}
	}
	
	// Store mapping for sync operations
	windowsFileMap[file] = data
	
	return data, nil
}

func munmap(data []byte) error {
	// Find and remove from map
	windowsFileMutex.Lock()
	defer windowsFileMutex.Unlock()
	
	for file, mappedData := range windowsFileMap {
		if len(mappedData) == len(data) && &mappedData[0] == &data[0] {
			delete(windowsFileMap, file)
			break
		}
	}
	return nil
}

func msync(data []byte) error {
	// On Windows, write data back to file
	windowsFileMutex.Lock()
	defer windowsFileMutex.Unlock()
	
	for file, mappedData := range windowsFileMap {
		// Check if this is the same slice (by comparing pointers)
		if len(mappedData) == len(data) && len(data) > 0 && &mappedData[0] == &data[0] {
			_, err := file.WriteAt(data, 0)
			if err != nil {
				return fmt.Errorf("failed to sync file: %w", err)
			}
			return file.Sync()
		}
	}
	return nil
}
