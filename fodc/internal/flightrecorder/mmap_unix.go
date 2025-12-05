//go:build !windows
// +build !windows

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
	"syscall"
	"unsafe"

	"golang.org/x/sys/unix"
)

var (
	pageSizeOnce sync.Once
	pageSize     int
)

// getPageSize returns the system page size, detecting it once.
func getPageSize() int {
	pageSizeOnce.Do(func() {
		// Try to get page size from syscall
		pageSize = os.Getpagesize()
		if pageSize == 0 {
			// Fallback to macOS default (16KB) if detection fails
			pageSize = 16384
		}
	})
	return pageSize
}

func mmapFile(file *os.File, size int) ([]byte, error) {
	data, err := unix.Mmap(int(file.Fd()), 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap failed: %w", err)
	}
	return data, nil
}

func munmap(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	return unix.Munmap(data)
}

// msync syncs a byte slice to disk, handling page alignment requirements
// The address and length must be aligned to page boundaries.
// This function calculates the page-aligned region that contains the data.
func msync(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	ps := getPageSize()
	psUint := uintptr(ps)

	// Get the base address and length of the slice
	baseAddr := uintptr(unsafe.Pointer(&data[0]))
	length := uintptr(len(data))

	// Calculate page-aligned start address (round down to page boundary)
	pageAlignedStart := baseAddr &^ (psUint - 1)

	// Calculate page-aligned end address (round up to page boundary)
	pageAlignedEnd := (baseAddr + length + psUint - 1) &^ (psUint - 1)

	// Calculate the page-aligned length
	pageAlignedLength := pageAlignedEnd - pageAlignedStart

	// Perform the msync syscall with page-aligned address and length
	_, _, errno := syscall.Syscall(
		syscall.SYS_MSYNC,
		pageAlignedStart,
		pageAlignedLength,
		syscall.MS_SYNC,
	)
	if errno != 0 {
		return fmt.Errorf("msync failed: %w", errno)
	}
	return nil
}
