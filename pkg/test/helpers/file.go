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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package helpers

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/dustin/go-humanize"
)

// PrintDiskUsage prints the disk usage of the specified directory and its subdirectories.
func PrintDiskUsage(dir string, maxDepth, curDepth int) {
	// Calculate the total size of all files and directories within the directory
	var totalSize int64
	err := filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			totalSize += info.Size()
		}
		return nil
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}

	// Print the disk usage of the current directory
	fmt.Printf("%s: %s\n", dir, humanize.Bytes(uint64(totalSize)))

	// Recursively print the disk usage of subdirectories
	if curDepth < maxDepth {
		files, err := os.ReadDir(dir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return
		}
		for _, file := range files {
			if file.IsDir() {
				subdir := filepath.Join(dir, file.Name())
				PrintDiskUsage(subdir, maxDepth, curDepth+1)
			}
		}
	}
}
