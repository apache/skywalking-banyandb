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
//go:build linux
// +build linux

package fadvis

import (
	"golang.org/x/sys/unix"
	"os"
)

// Apply applies POSIX_FADV_DONTNEED to the specified file path.
// This tells the kernel that the file data is not expected to be accessed
// in the near future, allowing it to be removed from the page cache.
func Apply(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Ensure data is synced to disk before advising
	if err := f.Sync(); err != nil {
		return err
	}

	return unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
}
