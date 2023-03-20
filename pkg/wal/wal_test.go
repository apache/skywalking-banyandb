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

// Package version can be used to implement embedding versioning details from
// git branches and tags into the binary importing this package.
package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWAL(t *testing.T) {
	options := &Options{
		Compression: true,
		FileSize:    67108864,
		BufferSize:  16,
	}

	log, _ := New("test", options)
	go func() {
		for {
			log.Write(1, time.Now(), []byte{0x01})
		}
	}()
	// Rotate test.
	segment, _ := log.Rotate()
	segmentID := segment.GetSegmentID()
	assert.Equal(t, int(segmentID), 1)
	// ReadALL test.
	segments, _ := log.ReadAllSegments()
	for index, segment := range segments {
		id := segment.GetSegmentID()
		assert.Equal(t, int(id), index+1)
	}
	// Delete test.
	log.Delete(segmentID)
	segments, _ = log.ReadAllSegments()
	for _, segment := range segments {
		segmentID := segment.GetSegmentID()
		assert.Equal(t, int(segmentID), 2)
	}
	// Delete test dir.
	path, _ := filepath.Abs("test")
	os.RemoveAll(path)
}
