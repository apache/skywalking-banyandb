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

// Package wal (Write-ahead logging) is an independent component to ensure data reliability.
package wal

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	path         = "benchmark"
	baseTime     = time.Now().UnixMilli()
	data         = newBinaryData()
	dataLen      = len(data)
	seriesID1    = newSeriesIDList(1)
	seriesID20   = newSeriesIDList(20)
	seriesID100  = newSeriesIDList(100)
	seriesID500  = newSeriesIDList(500)
	seriesID1000 = newSeriesIDList(1000)
	callback     = func(seriesID []byte, t time.Time, bytes []byte, err error) {}
)

func Benchmark_SeriesID_1(b *testing.B) {
	wal := newWAL(&Options{SyncFlush: true})
	defer closeWAL(wal)

	seriesID := seriesID1
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_20(b *testing.B) {
	wal := newWAL(&Options{SyncFlush: true})
	defer closeWAL(wal)

	seriesID := seriesID20
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_100(b *testing.B) {
	wal := newWAL(&Options{SyncFlush: true})
	defer closeWAL(wal)

	seriesID := seriesID100
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_500(b *testing.B) {
	wal := newWAL(&Options{SyncFlush: true})
	defer closeWAL(wal)

	seriesID := seriesID500
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_1000(b *testing.B) {
	wal := newWAL(&Options{SyncFlush: true})
	defer closeWAL(wal)

	seriesID := seriesID1000
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_1000_Buffer_64K(b *testing.B) {
	wal := newWAL(&Options{SyncFlush: true, BufferSize: 1024 * 64})
	defer closeWAL(wal)

	seriesID := seriesID1000
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_1000_Buffer_128K(b *testing.B) {
	wal := newWAL(&Options{SyncFlush: true, BufferSize: 1024 * 128})
	defer closeWAL(wal)

	seriesID := seriesID1000
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_1000_Buffer_512K(b *testing.B) {
	wal := newWAL(&Options{SyncFlush: true, BufferSize: 1024 * 512})
	defer closeWAL(wal)

	seriesID := seriesID1000
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_1000_Buffer_1MB(b *testing.B) {
	wal := newWAL(&Options{SyncFlush: true, BufferSize: 1024 * 1024})
	defer closeWAL(wal)

	seriesID := seriesID1000
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_1000_Buffer_2MB(b *testing.B) {
	wal := newWAL(&Options{SyncFlush: true, BufferSize: 1024 * 1024 * 2})
	defer closeWAL(wal)

	seriesID := seriesID1000
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_1000_Buffer_64K_NoSyncFlush(b *testing.B) {
	wal := newWAL(&Options{BufferSize: 1024 * 64, SyncFlush: false})
	defer closeWAL(wal)

	seriesID := seriesID1000
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_1000_Buffer_128K_NoSyncFlush(b *testing.B) {
	wal := newWAL(&Options{BufferSize: 1024 * 128, SyncFlush: false})
	defer closeWAL(wal)

	seriesID := seriesID1000
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_1000_Buffer_512K_NoSyncFlush(b *testing.B) {
	wal := newWAL(&Options{BufferSize: 1024 * 512, SyncFlush: false})
	defer closeWAL(wal)

	seriesID := seriesID1000
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_1000_Buffer_1MB_NoSyncFlush(b *testing.B) {
	wal := newWAL(&Options{BufferSize: 1024 * 1024, SyncFlush: false})
	defer closeWAL(wal)

	seriesID := seriesID1000
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_1000_Buffer_2MB_NoSyncFlush(b *testing.B) {
	wal := newWAL(&Options{BufferSize: 1024 * 1024 * 2, SyncFlush: false})
	defer closeWAL(wal)

	seriesID := seriesID1000
	seriesIDLen := len(seriesID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), data[i%dataLen].binary, callback)
	}
	b.StopTimer()
}

func Benchmark_SeriesID_1000_Buffer_64K_NoSyncFlush_And_Rotate_16MB(b *testing.B) {
	wal := newWAL(&Options{BufferSize: 1024 * 64, SyncFlush: false})
	defer closeWAL(wal)

	seriesID := seriesID1000
	seriesIDLen := len(seriesID)

	rotateSize := 1024 * 1024 * 16 // 16MB
	rotateChan := make(chan struct{})
	rotateMessage := struct{}{}
	seriesIDVolume := 16
	timeVolume := 8
	var logVolume int
	var binaryData []byte

	b.ResetTimer()
	go func() {
		for range rotateChan {
			segment, err := wal.Rotate()
			if err != nil {
				panic(err)
			}
			wal.Delete(segment.GetSegmentID())
		}
	}()
	for i := 0; i < b.N; i++ {
		binaryData = data[i%dataLen].binary
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), binaryData, callback)

		logVolume += seriesIDVolume + timeVolume + len(binaryData)
		if logVolume >= rotateSize {
			rotateChan <- rotateMessage
			logVolume = 0
		}
	}
	b.StopTimer()
}

func Benchmark_SeriesID_1000_Buffer_64K_NoSyncFlush_And_Rotate_32MB(b *testing.B) {
	wal := newWAL(&Options{BufferSize: 1024 * 64, SyncFlush: false})
	defer closeWAL(wal)

	seriesID := seriesID1000
	seriesIDLen := len(seriesID)

	rotateSize := 1024 * 1024 * 32 // 32MB
	rotateChan := make(chan struct{})
	rotateMessage := struct{}{}
	seriesIDVolume := 16
	timeVolume := 8
	var logVolume int
	var binaryData []byte

	b.ResetTimer()
	go func() {
		for range rotateChan {
			segment, err := wal.Rotate()
			if err != nil {
				panic(err)
			}
			wal.Delete(segment.GetSegmentID())
		}
	}()
	for i := 0; i < b.N; i++ {
		binaryData = data[i%dataLen].binary
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), binaryData, callback)

		logVolume += seriesIDVolume + timeVolume + len(binaryData)
		if logVolume >= rotateSize {
			rotateChan <- rotateMessage
			logVolume = 0
		}
	}
	b.StopTimer()
}

func Benchmark_SeriesID_1000_Buffer_64K_NoSyncFlush_And_Rotate_64MB(b *testing.B) {
	wal := newWAL(&Options{BufferSize: 1024 * 64, SyncFlush: false})
	defer closeWAL(wal)

	seriesID := seriesID1000
	seriesIDLen := len(seriesID)

	rotateSize := 1024 * 1024 * 64 // 64MB
	rotateChan := make(chan struct{})
	rotateMessage := struct{}{}
	seriesIDVolume := 16
	timeVolume := 8
	var logVolume int
	var binaryData []byte

	b.ResetTimer()
	go func() {
		for range rotateChan {
			segment, err := wal.Rotate()
			if err != nil {
				panic(err)
			}
			wal.Delete(segment.GetSegmentID())
		}
	}()
	for i := 0; i < b.N; i++ {
		binaryData = data[i%dataLen].binary
		wal.Write(seriesID[i%seriesIDLen].key, time.UnixMilli(baseTime+1), binaryData, callback)

		logVolume += seriesIDVolume + timeVolume + len(binaryData)
		if logVolume >= rotateSize {
			rotateChan <- rotateMessage
			logVolume = 0
		}
	}
	b.StopTimer()
}

func newWAL(options *Options) WAL {
	os.RemoveAll(path)

	logger.Init(logger.Logging{Level: "error"})
	logPath, _ := filepath.Abs(path)
	if options == nil {
		options = &Options{
			BufferSize:          1024 * 64, // 64KB
			BufferBatchInterval: 3 * time.Second,
		}
	}
	wal, err := New(logPath, options)
	if err != nil {
		panic(err)
	}
	return wal
}

func closeWAL(wal WAL) {
	err := wal.Close()
	if err != nil {
		panic(err)
	}

	err = os.RemoveAll(path)
	if err != nil {
		panic(err)
	}
}

type SeriesID struct {
	key []byte
}

func newSeriesIDList(series int) []SeriesID {
	var seriesIDSet []SeriesID
	for i := 0; i < series; i++ {
		seriesID := SeriesID{key: []byte(fmt.Sprintf("series-%d", i))}
		seriesIDSet = append(seriesIDSet, seriesID)
	}
	return seriesIDSet
}

type Data struct {
	binary []byte
}

func newBinaryData() []Data {
	var data []Data
	for i := 0; i < 2000; i++ {
		data = append(data, Data{binary: []byte(randStr())})
	}
	return data
}

func randStr() string {
	bytes := []byte("0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\"',:{}[]")
	result := []byte{}

	var err error
	var strLengthLower int64 = 200
	var strLengthUpper int64 = 1024
	var strLength *big.Int
	var strRandIndex *big.Int
	strLength, err = rand.Int(rand.Reader, big.NewInt(strLengthUpper))
	if err != nil {
		panic(err)
	}
	if strLength.Int64() < strLengthLower {
		strLength = big.NewInt(strLengthLower)
	}
	for i := 0; i < int(strLength.Int64()); i++ {
		strRandIndex, err = rand.Int(rand.Reader, big.NewInt(int64(len(bytes))))
		if err != nil {
			panic(err)
		}

		result = append(result, bytes[strRandIndex.Int64()])
	}
	return string(result)
}
