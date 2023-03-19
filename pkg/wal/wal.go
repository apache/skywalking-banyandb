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
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/golang/snappy"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
)

const (
	segmentNamePrefix   = "seg"
	segmentNameSuffix   = ".wal"
	entryLength         = 8
	seriesIDLength      = 8
	countLength         = 4
	timestampLength     = 8
	binaryLength        = 2
	flushSuccessFlag    = "success"
	flushFailFlag       = "fail"
	bufferBatchInterval = 500
	parseTimeStr        = "2006-01-02 15:04:05"
)

// SegmentID identities a segment in a WAL.
type SegmentID uint64

type Log struct {
	path            string
	options         Options
	segmentIndexMap map[SegmentID]*segment
	workSegment     *segment
	writeChannel    chan logRequest
	flushChannel    chan string
	buffer          buffer
}

// Options for creating Write-ahead Logging.
type Options struct {
	Compression bool
	FileSize    int
	BufferSize  int
}

type segment struct {
	segmentID  SegmentID
	path       string
	file       *os.File
	logEntries []*logEntry
}

type logRequest struct {
	seriesID  common.SeriesID
	timestamp time.Time
	data      []byte
}

type logEntry struct {
	entryLength  int64
	seriesID     common.SeriesID
	count        int32
	timestamp    []time.Time
	binaryLength int16
	binary       []byte
}

type buffer struct {
	timestampMap map[common.SeriesID][]time.Time
	valueMap     map[common.SeriesID][]byte
}

// DefaultOptions for Open().
var DefaultOptions = &Options{
	Compression: true,
	FileSize:    67108864, // 64MB
	BufferSize:  16384,    // 16KB
}

// Segment allows reading underlying segments that hold WAl entities.
type Segment interface {
	GetSegmentID() SegmentID
	GetLogEntries() []*logEntry
}

// WAL denotes a Write-ahead logging.
// Modules who want their data reliable could write data to an instance of WAL.
// A WAL combines several segments, ingesting data on a single opened one.
// Rotating the WAL will create a new segment, marking it as opened and persisting previous segments on the disk.
type WAL interface {
	// Write a logging entity.
	// It will return immediately when the data is written in the buffer,
	// The returned function will be called when the entity is flushed on the persistent storage.
	Write(seriesID common.SeriesID, timestamp time.Time, data []byte) error
	// Read specified segment by SegmentID.
	Read(segmentID SegmentID) (Segment, error)
	// ReadAllSegments reads all segments sorted by their creation time in ascending order.
	ReadAllSegments() ([]Segment, error)
	// Rotate closes the open segment and opens a new one, returning the closed segment details.
	Rotate() (Segment, error)
	// Delete the specified segment.
	Delete(segmentID SegmentID) error
	// Close all of segments and stop WAL work.
	Close() error
}

// New creates a WAL instance in the specified path.
func New(path string, options *Options) (WAL, error) {
	//  Check configuration options.
	if options == nil {
		options = DefaultOptions
	}
	if options.FileSize <= 0 {
		options.FileSize = DefaultOptions.FileSize
	}
	if options.BufferSize <= 0 {
		options.BufferSize = DefaultOptions.BufferSize
	}

	// Inital WAL path.
	path, error := filepath.Abs(path)
	if error != nil {
		return nil, errors.Wrap(error, "Can not get absolute path.")
	}
	log := &Log{path: path, options: *options}
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return nil, err
	}

	if err := log.load(); err != nil {
		return nil, err
	}
	log.runFlushTask()
	return log, nil
}

func (log *Log) runFlushTask() {
	go func() {
		bufferSize := 0
		for {
			timer := time.NewTimer(bufferBatchInterval * time.Millisecond)
			select {
			case logRequest := <-log.writeChannel:
				bufferSize += seriesIDLength + timestampLength + len(logRequest.data)
				error := log.buffer.write(logRequest)
				if error != nil {
					errors.Wrap(error, "Can not operate WAL buffer.")
				}
				if bufferSize > log.options.BufferSize {
					// Clone buffer,avoiding to block write.
					buffer := log.buffer
					// Clear buffer to receive Log request.
					log.clearBuffer()
					if log.asyncBatchflush(buffer) != nil {
						errors.New("Fail to flush WAL.")
					}
				}
			case <-timer.C:
				buffer := log.buffer
				log.clearBuffer()
				if log.asyncBatchflush(buffer) != nil {
					errors.New("Fail to flush WAL.")
				}
			}
		}
	}()
}

func (log *Log) asyncBatchflush(buffer buffer) error {
	go func() error {
		// Convert to byte.
		bytesBuffer := bytes.NewBuffer([]byte{})
		for seriesID, timestamp := range buffer.timestampMap {
			count := 0
			var timeBytes []byte
			for _, timestamp := range timestamp {
				time := []byte(timestamp.String())
				count += len(time)
				timeBytes = append(timeBytes, time...)
			}
			count /= timestampLength
			entryLength := seriesIDLength + count + len(timeBytes) + binaryLength + len(buffer.valueMap[seriesID])
			binary.Write(bytesBuffer, binary.LittleEndian, int64(entryLength))
			binary.Write(bytesBuffer, binary.LittleEndian, uint64(seriesID))
			binary.Write(bytesBuffer, binary.LittleEndian, int32(count))
			binary.Write(bytesBuffer, binary.LittleEndian, timeBytes)
			binary.Write(bytesBuffer, binary.LittleEndian, int16(len(buffer.valueMap[seriesID])))
			binary.Write(bytesBuffer, binary.LittleEndian, buffer.valueMap[seriesID])
		}

		// Compression and flush.
		compressionData := snappy.Encode(nil, []byte(bytesBuffer.Bytes()))
		error := os.WriteFile(log.workSegment.path, compressionData, os.ModeAppend.Perm())
		if error != nil {
			log.flushChannel <- flushFailFlag
			errors.Wrap(error, "Write WAL segment error.")
		}
		syncError := log.workSegment.file.Sync()
		if syncError != nil {
			log.flushChannel <- flushFailFlag
			errors.Wrap(syncError, "Can not sync WAL segment.")
		}
		log.flushChannel <- flushSuccessFlag
		return nil
	}()
	return nil
}

func (log *Log) clearBuffer() error {
	for si := range log.buffer.timestampMap {
		delete(log.buffer.timestampMap, si)
	}
	for si := range log.buffer.valueMap {
		delete(log.buffer.valueMap, si)
	}
	return nil
}

func (log *Log) load() error {
	files, error := os.ReadDir(log.path)
	if error != nil {
		return errors.Wrap(error, "Can not read dir.")
	}
	// Load all of WAL segments.
	var workSegmentID SegmentID
	log.segmentIndexMap = make(map[SegmentID]*segment)
	for _, file := range files {
		name := file.Name()
		segmentID, err := strconv.ParseUint(name[3:19], 10, 64)
		if err != nil {
			errors.Wrap(error, "Parse file name error.")
		}
		if segmentID > uint64(workSegmentID) {
			workSegmentID = SegmentID(segmentID)
		}
		segment := &segment{
			segmentID: SegmentID(segmentID),
			path:      filepath.Join(log.path, segmentName(segmentID)),
		}
		segment.parseLogEntries()
		log.segmentIndexMap[SegmentID(segmentID)] = segment
	}

	// If load first time.
	if len(log.segmentIndexMap) == 0 {
		segment := &segment{
			segmentID: 1,
			path:      filepath.Join(log.path, segmentName(1)),
		}
		log.segmentIndexMap[1] = segment
		log.workSegment = segment
	} else {
		log.workSegment = log.segmentIndexMap[workSegmentID]
	}
	log.workSegment.file, error = os.OpenFile(log.workSegment.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModeAppend.Perm())
	if error != nil {
		errors.Wrap(error, "Open WAL segment error.")
	}
	return nil
}

func segmentName(index uint64) string {
	return fmt.Sprintf("%v%016x%v", segmentNamePrefix, index, segmentNameSuffix)
}

func (log *Log) Write(seriesID common.SeriesID, timestamp time.Time, data []byte) error {
	log.writeChannel <- logRequest{
		seriesID:  seriesID,
		timestamp: timestamp,
		data:      data,
	}

	// Wait for flush response.
	result := <-log.flushChannel
	switch result {
	case flushFailFlag:
		return errors.New("Write WAL error.")
	case flushSuccessFlag:
		return nil
	}
	return nil
}

func (log *Log) Read(segmentID SegmentID) (Segment, error) {
	segment := log.segmentIndexMap[segmentID]
	return segment, nil
}

func (log *Log) ReadAllSegments() ([]Segment, error) {
	segments := make([]Segment, 0)
	for _, segment := range log.segmentIndexMap {
		segments = append(segments, segment)
	}
	return segments, nil
}

func (log *Log) Rotate() (Segment, error) {
	if error := log.workSegment.file.Close(); error != nil {
		errors.Wrap(error, "Close WAL segment error.")
	}
	oldSegment := log.workSegment
	// Create new segment.
	segmentID := log.workSegment.segmentID + 1
	segment := &segment{
		segmentID: segmentID,
		path:      filepath.Join(log.path, segmentName(uint64(segmentID))),
	}
	var err error
	segment.file, err = os.OpenFile(segment.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModeAppend.Perm())
	if err != nil {
		errors.Wrap(err, "Open WAL segment error.")
	}

	// Update segment information.
	log.segmentIndexMap[segmentID] = segment
	log.workSegment = segment
	return oldSegment, nil
}

func (log *Log) Delete(segmentID SegmentID) error {
	// Segment which will be deleted must be closed.
	error := os.Remove(log.segmentIndexMap[segmentID].path)
	if error != nil {
		errors.Wrap(error, "Delete WAL segment error.")
	}
	delete(log.segmentIndexMap, segmentID)
	return nil
}

func (log *Log) Close() error {
	buffer := log.buffer
	log.asyncBatchflush(buffer)
	error := log.workSegment.file.Close()
	if error != nil {
		errors.Wrap(error, "Fail to close WAL segment.")
	}
	return nil
}

func (segment *segment) GetSegmentID() SegmentID {
	return segment.segmentID
}

func (segment *segment) GetLogEntries() []*logEntry {
	return segment.logEntries
}

func (segment *segment) parseLogEntries() error {
	filebytes, error := os.ReadFile(segment.path)
	if error != nil {
		errors.Wrap(error, "Fail to read WAL segment")
	}
	var logEntries []*logEntry
	pos := 0
	var data []byte
	var buffer *bytes.Buffer
	var length int64
	var seriesID common.SeriesID
	var count int32
	var timestamp []time.Time
	var binaryLen int16
	for {
		if len(filebytes) <= pos+entryLength {
			break
		}
		// Parse entryLength.
		data = filebytes[pos : pos+entryLength]
		buffer = bytes.NewBuffer(data)
		binary.Write(buffer, binary.LittleEndian, &length)
		pos += int(entryLength)
		if len(filebytes) < pos+int(length) {
			break
		}
		// Parse seriesID.
		data = filebytes[pos : pos+seriesIDLength]
		buffer = bytes.NewBuffer(data)
		binary.Write(buffer, binary.LittleEndian, &seriesID)
		pos += seriesIDLength
		// Parse count.
		data = filebytes[pos : pos+countLength]
		buffer = bytes.NewBuffer(data)
		binary.Write(buffer, binary.LittleEndian, &count)
		pos += countLength
		// Parse timestamp.
		for i := 0; i <= int(count); i++ {
			timeStr := string(filebytes[pos : pos+timestampLength])
			time, error := time.Parse(parseTimeStr, timeStr)
			if error != nil {
				errors.Wrap(error, "Parse time error.")
			}
			timestamp = append(timestamp, time)
			pos += timestampLength
		}
		// Parse binary Length.
		data = filebytes[pos : pos+binaryLength]
		buffer = bytes.NewBuffer(data)
		binary.Write(buffer, binary.LittleEndian, &binaryLen)
		pos += binaryLength
		// Parse value.
		data = filebytes[pos : pos+int(binaryLen)]
		logEntry := &logEntry{
			entryLength:  length,
			seriesID:     seriesID,
			count:        count,
			timestamp:    timestamp,
			binaryLength: binaryLen,
			binary:       data,
		}
		logEntries = append(logEntries, logEntry)
		pos += int(binaryLen)
		if pos == len(filebytes) {
			break
		}
	}
	segment.logEntries = logEntries
	return nil
}

func (buffer *buffer) write(request logRequest) error {
	buffer.timestampMap[request.seriesID] = append(buffer.timestampMap[request.seriesID], request.timestamp)
	buffer.valueMap[request.seriesID] = append(buffer.valueMap[request.seriesID], request.data...)
	return nil
}
