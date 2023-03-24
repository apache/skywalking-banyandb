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
	"github.com/apache/skywalking-banyandb/pkg/logger"
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

// Log implements the WAL interface.
type Log struct {
	buffer          buffer
	l               *logger.Logger
	segmentIndexMap map[SegmentID]*segment
	workSegment     *segment
	writeChannel    chan logRequest
	flushChannel    chan string
	path            string
	options         Options
}

// Options for creating Write-ahead Logging.
type Options struct {
	Compression bool
	FileSize    int
	BufferSize  int
}

type segment struct {
	file       *os.File
	path       string
	logEntries []*logEntry
	segmentID  SegmentID
}

type logRequest struct {
	timestamp time.Time
	data      []byte
	seriesID  []byte
}

type logEntry struct {
	timestamp    []time.Time
	binary       []byte
	entryLength  int64
	seriesID     common.SeriesID
	count        int32
	binaryLength int16
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
	Write(seriesID []byte, timestamp time.Time, data []byte) error
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

	// Initial WAL path.
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, errors.Wrap(err, "Can not get absolute path")
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
			case request := <-log.writeChannel:
				bufferSize += seriesIDLength + timestampLength + len(request.data)
				err := log.buffer.write(request)
				if err != nil {
					log.l.Error().Err(err).Msg("Fail to write to buffer")
				}
				if bufferSize > log.options.BufferSize {
					// Clone buffer,avoiding to block write.
					buf := log.buffer
					flushCh := log.flushChannel
					log.flushChannel = make(chan string)
					// Clear buffer to receive Log request.
					log.clearBuffer()
					log.asyncBatchflush(buf, flushCh)
				}
			case <-timer.C:
				buf := log.buffer
				flushCh := log.flushChannel
				log.flushChannel = make(chan string)
				log.clearBuffer()
				log.asyncBatchflush(buf, flushCh)
			}
		}
	}()
}

func (log *Log) asyncBatchflush(buffer buffer, flushCh chan string) {
	go func() {
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
			var err error
			err = binary.Write(bytesBuffer, binary.LittleEndian, int64(entryLength))
			if err != nil {
				log.l.Error().Err(err).Msg("EntryLength fail to convert to byte")
			}
			err = binary.Write(bytesBuffer, binary.LittleEndian, uint64(seriesID))
			if err != nil {
				log.l.Error().Err(err).Msg("SeriesID fail to convert to byte")
			}
			err = binary.Write(bytesBuffer, binary.LittleEndian, int32(count))
			if err != nil {
				log.l.Error().Err(err).Msg("Count fail to convert to byte")
			}
			err = binary.Write(bytesBuffer, binary.LittleEndian, timeBytes)
			if err != nil {
				log.l.Error().Err(err).Msg("Timestamp fail to convert to byte")
			}
			err = binary.Write(bytesBuffer, binary.LittleEndian, int16(len(buffer.valueMap[seriesID])))
			if err != nil {
				log.l.Error().Err(err).Msg("Binary Length fail to convert to byte")
			}
			err = binary.Write(bytesBuffer, binary.LittleEndian, buffer.valueMap[seriesID])
			if err != nil {
				log.l.Error().Err(err).Msg("Value fail to convert to byte")
			}
		}

		// Compression and flush.
		compressionData := snappy.Encode(nil, bytesBuffer.Bytes())
		err := os.WriteFile(log.workSegment.path, compressionData, os.ModeAppend.Perm())
		if err != nil {
			flushCh <- flushFailFlag
			log.l.Error().Err(err).Msg("Write WAL segment error")
		}
		syncError := log.workSegment.file.Sync()
		if syncError != nil {
			flushCh <- flushFailFlag
			log.l.Error().Err(err).Msg("Can not sync WAL segment")
		}
		flushCh <- flushSuccessFlag
	}()
}

func (log *Log) clearBuffer() {
	for si := range log.buffer.timestampMap {
		delete(log.buffer.timestampMap, si)
	}
	for si := range log.buffer.valueMap {
		delete(log.buffer.valueMap, si)
	}
}

func (log *Log) load() error {
	files, err := os.ReadDir(log.path)
	if err != nil {
		return errors.Wrap(err, "Can not read dir")
	}
	// Load all of WAL segments.
	var workSegmentID SegmentID
	log.segmentIndexMap = make(map[SegmentID]*segment)
	for _, file := range files {
		name := file.Name()
		segmentID, parseErr := strconv.ParseUint(name[3:19], 10, 64)
		if parseErr != nil {
			return errors.Wrap(parseErr, "Parse file name error")
		}
		if segmentID > uint64(workSegmentID) {
			workSegmentID = SegmentID(segmentID)
		}
		segment := &segment{
			segmentID: SegmentID(segmentID),
			path:      filepath.Join(log.path, segmentName(segmentID)),
		}
		if segment.parseLogEntries() != nil {
			return errors.New("Fail to parse log entries")
		}
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
	log.workSegment.file, err = os.OpenFile(log.workSegment.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModeAppend.Perm())
	if err != nil {
		return errors.Wrap(err, "Open WAL segment error")
	}
	return nil
}

func segmentName(index uint64) string {
	return fmt.Sprintf("%v%016x%v", segmentNamePrefix, index, segmentNameSuffix)
}

// Write a logging entity.
// It will return immediately when the data is written in the buffer,
// The returned function will be called when the entity is flushed on the persistent storage.
func (log *Log) Write(seriesID []byte, timestamp time.Time, data []byte) error {
	log.writeChannel <- logRequest{
		seriesID:  seriesID,
		timestamp: timestamp,
		data:      data,
	}

	// Wait for flush response.
	result := <-log.flushChannel
	switch result {
	case flushFailFlag:
		return errors.New("Write WAL error")
	case flushSuccessFlag:
		return nil
	}
	return nil
}

// Read specified segment by SegmentID.
func (log *Log) Read(segmentID SegmentID) (Segment, error) {
	segment := log.segmentIndexMap[segmentID]
	return segment, nil
}

// ReadAllSegments reads all segments sorted by their creation time in ascending order.
func (log *Log) ReadAllSegments() ([]Segment, error) {
	segments := make([]Segment, 0)
	for _, segment := range log.segmentIndexMap {
		segments = append(segments, segment)
	}
	return segments, nil
}

// Rotate closes the open segment and opens a new one, returning the closed segment details.
func (log *Log) Rotate() (Segment, error) {
	if err := log.workSegment.file.Close(); err != nil {
		return nil, errors.Wrap(err, "Close WAL segment error")
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
		return nil, errors.Wrap(err, "Open WAL segment error")
	}

	// Update segment information.
	log.segmentIndexMap[segmentID] = segment
	log.workSegment = segment
	return oldSegment, nil
}

// Delete the specified segment.
func (log *Log) Delete(segmentID SegmentID) error {
	// Segment which will be deleted must be closed.
	err := os.Remove(log.segmentIndexMap[segmentID].path)
	if err != nil {
		return errors.Wrap(err, "Delete WAL segment error")
	}
	delete(log.segmentIndexMap, segmentID)
	return nil
}

// Close all of segments and stop WAL work.
func (log *Log) Close() error {
	buf := log.buffer
	flushCh := log.flushChannel
	log.flushChannel = make(chan string)
	log.asyncBatchflush(buf, flushCh)
	err := log.workSegment.file.Close()
	if err != nil {
		return errors.Wrap(err, "Fail to close WAL segment")
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
	var err error
	filebytes, err := os.ReadFile(segment.path)
	if err != nil {
		return errors.Wrap(err, "Fail to read WAL segment")
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
		err = binary.Write(buffer, binary.LittleEndian, &length)
		if err != nil {
			return errors.Wrap(err, "Entrylength fail to convert from byte")
		}
		pos += int(entryLength)
		if len(filebytes) < pos+int(length) {
			break
		}
		// Parse seriesID.
		data = filebytes[pos : pos+seriesIDLength]
		buffer = bytes.NewBuffer(data)
		err = binary.Write(buffer, binary.LittleEndian, &seriesID)
		if err != nil {
			return errors.Wrap(err, "SeriesId fail to convert from byte")
		}
		pos += seriesIDLength
		// Parse count.
		data = filebytes[pos : pos+countLength]
		buffer = bytes.NewBuffer(data)
		err = binary.Write(buffer, binary.LittleEndian, &count)
		if err != nil {
			return errors.Wrap(err, "Count fail to convert from byte")
		}
		pos += countLength
		// Parse timestamp.
		for i := 0; i <= int(count); i++ {
			timeStr := string(filebytes[pos : pos+timestampLength])
			time, parseErr := time.Parse(parseTimeStr, timeStr)
			if parseErr != nil {
				return errors.Wrap(parseErr, "Parse time error")
			}
			timestamp = append(timestamp, time)
			pos += timestampLength
		}
		// Parse binary Length.
		data = filebytes[pos : pos+binaryLength]
		buffer = bytes.NewBuffer(data)
		err = binary.Write(buffer, binary.LittleEndian, &binaryLen)
		if err != nil {
			return errors.Wrap(err, "Binary Length fail to convert from byte")
		}
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
	var seriesID common.SeriesID
	buf := bytes.NewBuffer(request.seriesID)
	err := binary.Write(buf, binary.LittleEndian, &seriesID)
	if err != nil {
		return errors.Wrap(err, "SeriesId fail to convert from byte")
	}
	buffer.timestampMap[seriesID] = append(buffer.timestampMap[seriesID], request.timestamp)
	buffer.valueMap[seriesID] = append(buffer.valueMap[seriesID], request.data...)
	return nil
}
