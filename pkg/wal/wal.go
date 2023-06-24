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
	"container/list"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/golang/snappy"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	moduleName             = "wal"
	segmentNamePrefix      = "seg"
	segmentNameSuffix      = ".wal"
	batchWriteLength       = 8
	entryLength            = 8
	seriesIDLength         = 2
	seriesCountLength      = 4
	timestampLength        = 8
	timestampsBinaryLength = 2
	valuesBinaryLength     = 2
	parseTimeStr           = "2006-01-02 15:04:05"
)

// DefaultOptions for Open().
var DefaultOptions = &Options{
	FileSize:            67108864, // 64MB
	BufferSize:          16384,    // 16KB
	BufferBatchInterval: 3 * time.Second,
}

// Options for creating Write-ahead Logging.
type Options struct {
	FileSize            int
	BufferSize          int
	BufferBatchInterval time.Duration
	FlushQueueSize      int
}

// WAL denotes a Write-ahead logging.
// Modules who want their data reliable could write data to an instance of WAL.
// A WAL combines several segments, ingesting data on a single opened one.
// Rotating the WAL will create a new segment, marking it as opened and persisting previous segments on the disk.
type WAL interface {
	// Write a logging entity.
	// It will return immediately when the data is written in the buffer,
	// The callback function will be called when the entity is flushed on the persistent storage.
	Write(seriesID common.SeriesIDV2, timestamp time.Time, data []byte, callback func(common.SeriesIDV2, time.Time, []byte, error))
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

// SegmentID identities a segment in a WAL.
type SegmentID uint64

// Segment allows reading underlying segments that hold WAl entities.
type Segment interface {
	GetSegmentID() SegmentID
	GetLogEntries() []LogEntry
}

// LogEntry used for attain detail value of WAL entry.
type LogEntry interface {
	GetSeriesID() common.SeriesIDV2
	GetTimestamps() []time.Time
	GetValues() *list.List
}

// Log implements the WAL interface.
type Log struct {
	buffer           buffer
	logger           *logger.Logger
	bytesBuffer      *bytes.Buffer
	timestampsBuffer *bytes.Buffer
	segmentIndexMap  map[SegmentID]*segment
	workSegment      *segment
	writeChannel     chan logRequest
	flushChannel     chan buffer
	path             string
	options          Options
}

type segment struct {
	file       *os.File
	path       string
	logEntries []LogEntry
	segmentID  SegmentID
}

type logRequest struct {
	seriesID  common.SeriesIDV2
	timestamp time.Time
	callback  func(common.SeriesIDV2, time.Time, []byte, error)
	data      []byte
}

type logEntry struct {
	timestamps  []time.Time
	values      *list.List
	seriesID    common.SeriesIDV2
	entryLength int64
	count       int32
}

type buffer struct {
	timestampMap map[common.SeriesIDV2][]time.Time
	valueMap     map[common.SeriesIDV2][]byte
	callbackMap  map[common.SeriesIDV2][]func(common.SeriesIDV2, time.Time, []byte, error)
	count        int
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
	if options.BufferBatchInterval <= 0 {
		options.BufferBatchInterval = DefaultOptions.BufferBatchInterval
	}

	// Initial WAL path.
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, errors.Wrap(err, "Can not get absolute path: "+path)
	}
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return nil, err
	}

	log := &Log{path: path, options: *options, logger: logger.GetLogger(moduleName)}

	if err := log.load(); err != nil {
		return nil, err
	}
	log.startAsyncFlushTask()

	log.logger.Info().Msgf("WAL initialized at %s", path)
	return log, nil
}

// Write a logging entity.
// It will return immediately when the data is written in the buffer,
// The callback function will be called when the entity is flushed on the persistent storage.
func (log *Log) Write(seriesID common.SeriesIDV2, timestamp time.Time, data []byte, callback func(common.SeriesIDV2, time.Time, []byte, error)) {
	log.writeChannel <- logRequest{
		seriesID:  seriesID,
		timestamp: timestamp,
		data:      data,
		callback:  callback,
	}
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
	segment.file, err = os.OpenFile(segment.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModePerm)
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
	log.logger.Info().Msg("Closing WAL...")
	close(log.writeChannel)
	close(log.flushChannel)
	err := log.workSegment.file.Close()
	if err != nil {
		return errors.Wrap(err, "Close WAL error")
	}
	log.logger.Info().Msg("Closed WAL")
	return nil
}

func (log *Log) startAsyncFlushTask() {
	go func() {
		log.logger.Info().Msg("Start batch task...")
		bufferSize := 0
		for {
			timer := time.NewTimer(log.options.BufferBatchInterval)
			select {
			case request, ok := <-log.writeChannel:
				if !ok {
					log.logger.Info().Msg("Exit selector when write-channel closed!")
					return
				}
				log.buffer.write(request)
				if log.logger.Debug().Enabled() {
					log.logger.Debug().Msg("Write request to buffer. elements: " + strconv.Itoa(log.buffer.count))
				}

				bufferSize += len(request.seriesID.Marshal()) + timestampLength + len(request.data)
				if bufferSize > log.options.BufferSize {
					// Clone buffer,avoiding to block write.
					buf := log.buffer
					// Clear buffer to receive Log request.
					log.newBuffer()
					bufferSize = 0
					// Send buffer to flushBuffer channel.
					log.flushChannel <- buf
					if log.logger.Debug().Enabled() {
						log.logger.Debug().Msg("Send buffer to flush-channel. elements: " + strconv.Itoa(buf.count))
					}
				}
			case <-timer.C:
				if bufferSize == 0 {
					continue
				}
				// Clone buffer,avoiding to block write.
				buf := log.buffer
				// Clear buffer to receive Log request.
				log.newBuffer()
				bufferSize = 0
				// Send buffer to flushBuffer channel.
				log.flushChannel <- buf
				if log.logger.Debug().Enabled() {
					log.logger.Debug().Msg("Send buffer to flush-channel. elements: " + strconv.Itoa(buf.count))
				}
			}
		}
	}()

	go func() {
		log.logger.Info().Msg("Start flush task...")

		for batch := range log.flushChannel {
			log.flushBuffer(batch)
			if log.logger.Debug().Enabled() {
				log.logger.Debug().Msg("Flushed buffer to WAL file. elements: " + strconv.Itoa(batch.count))
			}
		}

		log.logger.Info().Msg("Exit flush task when flush-channel closed!")
	}()

	log.logger.Info().Msg("Started WAL async flush task.")
}

func (log *Log) flushBuffer(buffer buffer) {
	log.bytesBuffer.Reset()
	// placeholder, preset batch length value is 0
	batchLength := 0
	log.writeToBytesBuffer(log.bytesBuffer, binary.LittleEndian, int64(batchLength))

	for seriesID, timestamps := range buffer.timestampMap {
		// Generate seriesID binary
		seriesIDBytes := seriesID.Marshal()
		seriesIDBytesLen := len(seriesIDBytes)

		// Generate timestamps compression binary
		log.timestampsBuffer.Reset()
		timestampWriter := encoding.NewWriter()
		timestampEncoder := encoding.NewXOREncoder(timestampWriter)
		timestampWriter.Reset(log.timestampsBuffer)
		for _, timestamp := range timestamps {
			timestampEncoder.Write(timeTouUnixNano(timestamp))
		}
		timestampWriter.Flush()
		timestampsBytes := log.timestampsBuffer.Bytes()
		timestampsBytesLen := len(timestampsBytes)

		// Generate values compression binary
		valuesBytes := snappy.Encode(nil, buffer.valueMap[seriesID])

		// Write entry data
		entryLength := seriesIDLength + seriesIDBytesLen + seriesCountLength + timestampsBinaryLength + timestampsBytesLen + len(valuesBytes)
		log.writeToBytesBuffer(log.bytesBuffer, binary.LittleEndian, int64(entryLength))
		log.writeToBytesBuffer(log.bytesBuffer, binary.LittleEndian, int16(seriesIDBytesLen))
		log.writeToBytesBuffer(log.bytesBuffer, binary.LittleEndian, seriesIDBytes)
		log.writeToBytesBuffer(log.bytesBuffer, binary.LittleEndian, int32(len(timestamps)))
		log.writeToBytesBuffer(log.bytesBuffer, binary.LittleEndian, int16(timestampsBytesLen))
		log.writeToBytesBuffer(log.bytesBuffer, binary.LittleEndian, timestampsBytes)
		log.writeToBytesBuffer(log.bytesBuffer, binary.LittleEndian, valuesBytes)
	}
	// Rewrite batch length
	batchBytes := log.bytesBuffer.Bytes()
	batchLength = len(batchBytes) - batchWriteLength
	rewriteInt64InBuf(batchBytes, int64(batchLength), 0, binary.LittleEndian)
	log.bytesBuffer.Reset()

	// Flush
	_, err := log.workSegment.file.Write(batchBytes)
	if err != nil {
		log.logger.Error().Err(err).Msg("Write WAL segment file error, file: " + log.workSegment.path)
		buffer.notifyRequests(err)
		return
	}
	err = log.workSegment.file.Sync()
	if err != nil {
		log.logger.Error().Err(err).Msg("Sync WAL segment file to disk error, file: " + log.workSegment.path)
		buffer.notifyRequests(err)
		return
	}
	buffer.notifyRequests(nil)
	if log.logger.Debug().Enabled() {
		log.logger.Debug().Msg("Flushed buffer to WAL. file: " + log.workSegment.path +
			", elements: " + strconv.Itoa(buffer.count) +
			", bytes: " + strconv.Itoa(len(batchBytes)))
	}
}

func (log *Log) newBuffer() {
	log.buffer.timestampMap = make(map[common.SeriesIDV2][]time.Time)
	log.buffer.valueMap = make(map[common.SeriesIDV2][]byte)
	log.buffer.callbackMap = make(map[common.SeriesIDV2][]func(common.SeriesIDV2, time.Time, []byte, error))
	log.buffer.count = 0
}

func (log *Log) load() error {
	files, err := os.ReadDir(log.path)
	if err != nil {
		return errors.Wrap(err, "Can not read dir: "+log.path)
	}
	// Load all of WAL segments.
	var workSegmentID SegmentID
	log.segmentIndexMap = make(map[SegmentID]*segment)
	for _, file := range files {
		name := file.Name()
		segmentID, parsePathErr := strconv.ParseUint(name[3:19], 10, 64)
		if parsePathErr != nil {
			return errors.Wrap(parsePathErr, "Parse file name error, name: "+name)
		}
		if segmentID > uint64(workSegmentID) {
			workSegmentID = SegmentID(segmentID)
		}
		segment := &segment{
			segmentID: SegmentID(segmentID),
			path:      filepath.Join(log.path, segmentName(segmentID)),
		}
		if err = segment.parseLogEntries(); err != nil {
			return errors.Wrap(err, "Fail to parse log entries")
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
	log.workSegment.file, err = os.OpenFile(log.workSegment.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "Open WAL segment error, file: "+log.workSegment.path)
	}

	log.writeChannel = make(chan logRequest)
	log.flushChannel = make(chan buffer, log.options.FlushQueueSize)
	log.bytesBuffer = bytes.NewBuffer([]byte{})
	log.timestampsBuffer = bytes.NewBuffer([]byte{})
	log.buffer = buffer{
		timestampMap: make(map[common.SeriesIDV2][]time.Time),
		valueMap:     make(map[common.SeriesIDV2][]byte),
		callbackMap:  make(map[common.SeriesIDV2][]func(common.SeriesIDV2, time.Time, []byte, error)),
		count:        0,
	}
	return nil
}

func (log *Log) writeToBytesBuffer(buf *bytes.Buffer, order binary.ByteOrder, v any) {
	err := binary.Write(buf, order, v)
	if err != nil {
		log.logger.Error().Err(err).Msg("Write to bytes buffer error")
	}
}

func (segment *segment) GetSegmentID() SegmentID {
	return segment.segmentID
}

func (segment *segment) GetLogEntries() []LogEntry {
	return segment.logEntries
}

func (segment *segment) parseLogEntries() error {
	var err error
	filebytes, err := os.ReadFile(segment.path)
	if err != nil {
		return errors.Wrap(err, "Fail to read WAL segment, path: "+segment.path)
	}
	var logEntries []LogEntry
	oldPos := 0
	pos := 0
	parseBatchWriteLenFlag := true
	var data []byte
	var batchWriteLen int64
	var entryLen int64
	var seriesIDLen int16
	var seriesID common.SeriesIDV2
	var seriesCount int32
	var timestampsBinaryLen int16
	var seriesBlockEndPos int
	var buffer *bytes.Buffer

	for {
		if parseBatchWriteLenFlag {
			if len(filebytes) <= batchWriteLength {
				break
			}
			data = filebytes[pos : pos+batchWriteLength]
			buffer = bytes.NewBuffer(data)
			err = binary.Read(buffer, binary.LittleEndian, &batchWriteLen)
			if err != nil {
				return errors.Wrap(err, "Read batch write length fail to convert from byte")
			}
			if len(filebytes) <= int(batchWriteLen) {
				break
			}
			pos += batchWriteLength
			oldPos = pos
			parseBatchWriteLenFlag = false
		}

		// Parse entryLength.
		data = filebytes[pos : pos+entryLength]
		buffer = bytes.NewBuffer(data)
		err = binary.Read(buffer, binary.LittleEndian, &entryLen)
		if err != nil {
			return errors.Wrap(err, "Read entry length fail to convert from byte")
		}
		pos += int(entryLength)

		// Mark series block end-position
		seriesBlockEndPos = pos + int(entryLen)
		if len(filebytes) < seriesBlockEndPos {
			break
		}

		// Parse seriesIDLen.
		data = filebytes[pos : pos+seriesIDLength]
		buffer = bytes.NewBuffer(data)
		err = binary.Read(buffer, binary.LittleEndian, &seriesIDLen)
		if err != nil {
			return errors.Wrap(err, "Read seriesIDLen fail to convert from byte")
		}
		pos += seriesIDLength

		// Parse seriesID.
		data = filebytes[pos : pos+int(seriesIDLen)]
		seriesID = common.ParseSeriesIDV2(data)
		pos += int(seriesIDLen)

		// Parse series count.
		data = filebytes[pos : pos+seriesCountLength]
		buffer = bytes.NewBuffer(data)
		err = binary.Read(buffer, binary.LittleEndian, &seriesCount)
		if err != nil {
			return errors.Wrap(err, "Read series count fail to convert from byte")
		}
		pos += seriesCountLength

		// Parse timestamps compression binary.
		data = filebytes[pos : pos+timestampsBinaryLength]
		buffer = bytes.NewBuffer(data)
		err = binary.Read(buffer, binary.LittleEndian, &timestampsBinaryLen)
		if err != nil {
			return errors.Wrap(err, "Read timestamps compression binary length fail to convert from byte")
		}
		pos += timestampsBinaryLength
		data = filebytes[pos : pos+int(timestampsBinaryLen)]
		timestampReader := encoding.NewReader(bytes.NewReader(data))
		timestampDecoder := encoding.NewXORDecoder(timestampReader)
		var timestamps []time.Time
		for i := 0; i < int(seriesCount); i++ {
			if !timestampDecoder.Next() {
				return errors.Wrap(err, "Timestamps length not match series count, index: "+strconv.Itoa(i))
			}
			unixNano := timestampDecoder.Value()
			timestamps = append(timestamps, unixNanoToTime(unixNano))
		}
		pos += int(timestampsBinaryLen)

		// Parse values compression binary.
		data = filebytes[pos:seriesBlockEndPos]
		data, err = snappy.Decode(nil, data)
		if err != nil {
			return errors.Wrap(err, "Read values compression binary fail to snappy decode")
		}
		values := parseValuesBinary(data)
		if values.Len() != len(timestamps) {
			return errors.New("Timestamps length and values length not match: " + strconv.Itoa(len(timestamps)) + " vs " + strconv.Itoa(values.Len()))
		}
		pos = seriesBlockEndPos

		logEntry := &logEntry{
			entryLength: entryLen,
			seriesID:    seriesID,
			count:       int32(len(timestamps)),
			timestamps:  timestamps,
			values:      values,
		}
		logEntries = append(logEntries, logEntry)
		if pos == len(filebytes) {
			break
		}
		if pos-oldPos == int(batchWriteLen) {
			parseBatchWriteLenFlag = true
		}
	}
	segment.logEntries = logEntries
	return nil
}

func (logEntry *logEntry) GetSeriesID() common.SeriesIDV2 {
	return logEntry.seriesID
}

func (logEntry *logEntry) GetTimestamps() []time.Time {
	return logEntry.timestamps
}

func (logEntry *logEntry) GetValues() *list.List {
	return logEntry.values
}

func (buffer *buffer) write(request logRequest) {
	seriesID := request.seriesID
	buffer.timestampMap[seriesID] = append(buffer.timestampMap[seriesID], request.timestamp)

	// Value item: binary-length(2-bytes) + binary data(n-bytes)
	binaryLength := int16ToBytes(int16(len(request.data)))
	buffer.valueMap[seriesID] = append(buffer.valueMap[seriesID], binaryLength...)
	buffer.valueMap[seriesID] = append(buffer.valueMap[seriesID], request.data...)

	buffer.callbackMap[seriesID] = append(buffer.callbackMap[seriesID], request.callback)
	buffer.count++
}

func (buffer *buffer) notifyRequests(err error) {
	for seriesID, callbacks := range buffer.callbackMap {
		timestamps := buffer.timestampMap[seriesID]
		values := buffer.valueMap[seriesID]
		valuePos := 0
		var valueItem []byte
		for index, callback := range callbacks {
			valuePos, valueItem = readBinarySplit(values, valuePos, valuesBinaryLength)
			runCallbackWithTryCatch(func() {
				callback(seriesID, timestamps[index], valueItem, err)
			})
		}
	}
}

func segmentName(index uint64) string {
	return fmt.Sprintf("%v%016x%v", segmentNamePrefix, index, segmentNameSuffix)
}

func runCallbackWithTryCatch(callback func()) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Run callback error is %v\n", err)
		}
	}()
	callback()
}

func parseValuesBinary(binary []byte) *list.List {
	values := list.New()
	pos := 0
	for {
		nextPos, binaryItem := readBinarySplit(binary, pos, valuesBinaryLength)
		if binaryItem == nil {
			break
		}
		values.PushBack(binaryItem)
		pos = nextPos
	}
	return values
}

func readBinarySplit(raw []byte, pos int, offsetLen int) (int, []byte) {
	if pos == len(raw) {
		return pos, nil
	}

	data := raw[pos : pos+offsetLen]
	binaryLen := bytesToInt16(data)
	pos += offsetLen

	data = raw[pos : pos+int(binaryLen)]
	pos += int(binaryLen)
	return pos, data
}

func rewriteInt64InBuf(buf []byte, value int64, offset int, order binary.ByteOrder) {
	_ = buf[offset+7] // early bounds check to guarantee safety of writes below
	if order == binary.LittleEndian {
		buf[offset+0] = byte(value)
		buf[offset+1] = byte(value >> 8)
		buf[offset+2] = byte(value >> 16)
		buf[offset+3] = byte(value >> 24)
		buf[offset+4] = byte(value >> 32)
		buf[offset+5] = byte(value >> 40)
		buf[offset+6] = byte(value >> 48)
		buf[offset+7] = byte(value >> 56)
	} else {
		buf[offset+0] = byte(value >> 56)
		buf[offset+1] = byte(value >> 48)
		buf[offset+2] = byte(value >> 40)
		buf[offset+3] = byte(value >> 32)
		buf[offset+4] = byte(value >> 24)
		buf[offset+5] = byte(value >> 16)
		buf[offset+6] = byte(value >> 8)
		buf[offset+7] = byte(value)
	}
}

func int16ToBytes(i int16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(i))
	return buf
}

func bytesToInt16(buf []byte) int16 {
	return int16(binary.LittleEndian.Uint16(buf))
}

func timeTouUnixNano(time time.Time) uint64 {
	return uint64(time.UnixNano())
}

func unixNanoToTime(unixNano uint64) time.Time {
	return time.Unix(0, int64(unixNano))
}
