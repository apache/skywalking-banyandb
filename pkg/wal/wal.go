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
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	moduleName             = "wal"
	segmentNamePrefix      = "seg"
	segmentNameSuffix      = ".wal"
	batchLength            = 8
	entryLength            = 8
	seriesIDLength         = 2
	seriesCountLength      = 4
	timestampVolumeLength  = 8
	timestampsBinaryLength = 2
	valuesBinaryLength     = 2
	parseTimeStr           = "2006-01-02 15:04:05"
	maxRetries             = 3
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

// log implements the WAL interface.
type log struct {
	entryCloser      *run.Closer
	buffer           buffer
	logger           *logger.Logger
	bytesBuffer      *bytes.Buffer
	timestampsBuffer *bytes.Buffer
	segmentMap       map[SegmentID]*segment
	workSegment      *segment
	writeChannel     chan logRequest
	flushChannel     chan buffer
	path             string
	options          Options
	writeWaitGroup   sync.WaitGroup
	flushWaitGroup   sync.WaitGroup
	workSegmentMutex sync.Mutex
	segmentMapMutex  sync.RWMutex
	closerOnce       sync.Once
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
	walOptions := DefaultOptions
	if options != nil {
		fileSize := options.FileSize
		if fileSize <= 0 {
			fileSize = DefaultOptions.FileSize
		}
		bufferSize := options.BufferSize
		if bufferSize <= 0 {
			bufferSize = DefaultOptions.BufferSize
		}
		bufferBatchInterval := options.BufferBatchInterval
		if bufferBatchInterval <= 0 {
			bufferBatchInterval = DefaultOptions.BufferBatchInterval
		}
		walOptions = &Options{
			FileSize:            fileSize,
			BufferSize:          bufferSize,
			BufferBatchInterval: bufferBatchInterval,
		}
	}

	// Initial WAL path.
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, errors.Wrap(err, "Can not get absolute path: "+path)
	}
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return nil, err
	}

	log := &log{
		path:             path,
		options:          *walOptions,
		logger:           logger.GetLogger(moduleName),
		writeChannel:     make(chan logRequest),
		flushChannel:     make(chan buffer, walOptions.FlushQueueSize),
		bytesBuffer:      bytes.NewBuffer([]byte{}),
		timestampsBuffer: bytes.NewBuffer([]byte{}),
		entryCloser:      run.NewCloser(1),
		buffer: buffer{
			timestampMap: make(map[common.SeriesIDV2][]time.Time),
			valueMap:     make(map[common.SeriesIDV2][]byte),
			callbackMap:  make(map[common.SeriesIDV2][]func(common.SeriesIDV2, time.Time, []byte, error)),
			count:        0,
		},
	}
	if err := log.load(); err != nil {
		return nil, err
	}

	log.writeWaitGroup.Add(1)
	log.flushWaitGroup.Add(1)
	log.start()

	log.logger.Info().Str("path", path).Msg("WAL has be initialized")
	return log, nil
}

// Write a logging entity.
// It will return immediately when the data is written in the buffer,
// The callback function will be called when the entity is flushed on the persistent storage.
func (log *log) Write(seriesID common.SeriesIDV2, timestamp time.Time, data []byte, callback func(common.SeriesIDV2, time.Time, []byte, error)) {
	if !log.entryCloser.AddRunning() {
		return
	}
	defer log.entryCloser.Done()

	log.writeChannel <- logRequest{
		seriesID:  seriesID,
		timestamp: timestamp,
		data:      data,
		callback:  callback,
	}
}

// Read specified segment by SegmentID.
func (log *log) Read(segmentID SegmentID) (Segment, error) {
	if !log.entryCloser.AddRunning() {
		return nil, errors.New("WAL is closed")
	}
	defer log.entryCloser.Done()

	log.segmentMapMutex.RLock()
	defer log.segmentMapMutex.RUnlock()

	segment := log.segmentMap[segmentID]
	return segment, nil
}

// ReadAllSegments reads all segments sorted by their creation time in ascending order.
func (log *log) ReadAllSegments() ([]Segment, error) {
	if !log.entryCloser.AddRunning() {
		return nil, errors.New("WAL is closed")
	}
	defer log.entryCloser.Done()

	log.segmentMapMutex.RLock()
	defer log.segmentMapMutex.RUnlock()

	segments := make([]Segment, 0)
	for _, segment := range log.segmentMap {
		segments = append(segments, segment)
	}
	return segments, nil
}

// Rotate closes the open segment and opens a new one, returning the closed segment details.
func (log *log) Rotate() (Segment, error) {
	if !log.entryCloser.AddRunning() {
		return nil, errors.New("WAL is closed")
	}
	defer log.entryCloser.Done()

	oldWorkSegment, err := log.swapWorkSegment()
	if err != nil {
		return nil, err
	}

	log.segmentMapMutex.Lock()
	defer log.segmentMapMutex.Unlock()

	// Update segment information.
	workSegment := log.workSegment
	log.segmentMap[workSegment.segmentID] = workSegment
	return oldWorkSegment, nil
}

// Delete the specified segment.
func (log *log) Delete(segmentID SegmentID) error {
	if !log.entryCloser.AddRunning() {
		return errors.New("WAL is closed")
	}
	defer log.entryCloser.Done()

	log.segmentMapMutex.Lock()
	defer log.segmentMapMutex.Unlock()

	// Segment which will be deleted must be closed.
	if segmentID == log.workSegment.segmentID {
		return errors.New("Can not delete the segment which is working")
	}

	err := os.Remove(log.segmentMap[segmentID].path)
	if err != nil {
		return errors.Wrap(err, "Delete WAL segment error")
	}
	delete(log.segmentMap, segmentID)
	return nil
}

// Close all of segments and stop WAL work.
func (log *log) Close() error {
	log.closerOnce.Do(func() {
		log.logger.Info().Msg("Closing WAL...")

		log.entryCloser.Done()
		log.entryCloser.CloseThenWait()

		close(log.writeChannel)
		log.writeWaitGroup.Wait()

		close(log.flushChannel)
		log.flushWaitGroup.Wait()

		if err := log.flushBuffer(log.buffer); err != nil {
			log.logger.Err(err).Msg("Flushing buffer failed")
		}
		if err := log.workSegment.file.Close(); err != nil {
			log.logger.Err(err).Msg("Close work segment file error")
		}
		log.logger.Info().Msg("Closed WAL")
	})
	return nil
}

func (log *log) start() {
	go func() {
		log.logger.Info().Msg("Start batch task...")

		defer log.writeWaitGroup.Done()

		bufferVolume := 0
		for {
			timer := time.NewTimer(log.options.BufferBatchInterval)
			select {
			case request, chOpen := <-log.writeChannel:
				if !chOpen {
					log.logger.Info().Msg("Stop batch task when write-channel closed!")
					return
				}

				log.buffer.write(request)
				if log.logger.Debug().Enabled() {
					log.logger.Debug().Msg("Write request to buffer. elements: " + strconv.Itoa(log.buffer.count))
				}

				bufferVolume += len(request.seriesID.Marshal()) + timestampVolumeLength + len(request.data)
				if bufferVolume > log.options.BufferSize {
					log.triggerFlushing()
					bufferVolume = 0
				}
				continue
			case <-timer.C:
				if bufferVolume == 0 {
					continue
				}
				log.triggerFlushing()
				bufferVolume = 0
				continue
			}
		}
	}()

	go func() {
		log.logger.Info().Msg("Start flush task...")

		defer log.flushWaitGroup.Done()

		for batch := range log.flushChannel {
			startTime := time.Now()

			var err error
			for i := 0; i < maxRetries; i++ {
				if err = log.flushBuffer(batch); err != nil {
					log.logger.Err(err).Msg("Flushing buffer failed. Retrying...")
					time.Sleep(100 * time.Millisecond)
					continue
				}
				break
			}
			if log.logger.Debug().Enabled() {
				log.logger.Debug().Msg("Flushed buffer to WAL file. elements: " +
					strconv.Itoa(batch.count) + ", cost: " + time.Since(startTime).String())
			}

			batch.notifyRequests(err)
		}
		log.logger.Info().Msg("Stop flush task when flush-channel closed!")
	}()

	log.logger.Info().Msg("Started WAL")
}

func (log *log) triggerFlushing() {
	for {
		select {
		case log.flushChannel <- log.buffer:
			if log.logger.Debug().Enabled() {
				log.logger.Debug().Msg("Send buffer to flush-channel. elements: " + strconv.Itoa(log.buffer.count))
			}
			log.newBuffer()
			return
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (log *log) newBuffer() {
	log.buffer.timestampMap = make(map[common.SeriesIDV2][]time.Time)
	log.buffer.valueMap = make(map[common.SeriesIDV2][]byte)
	log.buffer.callbackMap = make(map[common.SeriesIDV2][]func(common.SeriesIDV2, time.Time, []byte, error))
	log.buffer.count = 0
}

func (log *log) flushBuffer(buffer buffer) error {
	if buffer.count == 0 {
		return nil
	}

	defer func() {
		log.bytesBuffer.Reset()
		log.timestampsBuffer.Reset()
	}()

	// placeholder, preset batch length value is 0
	batchLen := 0
	if err := binary.Write(log.bytesBuffer, binary.LittleEndian, int64(batchLen)); err != nil {
		return errors.Wrap(err, "Write batch length error")
	}
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
		entryLen := seriesIDLength + seriesIDBytesLen + seriesCountLength + timestampsBinaryLength + timestampsBytesLen + len(valuesBytes)
		if err := binary.Write(log.bytesBuffer, binary.LittleEndian, int64(entryLen)); err != nil {
			return errors.Wrap(err, "Write entry length error")
		}
		if err := binary.Write(log.bytesBuffer, binary.LittleEndian, int16(seriesIDBytesLen)); err != nil {
			return errors.Wrap(err, "Write seriesID length error")
		}
		if err := binary.Write(log.bytesBuffer, binary.LittleEndian, seriesIDBytes); err != nil {
			return errors.Wrap(err, "Write seriesID error")
		}
		if err := binary.Write(log.bytesBuffer, binary.LittleEndian, int32(len(timestamps))); err != nil {
			return errors.Wrap(err, "Write series count error")
		}
		if err := binary.Write(log.bytesBuffer, binary.LittleEndian, int16(timestampsBytesLen)); err != nil {
			return errors.Wrap(err, "Write timestamps length error")
		}
		if err := binary.Write(log.bytesBuffer, binary.LittleEndian, timestampsBytes); err != nil {
			return errors.Wrap(err, "Write timestamps error")
		}
		if err := binary.Write(log.bytesBuffer, binary.LittleEndian, valuesBytes); err != nil {
			return errors.Wrap(err, "Write values error")
		}
	}
	// Rewrite batch length
	batchBytes := log.bytesBuffer.Bytes()
	batchLen = len(batchBytes) - batchLength
	rewriteInt64InBuf(batchBytes, int64(batchLen), 0, binary.LittleEndian)

	return log.writeWorkSegment(batchBytes)
}

func (log *log) swapWorkSegment() (Segment, error) {
	log.workSegmentMutex.Lock()
	defer log.workSegmentMutex.Unlock()

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
	log.workSegment = segment

	return oldSegment, nil
}

func (log *log) writeWorkSegment(data []byte) error {
	log.workSegmentMutex.Lock()
	defer log.workSegmentMutex.Unlock()

	// Write batch data to WAL segment file
	if _, err := log.workSegment.file.Write(data); err != nil {
		return errors.Wrap(err, "Write WAL segment file error, file: "+log.workSegment.path)
	}
	if err := log.workSegment.file.Sync(); err != nil {
		log.logger.Warn().Msg("Sync WAL segment file to disk failed, file: " + log.workSegment.path)
	}
	return nil
}

func (log *log) load() error {
	files, err := os.ReadDir(log.path)
	if err != nil {
		return errors.Wrap(err, "Can not read dir: "+log.path)
	}
	// Load all of WAL segments.
	var workSegmentID SegmentID
	log.segmentMap = make(map[SegmentID]*segment)
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
		log.segmentMap[SegmentID(segmentID)] = segment

		if log.logger.Debug().Enabled() {
			log.logger.Debug().Msg("Loaded segment file: " + segment.path)
		}
	}

	// If load first time.
	if len(log.segmentMap) == 0 {
		segment := &segment{
			segmentID: 1,
			path:      filepath.Join(log.path, segmentName(1)),
		}
		log.segmentMap[1] = segment
		log.workSegment = segment
	} else {
		log.workSegment = log.segmentMap[workSegmentID]
	}
	log.workSegment.file, err = os.OpenFile(log.workSegment.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
	if err != nil {
		return errors.Wrap(err, "Open WAL segment error, file: "+log.workSegment.path)
	}
	return nil
}

func (segment *segment) GetSegmentID() SegmentID {
	return segment.segmentID
}

func (segment *segment) GetLogEntries() []LogEntry {
	return segment.logEntries
}

func (segment *segment) parseLogEntries() error {
	segmentBytes, err := os.ReadFile(segment.path)
	if err != nil {
		return errors.Wrap(err, "Read WAL segment failed, path: "+segment.path)
	}

	var logEntries []LogEntry
	var data []byte
	var batchLen int64
	var entryLen int64
	var seriesIDLen int16
	var seriesID common.SeriesIDV2
	var seriesCount int32
	var timestampsBinaryLen int16
	var entryEndPos int
	var bytesBuf *bytes.Buffer

	oldPos := 0
	pos := 0
	parseNextBatchFlag := true

	for {
		if parseNextBatchFlag {
			if len(segmentBytes) <= batchLength {
				break
			}
			data = segmentBytes[pos : pos+batchLength]
			bytesBuf = bytes.NewBuffer(data)
			if err = binary.Read(bytesBuf, binary.LittleEndian, &batchLen); err != nil {
				return errors.Wrap(err, "Read batch length fail to convert from bytes")
			}
			if len(segmentBytes) <= int(batchLen) {
				break
			}

			pos += batchLength
			oldPos = pos
			parseNextBatchFlag = false
		}

		// Parse entryLength.
		data = segmentBytes[pos : pos+entryLength]
		bytesBuf = bytes.NewBuffer(data)
		if err = binary.Read(bytesBuf, binary.LittleEndian, &entryLen); err != nil {
			return errors.Wrap(err, "Read entry length fail to convert from byte")
		}
		pos += entryLength

		// Mark entry end-position
		entryEndPos = pos + int(entryLen)
		if len(segmentBytes) < entryEndPos {
			break
		}

		// Parse seriesIDLen.
		data = segmentBytes[pos : pos+seriesIDLength]
		bytesBuf = bytes.NewBuffer(data)
		if err = binary.Read(bytesBuf, binary.LittleEndian, &seriesIDLen); err != nil {
			return errors.Wrap(err, "Read seriesID length fail to convert from byte")
		}
		pos += seriesIDLength

		// Parse seriesID.
		data = segmentBytes[pos : pos+int(seriesIDLen)]
		seriesID = common.ParseSeriesIDV2(data)
		pos += int(seriesIDLen)

		// Parse series count.
		data = segmentBytes[pos : pos+seriesCountLength]
		bytesBuf = bytes.NewBuffer(data)
		if err = binary.Read(bytesBuf, binary.LittleEndian, &seriesCount); err != nil {
			return errors.Wrap(err, "Read series count fail to convert from byte")
		}
		pos += seriesCountLength

		// Parse timestamps compression binary.
		data = segmentBytes[pos : pos+timestampsBinaryLength]
		bytesBuf = bytes.NewBuffer(data)
		if err = binary.Read(bytesBuf, binary.LittleEndian, &timestampsBinaryLen); err != nil {
			return errors.Wrap(err, "Read timestamps compression binary length fail to convert from byte")
		}
		pos += timestampsBinaryLength
		data = segmentBytes[pos : pos+int(timestampsBinaryLen)]
		timestampReader := encoding.NewReader(bytes.NewReader(data))
		timestampDecoder := encoding.NewXORDecoder(timestampReader)
		var timestamps []time.Time
		for i := 0; i < int(seriesCount); i++ {
			if !timestampDecoder.Next() {
				return errors.Wrap(err, "Timestamps length not match series count, index: "+strconv.Itoa(i))
			}
			timestamps = append(timestamps, unixNanoToTime(timestampDecoder.Value()))
		}
		pos += int(timestampsBinaryLen)

		// Parse values compression binary.
		data = segmentBytes[pos:entryEndPos]
		if data, err = snappy.Decode(nil, data); err != nil {
			return errors.Wrap(err, "Decode values compression binary fail to snappy decode")
		}
		values := parseValuesBinary(data)
		if values.Len() != len(timestamps) {
			return errors.New("Timestamps length and values length not match: " + strconv.Itoa(len(timestamps)) + " vs " + strconv.Itoa(values.Len()))
		}
		pos = entryEndPos

		logEntry := &logEntry{
			entryLength: entryLen,
			seriesID:    seriesID,
			count:       seriesCount,
			timestamps:  timestamps,
			values:      values,
		}
		logEntries = append(logEntries, logEntry)

		if pos == len(segmentBytes) {
			break
		}
		if pos-oldPos == int(batchLen) {
			parseNextBatchFlag = true
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
			valuePos, valueItem = readValuesBinary(values, valuePos, valuesBinaryLength)
			tryCallback(func() {
				callback(seriesID, timestamps[index], valueItem, err)
			})
		}
	}
}

func segmentName(index uint64) string {
	return fmt.Sprintf("%v%016x%v", segmentNamePrefix, index, segmentNameSuffix)
}

func tryCallback(callback func()) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("Run callback error is %v\n", err)
		}
	}()
	callback()
}

func parseValuesBinary(binary []byte) *list.List {
	values := list.New()
	position := 0
	for {
		nextPosition, value := readValuesBinary(binary, position, valuesBinaryLength)
		if value == nil {
			break
		}
		values.PushBack(value)
		position = nextPosition
	}
	return values
}

func readValuesBinary(raw []byte, position int, offsetLen int) (int, []byte) {
	if position == len(raw) {
		return position, nil
	}

	data := raw[position : position+offsetLen]
	binaryLen := bytesToInt16(data)
	position += offsetLen

	data = raw[position : position+int(binaryLen)]
	position += int(binaryLen)
	return position, data
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
