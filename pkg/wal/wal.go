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
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

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
	maxSegmentID           = uint64(math.MaxUint64) - 1
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
	writeCloser      *run.ChannelCloser
	flushCloser      *run.ChannelCloser
	chanGroupCloser  *run.ChannelGroupCloser
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
	rwMutex          sync.RWMutex
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
	entryLength uint64
	count       uint32
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

	writeCloser := run.NewChannelCloser()
	flushCloser := run.NewChannelCloser()
	chanGroupCloser := run.NewChannelGroupCloser(writeCloser, flushCloser)
	log := &log{
		path:             path,
		options:          *walOptions,
		logger:           logger.GetLogger(moduleName),
		writeChannel:     make(chan logRequest),
		flushChannel:     make(chan buffer, walOptions.FlushQueueSize),
		bytesBuffer:      bytes.NewBuffer([]byte{}),
		timestampsBuffer: bytes.NewBuffer([]byte{}),
		writeCloser:      writeCloser,
		flushCloser:      flushCloser,
		chanGroupCloser:  chanGroupCloser,
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
	log.start()

	log.logger.Info().Str("path", path).Msg("WAL has be initialized")
	return log, nil
}

// Write a logging entity.
// It will return immediately when the data is written in the buffer,
// The callback function will be called when the entity is flushed on the persistent storage.
func (log *log) Write(seriesID common.SeriesIDV2, timestamp time.Time, data []byte, callback func(common.SeriesIDV2, time.Time, []byte, error)) {
	if !log.writeCloser.AddSender() {
		return
	}
	defer log.writeCloser.SenderDone()

	log.writeChannel <- logRequest{
		seriesID:  seriesID,
		timestamp: timestamp,
		data:      data,
		callback:  callback,
	}
}

// Read specified segment by SegmentID.
func (log *log) Read(segmentID SegmentID) (Segment, error) {
	log.rwMutex.RLock()
	defer log.rwMutex.RUnlock()

	segment := log.segmentMap[segmentID]
	return segment, nil
}

// ReadAllSegments reads all segments sorted by their creation time in ascending order.
func (log *log) ReadAllSegments() ([]Segment, error) {
	log.rwMutex.RLock()
	defer log.rwMutex.RUnlock()

	segments := make([]Segment, 0)
	for _, segment := range log.segmentMap {
		segments = append(segments, segment)
	}
	return segments, nil
}

// Rotate closes the open segment and opens a new one, returning the closed segment details.
func (log *log) Rotate() (Segment, error) {
	log.rwMutex.Lock()
	defer log.rwMutex.Unlock()

	newSegmentID := uint64(log.workSegment.segmentID) + 1
	if newSegmentID > maxSegmentID {
		return nil, errors.New("Segment ID overflow uint64," +
			" please delete all WAL segment files and restart")
	}
	if err := log.workSegment.file.Close(); err != nil {
		return nil, errors.Wrap(err, "Close WAL segment error")
	}

	// Create new segment.
	oldWorkSegment := log.workSegment
	segment := &segment{
		segmentID: SegmentID(newSegmentID),
		path:      filepath.Join(log.path, segmentName(newSegmentID)),
	}
	if err := segment.openFile(true); err != nil {
		return nil, errors.Wrap(err, "Open WAL segment error")
	}
	log.workSegment = segment

	// Update segment information.
	log.segmentMap[log.workSegment.segmentID] = log.workSegment
	return oldWorkSegment, nil
}

// Delete the specified segment.
func (log *log) Delete(segmentID SegmentID) error {
	log.rwMutex.Lock()
	defer log.rwMutex.Unlock()

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
	var globalErr error
	log.closerOnce.Do(func() {
		log.logger.Info().Msg("Closing WAL...")

		log.chanGroupCloser.CloseThenWait()

		if err := log.flushBuffer(log.buffer); err != nil {
			globalErr = multierr.Append(globalErr, err)
		}
		if err := log.workSegment.file.Close(); err != nil {
			globalErr = multierr.Append(globalErr, err)
		}
		log.logger.Info().Msg("Closed WAL")
	})
	return globalErr
}

func (log *log) start() {
	var initialTasks sync.WaitGroup
	initialTasks.Add(2)

	go func() {
		if !log.writeCloser.AddReceiver() {
			panic("writeCloser already closed")
		}
		defer log.writeCloser.ReceiverDone()

		log.logger.Info().Msg("Start batch task...")
		initialTasks.Done()

		bufferVolume := 0
		for {
			timer := time.NewTimer(log.options.BufferBatchInterval)
			select {
			case request, chOpen := <-log.writeChannel:
				if !chOpen {
					timer.Stop()
					log.logger.Info().Msg("Stop batch task when write-channel closed")
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
			case <-timer.C:
				if bufferVolume == 0 {
					continue
				}
				log.triggerFlushing()
				bufferVolume = 0
			case <-log.writeCloser.CloseNotify():
				timer.Stop()
				log.logger.Info().Msg("Stop batch task when close notify")
				return
			}
		}
	}()

	go func() {
		if !log.flushCloser.AddReceiver() {
			panic("flushCloser already closed")
		}
		defer log.flushCloser.ReceiverDone()

		log.logger.Info().Msg("Start flush task...")
		initialTasks.Done()

		for {
			select {
			case batch, chOpen := <-log.flushChannel:
				if !chOpen {
					log.logger.Info().Msg("Stop flush task when flush-channel closed")
					return
				}

				startTime := time.Now()
				var err error
				for i := 0; i < maxRetries; i++ {
					if err = log.flushBuffer(batch); err != nil {
						log.logger.Err(err).Msg("Flushing buffer failed. Retrying...")
						time.Sleep(time.Second)
						continue
					}
					break
				}
				if log.logger.Debug().Enabled() {
					log.logger.Debug().Msg("Flushed buffer to WAL file. elements: " +
						strconv.Itoa(batch.count) + ", cost: " + time.Since(startTime).String())
				}

				batch.notifyRequests(err)
			case <-log.flushCloser.CloseNotify():
				log.logger.Info().Msg("Stop flush task when close notify")
				return
			}
		}
	}()

	initialTasks.Wait()
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
	log.buffer = buffer{
		timestampMap: make(map[common.SeriesIDV2][]time.Time),
		valueMap:     make(map[common.SeriesIDV2][]byte),
		callbackMap:  make(map[common.SeriesIDV2][]func(common.SeriesIDV2, time.Time, []byte, error)),
		count:        0,
	}
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
	var batchLen uint64
	if err := log.writeBatchLength(batchLen); err != nil {
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
			timestampEncoder.Write(timeToUnixNano(timestamp))
		}
		timestampWriter.Flush()
		timestampsBytes := log.timestampsBuffer.Bytes()
		timestampsBytesLen := len(timestampsBytes)

		// Generate values compression binary
		valuesBytes := snappy.Encode(nil, buffer.valueMap[seriesID])

		// Write entry data
		entryLen := seriesIDLength + seriesIDBytesLen + seriesCountLength + timestampsBinaryLength + timestampsBytesLen + len(valuesBytes)
		if err := log.writeEntryLength(uint64(entryLen)); err != nil {
			return errors.Wrap(err, "Write entry length error")
		}
		if err := log.writeSeriesIDLength(uint16(seriesIDBytesLen)); err != nil {
			return errors.Wrap(err, "Write seriesID length error")
		}
		if err := log.writeSeriesID(seriesIDBytes); err != nil {
			return errors.Wrap(err, "Write seriesID error")
		}
		if err := log.writeSeriesCount(uint32(len(timestamps))); err != nil {
			return errors.Wrap(err, "Write series count error")
		}
		if err := log.writeTimestampsLength(uint16(timestampsBytesLen)); err != nil {
			return errors.Wrap(err, "Write timestamps length error")
		}
		if err := log.writeTimestamps(timestampsBytes); err != nil {
			return errors.Wrap(err, "Write timestamps error")
		}
		if err := log.writeData(valuesBytes); err != nil {
			return errors.Wrap(err, "Write values error")
		}
	}
	// Rewrite batch length
	batchBytes := log.bytesBuffer.Bytes()
	batchLen = uint64(len(batchBytes)) - batchLength
	rewriteBatchLength(batchBytes, batchLen)

	return log.writeWorkSegment(batchBytes)
}

func (log *log) writeBatchLength(data uint64) error {
	return binary.Write(log.bytesBuffer, binary.LittleEndian, data)
}

func (log *log) writeEntryLength(data uint64) error {
	return binary.Write(log.bytesBuffer, binary.LittleEndian, data)
}

func (log *log) writeSeriesIDLength(data uint16) error {
	return binary.Write(log.bytesBuffer, binary.LittleEndian, data)
}

func (log *log) writeSeriesID(data []byte) error {
	return binary.Write(log.bytesBuffer, binary.LittleEndian, data)
}

func (log *log) writeSeriesCount(data uint32) error {
	return binary.Write(log.bytesBuffer, binary.LittleEndian, data)
}

func (log *log) writeTimestampsLength(data uint16) error {
	return binary.Write(log.bytesBuffer, binary.LittleEndian, data)
}

func (log *log) writeTimestamps(data []byte) error {
	return binary.Write(log.bytesBuffer, binary.LittleEndian, data)
}

func (log *log) writeData(data []byte) error {
	return binary.Write(log.bytesBuffer, binary.LittleEndian, data)
}

func (log *log) writeWorkSegment(data []byte) error {
	log.rwMutex.RLock()
	defer log.rwMutex.RUnlock()

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
		segmentID, parsePathErr := parseSegmentID(name)
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
		segmentID := SegmentID(1)
		segment := &segment{
			segmentID: segmentID,
			path:      filepath.Join(log.path, segmentName(uint64(segmentID))),
		}
		log.segmentMap[segmentID] = segment
		log.workSegment = segment
	} else {
		log.workSegment = log.segmentMap[workSegmentID]
	}
	if err = log.workSegment.openFile(false); err != nil {
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

func (segment *segment) openFile(overwrite bool) error {
	var err error
	if overwrite {
		segment.file, err = os.OpenFile(segment.path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModePerm)
	} else {
		segment.file, err = os.OpenFile(segment.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, os.ModePerm)
	}
	return err
}

func (segment *segment) parseLogEntries() error {
	segmentBytes, err := os.ReadFile(segment.path)
	if err != nil {
		return errors.Wrap(err, "Read WAL segment failed, path: "+segment.path)
	}

	var logEntries []LogEntry
	var data []byte
	var batchLen uint64
	var entryLen uint64
	var seriesIDLen uint16
	var seriesID common.SeriesIDV2
	var seriesCount uint32
	var timestampsBinaryLen uint16
	var entryEndPosition uint64

	var oldPos uint64
	var pos uint64
	parseNextBatchFlag := true
	segmentBytesLen := uint64(len(segmentBytes))

	for {
		if parseNextBatchFlag {
			if segmentBytesLen <= batchLength {
				break
			}
			data = segmentBytes[pos : pos+batchLength]
			batchLen, err = segment.parseBatchLength(data)
			if err != nil {
				return errors.Wrap(err, "Parse batch length error")
			}

			if segmentBytesLen <= batchLen {
				break
			}

			pos += batchLength
			oldPos = pos
			parseNextBatchFlag = false
		}

		// Parse entryLength.
		data = segmentBytes[pos : pos+entryLength]

		entryLen, err = segment.parseEntryLength(data)
		if err != nil {
			return errors.Wrap(err, "Parse entry length error")
		}
		pos += entryLength

		// Mark entry end-position
		entryEndPosition = pos + entryLen
		if segmentBytesLen < entryEndPosition {
			break
		}

		// Parse seriesIDLength.
		data = segmentBytes[pos : pos+seriesIDLength]
		seriesIDLen, err = segment.parseSeriesIDLength(data)
		if err != nil {
			return errors.Wrap(err, "Parse seriesID length error")
		}
		pos += seriesIDLength

		// Parse seriesID.
		data = segmentBytes[pos : pos+uint64(seriesIDLen)]
		seriesID = segment.parseSeriesID(data)
		pos += uint64(seriesIDLen)

		// Parse series count.
		data = segmentBytes[pos : pos+seriesCountLength]
		seriesCount, err = segment.parseSeriesCountLength(data)
		if err != nil {
			return errors.Wrap(err, "Parse series count length error")
		}
		pos += seriesCountLength

		// Parse timestamps compression binary.
		data = segmentBytes[pos : pos+timestampsBinaryLength]
		timestampsBinaryLen, err = segment.parseTimestampsLength(data)
		if err != nil {
			return errors.Wrap(err, "Parse timestamps length error")
		}
		pos += timestampsBinaryLength
		data = segmentBytes[pos : pos+uint64(timestampsBinaryLen)]
		var timestamps []time.Time
		timestamps, err = segment.parseTimestamps(seriesCount, data)
		if err != nil {
			return errors.Wrap(err, "Parse timestamps compression binary error")
		}
		pos += uint64(timestampsBinaryLen)

		// Parse values compression binary.
		data = segmentBytes[pos:entryEndPosition]
		values, err := segment.parseValuesBinary(data)
		if err != nil {
			return errors.Wrap(err, "Parse values compression binary error")
		}
		if values.Len() != int(seriesCount) {
			return errors.New("values binary items not match series count. series count: " +
				strconv.Itoa(int(seriesCount)) + ", values binary items: " + strconv.Itoa(values.Len()))
		}
		pos = entryEndPosition

		logEntry := &logEntry{
			entryLength: entryLen,
			seriesID:    seriesID,
			count:       seriesCount,
			timestamps:  timestamps,
			values:      values,
		}
		logEntries = append(logEntries, logEntry)

		if pos == segmentBytesLen {
			break
		}
		if pos-oldPos == batchLen {
			parseNextBatchFlag = true
		}
	}
	segment.logEntries = logEntries
	return nil
}

func (segment *segment) parseBatchLength(data []byte) (uint64, error) {
	var batchLen uint64
	buf := bytes.NewBuffer(data)
	if err := binary.Read(buf, binary.LittleEndian, &batchLen); err != nil {
		return 0, err
	}
	return batchLen, nil
}

func (segment *segment) parseEntryLength(data []byte) (uint64, error) {
	var entryLen uint64
	buf := bytes.NewBuffer(data)
	if err := binary.Read(buf, binary.LittleEndian, &entryLen); err != nil {
		return 0, err
	}
	return entryLen, nil
}

func (segment *segment) parseSeriesIDLength(data []byte) (uint16, error) {
	var seriesIDLen uint16
	buf := bytes.NewBuffer(data)
	if err := binary.Read(buf, binary.LittleEndian, &seriesIDLen); err != nil {
		return 0, err
	}
	return seriesIDLen, nil
}

func (segment *segment) parseSeriesID(data []byte) common.SeriesIDV2 {
	return common.ParseSeriesIDV2(data)
}

func (segment *segment) parseSeriesCountLength(data []byte) (uint32, error) {
	var seriesCount uint32
	buf := bytes.NewBuffer(data)
	if err := binary.Read(buf, binary.LittleEndian, &seriesCount); err != nil {
		return 0, err
	}
	return seriesCount, nil
}

func (segment *segment) parseTimestampsLength(data []byte) (uint16, error) {
	var timestampsLen uint16
	buf := bytes.NewBuffer(data)
	if err := binary.Read(buf, binary.LittleEndian, &timestampsLen); err != nil {
		return 0, err
	}
	return timestampsLen, nil
}

func (segment *segment) parseTimestamps(seriesCount uint32, data []byte) ([]time.Time, error) {
	timestampReader := encoding.NewReader(bytes.NewReader(data))
	timestampDecoder := encoding.NewXORDecoder(timestampReader)
	var timestamps []time.Time
	for i := 0; i < int(seriesCount); i++ {
		if !timestampDecoder.Next() {
			return nil, errors.New("Timestamps length not match series count")
		}
		timestamps = append(timestamps, unixNanoToTime(timestampDecoder.Value()))
	}
	return timestamps, nil
}

func (segment *segment) parseValuesBinary(data []byte) (*list.List, error) {
	var err error
	if data, err = snappy.Decode(nil, data); err != nil {
		return nil, errors.Wrap(err, "Decode values compression binary error")
	}

	values := list.New()
	position := 0
	for {
		nextPosition, value := readValuesBinary(data, position, valuesBinaryLength)
		if value == nil {
			break
		}
		values.PushBack(value)
		position = nextPosition
	}
	return values, nil
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
	binaryLength := uint16ToBytes(uint16(len(request.data)))
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
			buffer.runningCallback(func() {
				callback(seriesID, timestamps[index], valueItem, err)
			})
		}
	}
}

func (buffer *buffer) runningCallback(callback func()) {
	defer func() {
		_ = recover()
	}()
	callback()
}

func segmentName(segmentID uint64) string {
	return fmt.Sprintf("%v%016x%v", segmentNamePrefix, segmentID, segmentNameSuffix)
}

// Parse segment ID. segmentName example: seg0000000000000001.wal.
func parseSegmentID(segmentName string) (uint64, error) {
	_ = segmentName[22:] // early bounds check to guarantee safety of reads below
	if !strings.HasPrefix(segmentName, segmentNamePrefix) {
		return 0, errors.New("Invalid segment name: " + segmentName)
	}
	if !strings.HasSuffix(segmentName, segmentNameSuffix) {
		return 0, errors.New("Invalid segment name: " + segmentName)
	}
	return strconv.ParseUint(segmentName[3:19], 10, 64)
}

func readValuesBinary(raw []byte, position int, offsetLen int) (int, []byte) {
	if position == len(raw) {
		return position, nil
	}

	data := raw[position : position+offsetLen]
	binaryLen := bytesToUint16(data)
	position += offsetLen

	data = raw[position : position+int(binaryLen)]
	position += int(binaryLen)
	return position, data
}

func rewriteBatchLength(batchBytes []byte, batchLen uint64) {
	_ = batchBytes[7] // early bounds check to guarantee safety of writes below
	batchBytes[0] = byte(batchLen)
	batchBytes[1] = byte(batchLen >> 8)
	batchBytes[2] = byte(batchLen >> 16)
	batchBytes[3] = byte(batchLen >> 24)
	batchBytes[4] = byte(batchLen >> 32)
	batchBytes[5] = byte(batchLen >> 40)
	batchBytes[6] = byte(batchLen >> 48)
	batchBytes[7] = byte(batchLen >> 56)
}

func uint16ToBytes(i uint16) []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, i)
	return buf
}

func bytesToUint16(buf []byte) uint16 {
	return binary.LittleEndian.Uint16(buf)
}

func timeToUnixNano(time time.Time) uint64 {
	return uint64(time.UnixNano())
}

func unixNanoToTime(unixNano uint64) time.Time {
	return time.Unix(0, int64(unixNano))
}
