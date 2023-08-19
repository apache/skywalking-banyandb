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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/pkg/convert"
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
	defaultSyncFlush       = false
)

// DefaultOptions for Open().
var DefaultOptions = &Options{
	FileSize:            67108864, // 64MB
	BufferSize:          65535,    // 16KB
	BufferBatchInterval: 3 * time.Second,
	SyncFlush:           defaultSyncFlush,
}

// Options for creating Write-ahead Logging.
type Options struct {
	FileSize            int
	BufferSize          int
	BufferBatchInterval time.Duration
	FlushQueueSize      int
	SyncFlush           bool
}

// WAL denotes a Write-ahead logging.
// Modules who want their data reliable could write data to an instance of WAL.
// A WAL combines several segments, ingesting data on a single opened one.
// Rotating the WAL will create a new segment, marking it as opened and persisting previous segments on the disk.
type WAL interface {
	// Write a logging entity.
	// It will return immediately when the data is written in the buffer,
	// The callback function will be called when the entity is flushed on the persistent storage.
	Write(seriesID []byte, timestamp time.Time, data []byte, callback func([]byte, time.Time, []byte, error))
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
	GetSeriesID() []byte
	GetTimestamps() []time.Time
	GetValues() *list.List
}

// log implements the WAL interface.
type log struct {
	writeCloser     *run.ChannelCloser
	flushCloser     *run.ChannelCloser
	chanGroupCloser *run.ChannelGroupCloser
	buffer          buffer
	logger          *logger.Logger
	bufferWriter    *bufferWriter
	segmentMap      map[SegmentID]*segment
	workSegment     *segment
	writeChannel    chan logRequest
	flushChannel    chan buffer
	path            string
	options         Options
	rwMutex         sync.RWMutex
	closerOnce      sync.Once
}

type segment struct {
	file       *os.File
	path       string
	logEntries []LogEntry
	segmentID  SegmentID
}

type logRequest struct {
	seriesID  []byte
	timestamp time.Time
	callback  func([]byte, time.Time, []byte, error)
	data      []byte
}

type logEntry struct {
	timestamps  []time.Time
	values      *list.List
	seriesID    []byte
	entryLength uint64
	count       uint32
}

type buffer struct {
	timestampMap map[logSeriesID][]time.Time
	valueMap     map[logSeriesID][]byte
	callbackMap  map[logSeriesID][]func([]byte, time.Time, []byte, error)
	count        int
}

type bufferWriter struct {
	buf           *bytes.Buffer
	timestampsBuf *bytes.Buffer
	seriesID      *logSeriesID
	dataBuf       []byte
	batchLen      uint64
	seriesCount   uint32
	dataLen       int
}

type logSeriesID struct {
	key     string
	byteLen int
}

func newLogSeriesID(b []byte) logSeriesID {
	return logSeriesID{key: convert.BytesToString(b), byteLen: len(b)}
}

func (s logSeriesID) string() string {
	return s.key
}

func (s logSeriesID) bytes() []byte {
	return convert.StringToBytes(s.key)
}

func (s logSeriesID) len() int {
	return s.byteLen
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
			SyncFlush:           options.SyncFlush,
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
		path:            path,
		options:         *walOptions,
		logger:          logger.GetLogger(moduleName),
		writeChannel:    make(chan logRequest),
		flushChannel:    make(chan buffer, walOptions.FlushQueueSize),
		bufferWriter:    newBufferWriter(),
		writeCloser:     writeCloser,
		flushCloser:     flushCloser,
		chanGroupCloser: chanGroupCloser,
		buffer: buffer{
			timestampMap: make(map[logSeriesID][]time.Time),
			valueMap:     make(map[logSeriesID][]byte),
			callbackMap:  make(map[logSeriesID][]func([]byte, time.Time, []byte, error)),
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
func (log *log) Write(seriesID []byte, timestamp time.Time, data []byte, callback func([]byte, time.Time, []byte, error)) {
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
	sort.Slice(segments, func(i, j int) bool { return segments[i].GetSegmentID() < segments[j].GetSegmentID() })
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

				bufferVolume += len(request.seriesID) + timestampVolumeLength + len(request.data)
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
			timer.Stop()
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
	log.flushChannel <- log.buffer
	if log.logger.Debug().Enabled() {
		log.logger.Debug().Msg("Send buffer to flush-channel. elements: " + strconv.Itoa(log.buffer.count))
	}

	log.newBuffer()
}

func (log *log) newBuffer() {
	log.buffer = buffer{
		timestampMap: make(map[logSeriesID][]time.Time),
		valueMap:     make(map[logSeriesID][]byte),
		callbackMap:  make(map[logSeriesID][]func([]byte, time.Time, []byte, error)),
		count:        0,
	}
}

func (log *log) flushBuffer(buffer buffer) error {
	if buffer.count == 0 {
		return nil
	}

	var err error
	if err = log.bufferWriter.Reset(); err != nil {
		return errors.Wrap(err, "Reset buffer writer error")
	}

	for seriesID, timestamps := range buffer.timestampMap {
		log.bufferWriter.ResetSeries()

		if err = log.bufferWriter.WriteSeriesID(seriesID); err != nil {
			return errors.Wrap(err, "Write seriesID error")
		}
		log.bufferWriter.WriteTimestamps(timestamps)
		log.bufferWriter.WriteData(buffer.valueMap[seriesID])
		if err = log.bufferWriter.AddSeries(); err != nil {
			return errors.Wrap(err, "Add series error")
		}
	}

	return log.writeWorkSegment(log.bufferWriter.Bytes())
}

func (log *log) writeWorkSegment(data []byte) error {
	log.rwMutex.RLock()
	defer log.rwMutex.RUnlock()

	// Write batch data to WAL segment file
	if _, err := log.workSegment.file.Write(data); err != nil {
		return errors.Wrap(err, "Write WAL segment file error, file: "+log.workSegment.path)
	}
	if log.options.SyncFlush {
		if err := log.workSegment.file.Sync(); err != nil {
			log.logger.Warn().Msg("Sync WAL segment file to disk failed, file: " + log.workSegment.path)
		}
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

func newBufferWriter() *bufferWriter {
	return &bufferWriter{
		buf:           bytes.NewBuffer([]byte{}),
		timestampsBuf: bytes.NewBuffer([]byte{}),
		dataBuf:       make([]byte, 128),
	}
}

func (w *bufferWriter) Reset() error {
	w.ResetSeries()
	w.buf.Reset()
	w.batchLen = 0

	// pre-placement padding
	err := w.writeBatchLength(0)
	return err
}

func (w *bufferWriter) ResetSeries() {
	w.timestampsBuf.Reset()
	w.dataLen = 0
	w.seriesID = nil
	w.seriesCount = 0
}

func (w *bufferWriter) AddSeries() error {
	seriesIDBytesLen := uint16(w.seriesID.len())
	timestampsBytesLen := uint16(w.timestampsBuf.Len())
	entryLen := seriesIDLength + uint64(seriesIDBytesLen) + seriesCountLength + timestampsBinaryLength + uint64(timestampsBytesLen) + uint64(w.dataLen)

	var err error
	if err = w.writeEntryLength(entryLen); err != nil {
		return err
	}
	if err = w.writeSeriesIDLength(seriesIDBytesLen); err != nil {
		return err
	}
	if err = w.writeSeriesID(w.seriesID); err != nil {
		return err
	}
	if err = w.writeSeriesCount(w.seriesCount); err != nil {
		return err
	}
	if err = w.writeTimestampsLength(timestampsBytesLen); err != nil {
		return err
	}
	if err = w.writeTimestamps(w.timestampsBuf.Bytes()); err != nil {
		return err
	}
	if err = w.writeData(w.dataBuf[:w.dataLen]); err != nil {
		return err
	}
	w.batchLen += entryLen
	return nil
}

func (w *bufferWriter) Bytes() []byte {
	batchBytes := w.buf.Bytes()
	batchLen := uint64(len(batchBytes)) - batchLength
	return w.rewriteBatchLength(batchBytes, batchLen)
}

func (w *bufferWriter) WriteSeriesID(seriesID logSeriesID) error {
	w.seriesID = &seriesID
	return nil
}

func (w *bufferWriter) WriteTimestamps(timestamps []time.Time) {
	timestampWriter := encoding.NewWriter()
	timestampEncoder := encoding.NewXOREncoder(timestampWriter)
	timestampWriter.Reset(w.timestampsBuf)
	for _, timestamp := range timestamps {
		timestampEncoder.Write(timeToUnixNano(timestamp))
	}
	timestampWriter.Flush()
	w.seriesCount = uint32(len(timestamps))
}

func (w *bufferWriter) WriteData(data []byte) {
	maxEncodedLen := snappy.MaxEncodedLen(len(data))
	dataBufLen := len(w.dataBuf)
	if dataBufLen < maxEncodedLen {
		newCapacity := (dataBufLen * 2) - (dataBufLen / 2)
		if newCapacity < maxEncodedLen {
			newCapacity = maxEncodedLen
		}
		w.dataBuf = make([]byte, newCapacity)
	}
	snappyData := snappy.Encode(w.dataBuf, data)
	w.dataLen = len(snappyData)
}

func (w *bufferWriter) writeBatchLength(data uint64) error {
	return writeUint64(w.buf, data)
}

func (w *bufferWriter) rewriteBatchLength(b []byte, batchLen uint64) []byte {
	_ = b[7] // early bounds check to guarantee safety of writes below
	b[0] = byte(batchLen)
	b[1] = byte(batchLen >> 8)
	b[2] = byte(batchLen >> 16)
	b[3] = byte(batchLen >> 24)
	b[4] = byte(batchLen >> 32)
	b[5] = byte(batchLen >> 40)
	b[6] = byte(batchLen >> 48)
	b[7] = byte(batchLen >> 56)
	return b
}

func (w *bufferWriter) writeEntryLength(data uint64) error {
	return writeUint64(w.buf, data)
}

func (w *bufferWriter) writeSeriesIDLength(data uint16) error {
	return writeUint16(w.buf, data)
}

func (w *bufferWriter) writeSeriesID(data *logSeriesID) error {
	_, err := w.buf.WriteString(data.string())
	return err
}

func (w *bufferWriter) writeSeriesCount(data uint32) error {
	return writeUint32(w.buf, data)
}

func (w *bufferWriter) writeTimestampsLength(data uint16) error {
	return writeUint16(w.buf, data)
}

func (w *bufferWriter) writeTimestamps(data []byte) error {
	_, err := w.buf.Write(data)
	return err
}

func (w *bufferWriter) writeData(data []byte) error {
	_, err := w.buf.Write(data)
	return err
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
	var seriesID []byte
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

func (segment *segment) parseSeriesID(data []byte) []byte {
	return newLogSeriesID(data).bytes()
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

func (logEntry *logEntry) GetSeriesID() []byte {
	return logEntry.seriesID
}

func (logEntry *logEntry) GetTimestamps() []time.Time {
	return logEntry.timestamps
}

func (logEntry *logEntry) GetValues() *list.List {
	return logEntry.values
}

func (buffer *buffer) write(request logRequest) {
	key := newLogSeriesID(request.seriesID)
	buffer.timestampMap[key] = append(buffer.timestampMap[key], request.timestamp)

	// Value item: binary-length(2-bytes) + binary data(n-bytes)
	binaryLen := uint16(len(request.data))
	buffer.valueMap[key] = append(buffer.valueMap[key], byte(binaryLen), byte(binaryLen>>8))
	buffer.valueMap[key] = append(buffer.valueMap[key], request.data...)

	buffer.callbackMap[key] = append(buffer.callbackMap[key], request.callback)
	buffer.count++
}

func (buffer *buffer) notifyRequests(err error) {
	var timestamps []time.Time
	var values []byte
	var valueItem []byte
	var valuePos int
	for seriesID, callbacks := range buffer.callbackMap {
		timestamps = buffer.timestampMap[seriesID]
		values = buffer.valueMap[seriesID]
		valuePos = 0
		for index, callback := range callbacks {
			valuePos, valueItem = readValuesBinary(values, valuePos, valuesBinaryLength)
			if callback != nil {
				buffer.runningCallback(func() {
					callback(seriesID.bytes(), timestamps[index], valueItem, err)
				})
			}
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
	return strconv.ParseUint(segmentName[3:19], 16, 64)
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

func writeUint16(buffer *bytes.Buffer, data uint16) error {
	var err error
	if err = buffer.WriteByte(byte(data)); err != nil {
		return err
	}
	if err = buffer.WriteByte(byte(data >> 8)); err != nil {
		return err
	}
	return err
}

func writeUint32(buffer *bytes.Buffer, data uint32) error {
	var err error
	if err = buffer.WriteByte(byte(data)); err != nil {
		return err
	}
	if err = buffer.WriteByte(byte(data >> 8)); err != nil {
		return err
	}
	if err = buffer.WriteByte(byte(data >> 16)); err != nil {
		return err
	}
	if err = buffer.WriteByte(byte(data >> 24)); err != nil {
		return err
	}
	return err
}

func writeUint64(buffer *bytes.Buffer, data uint64) error {
	var err error
	if err = buffer.WriteByte(byte(data)); err != nil {
		return err
	}
	if err = buffer.WriteByte(byte(data >> 8)); err != nil {
		return err
	}
	if err = buffer.WriteByte(byte(data >> 16)); err != nil {
		return err
	}
	if err = buffer.WriteByte(byte(data >> 24)); err != nil {
		return err
	}
	if err = buffer.WriteByte(byte(data >> 32)); err != nil {
		return err
	}
	if err = buffer.WriteByte(byte(data >> 40)); err != nil {
		return err
	}
	if err = buffer.WriteByte(byte(data >> 48)); err != nil {
		return err
	}
	if err = buffer.WriteByte(byte(data >> 56)); err != nil {
		return err
	}
	return err
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
