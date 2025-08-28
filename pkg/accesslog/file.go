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

package accesslog

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

const (
	// DefaultBatchSize is the default number of requests to batch before writing to the file.
	DefaultBatchSize = 100
	// DefaultFlushInterval is the default interval at which to flush the batch to the file.
	DefaultFlushInterval = 1 * time.Second
)

type fileLog struct {
	file          *os.File // Single file for all goroutines
	validRequests chan proto.Message
	closer        *run.Closer
	sampled       bool
}

// NewFileLog creates a new file access log.
// sampled: if true (default), requests may be dropped when the channel is full.
// If false, requests are never dropped but use buffered channel to prevent blocking.
func NewFileLog(root, template string, interval time.Duration, log *logger.Logger, sampled bool) (Log, error) {
	var validRequests chan proto.Message
	if sampled {
		validRequests = make(chan proto.Message, 100)
	} else {
		// For non-sampled mode, use buffered channel to prevent blocking on writes
		validRequests = make(chan proto.Message, 1000) // Buffer to handle burst writes
	}

	f := &fileLog{
		validRequests: validRequests,
		closer:        run.NewCloser(1),
		sampled:       sampled,
	}

	// Create single file for both sampled and non-sampled modes
	var err error
	if f.file, err = createFile(root, template); err != nil {
		return nil, err
	}

	go startConsumer(f, root, template, interval, log)
	return f, nil
}

func (f *fileLog) Write(req proto.Message) error {
	if f == nil {
		return nil
	}

	if f.sampled {
		// Sampled mode: may drop requests if channel is full
		select {
		case f.validRequests <- req:
		default:
			return fmt.Errorf("access log is full")
		}
	} else {
		// Non-sampled mode: never drop requests, block until buffer has space
		f.validRequests <- req
	}
	return nil
}

func (f *fileLog) Close() error {
	if f == nil {
		return nil
	}
	f.closer.CloseThenWait()

	if f.file != nil {
		f.file.Close()
		f.file = nil
	}

	// Close the channel after all consumers are done
	close(f.validRequests)
	return nil
}

// startConsumer starts a consumer goroutine that handles file rotation and request processing.
func startConsumer(f *fileLog, root, template string, interval time.Duration, log *logger.Logger) {
	defer f.closer.Done()

	rotationTicker := time.NewTicker(interval)
	defer rotationTicker.Stop()

	flushTicker := time.NewTicker(DefaultFlushInterval)
	defer flushTicker.Stop()

	batch := make([]proto.Message, 0, DefaultBatchSize)

	for {
		select {
		case <-f.closer.CloseNotify():
			// Before closing, flush any remaining requests in the batch.
			flushBatch(f.file, batch, log)
			return
		case <-rotationTicker.C:
			flushBatch(f.file, batch, log)
			batch = batch[:0]
			rotateFile(root, template, f, log)
		case <-flushTicker.C:
			if len(batch) > 0 {
				flushBatch(f.file, batch, log)
				batch = batch[:0]
			}
		case req, ok := <-f.validRequests:
			if !ok {
				// Channel closed, flush any remaining requests and exit.
				flushBatch(f.file, batch, log)
				return
			}
			batch = append(batch, req)
			if len(batch) >= DefaultBatchSize {
				flushBatch(f.file, batch, log)
				batch = batch[:0]
			}
		}
	}
}

// flushBatch marshals and writes a batch of requests to the specified file.
func flushBatch(file *os.File, batch []proto.Message, log *logger.Logger) {
	if file == nil || len(batch) == 0 {
		return
	}

	var buffer bytes.Buffer
	for _, req := range batch {
		data, err := protojson.Marshal(req)
		if err != nil {
			log.Error().Err(err).Msg("failed to marshal request")
			continue
		}
		buffer.Write(data)
		buffer.WriteString("\n")
	}

	if _, err := file.Write(buffer.Bytes()); err != nil {
		log.Error().Err(err).Msg("failed to write requests to file")
	}
}

// rotateFile handles the common logic for file rotation with single file.
func rotateFile(root, template string, f *fileLog, log *logger.Logger) {
	// Close current file
	if f.file != nil {
		f.file.Close()
		f.file = nil
	}

	// Create new file
	newFile, err := createFile(root, template)
	if err != nil {
		log.Error().Err(err).Msg("failed to open file for writing")
		return
	}
	f.file = newFile
}

func createFile(root string, template string) (*os.File, error) {
	timestamp := time.Now().Format("20060102_150405")
	fileName := path.Join(root, fmt.Sprintf(template, timestamp))
	return os.Create(fileName)
}
