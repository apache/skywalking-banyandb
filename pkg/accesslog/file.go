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
	"fmt"
	"os"
	"path"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type fileLog struct {
	file          *os.File
	validRequests chan proto.Message
	closer        *run.Closer
}

// NewFileLog creates a new file access log.
func NewFileLog(root, template string, interval time.Duration, log *logger.Logger) (Log, error) {
	var file *os.File
	var err error
	if file, err = createFile(root, template); err != nil {
		return nil, err
	}
	f := &fileLog{
		validRequests: make(chan proto.Message, 100),
		file:          file,
		closer:        run.NewCloser(1),
	}
	go func() {
		defer f.closer.Done()
		defer close(f.validRequests)

		ticker := time.NewTicker(interval)

		for {
			select {
			case <-f.closer.CloseNotify():
				if f.file != nil {
					f.file.Close()
				}
				ticker.Stop()
				return
			case <-ticker.C:
				if f.file != nil {
					f.file.Close()
				}
				if f.file, err = createFile(root, template); err != nil {
					log.Error().Err(err).Msg("failed to open file for writing")
					continue
				}
			case req := <-f.validRequests:
				if f.file != nil {
					data, err := protojson.Marshal(req)
					if err != nil {
						log.Error().Err(err).Msg("failed to marshal request")
						continue
					}
					if _, err := f.file.WriteString(string(data) + "\n"); err != nil {
						log.Error().Err(err).Msg("failed to write request to file")
					}
				}
			}
		}
	}()
	return f, nil
}

func (f *fileLog) Write(req proto.Message) error {
	if f == nil {
		return nil
	}
	select {
	case f.validRequests <- req:
	default:
		return fmt.Errorf("access log is full")
	}
	return nil
}

func (f *fileLog) Close() error {
	if f == nil {
		return nil
	}
	f.closer.CloseThenWait()
	return nil
}

func createFile(root string, template string) (*os.File, error) {
	fileName := path.Join(root, fmt.Sprintf(template, time.Now().Format("20060102_150405")))
	return os.Create(fileName)
}
