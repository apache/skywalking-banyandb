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

package tsdb

import (
	"context"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type segment struct {
	path string

	lst         []*block
	globalIndex kv.Store
	sync.Mutex
	l         *logger.Logger
	startTime time.Time
	endTime   time.Time
}

func (s *segment) contains(ts time.Time) bool {
	greaterAndEqualStart := s.startTime.Equal(ts) || s.startTime.Before(ts)
	if s.endTime.IsZero() {
		return greaterAndEqualStart
	}
	return greaterAndEqualStart && s.endTime.After(ts)
}

func newSegment(ctx context.Context, path string) (s *segment, err error) {
	s = &segment{
		path:      path,
		startTime: time.Now(),
	}
	parentLogger := ctx.Value(logger.ContextKey)
	if parentLogger != nil {
		if pl, ok := parentLogger.(*logger.Logger); ok {
			s.l = pl.Named("segment")
		}
	}
	indexPath, err := mkdir(globalIndexTemplate, path)
	if err != nil {
		return nil, err
	}
	if s.globalIndex, err = kv.OpenStore(0, indexPath, kv.StoreWithLogger(s.l)); err != nil {
		return nil, err
	}
	loadBlock := func(path string) error {
		var b *block
		if b, err = newBlock(context.WithValue(ctx, logger.ContextKey, s.l), blockOpts{
			path: path,
		}); err != nil {
			return err
		}
		{
			s.Lock()
			defer s.Unlock()
			s.lst = append(s.lst, b)
		}
		return nil
	}
	err = walkDir(path, blockPathPrefix, func(name, absolutePath string) error {
		return loadBlock(absolutePath)
	})
	if err != nil {
		return nil, err
	}
	if len(s.lst) < 1 {
		blockPath, err := mkdir(blockTemplate, path, time.Now().Format(blockFormat))
		if err != nil {
			return nil, err
		}
		err = loadBlock(blockPath)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *segment) close() {
	s.Lock()
	defer s.Unlock()
	for _, b := range s.lst {
		b.close()
	}
}
