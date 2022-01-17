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
	"strconv"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type segment struct {
	id     uint16
	path   string
	suffix string

	lst         []*block
	globalIndex kv.Store
	sync.Mutex
	l              *logger.Logger
	reporterStopCh chan struct{}
	TimeRange
}

func (s *segment) Report() bucket.Channel {
	ch := make(bucket.Channel)
	interval := s.Duration() >> 4
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	go func() {
		defer close(ch)
		for {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			select {
			case <-ticker.C:
				status := bucket.Status{
					Capacity: int(s.End.UnixNano() - s.Start.UnixNano()),
					Volume:   int(time.Now().UnixNano() - s.Start.UnixNano()),
				}
				ch <- status
				if status.Volume >= status.Capacity {
					return
				}
			case <-s.reporterStopCh:
				return
			}
		}
	}()
	return ch
}

func openSegment(ctx context.Context, suffix, path string, intervalRule IntervalRule) (s *segment, err error) {
	startTime, err := intervalRule.Unit.Parse(suffix)
	if err != nil {
		return nil, err
	}
	suffixInteger, err := strconv.Atoi(suffix)
	if err != nil {
		return nil, err
	}
	id := uint16(intervalRule.Unit)<<12 | ((uint16(suffixInteger) << 4) >> 4)
	s = &segment{
		id:             id,
		path:           path,
		suffix:         suffix,
		l:              logger.Fetch(ctx, "segment"),
		reporterStopCh: make(chan struct{}),
		TimeRange:      NewTimeRange(startTime, intervalRule.NextTime(startTime), true, false),
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
			segID: s.id,
			path:  path,
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
	err = walkDir(path, blockPathPrefix, func(_, absolutePath string) error {
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
	s.globalIndex.Close()
	close(s.reporterStopCh)
}
