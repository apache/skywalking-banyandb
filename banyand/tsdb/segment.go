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
	"errors"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/tsdb/bucket"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var errEndOfSegment = errors.New("reached the end of the segment")

type segment struct {
	globalIndex kv.Store
	bucket.Reporter
	l                   *logger.Logger
	blockController     *blockController
	blockManageStrategy *bucket.Strategy
	timestamp.TimeRange
	path      string
	suffix    string
	closeOnce sync.Once
	id        SectionID
}

func openSegment(ctx context.Context, startTime, endTime time.Time, path, suffix string,
	segmentSize, blockSize IntervalRule, blockQueue bucket.Queue, scheduler *timestamp.Scheduler,
) (s *segment, err error) {
	suffixInteger, err := strconv.Atoi(suffix)
	if err != nil {
		return nil, err
	}
	id := GenerateInternalID(segmentSize.Unit, suffixInteger)
	timeRange := timestamp.NewSectionTimeRange(startTime, endTime)
	s = &segment{
		id:        id,
		path:      path,
		suffix:    suffix,
		TimeRange: timeRange,
	}
	l := logger.Fetch(ctx, s.String())
	s.l = l
	segCtx := context.WithValue(ctx, logger.ContextKey, l)
	clock, segCtx := timestamp.GetClock(segCtx)
	s.blockController = newBlockController(segCtx, id, suffix, path, timeRange, blockSize, l, blockQueue, scheduler)
	s.Reporter = bucket.NewTimeBasedReporter(s.String(), timeRange, clock, scheduler)
	err = s.blockController.open()
	if err != nil {
		return nil, err
	}

	indexPath, err := mkdir(globalIndexTemplate, path)
	if err != nil {
		return nil, err
	}
	o := ctx.Value(optionsKey)
	if o != nil {
		options := o.(DatabaseOpts)
		if options.EnableGlobalIndex {
			memSize := options.GlobalIndexMemSize
			if memSize == 0 {
				memSize = defaultKVMemorySize
			}
			if s.globalIndex, err = kv.OpenStore(
				indexPath,
				kv.StoreWithLogger(s.l),
				kv.StoreWithMemTableSize(memSize),
			); err != nil {
				return nil, err
			}
		}
	}
	if !s.End.After(clock.Now()) {
		return
	}
	s.blockManageStrategy, err = bucket.NewStrategy(s.blockController, bucket.WithLogger(s.l))
	if err != nil {
		return nil, err
	}
	s.blockManageStrategy.Run()
	return s, nil
}

func (s *segment) close(ctx context.Context) (err error) {
	s.closeOnce.Do(func() {
		if err = s.blockController.close(ctx); err != nil {
			return
		}
		if s.globalIndex != nil {
			if err = s.globalIndex.Close(); err != nil {
				return
			}
		}
		if s.blockManageStrategy != nil {
			s.blockManageStrategy.Close()
		}
	})
	return nil
}

func (s *segment) closeBlock(ctx context.Context, id SectionID) error {
	return s.blockController.closeBlock(ctx, id)
}

func (s *segment) delete(ctx context.Context) error {
	if err := s.close(ctx); err != nil {
		return err
	}
	return os.RemoveAll(s.path)
}

func (s *segment) String() string {
	return "SegID-" + s.suffix
}

func (s *segment) Stats() observability.Statistics {
	if s.globalIndex == nil {
		return observability.Statistics{}
	}
	return s.globalIndex.Stats()
}
