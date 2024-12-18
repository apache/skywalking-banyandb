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

package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

func TestOpenTSDB(t *testing.T) {
	logger.Init(logger.Logging{
		Env:   "dev",
		Level: flags.LogLevel,
	})

	t.Run("create new TSDB", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		opts := TSDBOpts[*MockTSTable, any]{
			Location:        dir,
			SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
			TTL:             IntervalRule{Unit: DAY, Num: 3},
			ShardNum:        1,
			TSTableCreator:  MockTSTableCreator,
		}

		ctx := context.Background()
		mc := timestamp.NewMockClock()
		ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 00:00:00", time.Local)
		require.NoError(t, err)
		mc.Set(ts)
		ctx = timestamp.SetClock(ctx, mc)

		tsdb, err := OpenTSDB(ctx, opts)
		require.NoError(t, err)
		require.NotNil(t, tsdb)

		seg, err := tsdb.CreateSegmentIfNotExist(ts)
		require.NoError(t, err)
		defer seg.DecRef()

		db := tsdb.(*database[*MockTSTable, any])
		require.Equal(t, len(db.segmentController.segments()), 1)
		tsdb.Close()
	})

	t.Run("reopen existing TSDB", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		opts := TSDBOpts[*MockTSTable, any]{
			Location:        dir,
			SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
			TTL:             IntervalRule{Unit: DAY, Num: 3},
			ShardNum:        1,
			TSTableCreator:  MockTSTableCreator,
		}

		ctx := context.Background()
		mc := timestamp.NewMockClock()
		ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 00:00:00", time.Local)
		require.NoError(t, err)
		mc.Set(ts)
		ctx = timestamp.SetClock(ctx, mc)

		// Create new TSDB
		tsdb, err := OpenTSDB(ctx, opts)
		require.NoError(t, err)
		require.NotNil(t, tsdb)

		seg, err := tsdb.CreateSegmentIfNotExist(ts)
		require.NoError(t, err)
		seg.DecRef()

		db := tsdb.(*database[*MockTSTable, any])
		segs := db.segmentController.segments()
		require.Equal(t, len(segs), 1)
		for i := range segs {
			segs[i].DecRef()
		}
		tsdb.Close()

		// Reopen existing TSDB
		tsdb, err = OpenTSDB(ctx, opts)
		require.NoError(t, err)
		require.NotNil(t, tsdb)

		db = tsdb.(*database[*MockTSTable, any])
		segs = db.segmentController.segments()
		require.Equal(t, len(segs), 1)
		for i := range segs {
			segs[i].DecRef()
		}
		tsdb.Close()
	})

	t.Run("Changed options", func(t *testing.T) {
		dir, defFn := test.Space(require.New(t))
		defer defFn()

		opts := TSDBOpts[*MockTSTable, any]{
			Location:        dir,
			SegmentInterval: IntervalRule{Unit: DAY, Num: 1},
			TTL:             IntervalRule{Unit: DAY, Num: 3},
			ShardNum:        1,
			TSTableCreator:  MockTSTableCreator,
		}

		ctx := context.Background()
		mc := timestamp.NewMockClock()
		ts, err := time.ParseInLocation("2006-01-02 15:04:05", "2024-05-01 00:00:00", time.Local)
		require.NoError(t, err)
		mc.Set(ts)
		ctx = timestamp.SetClock(ctx, mc)

		// Create new TSDB
		tsdb, err := OpenTSDB(ctx, opts)
		require.NoError(t, err)
		require.NotNil(t, tsdb)

		seg, err := tsdb.CreateSegmentIfNotExist(ts)
		require.NoError(t, err)
		seg.DecRef()

		tsdb.UpdateOptions(&commonv1.ResourceOpts{
			ShardNum: 2,
			SegmentInterval: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  2,
			},
			Ttl: &commonv1.IntervalRule{
				Unit: commonv1.IntervalRule_UNIT_DAY,
				Num:  6,
			},
		})

		tsdb.Close()
	})
}
