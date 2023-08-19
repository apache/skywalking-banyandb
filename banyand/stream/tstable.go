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

package stream

import (
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/skl"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

const (
	defaultNumBufferShards  = 2
	defaultWriteConcurrency = 1000
	id                      = "tst"
)

var _ tsdb.TSTable = (*tsTable)(nil)

type tsTable struct {
	sst kv.TimeSeriesStore
	*tsdb.BlockExpiryTracker
	l                *logger.Logger
	buffer           *tsdb.Buffer
	closeBufferTimer *time.Timer
	position         common.Position
	bufferSize       int64
	lock             sync.Mutex
}

func (t *tsTable) SizeOnDisk() int64 {
	return t.sst.SizeOnDisk()
}

func (t *tsTable) openBuffer() (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.buffer != nil {
		return nil
	}
	bufferSize := int(t.bufferSize / defaultNumBufferShards)
	if t.buffer, err = tsdb.NewBuffer(t.l, t.position, bufferSize,
		defaultWriteConcurrency, defaultNumBufferShards, t.flush); err != nil {
		return fmt.Errorf("failed to create buffer: %w", err)
	}
	end := t.EndTime()
	now := time.Now()
	closeAfter := end.Sub(now)
	if now.After(end) {
		closeAfter = t.BlockExpiryDuration()
	}
	t.closeBufferTimer = time.AfterFunc(closeAfter, func() {
		if t.l.Debug().Enabled() {
			t.l.Debug().Msg("closing buffer")
		}
		t.lock.Lock()
		defer t.lock.Unlock()
		if t.buffer == nil {
			return
		}
		if err := t.buffer.Close(); err != nil {
			t.l.Error().Err(err).Msg("close buffer error")
		}
		t.buffer = nil
	})
	return nil
}

func (t *tsTable) Close() (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.buffer != nil {
		err = multierr.Append(err, t.buffer.Close())
	}
	return multierr.Combine(err, t.sst.Close())
}

func (t *tsTable) CollectStats() *badger.Statistics {
	return t.sst.CollectStats()
}

func (t *tsTable) Get(key []byte, ts time.Time) ([]byte, error) {
	if v, ok := t.buffer.Read(key, ts); ok {
		return v, nil
	}
	return t.sst.Get(key, uint64(ts.UnixNano()))
}

func (t *tsTable) Put(key []byte, val []byte, ts time.Time) error {
	if t.buffer != nil {
		return t.buffer.Write(key, val, ts)
	}

	if err := t.openBuffer(); err != nil {
		return err
	}
	return t.buffer.Write(key, val, ts)
}

func (t *tsTable) flush(shardIndex int, skl *skl.Skiplist) error {
	t.l.Info().Int("shard", shardIndex).Msg("flushing buffer")
	return t.sst.Handover(skl)
}

var _ tsdb.TSTableFactory = (*tsTableFactory)(nil)

type tsTableFactory struct {
	bufferSize        int64
	compressionMethod databasev1.CompressionMethod
	chunkSize         int
}

func (ttf *tsTableFactory) NewTSTable(blockExpiryTracker tsdb.BlockExpiryTracker, root string, position common.Position, l *logger.Logger) (tsdb.TSTable, error) {
	sst, err := kv.OpenTimeSeriesStore(path.Join(root, id), kv.TSSWithMemTableSize(ttf.bufferSize), kv.TSSWithLogger(l.Named(id)),
		kv.TSSWithZSTDCompression(ttf.chunkSize))
	if err != nil {
		return nil, fmt.Errorf("failed to create time series table: %w", err)
	}
	table := &tsTable{
		bufferSize:         ttf.bufferSize,
		l:                  l,
		position:           position,
		sst:                sst,
		BlockExpiryTracker: &blockExpiryTracker,
	}
	if table.IsActive() {
		if err := table.openBuffer(); err != nil {
			return nil, fmt.Errorf("failed to open buffer: %w", err)
		}
	}
	return table, nil
}
