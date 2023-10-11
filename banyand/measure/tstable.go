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

package measure

import (
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/skl"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

const (
	defaultNumBufferShards  = 2
	defaultWriteConcurrency = 1000
	defaultWriteWal         = true
	plain                   = "tst"
	encoded                 = "encoded"
)

var _ tsdb.TSTable = (*tsTable)(nil)

type tsTable struct {
	encoderSST        kv.TimeSeriesStore
	sst               kv.TimeSeriesStore
	bufferSupplier    *tsdb.BufferSupplier
	l                 *logger.Logger
	encoderBuffer     *tsdb.Buffer
	buffer            *tsdb.Buffer
	position          common.Position
	id                string
	bufferSize        int64
	encoderBufferSize int64
	lock              sync.RWMutex
}

func (t *tsTable) SizeOnDisk() int64 {
	return t.encoderSST.SizeOnDisk()
}

func (t *tsTable) openBuffer() (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.encoderBuffer != nil {
		return nil
	}
	bufferSize := int(t.encoderBufferSize / defaultNumBufferShards)
	if t.encoderBuffer, err = t.bufferSupplier.Borrow(encoded, t.id, bufferSize, t.encoderFlush); err != nil {
		return fmt.Errorf("failed to create encoder buffer: %w", err)
	}
	bufferSize = int(t.bufferSize / defaultNumBufferShards)
	if t.buffer, err = t.bufferSupplier.Borrow(plain, t.id, bufferSize, t.flush); err != nil {
		return fmt.Errorf("failed to create buffer: %w", err)
	}
	return nil
}

func (t *tsTable) Close() (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	for _, b := range []io.Closer{t.encoderBuffer, t.buffer, t.sst, t.encoderSST} {
		if b != nil {
			err = multierr.Append(err, b.Close())
		}
	}
	for _, b := range []string{encoded, plain} {
		t.bufferSupplier.Return(b, t.id)
	}
	return err
}

func (t *tsTable) CollectStats() *badger.Statistics {
	mergedMap := &sync.Map{}
	for _, s := range []*badger.Statistics{t.encoderSST.CollectStats(), t.sst.CollectStats()} {
		if s != nil && s.TableBuilderSize != nil {
			s.TableBuilderSize.Range(func(key, value interface{}) bool {
				val := value.(*atomic.Int64)
				if val.Load() > 0 {
					mergedMap.Store(key, val)
				}
				return true
			})
		}
	}
	return &badger.Statistics{
		TableBuilderSize: mergedMap,
	}
}

func (t *tsTable) Get(key []byte, ts time.Time) ([]byte, error) {
	if t.toEncode(key) {
		if v, ok := t.encoderBuffer.Read(key, ts); ok {
			return v, nil
		}
		return t.encoderSST.Get(key, uint64(ts.UnixNano()))
	}
	if v, ok := t.buffer.Read(key, ts); ok {
		return v, nil
	}
	return t.sst.Get(key, uint64(ts.UnixNano()))
}

func (t *tsTable) Put(key []byte, val []byte, ts time.Time) error {
	t.lock.RLock()
	if t.encoderBuffer != nil {
		defer t.lock.RUnlock()
		return t.writeToBuffer(key, val, ts)
	}
	t.lock.RUnlock()
	if err := t.openBuffer(); err != nil {
		return err
	}
	return t.writeToBuffer(key, val, ts)
}

func (t *tsTable) writeToBuffer(key []byte, val []byte, ts time.Time) error {
	if t.toEncode(key) {
		return t.encoderBuffer.Write(key, val, ts)
	}
	return t.buffer.Write(key, val, ts)
}

func (t *tsTable) encoderFlush(shardIndex int, skl *skl.Skiplist) error {
	t.l.Info().Int("shard", shardIndex).Msg("flushing encoder buffer")
	return t.encoderSST.Handover(skl)
}

func (t *tsTable) flush(shardIndex int, skl *skl.Skiplist) error {
	t.l.Info().Int("shard", shardIndex).Msg("flushing buffer")
	return t.sst.Handover(skl)
}

func (t *tsTable) toEncode(key []byte) bool {
	fieldSpec, _, err := pbv1.DecodeFieldFlag(key)
	if err != nil {
		t.l.Err(err).Msg("failed to decode field flag")
	}
	return fieldSpec.EncodingMethod == databasev1.EncodingMethod_ENCODING_METHOD_GORILLA
}

var _ tsdb.TSTableFactory = (*tsTableFactory)(nil)

type tsTableFactory struct {
	encoderPool       encoding.SeriesEncoderPool
	decoderPool       encoding.SeriesDecoderPool
	bufferSize        int64
	encoderBufferSize int64
	plainChunkSize    int64
	encodingChunkSize int
	compressionMethod databasev1.CompressionMethod
}

func (ttf *tsTableFactory) NewTSTable(bufferSupplier *tsdb.BufferSupplier, root string, position common.Position, l *logger.Logger) (tsdb.TSTable, error) {
	encoderSST, err := kv.OpenTimeSeriesStore(
		path.Join(root, encoded),
		kv.TSSWithMemTableSize(ttf.bufferSize),
		kv.TSSWithLogger(l.Named(encoded)),
		kv.TSSWithEncoding(ttf.encoderPool, ttf.decoderPool, ttf.encodingChunkSize),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create time series table: %w", err)
	}
	sst, err := kv.OpenTimeSeriesStore(
		path.Join(root, plain),
		kv.TSSWithMemTableSize(ttf.bufferSize),
		kv.TSSWithLogger(l.Named(plain)),
		kv.TSSWithZSTDCompression(int(ttf.plainChunkSize)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create time series table: %w", err)
	}
	table := &tsTable{
		bufferSize:        ttf.bufferSize,
		encoderBufferSize: ttf.encoderBufferSize,
		l:                 l,
		position:          position,
		encoderSST:        encoderSST,
		sst:               sst,
		id:                strings.Join([]string{position.Segment, position.Block}, "-"),
		bufferSupplier:    bufferSupplier,
	}
	return table, nil
}
