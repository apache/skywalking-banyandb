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
	"bytes"
	"context"
	"io"
	"math"
	"sort"
	"sync"
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var (
	maxIntBytes  = convert.Uint64ToBytes(math.MaxUint64)
	zeroIntBytes = convert.Uint64ToBytes(0)
)

var AnyEntry = Entry(nil)

type Entry []byte

type Entity []Entry

func (e Entity) Marshal() []byte {
	data := make([][]byte, len(e))
	for i, entry := range e {
		data[i] = entry
	}
	return bytes.Join(data, nil)
}

func (e Entity) Prepend(entry Entry) Entity {
	d := e
	d = append(Entity{entry}, d...)
	return d
}

func (e Entity) Copy() Entity {
	a := make(Entity, len(e))
	copy(a, e)
	return a
}

func NewEntity(len int) Entity {
	e := make(Entity, len)
	for i := 0; i < len; i++ {
		e[i] = AnyEntry
	}
	return e
}

type Path struct {
	prefix   []byte
	seekKey  []byte
	mask     []byte
	template []byte
	isFull   bool
	offset   int
}

func NewPath(entries []Entry) Path {
	p := Path{
		seekKey:  make([]byte, 0),
		mask:     make([]byte, 0),
		template: make([]byte, 0),
	}

	var encounterAny bool
	for _, e := range entries {
		if e == nil {
			encounterAny = true
			p.mask = append(p.mask, zeroIntBytes...)
			p.template = append(p.template, zeroIntBytes...)
			continue
		}
		entry := Hash(e)
		if !encounterAny {
			p.offset += 8
		}
		p.mask = append(p.mask, maxIntBytes...)
		p.template = append(p.template, entry...)
	}
	if !encounterAny {
		p.isFull = true
	}
	p.extractPrefix()
	return p
}

func (p *Path) extractPrefix() {
	p.prefix = p.template[:p.offset]
	p.seekKey = p.seekKey[:0]
	p.seekKey = append(p.seekKey, p.prefix...)
	for i := 0; i < len(p.template)-p.offset; i++ {
		p.seekKey = append(p.seekKey, 0)
	}
}

func (p Path) Prepend(entry Entry) Path {
	e := Hash(entry)
	prependFunc := func(src []byte, entry []byte) []byte {
		dst := make([]byte, len(src)+len(entry))
		copy(dst, entry)
		copy(dst[len(entry):], src)
		return dst
	}
	p.template = prependFunc(p.template, e)
	p.offset += len(e)
	p.extractPrefix()
	p.mask = prependFunc(p.mask, maxIntBytes)
	return p
}

type SeriesDatabase interface {
	observability.Observable
	io.Closer
	GetByID(id common.SeriesID) (Series, error)
	Get(entity Entity) (Series, error)
	GetByHashKey(key []byte) (Series, error)
	List(path Path) (SeriesList, error)
}

type blockDatabase interface {
	shardID() common.ShardID
	span(ctx context.Context, timeRange timestamp.TimeRange) ([]BlockDelegate, error)
	create(ctx context.Context, ts time.Time) (BlockDelegate, error)
	block(ctx context.Context, id GlobalItemID) (BlockDelegate, error)
}

var (
	_ SeriesDatabase = (*seriesDB)(nil)
	_ blockDatabase  = (*seriesDB)(nil)
)

type seriesDB struct {
	sync.Mutex
	l *logger.Logger

	segCtrl        *segmentController
	seriesMetadata kv.Store
	sID            common.ShardID
}

func (s *seriesDB) GetByHashKey(key []byte) (Series, error) {
	seriesID, err := s.seriesMetadata.Get(key)
	if err != nil && err != kv.ErrKeyNotFound {
		return nil, err
	}
	if err == nil {
		return newSeries(s.context(), bytesToSeriesID(seriesID), s), nil
	}
	s.Lock()
	defer s.Unlock()
	seriesID = Hash(key)
	err = s.seriesMetadata.Put(key, seriesID)
	if err != nil {
		return nil, err
	}
	return newSeries(s.context(), bytesToSeriesID(seriesID), s), nil
}

func (s *seriesDB) GetByID(id common.SeriesID) (Series, error) {
	return newSeries(s.context(), id, s), nil
}

func (s *seriesDB) block(ctx context.Context, id GlobalItemID) (BlockDelegate, error) {
	seg := s.segCtrl.get(id.segID)
	if seg == nil {
		return nil, nil
	}
	return seg.blockController.get(ctx, id.blockID)
}

func (s *seriesDB) shardID() common.ShardID {
	return s.sID
}

func (s *seriesDB) Get(entity Entity) (Series, error) {
	key := HashEntity(entity)
	return s.GetByHashKey(key)
}

func (s *seriesDB) List(path Path) (SeriesList, error) {
	if path.isFull {
		id, err := s.seriesMetadata.Get(path.prefix)
		if err != nil && err != kv.ErrKeyNotFound {
			return nil, err
		}
		if err == nil {
			seriesID := bytesToSeriesID(id)
			s.l.Debug().
				Hex("path", path.prefix).
				Uint64("series_id", uint64(seriesID)).
				Msg("got a series with a full path")
			return []Series{newSeries(s.context(), seriesID, s)}, nil
		}
		s.l.Debug().Hex("path", path.prefix).Msg("doesn't get any series")
		return nil, nil
	}
	result := make([]Series, 0)
	var err error
	errScan := s.seriesMetadata.Scan(path.prefix, path.seekKey, kv.DefaultScanOpts, func(_ int, key []byte, getVal func() ([]byte, error)) error {
		comparableKey := make([]byte, len(key))
		for i, b := range key {
			comparableKey[i] = path.mask[i] & b
		}
		if bytes.Equal(path.template, comparableKey) {
			id, errGetVal := getVal()
			if errGetVal != nil {
				err = multierr.Append(err, errGetVal)
				return nil
			}
			seriesID := bytesToSeriesID(id)
			s.l.Debug().
				Hex("path", path.prefix).
				Uint64("series_id", uint64(seriesID)).
				Msg("got a series")
			result = append(result, newSeries(s.context(), seriesID, s))
		}
		return nil
	})
	if errScan != nil {
		return nil, errScan
	}
	return result, err
}

func (s *seriesDB) span(ctx context.Context, timeRange timestamp.TimeRange) ([]BlockDelegate, error) {
	result := make([]BlockDelegate, 0)
	for _, s := range s.segCtrl.span(timeRange) {
		dd, err := s.blockController.span(ctx, timeRange)
		if err != nil {
			return nil, err
		}
		if dd == nil {
			continue
		}
		result = append(result, dd...)
	}
	return result, nil
}

func (s *seriesDB) create(ctx context.Context, ts time.Time) (BlockDelegate, error) {
	s.Lock()
	defer s.Unlock()
	timeRange := timestamp.NewInclusiveTimeRange(ts, ts)
	ss := s.segCtrl.span(timeRange)
	if len(ss) > 0 {
		s := ss[0]
		dd, err := s.blockController.span(ctx, timeRange)
		if err != nil {
			return nil, err
		}
		if len(dd) > 0 {
			return dd[0], nil
		}
		block, err := s.blockController.create(timeRange.Start)
		if err != nil {
			return nil, err
		}
		return block.delegate(ctx)
	}
	seg, err := s.segCtrl.create(s.segCtrl.Format(timeRange.Start), false)
	if err != nil {
		return nil, err
	}
	block, err := seg.blockController.create(timeRange.Start)
	if err != nil {
		return nil, err
	}
	return block.delegate(ctx)
}

func (s *seriesDB) context() context.Context {
	return context.WithValue(context.Background(), logger.ContextKey, s.l)
}

func (s *seriesDB) Stats() observability.Statistics {
	return s.seriesMetadata.Stats()
}

func (s *seriesDB) Close() error {
	return s.seriesMetadata.Close()
}

func newSeriesDataBase(ctx context.Context, shardID common.ShardID, path string, segCtrl *segmentController) (SeriesDatabase, error) {
	sdb := &seriesDB{
		sID:     shardID,
		segCtrl: segCtrl,
		l:       logger.Fetch(ctx, "series_database"),
	}
	o := ctx.Value(optionsKey)
	var memSize int64
	if o != nil {
		options := o.(DatabaseOpts)
		if options.SeriesMemSize > 1 {
			memSize = options.SeriesMemSize
		} else {
			memSize = defaultKVMemorySize
		}
	} else {
		memSize = defaultKVMemorySize
	}
	var err error
	sdb.seriesMetadata, err = kv.OpenStore(0, path+"/md",
		kv.StoreWithNamedLogger("metadata", sdb.l),
		kv.StoreWithMemTableSize(memSize),
	)
	if err != nil {
		return nil, err
	}
	return sdb, nil
}

// HashEntity runs hash function (e.g. with xxhash algorithm) on each segment of the Entity,
// and concatenates all uint64 in byte array. So the return length of the byte array will be
// 8 (every uint64 has 8 bytes) * length of the input entity.
func HashEntity(entity Entity) []byte {
	result := make([]byte, 0, len(entity)*8)
	for _, entry := range entity {
		result = append(result, Hash(entry)...)
	}
	return result
}

func SeriesID(entity Entity) common.SeriesID {
	return common.SeriesID(convert.Hash(HashEntity(entity)))
}

func Hash(entry []byte) []byte {
	return convert.Uint64ToBytes(convert.Hash(entry))
}

func bytesToSeriesID(data []byte) common.SeriesID {
	return common.SeriesID(convert.BytesToUint64(data))
}

type SeriesList []Series

func (a SeriesList) Len() int {
	return len(a)
}

func (a SeriesList) Less(i, j int) bool {
	return a[i].ID() < a[j].ID()
}

func (a SeriesList) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a SeriesList) Merge(other SeriesList) SeriesList {
	if len(other) == 0 {
		return a
	}
	sort.Sort(other)
	if len(a) == 0 {
		return other
	}
	final := SeriesList{}
	i := 0
	j := 0
	for i < len(a) && j < len(other) {
		if a[i].ID() < other[j].ID() {
			final = append(final, a[i])
			i++
		} else {
			// deduplication
			if a[i].ID() == other[j].ID() {
				i++
			}
			final = append(final, other[j])
			j++

		}
	}
	for ; i < len(a); i++ {
		final = append(final, a[i])
	}
	for ; j < len(other); j++ {
		final = append(final, other[j])
	}
	return final
}
