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
	"sync"
	"time"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var maxIntBytes = convert.Uint64ToBytes(math.MaxUint64)
var zeroIntBytes = convert.Uint64ToBytes(0)

var AnyEntry = Entry(nil)

type Entry []byte

type Entity []Entry

type Path struct {
	prefix   []byte
	mask     []byte
	template []byte
	isFull   bool
}

func NewPath(entries []Entry) Path {
	p := Path{
		mask:     make([]byte, 0),
		template: make([]byte, 0),
	}

	var offset int
	var encounterAny bool
	for _, e := range entries {
		if e == nil {
			encounterAny = true
			p.mask = append(p.mask, zeroIntBytes...)
			p.template = append(p.template, zeroIntBytes...)
			continue
		}
		entry := hash(e)
		if !encounterAny {
			offset += 8
		}
		p.mask = append(p.mask, maxIntBytes...)
		p.template = append(p.template, entry...)
	}
	if !encounterAny {
		p.isFull = true
	}
	p.prefix = p.template[:offset]
	return p
}

type SeriesDatabase interface {
	io.Closer
	Create(entity Entity) error
	List(path Path) (SeriesList, error)
}

var _ SeriesDatabase = (*seriesDB)(nil)

type seriesDB struct {
	sync.Mutex
	l *logger.Logger

	lst            []*segment
	seriesMetadata kv.Store
}

func (s *seriesDB) Close() error {
	for _, seg := range s.lst {
		seg.close()
	}
	return s.seriesMetadata.Close()
}

func newSeriesDataBase(ctx context.Context, path string) (SeriesDatabase, error) {
	sdb := &seriesDB{}
	parentLogger := ctx.Value(logger.ContextKey)
	if parentLogger == nil {
		return nil, logger.ErrNoLoggerInContext
	}
	if pl, ok := parentLogger.(*logger.Logger); ok {
		sdb.l = pl.Named("series")
	}
	var err error
	sdb.seriesMetadata, err = kv.OpenStore(0, path+"/md", kv.StoreWithNamedLogger("metadata", sdb.l))
	if err != nil {
		return nil, err
	}
	segPath, err := mkdir(segTemplate, path, time.Now().Format(segFormat))
	if err != nil {
		return nil, err
	}
	seg, err := newSegment(ctx, segPath)
	if err != nil {
		return nil, err
	}
	{
		sdb.Lock()
		defer sdb.Unlock()
		sdb.lst = append(sdb.lst, seg)
	}
	return sdb, nil
}

func (s *seriesDB) Create(entity Entity) error {
	key := hashEntity(entity)
	_, err := s.seriesMetadata.Get(key)
	if err != nil && err != kv.ErrKeyNotFound {
		return err
	}
	if err == nil {
		return nil
	}
	s.Lock()
	defer s.Unlock()
	err = s.seriesMetadata.Put(key, hash(key))
	if err != nil {
		return err
	}
	return nil
}

func (s *seriesDB) List(path Path) (SeriesList, error) {
	if path.isFull {
		id, err := s.seriesMetadata.Get(path.prefix)
		if err != nil && err != kv.ErrKeyNotFound {
			return nil, err
		}
		if err == nil {
			return []Series{newSeries(common.SeriesID(convert.BytesToUint64(id)))}, nil
		}
		return nil, nil
	}
	result := make([]Series, 0)
	var err error
	errScan := s.seriesMetadata.Scan(path.prefix, kv.DefaultScanOpts, func(_ int, key []byte, getVal func() ([]byte, error)) error {
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
			result = append(result, newSeries(common.SeriesID(convert.BytesToUint64(id))))
		}
		return nil
	})
	if errScan != nil {
		return nil, errScan
	}
	return result, err
}

func hashEntity(entity Entity) []byte {
	result := make(Entry, 0, len(entity)*8)
	for _, entry := range entity {
		result = append(result, hash(entry)...)
	}
	return result
}

func hash(entry []byte) []byte {
	return convert.Uint64ToBytes(convert.Hash(entry))
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
