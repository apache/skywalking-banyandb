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
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

type WriterBuilder interface {
	Family(name string, val []byte) WriterBuilder
	Time(ts time.Time) WriterBuilder
	Val(val []byte) WriterBuilder
	Build() (Writer, error)
}

type Writer interface {
	IndexWriter
	Write() (GlobalItemID, error)
	ItemID() GlobalItemID
}

var _ WriterBuilder = (*writerBuilder)(nil)

type writerBuilder struct {
	series *seriesSpan
	block  blockDelegate
	values []struct {
		family []byte
		val    []byte
	}
	ts            time.Time
	seriesIDBytes []byte
}

func (w *writerBuilder) Family(name string, val []byte) WriterBuilder {
	w.values = append(w.values, struct {
		family []byte
		val    []byte
	}{family: []byte(name), val: val})
	return w
}

func (w *writerBuilder) Time(ts time.Time) WriterBuilder {
	w.ts = ts
	for _, b := range w.series.blocks {
		if b.contains(ts) {
			w.block = b
			break
		}
	}
	return w
}

func (w *writerBuilder) Val(val []byte) WriterBuilder {
	w.values = append(w.values, struct {
		family []byte
		val    []byte
	}{val: val})
	return w
}

var ErrNoTime = errors.New("no time specified")
var ErrNoVal = errors.New("no value specified")

func (w *writerBuilder) Build() (Writer, error) {
	if w.block == nil {
		return nil, errors.WithStack(ErrNoTime)
	}
	if len(w.values) < 1 {
		return nil, errors.WithStack(ErrNoVal)
	}
	segID, blockID := w.block.identity()
	return &writer{
		block: w.block,
		ts:    w.ts,
		itemID: &GlobalItemID{
			ShardID:  w.series.shardID,
			segID:    segID,
			blockID:  blockID,
			SeriesID: w.series.seriesID,
			ID:       common.ItemID(uint64(w.ts.UnixNano())),
		},
		columns: w.values,
	}, nil
}

func newWriterBuilder(seriesSpan *seriesSpan) WriterBuilder {
	return &writerBuilder{
		series:        seriesSpan,
		seriesIDBytes: seriesSpan.seriesID.Marshal(),
	}
}

var _ Writer = (*writer)(nil)

type writer struct {
	block   blockDelegate
	ts      time.Time
	columns []struct {
		family []byte
		val    []byte
	}
	itemID *GlobalItemID
}

func (w *writer) ItemID() GlobalItemID {
	return *w.itemID
}

func (w *writer) WriteLSMIndex(field index.Field) error {
	t := index.FieldKey{
		SeriesID:  w.itemID.SeriesID,
		IndexRule: string(field.Key),
	}
	field.Key = t.Marshal()
	return w.block.writeLSMIndex(field, w.itemID.ID)
}

func (w *writer) WriteInvertedIndex(field index.Field) error {
	t := index.FieldKey{
		SeriesID:  w.itemID.SeriesID,
		IndexRule: string(field.Key),
	}
	field.Key = t.Marshal()
	return w.block.writeInvertedIndex(field, w.itemID.ID)
}

type dataBucket struct {
	seriesID common.SeriesID
	family   []byte
}

func (d dataBucket) marshal() []byte {
	return bytes.Join([][]byte{
		d.seriesID.Marshal(),
		hash(d.family),
	}, nil)
}

func (w *writer) Write() (GlobalItemID, error) {
	id := w.ItemID()
	for _, c := range w.columns {
		err := w.block.write(dataBucket{
			seriesID: w.itemID.SeriesID,
			family:   c.family,
		}.marshal(),
			c.val, w.ts)
		if err != nil {
			return id, err
		}
	}
	return id, w.block.writePrimaryIndex(index.Field{
		Key: index.FieldKey{
			SeriesID: id.SeriesID,
		}.Marshal(),
		Term: convert.Int64ToBytes(w.ts.UnixNano()),
	}, id.ID)
}
