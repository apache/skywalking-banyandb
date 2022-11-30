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
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

type IndexDatabase interface {
	WriterBuilder() IndexWriterBuilder
	Seek(field index.Field) ([]GlobalItemID, error)
}

type IndexWriter interface {
	WriteLSMIndex(field []index.Field) error
	WriteInvertedIndex(field []index.Field) error
}

type IndexWriterBuilder interface {
	Scope(scope Entry) IndexWriterBuilder
	Time(ts time.Time) IndexWriterBuilder
	GlobalItemID(itemID GlobalItemID) IndexWriterBuilder
	Build() (IndexWriter, error)
}

type IndexSeekBuilder interface{}

var _ IndexDatabase = (*indexDB)(nil)

type indexDB struct {
	shardID common.ShardID
	segCtrl *segmentController
}

func (i *indexDB) Seek(field index.Field) ([]GlobalItemID, error) {
	result := make([]GlobalItemID, 0)
	f, err := field.Marshal()
	if err != nil {
		return nil, err
	}
	for _, s := range i.segCtrl.segments() {
		err = s.globalIndex.GetAll(f, func(rawBytes []byte) error {
			id := &GlobalItemID{}
			errUnMarshal := id.UnMarshal(rawBytes)
			if errUnMarshal != nil {
				return errUnMarshal
			}
			result = append(result, *id)
			return nil
		})
		if err == kv.ErrKeyNotFound {
			return result, nil
		}
	}
	return result, err
}

func (i *indexDB) WriterBuilder() IndexWriterBuilder {
	return newIndexWriterBuilder(i.segCtrl)
}

func newIndexDatabase(_ context.Context, id common.ShardID, segCtrl *segmentController) (IndexDatabase, error) {
	return &indexDB{
		shardID: id,
		segCtrl: segCtrl,
	}, nil
}

var _ IndexWriterBuilder = (*indexWriterBuilder)(nil)

type indexWriterBuilder struct {
	scope        Entry
	segCtrl      *segmentController
	ts           time.Time
	globalItemID *GlobalItemID
}

func (i *indexWriterBuilder) Scope(scope Entry) IndexWriterBuilder {
	i.scope = scope
	return i
}

func (i *indexWriterBuilder) Time(ts time.Time) IndexWriterBuilder {
	i.ts = ts
	return i
}

func (i *indexWriterBuilder) GlobalItemID(itemID GlobalItemID) IndexWriterBuilder {
	i.globalItemID = &itemID
	return i
}

func (i *indexWriterBuilder) Build() (IndexWriter, error) {
	seg, err := i.segCtrl.create(i.ts, false)
	if err != nil {
		return nil, err
	}
	if i.globalItemID == nil {
		return nil, errors.WithStack(ErrNoVal)
	}
	return &indexWriter{
		scope:  i.scope,
		seg:    seg,
		ts:     i.ts,
		itemID: i.globalItemID,
	}, nil
}

func newIndexWriterBuilder(segCtrl *segmentController) IndexWriterBuilder {
	return &indexWriterBuilder{
		segCtrl: segCtrl,
	}
}

var _ IndexWriter = (*indexWriter)(nil)

type indexWriter struct {
	scope  Entry
	seg    *segment
	ts     time.Time
	itemID *GlobalItemID
}

func (i *indexWriter) WriteLSMIndex(fields []index.Field) (err error) {
	for _, field := range fields {
		if i.scope != nil {
			field.Key.SeriesID = GlobalSeriesID(i.scope)
		}
		key, errInternal := field.Marshal()
		if errInternal != nil {
			err = multierr.Append(err, errInternal)
			continue
		}
		err = multierr.Append(err, i.seg.globalIndex.PutWithVersion(key, i.itemID.Marshal(), uint64(i.ts.UnixNano())))
	}
	return err
}

func (i *indexWriter) WriteInvertedIndex(fields []index.Field) (err error) {
	for _, field := range fields {
		if i.scope != nil {
			field.Key.SeriesID = GlobalSeriesID(i.scope)
		}
		key, errInternal := field.Marshal()
		if errInternal != nil {
			err = multierr.Append(err, errInternal)
			continue
		}
		err = multierr.Append(err, i.seg.globalIndex.PutWithVersion(key, i.itemID.Marshal(), uint64(i.ts.UnixNano())))
	}
	return err
}

func GlobalSeriesID(scope Entry) common.SeriesID {
	return common.SeriesID(convert.Hash(scope))
}
