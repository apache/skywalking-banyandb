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

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

type IndexDatabase interface {
	WriterBuilder() IndexWriterBuilder
	Seek(field index.Field) ([]GlobalItemID, error)
}

type IndexWriter interface {
	WriteLSMIndex(field index.Field) error
	WriteInvertedIndex(field index.Field) error
}

type IndexWriterBuilder interface {
	Time(ts time.Time) IndexWriterBuilder
	GlobalItemID(itemID GlobalItemID) IndexWriterBuilder
	Build() (IndexWriter, error)
}

type IndexSeekBuilder interface {
}

var _ IndexDatabase = (*indexDB)(nil)

type indexDB struct {
	shardID common.ShardID
	lst     []*segment
}

func (i *indexDB) Seek(term index.Field) ([]GlobalItemID, error) {
	var result []GlobalItemID
	err := i.lst[0].globalIndex.GetAll(term.Marshal(), func(rawBytes []byte) error {
		id := &GlobalItemID{}
		err := id.UnMarshal(rawBytes)
		if err != nil {
			return err
		}
		result = append(result, *id)
		return nil
	})
	return result, err
}

func (i *indexDB) WriterBuilder() IndexWriterBuilder {
	return newIndexWriterBuilder(i.lst)
}

func newIndexDatabase(_ context.Context, id common.ShardID, lst []*segment) (IndexDatabase, error) {
	return &indexDB{
		shardID: id,
		lst:     lst,
	}, nil
}

var _ IndexWriterBuilder = (*indexWriterBuilder)(nil)

type indexWriterBuilder struct {
	segments     []*segment
	ts           time.Time
	seg          *segment
	globalItemID *GlobalItemID
}

func (i *indexWriterBuilder) Time(ts time.Time) IndexWriterBuilder {
	i.ts = ts
	for _, s := range i.segments {
		if s.contains(ts) {
			i.seg = s
			break
		}
	}
	return i
}

func (i *indexWriterBuilder) GlobalItemID(itemID GlobalItemID) IndexWriterBuilder {
	i.globalItemID = &itemID
	return i
}

func (i *indexWriterBuilder) Build() (IndexWriter, error) {
	if i.seg == nil {
		return nil, errors.WithStack(ErrNoTime)
	}
	if i.globalItemID == nil {
		return nil, errors.WithStack(ErrNoVal)
	}
	return &indexWriter{
		seg:    i.seg,
		ts:     i.ts,
		itemID: i.globalItemID,
	}, nil
}

func newIndexWriterBuilder(segments []*segment) IndexWriterBuilder {
	return &indexWriterBuilder{
		segments: segments,
	}
}

var _ IndexWriter = (*indexWriter)(nil)

type indexWriter struct {
	seg    *segment
	ts     time.Time
	itemID *GlobalItemID
}

func (i *indexWriter) WriteLSMIndex(field index.Field) error {
	return i.seg.globalIndex.PutWithVersion(field.Marshal(), i.itemID.Marshal(), uint64(i.ts.UnixNano()))
}

func (i *indexWriter) WriteInvertedIndex(field index.Field) error {
	return i.seg.globalIndex.PutWithVersion(field.Marshal(), i.itemID.Marshal(), uint64(i.ts.UnixNano()))
}
