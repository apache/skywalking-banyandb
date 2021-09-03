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
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
)

type IndexDatabase interface {
	IndexWriterBuilder() IndexWriterBuilder
}

type IndexWriter interface {
	WriteLSMIndex(name string, val []byte) error
	WriteInvertedIndex(name string, val []byte) error
}

type IndexWriterBuilder interface {
	Time(ts time.Time) IndexWriterBuilder
	GlobalItemID(itemID ItemID) IndexWriterBuilder
	Build() (IndexWriter, error)
}

var _ IndexDatabase = (*indexDB)(nil)

type indexDB struct {
	shardID common.ShardID
	lst     []*segment
}

func (i *indexDB) IndexWriterBuilder() IndexWriterBuilder {
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
	globalItemID *ItemID
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

func (i *indexWriterBuilder) GlobalItemID(itemID ItemID) IndexWriterBuilder {
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
	itemID *ItemID
}

func (i *indexWriter) WriteLSMIndex(name string, val []byte) error {
	return i.seg.globalIndex.Put(bytes.Join([][]byte{[]byte(name), val}, nil), i.itemID.Marshal())
}

func (i *indexWriter) WriteInvertedIndex(name string, val []byte) error {
	return i.seg.globalIndex.Put(bytes.Join([][]byte{[]byte(name), val}, nil), i.itemID.Marshal())
}
