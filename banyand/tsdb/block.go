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
	"io"

	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type block struct {
	path string
	l    *logger.Logger

	store       kv.TimeSeriesStore
	treeIndex   kv.Store
	closableLst []io.Closer
	//revertedIndex kv.Store
}

type blockOpts struct {
	path          string
	compressLevel int
	valueSize     int
}

func newBlock(ctx context.Context, opts blockOpts) (b *block, err error) {
	b = &block{
		path: opts.path,
	}
	parentLogger := ctx.Value(logger.ContextKey)
	if parentLogger != nil {
		if pl, ok := parentLogger.(*logger.Logger); ok {
			b.l = pl.Named("block")
		}
	}
	if b.store, err = kv.OpenTimeSeriesStore(0, b.path+"/store", opts.compressLevel, opts.valueSize,
		kv.TSSWithLogger(b.l)); err != nil {
		return nil, err
	}
	if b.treeIndex, err = kv.OpenStore(0, b.path+"/t_index", kv.StoreWithLogger(b.l)); err != nil {
		return nil, err
	}
	b.closableLst = append(b.closableLst, b.store, b.treeIndex)
	return b, nil
}

func (b *block) close() {
	for _, closer := range b.closableLst {
		_ = closer.Close()
	}
}
