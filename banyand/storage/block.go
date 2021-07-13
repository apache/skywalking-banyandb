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
	"fmt"

	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type block struct {
	path   string
	plugin Plugin

	l *logger.Logger

	stores   map[string]kv.Store
	tsStores map[string]kv.TimeSeriesStore

	shardID int
}

func newBlock(shardID int, path string, plugin Plugin) (*block, error) {
	l := logger.GetLogger("block")
	return &block{
		shardID:  shardID,
		path:     path,
		plugin:   plugin,
		l:        l,
		stores:   make(map[string]kv.Store),
		tsStores: make(map[string]kv.TimeSeriesStore),
	}, nil
}

func (b *block) init() (err error) {
	meta := b.plugin.Meta()
	kvDefines := meta.KVSpecs
	if kvDefines == nil || len(kvDefines) < 1 {
		return nil
	}
	if err = b.createKV(kvDefines); err != nil {
		return err
	}
	return nil
}

func (b *block) createKV(defines []KVSpec) (err error) {
	for _, define := range defines {
		storeID := define.Name
		path := fmt.Sprintf("%s/%s", b.path, storeID)
		b.l.Info().Str("path", path).Uint8("type", uint8(define.Type)).Msg("open kv store")
		switch define.Type {
		case KVTypeNormal:
			var s kv.Store
			opts := make([]kv.StoreOptions, 0)
			opts = append(opts, kv.StoreWithLogger(b.l))
			if s, err = kv.OpenStore(b.shardID, path, opts...); err != nil {
				return fmt.Errorf("failed to open normal store: %w", err)
			}
			b.stores[storeID] = s
		case KVTypeTimeSeries:
			var s kv.TimeSeriesStore
			if s, err = kv.OpenTimeSeriesStore(b.shardID, path, define.CompressLevel, define.ValueSize, kv.TSSWithLogger(b.l)); err != nil {
				return fmt.Errorf("failed to open time series store: %w", err)
			}
			b.tsStores[storeID] = s
		}
	}
	return nil
}

func (b *block) close() {
	for _, store := range b.stores {
		_ = store.Close()
	}
	for _, store := range b.tsStores {
		_ = store.Close()
	}
}
