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

package lsm

import (
	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var _ index.Store = (*store)(nil)

type store struct {
	lsm kv.Store
	l   *logger.Logger
}

func (s *store) Stats() observability.Statistics {
	return s.lsm.Stats()
}

func (s *store) Close() error {
	return s.lsm.Close()
}

func (s *store) Write(fields []index.Field, itemID common.ItemID) (err error) {
	for _, field := range fields {
		f, errInternal := field.Marshal()
		if errInternal != nil {
			err = multierr.Append(err, errInternal)
			continue
		}
		itemIDInt := uint64(itemID)
		err = multierr.Append(err, s.lsm.PutWithVersion(f, convert.Uint64ToBytes(itemIDInt), itemIDInt))
	}
	return err
}

type StoreOpts struct {
	Path         string
	Logger       *logger.Logger
	MemTableSize int64
}

func NewStore(opts StoreOpts) (index.Store, error) {
	var err error
	var lsm kv.Store
	if lsm, err = kv.OpenStore(
		0,
		opts.Path+"/lsm",
		kv.StoreWithLogger(opts.Logger),
		kv.StoreWithMemTableSize(opts.MemTableSize)); err != nil {
		return nil, err
	}
	return &store{
		lsm: lsm,
		l:   opts.Logger,
	}, nil
}
