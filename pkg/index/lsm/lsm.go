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
	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/kv"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var _ index.Store = (*store)(nil)

type store struct {
	lsm kv.Store
}

func (s *store) Close() error {
	return s.lsm.Close()
}

func (s *store) Write(field index.Field, itemID common.ItemID) error {
	itemIDInt := uint64(itemID)
	return s.lsm.PutWithVersion(field.Marshal(), convert.Uint64ToBytes(itemIDInt), itemIDInt)
}

type StoreOpts struct {
	Path   string
	Logger *logger.Logger
}

func NewStore(opts StoreOpts) (index.Store, error) {
	var err error
	var lsm kv.Store
	if lsm, err = kv.OpenStore(0, opts.Path, kv.StoreWithLogger(opts.Logger)); err != nil {
		return nil, err
	}
	return &store{
		lsm: lsm,
	}, nil
}
