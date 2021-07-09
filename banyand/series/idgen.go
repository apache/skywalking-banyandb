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

package series

import (
	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/pkg/bytes"
	"github.com/apache/skywalking-banyandb/pkg/convert"
)

var ErrInvalidID = errors.New("invalid id")

type IDGen interface {
	Next(shard uint, ts uint64) []byte
	ParseShardID(ID []byte) (uint, error)
	ParseTS(ID []byte) (uint64, error)
}

func NewIDGen() IDGen {
	return &localID{}
}

var _ IDGen = (*localID)(nil)

type localID struct {
}

func (l *localID) Next(shard uint, ts uint64) []byte {
	return bytes.Join(convert.Uint32ToBytes(uint32(shard)), convert.Uint64ToBytes(ts))
}

func (l *localID) ParseShardID(ID []byte) (uint, error) {
	if len(ID) < 12 {
		return 0, ErrInvalidID
	}
	return uint(convert.BytesToUint32(ID[:4])), nil
}

func (l *localID) ParseTS(ID []byte) (uint64, error) {
	if len(ID) < 12 {
		return 0, ErrInvalidID
	}
	return convert.BytesToUint64(ID[4:]), nil
}
