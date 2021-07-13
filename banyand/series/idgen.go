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
	"time"
)

const (
	timeStampLen = 41 + 1
	shardIDLen   = 22
)

var startTime = uint64(time.Date(2021, 07, 01, 23, 0, 0, 0, time.UTC).UTC().UnixNano() / 1e6)

type IDGen interface {
	Next(shard uint, ts uint64) uint64
	ParseShardID(ID uint64) (uint, error)
	ParseTS(ID uint64) (uint64, error)
}

func NewIDGen() IDGen {
	return &localID{}
}

var _ IDGen = (*localID)(nil)

type localID struct {
}

func (l *localID) Next(shard uint, ts uint64) uint64 {
	df := ts/1e6 - startTime
	id := (df << shardIDLen) | uint64(shard)
	return id
}

func (l *localID) ParseShardID(ID uint64) (uint, error) {
	shardID := (ID << timeStampLen) >> timeStampLen
	return uint(shardID), nil
}

func (l *localID) ParseTS(ID uint64) (uint64, error) {
	return (ID>>shardIDLen + startTime) * 1e6, nil
}
