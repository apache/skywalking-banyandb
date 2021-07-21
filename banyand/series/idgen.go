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

var startTime = uint64(time.Date(2021, 07, 01, 23, 0, 0, 0, time.UTC).UTC().UnixNano() / 1e6)

type IDGen interface {
	Next(ts uint64) uint64
	ParseTS(ID uint64) (uint64, error)
}

func NewIDGen() IDGen {
	return &localID{}
}

var _ IDGen = (*localID)(nil)

type localID struct {
}

func (l *localID) Next(ts uint64) uint64 {
	df := ts/1e6 - startTime
	return df
}

func (l *localID) ParseTS(ID uint64) (uint64, error) {
	return (ID + startTime) * 1e6, nil
}
