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
	"errors"

	common_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/tsdb"
)

var ErrInvalidUnit = errors.New("invalid interval rule's unit")

func ToIntervalRule(ir *common_v1.IntervalRule) (result tsdb.IntervalRule, err error) {
	switch ir.Unit {
	case common_v1.IntervalRule_UNIT_DAY:
		result.Unit = tsdb.DAY
	case common_v1.IntervalRule_UNIT_HOUR:
		result.Unit = tsdb.HOUR
	default:
		return result, ErrInvalidUnit
	}
	result.Num = int(ir.Num)
	return result, err
}
