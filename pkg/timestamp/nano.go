// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package timestamp

import (
	"math"
	"time"
	// link runtime pkg fastrand.
	_ "unsafe"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/timestamppb"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

var (
	maxNanoSecond = uint32(time.Millisecond - 1)
	mSecond       = int64(time.Millisecond)
)

// FastRandN is a fast thread local random function.
//
//go:linkname fastRandN runtime.fastrandn
func fastRandN(n uint32) uint32

// MToN convert time unix millisends to nanoseconds.
func MToN(ms time.Time) time.Time {
	ns := ms.UnixNano()
	if ms.Nanosecond()%int(mSecond) > 0 {
		ns = ns / mSecond * mSecond
	}
	nns := ns + int64(fastRandN(maxNanoSecond))
	return time.Unix(ms.Unix(), nns%int64(time.Second))
}

// NowMilli returns a time based on a unix millisecond.
func NowMilli() time.Time {
	return time.UnixMilli(time.Now().UnixMilli())
}

const (
	// MinNanoTime is the minimum time that can be represented.
	//
	// 1677-09-21 00:12:43.145224192 +0000 UTC.
	MinNanoTime = int64(math.MinInt64)

	// MaxNanoTime is the maximum time that can be represented.
	//
	// 2262-04-11 23:47:16.854775807 +0000 UTC.
	MaxNanoTime = int64(math.MaxInt64)
)

var (
	minNanoTime = time.Unix(0, MinNanoTime).UTC()
	maxNanoTime = time.Unix(0, MaxNanoTime).UTC()

	maxMilliTime       = time.UnixMilli(maxNanoTime.UnixMilli())
	maxMilliPbTime     = timestamppb.New(maxMilliTime)
	defaultBeginPbTime = timestamppb.New(time.Unix(0, 0))

	// DefaultTimeRange for the input time range.
	DefaultTimeRange = &modelv1.TimeRange{
		Begin: defaultBeginPbTime,
		End:   maxMilliPbTime,
	}

	errTimeOutOfRange     = errors.Errorf("time is out of range %d - %d", MinNanoTime, MaxNanoTime)
	errTimeNotMillisecond = errors.Errorf("time is not millisecond precision")
	errTimeEmpty          = errors.Errorf("time is empty")
)

// Check checks that a time is valid.
func Check(t time.Time) error {
	if t.Before(minNanoTime) || t.After(maxNanoTime) {
		return errTimeOutOfRange
	}
	if t.Nanosecond()%int(mSecond) > 0 {
		return errTimeNotMillisecond
	}
	return nil
}

// CheckPb checks that a protobuf timestamp is valid.
func CheckPb(t *timestamppb.Timestamp) error {
	if t == nil {
		return errTimeEmpty
	}
	return Check(t.AsTime())
}

// CheckTimeRange checks that a protobuf time range is valid.
func CheckTimeRange(timeRange *modelv1.TimeRange) error {
	if timeRange == nil {
		return errTimeEmpty
	}
	err := CheckPb(timeRange.Begin)
	if err != nil {
		return err
	}
	return CheckPb(timeRange.End)
}
