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

package grpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// base anchors all test timestamps to a fixed, deterministic instant.
var base = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// TestClampTimeRangeBegin_EmptyCreatedAts_ReturnsBeginUnchanged verifies that
// when no createdAts are supplied the begin time is returned unchanged with empty=false.
func TestClampTimeRangeBegin_EmptyCreatedAts_ReturnsBeginUnchanged(t *testing.T) {
	begin := base
	end := base.Add(time.Hour)
	gotBegin, empty := clampTimeRangeBegin(begin, end, nil)
	assert.Equal(t, begin, gotBegin)
	assert.False(t, empty)
}

// TestClampTimeRangeBegin_AllZeroCreatedAts_ReturnsBeginUnchanged verifies that
// zero-value createdAts entries are ignored and begin is returned unchanged.
func TestClampTimeRangeBegin_AllZeroCreatedAts_ReturnsBeginUnchanged(t *testing.T) {
	begin := base
	end := base.Add(time.Hour)
	gotBegin, empty := clampTimeRangeBegin(begin, end, []time.Time{{}, {}, {}})
	assert.Equal(t, begin, gotBegin)
	assert.False(t, empty)
}

// TestClampTimeRangeBegin_BeginBeforeMaxCreatedAt_ClampsToMax verifies that when
// begin is before the maximum createdAt, begin is advanced to maxCreatedAt.
func TestClampTimeRangeBegin_BeginBeforeMaxCreatedAt_ClampsToMax(t *testing.T) {
	createdAt := base.Add(30 * time.Minute)
	begin := base // before createdAt
	end := base.Add(2 * time.Hour)
	gotBegin, empty := clampTimeRangeBegin(begin, end, []time.Time{createdAt})
	assert.Equal(t, createdAt, gotBegin, "begin must be advanced to maxCreatedAt")
	assert.False(t, empty)
}

// TestClampTimeRangeBegin_BeginAfterMaxCreatedAt_ReturnsBeginUnchanged verifies
// that when begin is already after the maximum createdAt it is not changed.
func TestClampTimeRangeBegin_BeginAfterMaxCreatedAt_ReturnsBeginUnchanged(t *testing.T) {
	createdAt := base
	begin := base.Add(time.Hour) // already after createdAt
	end := base.Add(2 * time.Hour)
	gotBegin, empty := clampTimeRangeBegin(begin, end, []time.Time{createdAt})
	assert.Equal(t, begin, gotBegin)
	assert.False(t, empty)
}

// TestClampTimeRangeBegin_BeginEqualsMaxCreatedAt_ReturnsBeginUnchanged verifies
// that begin equal to maxCreatedAt is accepted as-is without being marked empty.
func TestClampTimeRangeBegin_BeginEqualsMaxCreatedAt_ReturnsBeginUnchanged(t *testing.T) {
	createdAt := base.Add(30 * time.Minute)
	begin := createdAt // exactly equal
	end := base.Add(2 * time.Hour)
	gotBegin, empty := clampTimeRangeBegin(begin, end, []time.Time{createdAt})
	assert.Equal(t, begin, gotBegin)
	assert.False(t, empty)
}

// TestClampTimeRangeBegin_ClampedBeginAfterEnd_ReturnsEmpty verifies that when
// the clamped begin lands after the end, empty=true is returned so the caller
// can short-circuit and return an empty query response.
func TestClampTimeRangeBegin_ClampedBeginAfterEnd_ReturnsEmpty(t *testing.T) {
	// End is far in the past; createdAt is recent → clamped begin > end.
	createdAt := base.Add(time.Hour)
	begin := base.Add(-time.Hour)      // before createdAt
	end := base.Add(-30 * time.Minute) // between begin and createdAt
	gotBegin, empty := clampTimeRangeBegin(begin, end, []time.Time{createdAt})
	assert.Equal(t, createdAt, gotBegin)
	assert.True(t, empty, "clamped begin > end must return empty=true")
}

// TestClampTimeRangeBegin_ClampedBeginEqualsEnd_NotEmpty verifies the boundary:
// when the clamped begin is exactly equal to end the range is a single point
// and empty is false (the caller should proceed with the query).
func TestClampTimeRangeBegin_ClampedBeginEqualsEnd_NotEmpty(t *testing.T) {
	createdAt := base.Add(30 * time.Minute)
	begin := base    // before createdAt
	end := createdAt // exactly equal to createdAt after clamping
	gotBegin, empty := clampTimeRangeBegin(begin, end, []time.Time{createdAt})
	assert.Equal(t, createdAt, gotBegin)
	assert.False(t, empty, "clamped begin == end is a valid single-point range, not empty")
}

// TestClampTimeRangeBegin_ZeroEnd_NeverEmpty verifies that a zero (unbounded) end
// never causes empty=true regardless of where begin lands after clamping.
func TestClampTimeRangeBegin_ZeroEnd_NeverEmpty(t *testing.T) {
	createdAt := base.Add(time.Hour)
	begin := base
	gotBegin, empty := clampTimeRangeBegin(begin, time.Time{}, []time.Time{createdAt})
	assert.Equal(t, createdAt, gotBegin)
	assert.False(t, empty, "zero end means unbounded — must never return empty=true")
}

// TestClampTimeRangeBegin_MultipleCreatedAts_UsesMaximum verifies that when
// multiple createdAts are provided, begin is clamped to the largest one.
func TestClampTimeRangeBegin_MultipleCreatedAts_UsesMaximum(t *testing.T) {
	ca1 := base.Add(10 * time.Minute)
	ca2 := base.Add(45 * time.Minute) // largest
	ca3 := base.Add(20 * time.Minute)
	begin := base
	end := base.Add(2 * time.Hour)
	gotBegin, empty := clampTimeRangeBegin(begin, end, []time.Time{ca1, ca2, ca3})
	assert.Equal(t, ca2, gotBegin, "begin must be clamped to the maximum createdAt across all groups")
	assert.False(t, empty)
}

// TestClampTimeRangeBegin_MixedZeroAndNonZeroCreatedAts_IgnoresZeros verifies that
// zero-value entries among the createdAts are ignored when computing the maximum.
func TestClampTimeRangeBegin_MixedZeroAndNonZeroCreatedAts_IgnoresZeros(t *testing.T) {
	ca := base.Add(20 * time.Minute)
	begin := base
	end := base.Add(time.Hour)
	gotBegin, empty := clampTimeRangeBegin(begin, end, []time.Time{{}, ca, {}})
	assert.Equal(t, ca, gotBegin)
	assert.False(t, empty)
}
