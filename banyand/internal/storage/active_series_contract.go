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

package storage

import (
	"sync/atomic"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var activeSeriesWindowNanos atomic.Int64 // 0 means one hour.

func activeSeriesWindow() time.Duration {
	if w := activeSeriesWindowNanos.Load(); w > 0 {
		return time.Duration(w)
	}
	return time.Hour
}

// SetActiveSeriesWindowForTest sets the window of the trackers created afterwards in this process and returns a
// function that restores the previous window. The window is a process-wide setting that applies to every tracker
// created afterwards, in all databases of the process. It exists for tests; production always uses one hour.
func SetActiveSeriesWindowForTest(d time.Duration) (restore func()) {
	if d <= 0 {
		panic("active series window must be positive")
	}
	prev := activeSeriesWindowNanos.Swap(int64(d))
	return func() { activeSeriesWindowNanos.Store(prev) }
}

var activeSeriesClockOverride atomic.Pointer[timestamp.Clock] // nil means the clock the owner passes.

// activeSeriesClock returns the clock a new tracker reads: the one set by SetActiveSeriesClockForTest, else c.
func activeSeriesClock(c timestamp.Clock) timestamp.Clock {
	if o := activeSeriesClockOverride.Load(); o != nil {
		return *o
	}
	return c
}

// SetActiveSeriesClockForTest makes the trackers created afterwards in this process read c and returns a function that
// restores the previous clock. The clock is a process-wide setting that applies to every such tracker, those of a
// database and those of a wqueue.Queue alike. It exists for tests; production trackers read the clock of their owner.
func SetActiveSeriesClockForTest(c timestamp.Clock) (restore func()) {
	prev := activeSeriesClockOverride.Swap(&c)
	return func() { activeSeriesClockOverride.Store(prev) }
}

// ActiveSeriesOf returns the active series tracker of owner, or nil when owner keeps none.
func ActiveSeriesOf(owner any) *ActiveSeries {
	if o, ok := owner.(interface{ ActiveSeries() *ActiveSeries }); ok {
		return o.ActiveSeries()
	}
	return nil
}
