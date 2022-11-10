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

package bucket

import (
	"errors"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

var ErrReporterClosed = errors.New("reporter is closed")

type Controller interface {
	Current() (Reporter, error)
	Next() (Reporter, error)
	OnMove(prev, next Reporter)
}

type Status struct {
	Capacity int
	Volume   int
}

type Channel chan Status

type Reporter interface {
	Report() (Channel, error)
	Stop()
	String() string
}

var _ Reporter = (*timeBasedReporter)(nil)

type timeBasedReporter struct {
	timestamp.TimeRange
	clock  timestamp.Clock
	closer *run.Closer
}

func NewTimeBasedReporter(timeRange timestamp.TimeRange, clock timestamp.Clock) Reporter {
	if timeRange.End.Before(clock.Now()) {
		return nil
	}
	t := &timeBasedReporter{
		TimeRange: timeRange,
		clock:     clock,
		closer:    run.NewCloser(0),
	}
	return t
}

func (tr *timeBasedReporter) Report() (Channel, error) {
	if tr.closer.Closed() {
		return nil, ErrReporterClosed
	}
	ch := make(Channel, 1)
	interval := tr.Duration() >> 4
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	go func() {
		defer close(ch)
		if tr.closer.AddRunning() {
			defer tr.closer.Done()
		} else {
			return
		}
		ticker := tr.clock.Ticker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				status := Status{
					Capacity: int(tr.End.UnixNano() - tr.Start.UnixNano()),
					Volume:   int(tr.clock.Now().UnixNano() - tr.Start.UnixNano()),
				}
				ch <- status
				if status.Volume >= status.Capacity {
					return
				}
			case <-tr.closer.CloseNotify():
				return
			}
		}
	}()
	return ch, nil
}

func (tr *timeBasedReporter) Stop() {
	tr.closer.CloseThenWait()
}
