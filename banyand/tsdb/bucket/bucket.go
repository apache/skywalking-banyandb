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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/apache/skywalking-banyandb/pkg/logger"
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
	// TODO: refactor Report to return a status. It's too complicated to return a channel
	Report() (Channel, error)
	String() string
}

var (
	_             Reporter = (*dummyReporter)(nil)
	_             Reporter = (*timeBasedReporter)(nil)
	DummyReporter          = &dummyReporter{}
)

type dummyReporter struct{}

func (*dummyReporter) Report() (Channel, error) {
	return nil, ErrReporterClosed
}

func (*dummyReporter) Stop() {
}

func (*dummyReporter) String() string {
	return "dummy-reporter"
}

type timeBasedReporter struct {
	timestamp.TimeRange
	name      string
	scheduler *timestamp.Scheduler
	count     *atomic.Uint32
	clock     timestamp.Clock
}

func NewTimeBasedReporter(name string, timeRange timestamp.TimeRange, clock timestamp.Clock, scheduler *timestamp.Scheduler) Reporter {
	if timeRange.End.Before(clock.Now()) {
		return DummyReporter
	}
	t := &timeBasedReporter{
		TimeRange: timeRange,
		scheduler: scheduler,
		clock:     clock,
		name:      name,
		count:     &atomic.Uint32{},
	}
	return t
}

func (tr *timeBasedReporter) Report() (Channel, error) {
	if tr.scheduler.Closed() {
		return nil, ErrReporterClosed
	}
	now := tr.clock.Now()
	if now.After(tr.End) {
		return nil, ErrReporterClosed
	}
	ch := make(Channel, 1)
	interval := tr.Duration() >> 4
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	ms := interval / time.Millisecond
	if err := tr.scheduler.Register(
		fmt.Sprintf("%s-%d", tr.name, tr.count.Add(1)),
		cron.Descriptor,
		fmt.Sprintf("@every %dms", ms),
		func(now time.Time, l *logger.Logger) bool {
			status := Status{
				Capacity: int(tr.End.UnixNano() - tr.Start.UnixNano()),
				Volume:   int(now.UnixNano() - tr.Start.UnixNano()),
			}
			if e := l.Debug(); e.Enabled() {
				e.Int("volume", status.Volume).Int("capacity", status.Capacity).Int("progress%", status.Volume*100/status.Capacity).Msg("reporting a status")
			}
			select {
			case ch <- status:
			default:
				// TODO: this's too complicated, we should not use the channel anymore.
				if status.Volume >= status.Capacity {
					l.Warn().Int("volume", status.Volume).Int("capacity", status.Capacity).Int("progress%", status.Volume*100/status.Capacity).Msg("the end status must be reported")
					ch <- status
				} else {
					l.Warn().Int("volume", status.Volume).Int("capacity", status.Capacity).Int("progress%", status.Volume*100/status.Capacity).Msg("ignore a status")
				}
			}
			l.Info().Int("volume", status.Volume).Int("capacity", status.Capacity).Int("progress%", status.Volume*100/status.Capacity).Msg("reported a status")
			if status.Volume < status.Capacity {
				return true
			}
			close(ch)
			return false
		}); err != nil {
		close(ch)
		if errors.Is(err, timestamp.ErrSchedulerClosed) {
			return nil, ErrReporterClosed
		}
		return nil, err
	}
	return ch, nil
}
