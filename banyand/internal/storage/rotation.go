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

package storage

import (
	"time"

	"github.com/robfig/cron/v3"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

var (
	creationGap           = time.Hour
	newSegmentTimeGap     = creationGap.Nanoseconds()
	timeEventSnapDuration = (10 * time.Minute).Nanoseconds()
)

func (d *database[T, O]) Tick(ts int64) {
	if (ts - timeEventSnapDuration) < d.latestTickTime.Load() {
		return
	}
	d.latestTickTime.Store(ts)
	select {
	case d.tsEventCh <- ts:
	default:
	}
}

func (d *database[T, O]) startRotationTask() error {
	options := d.segmentController.getOptions()
	rt := newRetentionTask(d, options.TTL)
	go func(rt *retentionTask[T, O]) {
		for ts := range d.tsEventCh {
			func(ts int64) {
				d.rotationProcessOn.Store(true)
				defer d.rotationProcessOn.Store(false)
				t := time.Unix(0, ts)
				rt.run(t, d.logger)
				func() {
					ss := d.segmentController.segments()
					if len(ss) == 0 {
						return
					}
					defer func() {
						for i := 0; i < len(ss); i++ {
							ss[i].DecRef()
						}
					}()
					for i := range ss {
						if ss[i].End.UnixNano() < ts {
							ss[i].index.store.Reset()
						}
					}
					latest := ss[len(ss)-1]
					gap := latest.End.UnixNano() - ts
					// gap <=0 means the event is from the future
					// the segment will be created by a written event directly
					if gap <= 0 || gap > newSegmentTimeGap {
						return
					}
					d.incTotalRotationStarted(1)
					defer d.incTotalRotationFinished(1)
					start := options.SegmentInterval.nextTime(t)
					d.logger.Info().Time("segment_start", start).Time("event_time", t).Msg("create new segment")
					_, err := d.segmentController.create(start)
					if err != nil {
						d.logger.Error().Err(err).Msgf("failed to create new segment.")
						d.incTotalRotationErr(1)
					}
				}()
			}(ts)
		}
	}(rt)
	return d.scheduler.Register("retention", rt.option, rt.expr, rt.run)
}

type retentionTask[T TSTable, O any] struct {
	database *database[T, O]
	running  chan struct{}
	expr     string
	option   cron.ParseOption
	duration time.Duration
}

func newRetentionTask[T TSTable, O any](database *database[T, O], ttl IntervalRule) *retentionTask[T, O] {
	return &retentionTask[T, O]{
		database: database,
		option:   cron.Minute | cron.Hour,
		// Remove data which is
		expr:     "5 0",
		duration: ttl.estimatedDuration(),
		running:  make(chan struct{}, 1),
	}
}

func (rc *retentionTask[T, O]) run(now time.Time, l *logger.Logger) bool {
	select {
	case rc.running <- struct{}{}:
	default:
		return true
	}
	defer func() {
		<-rc.running
	}()

	rc.database.incTotalRetentionStarted(1)
	defer rc.database.incTotalRetentionFinished(1)
	deadline := now.Add(-rc.duration)
	start := time.Now()
	hasData, err := rc.database.segmentController.remove(deadline)
	if hasData {
		rc.database.incTotalRetentionHasData(1)
		rc.database.incTotalRetentionHasDataLatency(time.Since(start).Seconds())
	}
	if err != nil {
		l.Error().Err(err)
		rc.database.incTotalRetentionErr(1)
	}
	return true
}
