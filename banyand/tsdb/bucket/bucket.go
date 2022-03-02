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
	"fmt"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

type Controller interface {
	Current() Reporter
	Next() (Reporter, error)
	OnMove(prev, next Reporter)
}

type Status struct {
	Capacity int
	Volume   int
}

type Channel chan Status

type Reporter interface {
	Report() Channel
	Stop()
	String() string
}

type timeBasedReporter struct {
	timestamp.TimeRange
	reporterStopCh chan struct{}
}

func NewTimeBasedReporter(timeRange timestamp.TimeRange) Reporter {
	return &timeBasedReporter{
		TimeRange:      timeRange,
		reporterStopCh: make(chan struct{}),
	}
}

func (tr *timeBasedReporter) Report() Channel {
	ch := make(Channel)
	interval := tr.Duration() >> 4
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	go func() {
		defer close(ch)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fmt.Printf("tick for %v\n", interval)
				status := Status{
					Capacity: int(tr.End.UnixNano() - tr.Start.UnixNano()),
					Volume:   int(time.Now().UnixNano() - tr.Start.UnixNano()),
				}
				ch <- status
				if status.Volume >= status.Capacity {
					return
				}
			case <-tr.reporterStopCh:
				return
			}
		}
	}()
	return ch
}

func (tr *timeBasedReporter) Stop() {
	close(tr.reporterStopCh)
}
