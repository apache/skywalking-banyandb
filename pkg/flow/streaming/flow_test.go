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

package streaming

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/flow"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming/sink"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming/sources"
)

// numberRange generates a slice with `count` number of integers starting from `begin`,
// i.e. [begin, begin + count)
func numberRange(begin, count int) []int {
	result := make([]int, 0)
	for i := 0; i < count; i++ {
		result = append(result, begin+i)
	}
	return result
}

func TestStream_Filter(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		f        flow.UnaryFunc[bool]
		expected []interface{}
	}{
		{
			name:  "Even Number Filter",
			input: sources.NewSlice(numberRange(0, 10)),
			f: func(ctx context.Context, i interface{}) bool {
				return i.(int)%2 == 0
			},
			expected: []interface{}{
				flow.NewStreamRecordWithoutTS(0),
				flow.NewStreamRecordWithoutTS(2),
				flow.NewStreamRecordWithoutTS(4),
				flow.NewStreamRecordWithoutTS(6),
				flow.NewStreamRecordWithoutTS(8),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			snk := sink.NewSlice()
			s := New(tt.input).
				Filter(tt.f).
				To(snk)
			if errCh := s.OpenAsync(); errCh != nil {
				err := Await().AtMost(3 * time.Second).Until(func() bool {
					if len(snk.Value()) == len(tt.expected) {
						if assert.Equal(tt.expected, snk.Value()) {
							return true
						}
					}

					return false
				})
				assert.NoError(err)
			}
		})
	}
}

func TestStream_Mapper(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		f        flow.UnaryFunc[any]
		expected []interface{}
	}{
		{
			name:  "Multiplier Mapper",
			input: sources.NewSlice(numberRange(0, 10)),
			f: func(ctx context.Context, i interface{}) interface{} {
				return i.(int) * 2
			},
			expected: []interface{}{
				flow.NewStreamRecordWithoutTS(0),
				flow.NewStreamRecordWithoutTS(2),
				flow.NewStreamRecordWithoutTS(4),
				flow.NewStreamRecordWithoutTS(6),
				flow.NewStreamRecordWithoutTS(8),
				flow.NewStreamRecordWithoutTS(10),
				flow.NewStreamRecordWithoutTS(12),
				flow.NewStreamRecordWithoutTS(14),
				flow.NewStreamRecordWithoutTS(16),
				flow.NewStreamRecordWithoutTS(18),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			snk := sink.NewSlice()
			s := New(tt.input).
				Map(tt.f).
				To(snk)
			if errCh := s.OpenAsync(); errCh != nil {
				err := Await().AtMost(3 * time.Second).Until(func() bool {
					if len(snk.Value()) == len(tt.expected) {
						if assert.Equal(tt.expected, snk.Value()) {
							return true
						}
					}

					return false
				})
				assert.NoError(err)
			}
		})
	}
}

func TestStream_TopN(t *testing.T) {
	type record struct {
		service  string
		instance string
		value    int
	}

	tests := []struct {
		name     string
		input    []flow.StreamRecord
		expected []*Tuple2
	}{
		{
			name: "Smoke Test ASC",
			input: []flow.StreamRecord{
				flow.NewStreamRecord(&record{"e2e-service-provider", "instance-001", 10000}, 1000),
				flow.NewStreamRecord(&record{"e2e-service-consumer", "instance-001", 9900}, 2000),
				flow.NewStreamRecord(&record{"e2e-service-provider", "instance-002", 9800}, 3000),
				flow.NewStreamRecord(&record{"e2e-service-consumer", "instance-002", 9700}, 4000),
				flow.NewStreamRecord(&record{"e2e-service-provider", "instance-003", 9700}, 5000),
				flow.NewStreamRecord(&record{"e2e-service-consumer", "instance-004", 9600}, 6000),
				flow.NewStreamRecord(&record{"e2e-service-consumer", "instance-001", 9500}, 7000),
				flow.NewStreamRecord(&record{"e2e-service-provider", "instance-002", 9800}, 61000),
			},
			expected: []*Tuple2{
				{int64(9500), flow.Data{"e2e-service-consumer", int64(9500)}},
				{int64(9600), flow.Data{"e2e-service-consumer", int64(9600)}},
				{int64(9700), flow.Data{"e2e-service-consumer", int64(9700)}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			snk := sink.NewSlice()

			s := New(sources.NewSlice(tt.input)).
				Map(flow.UnaryFunc[any](func(ctx context.Context, item interface{}) interface{} {
					// groupBy
					return flow.Data{item.(*record).service, int64(item.(*record).value)}
				})).
				Window(NewSlidingTimeWindows(60*time.Second, 15*time.Second)).
				TopN(3, WithSortKeyExtractor(func(elem interface{}) int64 {
					return elem.(flow.Data)[1].(int64)
				})).
				To(snk)

			if errCh := s.OpenAsync(); errCh != nil {
				err := Await().AtMost(5 * time.Second).Until(func() bool {
					if len(snk.Value()) == 0 {
						return false
					}
					firstValue := snk.Value()[0].(flow.StreamRecord).Data()
					if len(firstValue.([]*Tuple2)) == len(tt.expected) {
						if assert.Equal(tt.expected, firstValue) {
							return true
						}
					}

					return false
				})
				assert.NoError(err)
			}
		})
	}
}

type AsyncTestBuilder struct {
	timeout  time.Duration
	interval time.Duration
}

func Await() *AsyncTestBuilder {
	return &AsyncTestBuilder{}
}

func (t *AsyncTestBuilder) AtMost(timeout time.Duration) *AsyncTestBuilder {
	t.timeout = timeout
	return t
}

func (t *AsyncTestBuilder) PollInterval(interval time.Duration) *AsyncTestBuilder {
	t.interval = interval
	return t
}

func (t *AsyncTestBuilder) Until(lambda func() bool) error {
	if t.timeout == 0 {
		return errors.New("timeout must be greater than 0")
	}
	timer := time.NewTimer(t.timeout)
	if t.interval == 0 {
		t.interval = t.timeout / 5
	}
	err := func() error {
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				return errors.New("await: timeout")
			default:
				if lambda() {
					return nil
				}
				if t.interval.Milliseconds() > 0 {
					time.Sleep(t.interval)
				}
			}
		}
	}()
	return err
}
