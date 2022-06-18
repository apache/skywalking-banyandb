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

package flow_test

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/streaming/flow"
	"github.com/apache/skywalking-banyandb/pkg/streaming/sink"
	"github.com/apache/skywalking-banyandb/pkg/streaming/sources"
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
		f        interface{}
		expected []interface{}
	}{
		{
			name:  "Even Number Filter",
			input: sources.NewSlice(numberRange(0, 10)),
			f: func(i int) bool {
				return i%2 == 0
			},
			expected: []interface{}{0, 2, 4, 6, 8},
		},
		{
			name:  "Even Number Filter with context",
			input: sources.NewSlice(numberRange(0, 10)),
			f: func(_ctx context.Context, i int) bool {
				return i%2 == 0
			},
			expected: []interface{}{0, 2, 4, 6, 8},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			snk := sink.NewSlice()
			s := flow.New(tt.input).
				Filter(tt.f).
				To(snk)
			if errCh := s.Open(); errCh != nil {
				timeout := time.NewTimer(10 * time.Second)
				err := func() error {
					defer timeout.Stop()
					for {
						select {
						case err := <-errCh:
							if err != nil {
								return err
							}
						case <-timeout.C:
							return errors.New("timeout")
						default:
							if len(snk.Value()) == len(tt.expected) {
								if assert.Equal(tt.expected, snk.Value()) {
									return nil
								}
							}
						}
					}
				}()
				assert.NoError(err)
			}
		})
	}
}

func TestStream_Mapper(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		f        interface{}
		expected []interface{}
	}{
		{
			name:  "Multiplier Mapper",
			input: sources.NewSlice(numberRange(0, 10)),
			f: func(i int) int {
				return i * 2
			},
			expected: []interface{}{0, 2, 4, 6, 8, 10, 12, 14, 16, 18},
		},
		{
			name:  "Multiplier Mapper with context",
			input: sources.NewSlice(numberRange(0, 10)),
			f: func(_ctx context.Context, i int) int {
				return i * 2
			},
			expected: []interface{}{0, 2, 4, 6, 8, 10, 12, 14, 16, 18},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := assert.New(t)
			snk := sink.NewSlice()
			s := flow.New(tt.input).
				Map(tt.f).
				To(snk)
			if errCh := s.Open(); errCh != nil {
				timeout := time.NewTimer(10 * time.Second)
				err := func() error {
					defer timeout.Stop()
					for {
						select {
						case err := <-errCh:
							if err != nil {
								return err
							}
						case <-timeout.C:
							return errors.Errorf("timeout: with result %v", snk.Value())
						default:
							if len(snk.Value()) == len(tt.expected) {
								if assert.Equal(tt.expected, snk.Value()) {
									return nil
								}
							}
						}
					}
				}()
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

func (t *AsyncTestBuilder) Util(lambda func() bool) error {
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
