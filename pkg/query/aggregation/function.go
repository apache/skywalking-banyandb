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

package aggregation

import "math"

var _ Int64Func = (*meanInt64Func)(nil)

type meanInt64Func struct {
	sum   int64
	count int64
}

func (m *meanInt64Func) In(val int64) {
	m.sum += val
	m.count++
}

func (m meanInt64Func) Val() int64 {
	if m.count == 0 {
		return 0
	}
	v := m.sum / m.count
	if v < 1 {
		return 1
	}
	return v
}

func (m *meanInt64Func) Reset() {
	m.sum = 0
	m.count = 0
}

var _ Int64Func = (*countInt64Func)(nil)

type countInt64Func struct {
	count int64
}

func (c *countInt64Func) In(_ int64) {
	c.count++
}

func (c countInt64Func) Val() int64 {
	return c.count
}

func (c *countInt64Func) Reset() {
	c.count = 0
}

var _ Int64Func = (*sumInt64Func)(nil)

type sumInt64Func struct {
	sum int64
}

func (s *sumInt64Func) In(val int64) {
	s.sum += val
}

func (s sumInt64Func) Val() int64 {
	return s.sum
}

func (s *sumInt64Func) Reset() {
	s.sum = 0
}

var _ Int64Func = (*maxInt64Func)(nil)

type maxInt64Func struct {
	val int64
}

func (m *maxInt64Func) In(val int64) {
	if val > m.val {
		m.val = val
	}
}

func (m maxInt64Func) Val() int64 {
	return m.val
}

func (m *maxInt64Func) Reset() {
	m.val = math.MinInt64
}

var _ Int64Func = (*minInt64Func)(nil)

type minInt64Func struct {
	val int64
}

func (m *minInt64Func) In(val int64) {
	if val < m.val {
		m.val = val
	}
}

func (m minInt64Func) Val() int64 {
	return m.val
}

func (m *minInt64Func) Reset() {
	m.val = math.MaxInt64
}
