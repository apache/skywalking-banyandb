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

type meanFunc[N Number] struct {
	sum   N
	count N
	zero  N
}

func (m *meanFunc[N]) In(vals ...N) {
	for _, val := range vals {
		m.sum += val
		m.count++
	}
}

func (m meanFunc[N]) Val() N {
	if m.count == m.zero {
		return m.zero
	}
	v := m.sum / m.count
	if v < 1 {
		return 1
	}
	return v
}

func (m *meanFunc[N]) Reset() {
	m.sum = m.zero
	m.count = m.zero
}

type countFunc[N Number] struct {
	count N
	zero  N
}

func (c *countFunc[N]) In(vals ...N) {
	for range vals {
		c.count++
	}
}

func (c countFunc[N]) Val() N {
	return c.count
}

func (c *countFunc[N]) Reset() {
	c.count = c.zero
}

type sumFunc[N Number] struct {
	sum  N
	zero N
}

func (s *sumFunc[N]) In(vals ...N) {
	for _, val := range vals {
		s.sum += val
	}
}

func (s sumFunc[N]) Val() N {
	return s.sum
}

func (s *sumFunc[N]) Reset() {
	s.sum = s.zero
}

type maxFunc[N Number] struct {
	val N
	min N
}

func (m *maxFunc[N]) In(vals ...N) {
	for _, val := range vals {
		if val > m.val {
			m.val = val
		}
	}
}

func (m maxFunc[N]) Val() N {
	return m.val
}

func (m *maxFunc[N]) Reset() {
	m.val = m.min
}

type minFunc[N Number] struct {
	val N
	max N
}

func (m *minFunc[N]) In(vals ...N) {
	for _, val := range vals {
		if val < m.val {
			m.val = val
		}
	}
}

func (m minFunc[N]) Val() N {
	return m.val
}

func (m *minFunc[N]) Reset() {
	m.val = m.max
}

// distributedMeanFunc is used for distributed mean aggregation on data nodes.
type distributedMeanFunc[N Number] struct {
	sum   N
	count N
	zero  N
}

func (m *distributedMeanFunc[N]) In(vals ...N) {
	switch len(vals) {
	case 1:
		m.sum += vals[0]
		m.count++
	case 2:
		m.sum += vals[0]
		m.count += vals[1]
	default:
		panic("expected 1 value (raw) or 2 values (sum, count) for distributed mean")
	}
}

func (m *distributedMeanFunc[N]) Val() N {
	if m.count == m.zero {
		return m.zero
	}
	v := m.sum / m.count
	if v < 1 {
		return 1
	}
	return v
}

func (m *distributedMeanFunc[N]) Reset() {
	m.sum = m.zero
	m.count = m.zero
}

// GetSumCount returns sum and count for distributed mean merge (used by measure plan).
func (m *distributedMeanFunc[N]) GetSumCount() (N, N) {
	return m.sum, m.count
}
