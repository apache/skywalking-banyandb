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

func (m *meanFunc[N]) In(val N) {
	m.sum += val
	m.count++
}

// InSlice folds vals in order, exactly as one In call per value would: the sum
// is accumulated left to right, so a float result is bit-identical.
func (m *meanFunc[N]) InSlice(vals []N) {
	sum := m.sum
	for _, v := range vals {
		sum += v
	}
	m.sum = sum
	m.count += N(len(vals))
}

// InSelected folds vals[sel[0]], vals[sel[1]], ... in that order.
func (m *meanFunc[N]) InSelected(vals []N, sel []uint16) {
	sum := m.sum
	for _, r := range sel {
		sum += vals[r]
	}
	m.sum = sum
	m.count += N(len(sel))
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

func (m meanFunc[N]) Partial() Partial[N] {
	return Partial[N]{Value: m.sum, Count: m.count}
}

func (m *meanFunc[N]) Reset() {
	m.sum = m.zero
	m.count = m.zero
}

type meanReduceFunc[N Number] struct {
	sum   N
	count N
	zero  N
}

func (m *meanReduceFunc[N]) Combine(p Partial[N]) {
	m.sum += p.Value
	m.count += p.Count
}

func (m meanReduceFunc[N]) Val() N {
	if m.count == m.zero {
		return m.zero
	}
	v := m.sum / m.count
	if v < 1 {
		return 1
	}
	return v
}

func (m *meanReduceFunc[N]) Reset() {
	m.sum = m.zero
	m.count = m.zero
}

type countFunc[N Number] struct {
	count N
	zero  N
}

func (c *countFunc[N]) In(_ N) {
	c.count++
}

// AddN increments the count by n in one operation, equivalent to calling
// In n times. It is an optional fast path a caller can reach via a type
// assertion (e.g. `interface{ AddN(int64) }`) when it already knows a run
// of n rows are all non-null and need no per-row inspection — the
// vectorized measure engine's run-folding (design §7.7) uses this for
// COUNT over a series-constant target, where every row in a span shares
// the same non-null target by construction.
func (c *countFunc[N]) AddN(n int64) {
	c.count += N(n)
}

// InSlice counts len(vals) values.
func (c *countFunc[N]) InSlice(vals []N) { c.count += N(len(vals)) }

// InSelected counts len(sel) values.
func (c *countFunc[N]) InSelected(_ []N, sel []uint16) { c.count += N(len(sel)) }

func (c countFunc[N]) Val() N {
	return c.count
}

func (c countFunc[N]) Partial() Partial[N] {
	return Partial[N]{Value: c.count}
}

func (c *countFunc[N]) Reset() {
	c.count = c.zero
}

type countReduceFunc[N Number] struct {
	sum  N
	zero N
}

func (c *countReduceFunc[N]) Combine(p Partial[N]) {
	c.sum += p.Value
}

func (c countReduceFunc[N]) Val() N {
	return c.sum
}

func (c *countReduceFunc[N]) Reset() {
	c.sum = c.zero
}

type sumFunc[N Number] struct {
	sum  N
	zero N
}

func (s *sumFunc[N]) In(val N) {
	s.sum += val
}

// InSlice folds vals in order, exactly as one In call per value would.
func (s *sumFunc[N]) InSlice(vals []N) {
	sum := s.sum
	for _, v := range vals {
		sum += v
	}
	s.sum = sum
}

// InSelected folds vals[sel[0]], vals[sel[1]], ... in that order.
func (s *sumFunc[N]) InSelected(vals []N, sel []uint16) {
	sum := s.sum
	for _, r := range sel {
		sum += vals[r]
	}
	s.sum = sum
}

func (s sumFunc[N]) Val() N {
	return s.sum
}

func (s sumFunc[N]) Partial() Partial[N] {
	return Partial[N]{Value: s.sum}
}

func (s *sumFunc[N]) Reset() {
	s.sum = s.zero
}

type sumReduceFunc[N Number] struct {
	sum  N
	zero N
}

func (s *sumReduceFunc[N]) Combine(p Partial[N]) {
	s.sum += p.Value
}

func (s sumReduceFunc[N]) Val() N {
	return s.sum
}

func (s *sumReduceFunc[N]) Reset() {
	s.sum = s.zero
}

type maxFunc[N Number] struct {
	val N
	min N
}

func (m *maxFunc[N]) In(val N) {
	if val > m.val {
		m.val = val
	}
}

// InSlice folds vals in order, exactly as one In call per value would.
func (m *maxFunc[N]) InSlice(vals []N) {
	cur := m.val
	for _, v := range vals {
		if v > cur {
			cur = v
		}
	}
	m.val = cur
}

// InSelected folds vals[sel[0]], vals[sel[1]], ... in that order.
func (m *maxFunc[N]) InSelected(vals []N, sel []uint16) {
	cur := m.val
	for _, r := range sel {
		if v := vals[r]; v > cur {
			cur = v
		}
	}
	m.val = cur
}

func (m maxFunc[N]) Val() N {
	return m.val
}

func (m maxFunc[N]) Partial() Partial[N] {
	return Partial[N]{Value: m.val}
}

func (m *maxFunc[N]) Reset() {
	m.val = m.min
}

type maxReduceFunc[N Number] struct {
	val N
	min N
}

func (m *maxReduceFunc[N]) Combine(p Partial[N]) {
	if p.Value > m.val {
		m.val = p.Value
	}
}

func (m maxReduceFunc[N]) Val() N {
	return m.val
}

func (m *maxReduceFunc[N]) Reset() {
	m.val = m.min
}

type minFunc[N Number] struct {
	val N
	max N
}

func (m *minFunc[N]) In(val N) {
	if val < m.val {
		m.val = val
	}
}

// InSlice folds vals in order, exactly as one In call per value would.
func (m *minFunc[N]) InSlice(vals []N) {
	cur := m.val
	for _, v := range vals {
		if v < cur {
			cur = v
		}
	}
	m.val = cur
}

// InSelected folds vals[sel[0]], vals[sel[1]], ... in that order.
func (m *minFunc[N]) InSelected(vals []N, sel []uint16) {
	cur := m.val
	for _, r := range sel {
		if v := vals[r]; v < cur {
			cur = v
		}
	}
	m.val = cur
}

func (m minFunc[N]) Val() N {
	return m.val
}

func (m minFunc[N]) Partial() Partial[N] {
	return Partial[N]{Value: m.val}
}

func (m *minFunc[N]) Reset() {
	m.val = m.max
}

type minReduceFunc[N Number] struct {
	val N
	max N
}

func (m *minReduceFunc[N]) Combine(p Partial[N]) {
	if m.val == m.max || p.Value < m.val {
		m.val = p.Value
	}
}

func (m minReduceFunc[N]) Val() N {
	return m.val
}

func (m *minReduceFunc[N]) Reset() {
	m.val = m.max
}
