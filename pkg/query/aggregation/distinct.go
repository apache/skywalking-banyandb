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

// Distinct accumulates the distinct values of one group for COUNT_DISTINCT
// (design doc §7.3). Unlike Map/Reduce, it is not Number-parameterized —
// the target may be a string, bytes, or tag/field value, not just int64 or
// float64 — so callers encode a value to its byte key themselves (see
// pkg/query/vectorized/measure/groupby.go's appendKeyComponent, the same
// encoder BatchAggregation's group key already uses) and pass the encoded
// bytes here.
type Distinct interface {
	// In records key as seen. It returns true the first time a given key
	// is seen, false for a repeat — the caller uses this to charge memory
	// only for values that actually grow the set.
	In(key []byte) (added bool)
	// Val returns the number of distinct keys seen so far.
	Val() int64
	// Reset clears the set, discarding all keys seen so far.
	Reset()
}

type distinctFunc struct {
	seen map[string]struct{}
}

// NewDistinct returns a Distinct that tracks exact distinct byte keys in
// memory. There is no function-dispatch parameter — unlike NewMap/NewReduce,
// COUNT_DISTINCT has exactly one behavior regardless of the target's type.
func NewDistinct() Distinct {
	d := &distinctFunc{}
	d.Reset()
	return d
}

func (d *distinctFunc) In(key []byte) bool {
	k := string(key)
	if _, ok := d.seen[k]; ok {
		return false
	}
	d.seen[k] = struct{}{}
	return true
}

func (d *distinctFunc) Val() int64 { return int64(len(d.seen)) }

func (d *distinctFunc) Reset() { d.seen = make(map[string]struct{}) }
