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

package bydbql

import (
	"math"
	"reflect"
)

type retainedAllocation struct {
	typ     reflect.Type
	address uintptr
}

type retainedExtent struct {
	bytes  int
	length int
}

type retainedSizer struct {
	seen map[retainedAllocation]retainedExtent
}

func deepSize(value reflect.Value) int {
	return (&retainedSizer{seen: make(map[retainedAllocation]retainedExtent)}).references(value)
}

// references counts out-of-line data. The containing struct/array already pays
// for inline fields. Pointer identity prevents shared syntax and cycles from
// charging repeatedly; partial overlaps may conservatively count more than once.
func (s *retainedSizer) references(value reflect.Value) int {
	switch value.Kind() {
	case reflect.Pointer:
		if value.IsNil() || !s.first(value) {
			return 0
		}
		element := value.Elem()
		return addRetainedSize(int(element.Type().Size()), s.references(element))
	case reflect.Array:
		total := 0
		for i := 0; i < value.Len(); i++ {
			total = addRetainedSize(total, s.references(value.Index(i)))
		}
		return total
	case reflect.Struct:
		// Generated messages own their fields, but not MessageState's process-wide
		// reflection descriptors. Traversing those can retain a whole descriptor graph.
		if value.Type().PkgPath() == "google.golang.org/protobuf/internal/impl" {
			return 0
		}
		total := 0
		for i := 0; i < value.NumField(); i++ {
			total = addRetainedSize(total, s.references(value.Field(i)))
		}
		return total
	case reflect.Slice:
		if value.IsNil() {
			return 0
		}
		key := retainedAllocation{typ: value.Type(), address: value.Pointer()}
		previous, seen := s.seen[key]
		size := multiplyRetainedSize(value.Cap(), int(value.Type().Elem().Size()))
		if seen && previous.bytes >= size && previous.length >= value.Len() {
			return 0
		}
		s.seen[key] = retainedExtent{bytes: max(previous.bytes, size), length: max(previous.length, value.Len())}
		total := max(0, size-previous.bytes)
		for i := previous.length; i < value.Len(); i++ {
			total = addRetainedSize(total, s.references(value.Index(i)))
		}
		return total
	case reflect.Map:
		if value.IsNil() || !s.first(value) {
			return 0
		}
		// Include the map header, spare buckets and key/value storage. This is
		// deliberately conservative rather than dependent on a Go map layout.
		slots := max(8, multiplyRetainedSize(value.Len(), 2))
		slotSize := addRetainedSize(16, addRetainedSize(int(value.Type().Key().Size()), int(value.Type().Elem().Size())))
		total := addRetainedSize(64, multiplyRetainedSize(slots, slotSize))
		entries := value.MapRange()
		for entries.Next() {
			total = addRetainedSize(total, addRetainedSize(s.references(entries.Key()), s.references(entries.Value())))
		}
		return total
	case reflect.Interface:
		if value.IsNil() {
			return 0
		}
		element := value.Elem()
		return addRetainedSize(int(element.Type().Size()), s.references(element))
	case reflect.String:
		return value.Len()
	default:
		return 0
	}
}

func (s *retainedSizer) first(value reflect.Value) bool {
	key := retainedAllocation{typ: value.Type(), address: value.Pointer()}
	if _, seen := s.seen[key]; seen {
		return false
	}
	s.seen[key] = retainedExtent{}
	return true
}

// Saturation makes an unrepresentable footprint uncacheable instead of allowing
// integer wraparound to turn it into a small or negative admission charge.
func addRetainedSize(left, right int) int {
	if right > math.MaxInt-left {
		return math.MaxInt
	}
	return left + right
}

func multiplyRetainedSize(left, right int) int {
	if left != 0 && right > math.MaxInt/left {
		return math.MaxInt
	}
	return left * right
}
