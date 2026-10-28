// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package native

import (
	"context"
	"slices"
	"unicode/utf8"
	"unsafe"
)

type wildcardStateKey struct {
	bits   []uint64
	prefix uint32
	size   uint8
}
type wildcardState struct {
	key   wildcardStateKey
	next  [256]int32
	chain int32
}

type wildcardAutomaton struct {
	ctx     context.Context
	budget  *WildcardBudget
	program *wildcardProgram
	err     error
	states  []*wildcardState
	buckets []int32
	scratch []uint64
	bytes   uint64
}

func newWildcardAutomaton(ctx context.Context, budget *WildcardBudget, pattern WildcardPattern) (*wildcardAutomaton, error) {
	program, err := budget.program(ctx, pattern)
	if err != nil {
		return nil, err
	}
	initialBytes := wildcardAllocationSize(uint64(unsafe.Sizeof(wildcardAutomaton{})))
	programBytes := uint64(len(pattern.Pattern))*16 + 512
	if programBytes > wildcardAutomatonBytes-initialBytes {
		return nil, ErrQueryLimit
	}
	if err = budget.reserve(ctx, initialBytes); err != nil {
		return nil, err
	}
	automaton := &wildcardAutomaton{
		ctx: ctx, budget: budget, program: program,
		bytes: initialBytes + programBytes,
	}
	if program.exact {
		return automaton, nil
	}
	words := (len(program.tokens) + 1 + 63) / 64
	if err = automaton.reserve(256*uint64(unsafe.Sizeof(int32(0))) + uint64(words)*8); err != nil {
		return nil, err
	}
	automaton.buckets = make([]int32, 256)
	automaton.scratch = make([]uint64, words)
	start := wildcardStateKey{bits: automaton.scratch}
	automaton.addPosition(start.bits, 0)
	automaton.intern(start)
	return automaton, automaton.err
}

// wildcardAllocationSize includes allocator size-class rounding and a small
// allocation header allowance in the tracked automaton ceiling.
func wildcardAllocationSize(size uint64) uint64 { return size + size/4 + 64 }

func (a *wildcardAutomaton) reserve(size uint64) error {
	size = wildcardAllocationSize(size)
	if a.bytes > wildcardAutomatonBytes || size > wildcardAutomatonBytes-a.bytes {
		return ErrQueryLimit
	}
	if err := a.budget.reserve(a.ctx, size); err != nil {
		return err
	}
	a.bytes += size
	return nil
}

func (a *wildcardAutomaton) addPosition(bits []uint64, position int) {
	bits[position/64] |= uint64(1) << (position % 64)
	if position < len(a.program.tokens) && a.program.tokens[position].kind == wildcardStar {
		position++
		bits[position/64] |= uint64(1) << (position % 64)
	}
}

func (a *wildcardAutomaton) intern(key wildcardStateKey) int {
	var hash uint64 = uint64(key.prefix)*1099511628211 + uint64(key.size)
	for _, word := range key.bits {
		hash = (hash ^ word) * 1099511628211
	}
	bucket := byte(hash ^ (hash >> 32))
	for index := a.buckets[bucket]; index != 0; index = a.states[index-1].chain {
		known := &a.states[index-1].key
		if known.prefix == key.prefix && known.size == key.size && slices.Equal(known.bits, key.bits) {
			return int(index)
		}
	}
	if err := a.reserve(uint64(unsafe.Sizeof(wildcardState{})) + uint64(len(key.bits))*8 + 16); err != nil {
		a.err = err
		return 0
	}
	if len(a.states) == cap(a.states) {
		capacity := max(16, cap(a.states)*2)
		if err := a.reserve(uint64(capacity) * uint64(unsafe.Sizeof((*wildcardState)(nil)))); err != nil {
			a.err = err
			return 0
		}
		grown := make([]*wildcardState, len(a.states), capacity)
		copy(grown, a.states)
		a.states = grown
	}
	key.bits = slices.Clone(key.bits)
	a.states = append(a.states, &wildcardState{key: key, chain: a.buckets[bucket]})
	index := int32(len(a.states))
	a.buckets[bucket] = index
	return int(index)
}

func (a *wildcardAutomaton) Start() int {
	if a.err != nil {
		return 0
	}
	return 1
}
func (a *wildcardAutomaton) CanMatch(state int) bool { return a.err == nil && state > 0 }
func (*wildcardAutomaton) WillAlwaysMatch(int) bool  { return false }
func (a *wildcardAutomaton) IsMatch(state int) bool {
	if !a.CanMatch(state) {
		return false
	}
	key := &a.states[state-1].key
	end := len(a.program.tokens)
	return key.size == 0 && key.bits[end/64]&(uint64(1)<<(end%64)) != 0
}

func (a *wildcardAutomaton) Accept(state int, input byte) int {
	if !a.CanMatch(state) {
		return 0
	}
	if err := a.budget.spendWork(a.ctx, 1); err != nil {
		a.err = err
		return 0
	}
	current := a.states[state-1]
	if cached := current.next[input]; cached != 0 {
		return int(cached - 1)
	}
	next := a.transition(current.key, input)
	current.next[input] = int32(next + 1)
	return next
}

func (a *wildcardAutomaton) transition(key wildcardStateKey, input byte) int {
	if key.size == 0 && input >= utf8.RuneSelf {
		if input < 0xc2 || input > 0xf4 {
			return 0
		}
		key.prefix, key.size = uint32(input), 1
		return a.intern(key)
	}
	var value rune
	if key.size > 0 {
		if input < 0x80 || input > 0xbf {
			return 0
		}
		first := byte(key.prefix)
		if key.size == 1 && (first == 0xe0 && input < 0xa0 || first == 0xed && input > 0x9f || first == 0xf0 && input < 0x90 || first == 0xf4 && input > 0x8f) {
			return 0
		}
		key.prefix |= uint32(input) << (8 * key.size)
		key.size++
		needed := uint8(2)
		if first >= 0xe0 {
			needed = 3
		}
		if first >= 0xf0 {
			needed = 4
		}
		if key.size < needed {
			return a.intern(key)
		}
		bytes := [4]byte{byte(key.prefix), byte(key.prefix >> 8), byte(key.prefix >> 16), byte(key.prefix >> 24)}
		value, _ = utf8.DecodeRune(bytes[:needed])
	} else {
		value = rune(input)
	}
	return a.transitionRune(key, value)
}

func (a *wildcardAutomaton) transitionRune(key wildcardStateKey, value rune) int {
	if err := a.budget.spendWork(a.ctx, uint64(len(a.program.tokens))); err != nil {
		a.err = err
		return 0
	}
	if a.program.ignoreCase {
		value = foldWildcardRune(value)
	}
	clear(a.scratch)
	next := wildcardStateKey{bits: a.scratch}
	for position, token := range a.program.tokens {
		if key.bits[position/64]&(uint64(1)<<(position%64)) == 0 {
			continue
		}
		switch token.kind {
		case wildcardStar:
			if value != '\n' || a.program.matchNewline {
				a.addPosition(next.bits, position)
			}
		case wildcardAny:
			if value != '\n' || a.program.matchNewline {
				a.addPosition(next.bits, position+1)
			}
		case wildcardLiteral:
			if token.value == value {
				a.addPosition(next.bits, position+1)
			}
		}
	}
	for _, word := range next.bits {
		if word != 0 {
			return a.intern(next)
		}
	}
	return 0
}
