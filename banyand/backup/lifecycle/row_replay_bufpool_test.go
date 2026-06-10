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

package lifecycle

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassExp(t *testing.T) {
	assert.Equal(t, uint(8), classExp(1))    // floored at min class
	assert.Equal(t, uint(8), classExp(256))  // 1<<8 == 256
	assert.Equal(t, uint(9), classExp(257))  // needs 1<<9
	assert.Equal(t, uint(10), classExp(580)) // typical measure body
	assert.Equal(t, uint(13), classExp(8192))
	assert.Equal(t, uint(19), classExp(491296)) // ~480KiB outlier
}

func TestMarshalBufferPoolReuseAndCapacity(t *testing.T) {
	p := newMarshalBufferPool(32 << 20)
	// Borrow for a 580B body: fresh buffer, cap >= 580, len 0.
	b := p.borrow(580)
	require.GreaterOrEqual(t, cap(b), 580)
	require.Len(t, b, 0)
	first := cap(b)
	b = append(b, make([]byte, 580)...)
	p.returnAll([][]byte{b})
	// Next borrow of the same class reuses the same backing array (same cap).
	b2 := p.borrow(580)
	assert.Equal(t, first, cap(b2), "should reuse the returned buffer of the same class")
	assert.Len(t, b2, 0)
}

func TestMarshalBufferPoolSizeClassSeparation(t *testing.T) {
	p := newMarshalBufferPool(32 << 20)
	small := p.borrow(580)  // class 10
	big := p.borrow(491296) // class 19
	smallCap, bigCap := cap(small), cap(big)
	p.returnAll([][]byte{small, big})
	// A small borrow must NOT pick up the big buffer (no pinning a 480KB buffer
	// for a 580B body).
	s2 := p.borrow(580)
	assert.Equal(t, smallCap, cap(s2))
	assert.Less(t, cap(s2), bigCap)
}

func TestMarshalBufferPoolLazyReclaim(t *testing.T) {
	p := newMarshalBufferPool(32 << 20)
	// Touch the big class once.
	big := p.borrow(491296)
	bigK := classExp(cap(big))
	p.returnAll([][]byte{big})
	require.Contains(t, p.classes, bigK)
	// Run reclaimAfter flushes that only touch the small class; the big class
	// should be dropped (its memory released to GC).
	for i := 0; i < bufPoolReclaimAfter; i++ {
		small := p.borrow(580)
		p.returnAll([][]byte{small})
	}
	assert.NotContains(t, p.classes, bigK, "idle big class should be reclaimed")
	assert.Contains(t, p.classes, classExp(1024), "active small class should remain")
}

func TestMarshalBufferPoolFreeLimit(t *testing.T) {
	const budget = 32 << 20
	p := newMarshalBufferPool(budget)
	bigK := classExp(491296) // class 19
	limit := p.freeLimit(bigK)
	require.Equal(t, budget>>bigK, limit) // 32MiB / 512KiB == 64

	// Return far more big buffers than the class limit in one batch; the free list
	// must be capped at the limit, the excess dropped to GC.
	lent := make([][]byte, limit*3)
	for i := range lent {
		lent[i] = make([]byte, 0, 1<<bigK)
	}
	p.returnAll(lent)
	assert.Len(t, p.classes[bigK].free, limit, "free list must be capped at freeLimit; excess dropped")
}
