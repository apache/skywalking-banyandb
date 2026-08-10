// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package trace

import (
	"bytes"
	"unsafe"

	"github.com/apache/skywalking-banyandb/pkg/convert"
	pkgpool "github.com/apache/skywalking-banyandb/pkg/pool"
)

const maxPooledDroppedTraceIDBytes = 4 << 20

type droppedTraceIDs struct {
	ids   []string
	slots []uint64
}

var droppedTraceIDPool = pkgpool.RegisterBounded("trace-dropped-ids", maxPooledDroppedTraceIDBytes, func() *droppedTraceIDs {
	return &droppedTraceIDs{}
}, func(dropped *droppedTraceIDs) int64 {
	return int64(cap(dropped.ids))*int64(unsafe.Sizeof("")) + int64(cap(dropped.slots))*int64(unsafe.Sizeof(uint64(0)))
})

func acquireDroppedTraceIDs() *droppedTraceIDs {
	return droppedTraceIDPool.Get()
}

func recordDroppedTraceID(droppedSet **droppedTraceIDs, traceID string) {
	if *droppedSet == nil {
		*droppedSet = acquireDroppedTraceIDs()
	}
	(*droppedSet).add(traceID)
}

func releaseDroppedTraceIDs(dropped *droppedTraceIDs) {
	if dropped == nil {
		return
	}
	clear(dropped.ids)
	clear(dropped.slots)
	dropped.ids = dropped.ids[:0]
	dropped.slots = dropped.slots[:0]
	droppedTraceIDPool.Put(dropped)
}

func (dropped *droppedTraceIDs) len() int {
	if dropped == nil {
		return 0
	}
	return len(dropped.ids)
}

func (dropped *droppedTraceIDs) add(traceID string) {
	if len(dropped.slots) > 0 {
		panic("cannot add a dropped trace ID after building the lookup index")
	}
	if len(dropped.ids) > 0 {
		lastTraceID := dropped.ids[len(dropped.ids)-1]
		if traceID == lastTraceID {
			return
		}
		if traceID < lastTraceID {
			panic("dropped trace IDs must be added in ascending order")
		}
	}
	dropped.ids = append(dropped.ids, traceID)
}

func (dropped *droppedTraceIDs) keepEncoded(data []byte) bool {
	if len(data) == 0 || idFormat(data[0]) != idFormatV1 || dropped == nil || len(dropped.ids) == 0 {
		return true
	}
	dropped.buildIndex()
	traceID := data[1:]
	mask := uint64(len(dropped.slots) - 1)
	traceHash := convert.Hash(traceID)
	slotIdx := traceHash & mask
	for {
		stored := dropped.slots[slotIdx]
		if stored == 0 {
			return true
		}
		storedIdx := uint32(stored)
		if uint32(stored>>32) == uint32(traceHash>>32) && bytes.Equal(convert.StringToBytes(dropped.ids[storedIdx-1]), traceID) {
			return false
		}
		slotIdx = (slotIdx + 1) & mask
	}
}

func (dropped *droppedTraceIDs) buildIndex() {
	if len(dropped.slots) > 0 {
		return
	}
	slotCount := 2
	for slotCount < len(dropped.ids)*2 {
		slotCount *= 2
	}
	if cap(dropped.slots) < slotCount {
		dropped.slots = make([]uint64, slotCount)
	} else {
		dropped.slots = dropped.slots[:slotCount]
		clear(dropped.slots)
	}
	mask := uint64(slotCount - 1)
	for traceIdx, traceID := range dropped.ids {
		traceHash := convert.HashStr(traceID)
		slotIdx := traceHash & mask
		for dropped.slots[slotIdx] != 0 {
			slotIdx = (slotIdx + 1) & mask
		}
		dropped.slots[slotIdx] = uint64(uint32(traceHash>>32))<<32 | uint64(uint32(traceIdx+1))
	}
}
