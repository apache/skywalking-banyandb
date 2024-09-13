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

package stream

import (
	"math"
	"sort"

	"github.com/apache/skywalking-banyandb/pkg/run"
)

// MergePolicy aims to choose an optimal combination
// that has the lowest write amplification.
type mergePolicy struct {
	maxParts           int
	minMergeMultiplier float64
	maxFanOutSize      run.Bytes
}

// NewDefaultMergePolicy create a MergePolicy with default parameters.
func newDefaultMergePolicy() *mergePolicy {
	return newMergePolicy(15, 1.7, run.Bytes(math.MaxInt64))
}

func newDefaultMergePolicyForTesting() *mergePolicy {
	return newMergePolicy(3, 1, run.Bytes(math.MaxInt64))
}

func newDisabledMergePolicyForTesting() *mergePolicy {
	return newMergePolicy(0, 0, 0)
}

// NewMergePolicy creates a MergePolicy with given parameters.
func newMergePolicy(maxParts int, minMergeMul float64, maxFanOutSize run.Bytes) *mergePolicy {
	return &mergePolicy{
		maxParts:           maxParts,
		minMergeMultiplier: minMergeMul,
		maxFanOutSize:      maxFanOutSize,
	}
}

func (l *mergePolicy) getPartsToMerge(dst, src []*partWrapper, freeDiskSize uint64) []*partWrapper {
	if len(src) < 2 {
		return dst
	}

	maxFanOut := min(freeDiskSize, uint64(l.maxFanOutSize))
	// Filter out too big parts.
	// This should reduce N for O(N^2) algorithm below.
	maxInPartBytes := uint64(float64(maxFanOut) / l.minMergeMultiplier)
	tmp := make([]*partWrapper, 0, len(src))
	for _, pw := range src {
		if pw.p.partMetadata.CompressedSizeBytes > maxInPartBytes {
			continue
		}
		tmp = append(tmp, pw)
	}
	src = tmp

	sortPartsForOptimalMerge(src)

	maxSrcParts := l.maxParts
	if maxSrcParts > len(src) {
		maxSrcParts = len(src)
	}
	minSrcParts := (maxSrcParts + 1) / 2
	if minSrcParts < 2 {
		minSrcParts = 2
	}

	// Exhaustive search for parts giving the lowest write amplification when merged.
	var pws []*partWrapper
	maxM := float64(0)
	for i := minSrcParts; i <= maxSrcParts; i++ {
		for j := 0; j <= len(src)-i; j++ {
			a := src[j : j+i]
			if a[0].p.partMetadata.CompressedSizeBytes*uint64(len(a)) < a[len(a)-1].p.partMetadata.CompressedSizeBytes {
				// Do not merge parts with too big difference in size,
				// since this results in unbalanced merges.
				continue
			}
			outSize := sumCompressedSize(a)
			if outSize > maxFanOut {
				// There is no need in verifying remaining parts with bigger sizes.
				break
			}
			m := float64(outSize) / float64(a[len(a)-1].p.partMetadata.CompressedSizeBytes)
			if m < maxM {
				continue
			}
			maxM = m
			pws = a
		}
	}

	minM := float64(l.maxParts) / 2
	if minM < l.minMergeMultiplier {
		minM = l.minMergeMultiplier
	}
	if maxM < minM {
		// There is no sense in merging parts with too small m,
		// since this leads to high disk write IO.
		return dst
	}
	return append(dst, pws...)
}

func sortPartsForOptimalMerge(pws []*partWrapper) {
	// Sort src parts by size and backwards timestamp.
	// This should improve adjacent points' locality in the merged parts.
	sort.Slice(pws, func(i, j int) bool {
		a := &pws[i].p.partMetadata
		b := &pws[j].p.partMetadata
		if a.CompressedSizeBytes == b.CompressedSizeBytes {
			return a.MinTimestamp > b.MinTimestamp
		}
		return a.CompressedSizeBytes < b.CompressedSizeBytes
	})
}

func sumCompressedSize(pws []*partWrapper) uint64 {
	n := uint64(0)
	for _, pw := range pws {
		n += pw.p.partMetadata.CompressedSizeBytes
	}
	return n
}
