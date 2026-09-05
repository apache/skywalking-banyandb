// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package nativeice

import (
	"testing"

	roaringpkg "github.com/RoaringBitmap/roaring"
)

const oneHitDocumentBitWidth = 31

// TestUnionPostingsDecodesOneHitDocumentNumber proves that the one-hit FST
// value keeps the local document number in its low 31 bits, below the norm.
func TestUnionPostingsDecodesOneHitDocumentNumber(t *testing.T) {
	const (
		oneHitDocumentNumber = uint64(7)
		oneHitNormBits       = uint64(3)
	)
	fstValue := fstValueEncodingOneHit | oneHitNormBits<<oneHitDocumentBitWidth | oneHitDocumentNumber
	reader := storedSegmentReader{
		path:   "one-hit selection test",
		footer: segmentFooter{documentCount: oneHitDocumentNumber + 1},
	}
	selected := roaringpkg.New()

	if postingsErr := reader.unionPostings(selected, fstValue); postingsErr != nil {
		t.Fatal(postingsErr)
	}
	if !selected.Contains(uint32(oneHitDocumentNumber)) {
		t.Fatalf("one-hit FST value selected %v, want document %d", selected.ToArray(), oneHitDocumentNumber)
	}
	if selected.Contains(uint32(oneHitNormBits)) {
		t.Fatalf("one-hit FST value selected norm bits %d as a document: %v", oneHitNormBits, selected.ToArray())
	}
	if selected.GetCardinality() != 1 {
		t.Fatalf("one-hit FST value selected %d documents, want 1", selected.GetCardinality())
	}
}
