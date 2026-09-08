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
	"context"
	"errors"
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

	if postingsErr := reader.unionPostings(context.Background(), selected, fstValue); postingsErr != nil {
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

func TestDecodePostingBitmapHonorsCancellation(t *testing.T) {
	postings := roaringpkg.New()
	for containerIndex := uint32(0); containerIndex < 5000; containerIndex++ {
		postings.Add(containerIndex << 16)
	}
	encoded, marshalErr := postings.MarshalBinary()
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if len(encoded) <= selectionDecodeReadSize {
		t.Fatalf("posting encoding has %d bytes, want more than one bounded read", len(encoded))
	}

	ctx := &cancelAfterChecksContext{Context: context.Background(), cancelAt: 2}
	_, decodeErr := decodePostingBitmap(ctx, encoded)
	if !errors.Is(decodeErr, context.Canceled) {
		t.Fatalf("decodePostingBitmap() error = %v, want context.Canceled", decodeErr)
	}
}

func TestUnionPostingBitmapHonorsCancellationBetweenBatches(t *testing.T) {
	postings := roaringpkg.New()
	postings.AddRange(0, selectionPostingBatchSize*2)
	selected := roaringpkg.New()
	ctx := &cancelAfterChecksContext{Context: context.Background(), cancelAt: 2}

	unionErr := unionPostingBitmap(ctx, selected, postings, selectionPostingBatchSize*2, "cancellation test")
	if !errors.Is(unionErr, context.Canceled) {
		t.Fatalf("unionPostingBitmap() error = %v, want context.Canceled", unionErr)
	}
	if selected.GetCardinality() != selectionPostingBatchSize {
		t.Fatalf("selected %d documents before cancellation, want %d", selected.GetCardinality(), selectionPostingBatchSize)
	}
}

type cancelAfterChecksContext struct {
	context.Context
	checks   int
	cancelAt int
}

func (c *cancelAfterChecksContext) Err() error {
	c.checks++
	if c.checks >= c.cancelAt {
		return context.Canceled
	}
	return nil
}
