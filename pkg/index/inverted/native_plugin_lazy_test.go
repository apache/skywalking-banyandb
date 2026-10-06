// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. Apache Software
// Foundation (ASF) licenses this file to you under the Apache License, Version
// 2.0 (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package inverted

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	roaringpkg "github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/internal/nativeice"
)

func TestNativePluginLoadDoesNotDecodeEveryDocument(t *testing.T) {
	const documentCount = 2000
	documents := make([]nativeice.EncodeDocument, documentCount)
	for documentIndex := range documents {
		identifier := []byte(fmt.Sprintf("series-%06d", documentIndex))
		documents[documentIndex] = nativeice.EncodeDocument{
			Identifier: identifier,
			Fields: []nativeice.EncodeField{{
				Name: "series", Value: identifier, Index: true,
				Terms: []nativeice.EncodeTerm{{Value: identifier, Frequency: 1}},
			}},
		}
	}
	payload, encodeErr := nativeice.EncodeSegment(nativeice.Generation{Documents: documents})
	require.NoError(t, encodeErr)

	allocations := testing.AllocsPerRun(3, func() {
		loaded, loadErr := nativeSegmentPluginLoad(newSegmentBytes(payload))
		require.NoError(t, loadErr)
		require.Equal(t, uint64(documentCount), loaded.Count())
	})
	// A reopen only parses segment framing and must not allocate once per
	// document. The old eager loader exceeded this bound by orders of magnitude.
	require.Less(t, allocations, float64(documentCount)/2)

	loaded, loadErr := nativeSegmentPluginLoad(newSegmentBytes(payload))
	require.NoError(t, loadErr)
	matching, matchErr := loaded.DocsMatchingTerms([]segmentTerm{nativeTestTerm{field: "series", term: []byte("series-001234")}})
	require.NoError(t, matchErr)
	require.Equal(t, []uint32{1234}, matching.ToArray())
}

func TestNativePluginMergeAndDedupCanQueryLoadedSegmentConcurrently(t *testing.T) {
	payload, encodeErr := nativeice.EncodeSegment(nativeice.Generation{Documents: []nativeice.EncodeDocument{{
		Identifier: []byte("id"),
		Fields:     []nativeice.EncodeField{{Name: "series", Value: []byte("series"), Index: true, Terms: []nativeice.EncodeTerm{{Value: []byte("series"), Frequency: 1}}}},
	}}})
	require.NoError(t, encodeErr)
	loaded, loadErr := nativeSegmentPluginLoad(newSegmentBytes(payload))
	require.NoError(t, loadErr)
	segment := loaded.(*nativePluginSegment)
	terms := []segmentTerm{nativeTestTerm{field: "series", term: []byte("series")}}
	errorsCh := make(chan error, 40)
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		for range 20 {
			if materializeErr := segment.materialize(nil); materializeErr != nil {
				errorsCh <- materializeErr
			}
		}
	}()
	go func() {
		defer wait.Done()
		for range 20 {
			matching, matchErr := segment.DocsMatchingTerms(terms)
			if matchErr != nil {
				errorsCh <- matchErr
				continue
			}
			if got := matching.ToArray(); len(got) != 1 || got[0] != 0 {
				errorsCh <- fmt.Errorf("unexpected matching documents: %v", got)
			}
		}
	}()
	wait.Wait()
	close(errorsCh)
	for testErr := range errorsCh {
		require.NoError(t, testErr)
	}
}

func TestNativePluginExactLookupsReuseDictionaryAllocations(t *testing.T) {
	const documentCount = 200_000
	documents := make([]nativeice.EncodeDocument, documentCount)
	multiplier := uint64(0x9e3779b97f4a7c15)
	for documentIndex := range documents {
		identifier := []byte(fmt.Sprintf("series-%08d-%064x", documentIndex, uint64(documentIndex)*multiplier))
		documents[documentIndex] = nativeice.EncodeDocument{
			Identifier: identifier,
			Fields: []nativeice.EncodeField{{
				Name: "series", Value: identifier, Index: true,
				Terms: []nativeice.EncodeTerm{{Value: identifier, Frequency: 1}},
			}},
		}
	}
	payload, encodeErr := nativeice.EncodeSegment(nativeice.Generation{Documents: documents})
	require.NoError(t, encodeErr)
	loaded, loadErr := nativeSegmentPluginLoad(newSegmentBytes(payload))
	require.NoError(t, loadErr)
	segment := loaded.(*nativePluginSegment)
	dictionary, dictionaryErr := segment.Dictionary("series")
	require.NoError(t, dictionaryErr)
	term := nativeTestTerm{field: "series", term: []byte(fmt.Sprintf("series-%08d-%064x", 42_000, uint64(42_000)*multiplier))}
	postings, postingsErr := dictionary.PostingsList(term.Term(), nil, nil)
	require.NoError(t, postingsErr)
	require.Equal(t, uint64(1), postings.Count())
	postingsAllocs := testing.AllocsPerRun(100, func() {
		lookupPostings, lookupErr := dictionary.PostingsList(term.Term(), nil, nil)
		require.NoError(t, lookupErr)
		iterator, iteratorErr := lookupPostings.Iterator(true, true, false, nil)
		require.NoError(t, iteratorErr)
		posting, nextErr := iterator.Next()
		require.NoError(t, nextErr)
		require.NotNil(t, posting)
		require.Equal(t, uint64(42_000), posting.Number())
		require.Equal(t, 1, posting.Frequency())
		require.NoError(t, iterator.Close())
	})
	require.LessOrEqual(t, postingsAllocs, float64(10), "loaded exact postings should avoid bitmap and frequency-map allocations")

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 20 {
		postings, postingsErr = dictionary.PostingsList(term.Term(), nil, nil)
		require.NoError(t, postingsErr)
		require.Equal(t, uint64(1), postings.Count())
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	t.Logf("warm exact allocation bytes: %d", after.TotalAlloc-before.TotalAlloc)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20), "warm exact lookups reloaded the term dictionary")

	missing, missingErr := dictionary.PostingsList([]byte("missing"), nil, nil)
	require.NoError(t, missingErr)
	require.Equal(t, uint64(0), missing.Count())
	require.Same(t, emptyNativePluginPostings, missing)
	missingTerm := []byte("missing")
	missingAllocs := testing.AllocsPerRun(100, func() {
		lookup, lookupErr := dictionary.PostingsList(missingTerm, nil, nil)
		require.NoError(t, lookupErr)
		require.Same(t, emptyNativePluginPostings, lookup)
		require.Zero(t, lookup.Count())
	})
	t.Logf("missing exact postings allocations: %.1f", missingAllocs)
	require.LessOrEqual(t, missingAllocs, float64(2), "missing exact postings should use the immutable empty result")
	// A miss must not consume the caller's reusable hit object: the next hit
	// still needs to be returned correctly without forcing a fresh allocation.
	reusable, reusableErr := dictionary.PostingsList(term.Term(), nil, nil)
	require.NoError(t, reusableErr)
	require.Equal(t, uint64(1), reusable.Count())
	missWithReuse, missWithReuseErr := dictionary.PostingsList(missingTerm, nil, reusable)
	require.NoError(t, missWithReuseErr)
	require.Same(t, emptyNativePluginPostings, missWithReuse)
	require.Equal(t, uint64(1), reusable.Count())
	hitAfterMiss, hitAfterMissErr := dictionary.PostingsList(term.Term(), nil, reusable)
	require.NoError(t, hitAfterMissErr)
	require.Equal(t, uint64(1), hitAfterMiss.Count())
}

func TestNativePluginLoadedDictionarySearchPrunesRareRange(t *testing.T) {
	const termCount = 5000
	documents := make([]nativeice.EncodeDocument, termCount)
	for documentIndex := range documents {
		term := []byte(fmt.Sprintf("term-%05d", documentIndex))
		documents[documentIndex] = nativeice.EncodeDocument{
			Identifier: []byte(fmt.Sprintf("id-%05d", documentIndex)),
			Fields: []nativeice.EncodeField{{
				Name: "terms", Index: true,
				Terms: []nativeice.EncodeTerm{{Value: term, Frequency: 1}},
			}},
		}
	}
	payload, encodeErr := nativeice.EncodeSegment(nativeice.Generation{Documents: documents})
	require.NoError(t, encodeErr)
	loaded, loadErr := nativeSegmentPluginLoad(newSegmentBytes(payload))
	require.NoError(t, loadErr)
	dictionary, dictionaryErr := loaded.Dictionary("terms")
	require.NoError(t, dictionaryErr)
	const selected = "term-04999"
	iterator := dictionary.Iterator(reviewPrefixAutomaton{prefix: []byte(selected)}, []byte(selected), []byte("term-05000"))
	entry, nextErr := iterator.Next()
	require.NoError(t, nextErr)
	require.NotNil(t, entry)
	require.Equal(t, selected, entry.Term())
	require.Equal(t, uint64(1), entry.Count())
	entry, nextErr = iterator.Next()
	require.NoError(t, nextErr)
	require.Nil(t, entry)
	require.NoError(t, iterator.Close())

	allocations := testing.AllocsPerRun(20, func() {
		candidate := dictionary.Iterator(reviewPrefixAutomaton{prefix: []byte(selected)}, []byte(selected), []byte("term-05000"))
		found, candidateErr := candidate.Next()
		require.NoError(t, candidateErr)
		require.NotNil(t, found)
		require.Equal(t, selected, found.Term())
		require.Equal(t, uint64(1), found.Count())
		require.NoError(t, candidate.Close())
	})
	// A selective range must not allocate one decoded term/posting per entry
	// in the 5,000-term FST. This bound leaves room for interface and iterator
	// bookkeeping while catching a materializing dictionary implementation.
	require.Less(t, allocations, float64(termCount)/10)
}

func TestNativeICEConcurrentFieldMetadataReads(t *testing.T) {
	payload, encodeErr := nativeice.EncodeSegment(nativeice.Generation{Documents: []nativeice.EncodeDocument{{
		Identifier: []byte("id"),
		Fields:     []nativeice.EncodeField{{Name: "series", Value: []byte("series"), Index: true, Terms: []nativeice.EncodeTerm{{Value: []byte("series"), Frequency: 1}}}},
	}}})
	require.NoError(t, encodeErr)
	reader, openErr := nativeice.OpenSegment(payload)
	require.NoError(t, openErr)
	defer func() { require.NoError(t, reader.Close()) }()
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		for range 100 {
			_, fieldsErr := reader.Fields()
			require.NoError(t, fieldsErr)
		}
	}()
	go func() {
		defer wait.Done()
		for range 100 {
			_, existsErr := reader.TermExists("series", []byte("series"))
			require.NoError(t, existsErr)
		}
	}()
	wait.Wait()
}

func TestNativePluginCachedDictionaryHandlesAreIndependentConcurrently(t *testing.T) {
	const documentCount = 12
	documents := make([]nativeice.EncodeDocument, documentCount)
	for documentIndex := range documents {
		terms := []nativeice.EncodeTerm{{Value: []byte("common"), Frequency: 2}}
		if documentIndex == 0 {
			terms = append(terms, nativeice.EncodeTerm{Value: []byte("single"), Frequency: 1})
		}
		documents[documentIndex] = nativeice.EncodeDocument{
			Identifier: []byte(fmt.Sprintf("id-%02d", documentIndex)),
			Fields:     []nativeice.EncodeField{{Name: "tag", Index: true, Terms: terms}},
		}
	}
	payload, encodeErr := nativeice.EncodeSegment(nativeice.Generation{Documents: documents})
	require.NoError(t, encodeErr)
	loaded, loadErr := nativeSegmentPluginLoad(newSegmentBytes(payload))
	require.NoError(t, loadErr)
	segment := loaded.(*nativePluginSegment)

	const workerCount = 24
	wrappers := make([]segmentDictionary, workerCount)
	for workerIndex := range wrappers {
		wrappers[workerIndex], loadErr = segment.Dictionary("tag")
		require.NoError(t, loadErr)
	}
	start := make(chan struct{})
	errorsCh := make(chan error, workerCount)
	var wait sync.WaitGroup
	for workerIndex, dictionary := range wrappers {
		wait.Add(1)
		go func(workerIndex int, dictionary segmentDictionary) {
			defer wait.Done()
			<-start
			for lookup := 0; lookup < 40; lookup++ {
				term := []byte("common")
				var except *roaringpkg.Bitmap
				want := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
				switch (workerIndex + lookup) % 5 {
				case 1:
					term = []byte("missing")
					want = nil
				case 2:
					except = roaringpkg.New()
					except.Add(2)
					except.Add(9)
					want = []uint64{0, 1, 3, 4, 5, 6, 7, 8, 10, 11}
				case 3:
					term = []byte("single")
					want = []uint64{0}
				case 4:
					term = []byte("single")
					except = roaringpkg.New()
					except.Add(0)
					want = nil
				}
				postings, postingsErr := dictionary.PostingsList(term, except, nil)
				if postingsErr != nil {
					errorsCh <- postingsErr
					return
				}
				if got := postings.Count(); got != uint64(len(want)) {
					errorsCh <- fmt.Errorf("worker %d lookup %q count = %d, want %d", workerIndex, term, got, len(want))
					return
				}
				iterator, iteratorErr := postings.Iterator(true, true, false, nil)
				if iteratorErr != nil {
					errorsCh <- iteratorErr
					return
				}
				for wantIndex, wantDocument := range want {
					posting, nextErr := iterator.Next()
					if nextErr != nil {
						_ = iterator.Close()
						errorsCh <- nextErr
						return
					}
					if posting == nil || posting.Number() != wantDocument {
						_ = iterator.Close()
						errorsCh <- fmt.Errorf("worker %d lookup %q posting %d = %v, want %d", workerIndex, term, wantIndex, posting, wantDocument)
						return
					}
				}
				if closeErr := iterator.Close(); closeErr != nil {
					errorsCh <- closeErr
					return
				}
			}
		}(workerIndex, dictionary)
	}
	close(start)
	wait.Wait()
	close(errorsCh)
	for testErr := range errorsCh {
		require.NoError(t, testErr)
	}
	for _, dictionary := range wrappers {
		require.NoError(t, dictionary.Close())
	}
	// The cached handles share the parent FST but all active decoder readers
	// have returned to their pool before the pinned reader is closed.
	require.NoError(t, segment.reader.Close())
}

func TestNativePluginPostingsIteratorSupportsBitmapOptimization(t *testing.T) {
	const documentCount = 4
	documents := make([]nativeice.EncodeDocument, documentCount)
	for documentIndex := range documents {
		terms := []nativeice.EncodeTerm{{Value: []byte("common"), Frequency: 7}}
		if documentIndex == 0 {
			terms = append(terms, nativeice.EncodeTerm{Value: []byte("single"), Frequency: 1})
		}
		documents[documentIndex] = nativeice.EncodeDocument{
			Identifier: []byte(fmt.Sprintf("id-%d", documentIndex)),
			Fields:     []nativeice.EncodeField{{Name: "tag", Index: true, Terms: terms}},
		}
	}
	payload, encodeErr := nativeice.EncodeSegment(nativeice.Generation{Documents: documents})
	require.NoError(t, encodeErr)
	loaded, loadErr := nativeSegmentPluginLoad(newSegmentBytes(payload))
	require.NoError(t, loadErr)
	segment := loaded.(*nativePluginSegment)
	dictionary, dictionaryErr := segment.Dictionary("tag")
	require.NoError(t, dictionaryErr)

	postings, postingsErr := dictionary.PostingsList([]byte("common"), nil, nil)
	require.NoError(t, postingsErr)
	iterator, iteratorErr := postings.Iterator(true, true, false, nil)
	require.NoError(t, iteratorErr)
	optimized, optimizedOK := iterator.(interface {
		ActualBitmap() *roaringpkg.Bitmap
		DocNum1Hit() (uint64, bool)
		ReplaceActual(*roaringpkg.Bitmap)
	})
	require.True(t, optimizedOK)
	require.Equal(t, uint64(documentCount), postings.Count())
	require.Equal(t, uint64(documentCount), optimized.ActualBitmap().GetCardinality())
	_, oneHit := optimized.DocNum1Hit()
	require.False(t, oneHit)
	optimized.ReplaceActual(roaringpkg.BitmapOf(1, 3))
	require.Equal(t, uint64(2), iterator.Count())
	for _, want := range []uint64{1, 3} {
		posting, nextErr := iterator.Next()
		require.NoError(t, nextErr)
		require.NotNil(t, posting)
		require.Equal(t, want, posting.Number())
		require.Equal(t, 7, posting.Frequency())
	}
	posting, nextErr := iterator.Next()
	require.NoError(t, nextErr)
	require.Nil(t, posting)
	optimized.ReplaceActual(nil)
	require.True(t, iterator.Empty())
	require.Zero(t, iterator.Count())
	require.Zero(t, iterator.Size())
	posting, nextErr = iterator.Next()
	require.NoError(t, nextErr)
	require.Nil(t, posting)
	require.NoError(t, iterator.Close())

	single, singleErr := dictionary.PostingsList([]byte("single"), nil, nil)
	require.NoError(t, singleErr)
	singleIterator, singleIteratorErr := single.Iterator(true, true, false, nil)
	require.NoError(t, singleIteratorErr)
	singleOptimized, optimizedOK := singleIterator.(interface {
		DocNum1Hit() (uint64, bool)
	})
	require.True(t, optimizedOK)
	documentNumber, oneHit := singleOptimized.DocNum1Hit()
	require.True(t, oneHit)
	require.Equal(t, uint64(0), documentNumber)
	require.NoError(t, singleIterator.Close())

	missing, missingErr := dictionary.PostingsList([]byte("missing"), nil, nil)
	require.NoError(t, missingErr)
	missingIterator, missingIteratorErr := missing.Iterator(true, true, false, nil)
	require.NoError(t, missingIteratorErr)
	require.True(t, missingIterator.Empty())
	require.NoError(t, missingIterator.Close())
}

type nativeTestTerm struct {
	field string
	term  []byte
}

func (t nativeTestTerm) Field() string { return t.field }
func (t nativeTestTerm) Term() []byte  { return t.term }
