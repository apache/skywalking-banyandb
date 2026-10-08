// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses
// this file to you under the Apache License, Version 2.0 (the "License"); you
// may not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package stream

import (
	"context"
	"crypto/sha256"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/encoding"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/index/nativeadapter"
	"github.com/apache/skywalking-banyandb/pkg/query/logical"
)

func TestNativeRangeExecuteEndpointsDeletionTimeAndPinnedView(t *testing.T) {
	root := t.TempDir()
	lock, err := fs.NewLocalFileSystem().CreateLockFile(filepath.Join(root, "lock"), 0o600)
	require.NoError(t, err)
	lease, err := native.NewFileRootLease(lock, root)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lease.Revoke())
		require.NoError(t, lock.Close())
	}()

	owner, err := native.NewOwner(native.OwnerOptions{Lease: lease, Path: filepath.Join(root, "index"), CompactionThreshold: -1})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	adapter := &nativeadapter.Adapter{Owner: owner}
	rule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: 7, Name: "latency"},
		Tags:     []string{"latency"},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
	}
	byteRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: 8, Name: "name"},
		Tags:     []string{"name"},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
	}
	key := index.FieldKey{IndexRuleID: 7, SeriesID: 11}
	byteKey := index.FieldKey{IndexRuleID: 8, SeriesID: 11}
	docs := make([]index.Document, 0, 5)
	for id, value := range []int64{10, 20, 30, 40} {
		docs = append(docs, index.Document{
			DocID: uint64(id + 1), Timestamp: (value * 10),
			Fields: []index.Field{
				index.NewIntField(key, value),
				index.NewBytesField(byteKey, []byte{byte('a' + id), byte('a' + id)}),
			},
		})
	}
	otherSeriesKey := key
	otherSeriesKey.SeriesID = 12
	otherSeriesByteKey := byteKey
	otherSeriesByteKey.SeriesID = 12
	docs = append(docs, index.Document{
		DocID: 5, Timestamp: 200,
		Fields: []index.Field{
			index.NewIntField(otherSeriesKey, 20),
			index.NewBytesField(otherSeriesByteKey, []byte("bb")),
		},
	})
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: docs}))
	floatRule := &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Id: 9, Name: "ratio"},
		Tags:     []string{"ratio"},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
	}
	floatKey := index.FieldKey{IndexRuleID: 9, SeriesID: 11}
	floatDocuments := make([]native.Document, 0, 3)
	for offset, value := range []float64{-1.5, 0, 2.25} {
		floatDocuments = append(floatDocuments, native.Document{
			Identifier: convert.Uint64ToBytes(uint64(7 + offset)), Timestamp: int64(500 + offset*100),
			Fields: []native.Field{
				{Name: floatKey.Marshal(), Index: true, Terms: []native.Term{{
					Value: encodeFloatTerm(value), Frequency: 1,
				}}},
				{Name: "_series_id", Index: true, Terms: []native.Term{{
					Value: convert.Uint64ToBytes(11), Frequency: 1,
				}}},
			},
		})
	}
	require.NoError(t, owner.Batch(context.Background(), native.Batch{Documents: floatDocuments}))
	require.NoError(t, waitNativePersistence(owner))
	require.NoError(t, owner.Batch(context.Background(), native.Batch{Deletes: [][]byte{convert.Uint64ToBytes(4)}}))
	require.NoError(t, waitNativePersistence(owner))

	searcher, err := adapter.NewSearcher(context.Background(), nativeadapter.SearcherOptions{MaxTerms: 32, MaxCandidates: 32})
	require.NoError(t, err)
	defer func() { require.NoError(t, searcher.Close()) }()
	fieldRange := func(lower, upper int64, includesLower, includesUpper bool) *rangeOp {
		return newRange(rule, index.NewIntRangeOpts(lower, upper, includesLower, includesUpper))
	}
	byteRange := func(lower, upper []byte, includesLower, includesUpper bool) *rangeOp {
		return newRange(byteRule, index.NewBytesRangeOpts(lower, upper, includesLower, includesUpper))
	}
	search := func(op *rangeOp, tr *index.RangeOpts) ([]uint64, []uint64) {
		list, timestamps, executeErr := op.Execute(func(databasev1.IndexRule_Type) (index.Searcher, error) { return searcher, nil }, 11, tr)
		require.NoError(t, executeErr)
		require.NotNil(t, timestamps)
		return list.ToSlice(), timestamps.ToSlice()
	}
	ids, timestamps := search(fieldRange(10, 30, true, false), nil)
	require.Equal(t, []uint64{1, 2}, ids)
	require.Equal(t, []uint64{100, 200}, timestamps)
	ids, timestamps = search(fieldRange(10, 30, false, true), nil)
	require.Equal(t, []uint64{2, 3}, ids)
	require.Equal(t, []uint64{200, 300}, timestamps)
	ids, timestamps = search(fieldRange(10, 30, true, true), nil)
	require.Equal(t, []uint64{1, 2, 3}, ids)
	require.Equal(t, []uint64{100, 200, 300}, timestamps)
	ids, timestamps = search(fieldRange(10, 30, false, false), nil)
	require.Equal(t, []uint64{2}, ids)
	require.Equal(t, []uint64{200}, timestamps)
	ids, timestamps = search(fieldRange(math.MinInt64, 20, true, true), nil)
	require.Equal(t, []uint64{1, 2}, ids)
	require.Equal(t, []uint64{100, 200}, timestamps)
	ids, timestamps = search(fieldRange(-100, 20, true, true), nil)
	require.Equal(t, []uint64{1, 2}, ids)
	require.Equal(t, []uint64{100, 200}, timestamps)
	ids, timestamps = search(byteRange([]byte("bb"), []byte("cc"), true, true), nil)
	require.Equal(t, []uint64{2, 3}, ids)
	require.Equal(t, []uint64{200, 300}, timestamps)
	ids, timestamps = search(byteRange([]byte("bb"), []byte("cc"), true, false), nil)
	require.Equal(t, []uint64{2}, ids)
	require.Equal(t, []uint64{200}, timestamps)
	ids, timestamps = search(byteRange([]byte("bb"), []byte("cc"), false, true), nil)
	require.Equal(t, []uint64{3}, ids)
	require.Equal(t, []uint64{300}, timestamps)
	ids, timestamps = search(byteRange([]byte("bb"), []byte("bb"), true, true), nil)
	require.Equal(t, []uint64{2}, ids)
	require.Equal(t, []uint64{200}, timestamps)
	floatRange := index.RangeOpts{
		Lower: &index.FloatTermValue{Value: -1.5}, Upper: &index.FloatTermValue{Value: 2.25},
		IncludesLower: true, IncludesUpper: true,
	}
	floatList, floatTimestamps, err := newRange(floatRule, floatRange).Execute(
		func(databasev1.IndexRule_Type) (index.Searcher, error) { return searcher, nil }, 11, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{7, 8, 9}, floatList.ToSlice())
	require.Equal(t, []uint64{500, 600, 700}, floatTimestamps.ToSlice())
	timeRange := index.NewIntRangeOpts(100, 300, true, false)
	ids, timestamps = search(fieldRange(10, 40, true, true), &timeRange)
	require.Equal(t, []uint64{1, 2}, ids)
	require.Equal(t, []uint64{100, 200}, timestamps)
	timeRangeExclusive := index.NewIntRangeOpts(100, 300, false, true)
	ids, timestamps = search(fieldRange(10, 40, true, true), &timeRangeExclusive)
	require.Equal(t, []uint64{2, 3}, ids)
	require.Equal(t, []uint64{200, 300}, timestamps)
	otherSeries, otherTimestamps, err := fieldRange(10, 30, true, true).Execute(func(databasev1.IndexRule_Type) (index.Searcher, error) { return searcher, nil }, 12, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{5}, otherSeries.ToSlice())
	require.Equal(t, []uint64{200}, otherTimestamps.ToSlice())
	greaterExpr, err := logical.ParseExpr(&modelv1.Condition{
		Name: "latency", Op: modelv1.Condition_BINARY_OP_GT,
		Value: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 10}}},
	})
	require.NoError(t, err)
	greaterList, greaterTimestamps, err := newRange(rule, greaterExpr.RangeOpts(false, false, false)).Execute(
		func(databasev1.IndexRule_Type) (index.Searcher, error) { return searcher, nil }, 11, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{2, 3}, greaterList.ToSlice())
	require.Equal(t, []uint64{200, 300}, greaterTimestamps.ToSlice())

	newDocs := index.Documents{{DocID: 6, Timestamp: 250, Fields: []index.Field{index.NewIntField(key, 25)}}}
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: newDocs}))
	old, oldTimestamps := search(fieldRange(10, 30, true, true), nil)
	require.Equal(t, []uint64{1, 2, 3}, old)
	require.Equal(t, []uint64{100, 200, 300}, oldTimestamps)
	require.NoError(t, searcher.Close())
	fresh, err := adapter.NewSearcher(context.Background(), nativeadapter.SearcherOptions{MaxTerms: 32, MaxCandidates: 32})
	require.NoError(t, err)
	freshList, freshTimestamps, err := fieldRange(10, 30, true, true).Execute(func(databasev1.IndexRule_Type) (index.Searcher, error) { return fresh, nil }, 11, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3, 6}, freshList.ToSlice())
	require.Equal(t, []uint64{100, 200, 250, 300}, freshTimestamps.ToSlice())
	require.NoError(t, fresh.Close())
	require.NoError(t, owner.Close())

	reopened, err := native.NewOwner(native.OwnerOptions{Lease: lease, Path: filepath.Join(root, "index"), CompactionThreshold: -1})
	require.NoError(t, err)
	reopenedAdapter := &nativeadapter.Adapter{Owner: reopened}
	reopenedSearcher, err := reopenedAdapter.NewSearcher(context.Background(), nativeadapter.SearcherOptions{MaxTerms: 32, MaxCandidates: 32})
	require.NoError(t, err)
	reopenedList, reopenedTimestamps, err := fieldRange(10, 30, true, true).Execute(
		func(databasev1.IndexRule_Type) (index.Searcher, error) { return reopenedSearcher, nil }, 11, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{1, 2, 3, 6}, reopenedList.ToSlice())
	require.Equal(t, []uint64{100, 200, 250, 300}, reopenedTimestamps.ToSlice())
	require.NoError(t, reopenedSearcher.Close())
	require.NoError(t, reopened.Close())
}

func TestNativeRangeExecuteCancellationAndClosedSearcher(t *testing.T) {
	owner, err := native.NewOwner(native.OwnerOptions{Lease: streamLease{}})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	adapter := &nativeadapter.Adapter{Owner: owner}
	key := index.FieldKey{IndexRuleID: 7, SeriesID: 11}
	require.NoError(t, adapter.Batch(context.Background(), index.Batch{Documents: []index.Document{
		{DocID: 1, Timestamp: 10, Fields: []index.Field{index.NewIntField(key, 10)}},
	}}))
	ctx, cancel := context.WithCancel(context.Background())
	searcher, err := adapter.NewSearcher(ctx, nativeadapter.SearcherOptions{MaxTerms: 32})
	require.NoError(t, err)
	cancel()
	op := newRange(&databasev1.IndexRule{Metadata: &commonv1.Metadata{Id: 7}}, index.NewIntRangeOpts(0, 20, true, true))
	_, _, err = op.Execute(func(databasev1.IndexRule_Type) (index.Searcher, error) { return searcher, nil }, 11, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, searcher.Close())
	closedSearcher, err := adapter.NewSearcher(context.Background(), nativeadapter.SearcherOptions{MaxTerms: 32})
	require.NoError(t, err)
	require.NoError(t, closedSearcher.Close())
	_, _, err = op.Execute(func(databasev1.IndexRule_Type) (index.Searcher, error) { return closedSearcher, nil }, 11, nil)
	require.ErrorIs(t, err, native.ErrViewClosed)
}

func TestNativeRangeExecuteRetainedLegacyMultiSegment(t *testing.T) {
	source := filepath.Join("..", "..", "..", "index", "testdata", "nidx01b", "index")
	sourceRoot := filepath.Dir(source)
	before := legacyIndexHashes(t, sourceRoot)
	root := t.TempDir()
	path := filepath.Join(root, "index")
	require.NoError(t, os.MkdirAll(path, 0o755))
	entries, err := os.ReadDir(source)
	require.NoError(t, err)
	for _, entry := range entries {
		// The retained corpus intentionally withholds segment 6 from its
		// newest, incomplete snapshot. Select its older complete generation.
		if entry.Name() == "00000000000b.snp" {
			continue
		}
		payload, readErr := os.ReadFile(filepath.Join(source, entry.Name()))
		require.NoError(t, readErr)
		require.NoError(t, os.WriteFile(filepath.Join(path, entry.Name()), payload, 0o600))
	}
	lock, err := fs.NewLocalFileSystem().CreateLockFile(filepath.Join(root, "lock"), 0o600)
	require.NoError(t, err)
	lease, err := native.NewFileRootLease(lock, root)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, lease.Revoke())
		require.NoError(t, lock.Close())
	}()
	destinationBefore := legacyIndexHashes(t, path)
	owner, err := native.NewOwner(native.OwnerOptions{Lease: lease, Path: path, CompactionThreshold: -1})
	require.NoError(t, err)
	defer func() { require.NoError(t, owner.Close()) }()
	adapter := &nativeadapter.Adapter{Owner: owner}
	searcher, err := adapter.NewSearcher(context.Background(), nativeadapter.SearcherOptions{MaxTerms: 32, MaxCandidates: 32})
	require.NoError(t, err)
	defer func() { require.NoError(t, searcher.Close()) }()
	rule := &databasev1.IndexRule{Metadata: &commonv1.Metadata{Id: 1}, Tags: []string{"nidx01b"}, Type: databasev1.IndexRule_TYPE_INVERTED}
	list, timestamps, err := newRange(rule, index.NewBytesRangeOpts([]byte("nidx01a"), []byte("nidx01c"), true, true)).Execute(
		func(databasev1.IndexRule_Type) (index.Searcher, error) { return searcher, nil }, 1, nil)
	require.NoError(t, err)
	require.Equal(t, []uint64{21, 23, 24, 25}, list.ToSlice())
	require.Equal(t, []uint64{0}, timestamps.ToSlice())
	require.NoError(t, searcher.Close())
	require.NoError(t, owner.Close())
	require.Equal(t, destinationBefore, legacyIndexHashes(t, path))
	require.Equal(t, before, legacyIndexHashes(t, sourceRoot))
}

func legacyIndexHashes(t *testing.T, path string) map[string][32]byte {
	t.Helper()
	hashes := make(map[string][32]byte)
	require.NoError(t, filepath.WalkDir(path, func(filePath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		payload, readErr := os.ReadFile(filePath)
		if readErr != nil {
			return readErr
		}
		relative, relativeErr := filepath.Rel(path, filePath)
		if relativeErr != nil {
			return relativeErr
		}
		hashes[relative] = sha256.Sum256(payload)
		return nil
	}))
	return hashes
}

func encodeFloatTerm(value float64) []byte {
	const width = 11
	result := make([]byte, width)
	result[0] = 0x20
	bits := uint64(encoding.Float64ToSortableInt64(value)) ^ 0x8000000000000000
	for index := width - 1; index > 0; index-- {
		result[index] = byte(bits & 0x7f)
		bits >>= 7
	}
	return result
}

func waitNativePersistence(owner *native.Owner) error {
	persisted := make(chan error, 1)
	if err := owner.Batch(context.Background(), native.Batch{PersistentCallback: func(err error) { persisted <- err }}); err != nil {
		return err
	}
	return <-persisted
}
