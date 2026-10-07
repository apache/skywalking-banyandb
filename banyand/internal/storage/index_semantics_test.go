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

package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query"
)

// seriesWithIndexedField builds one index.Document with an index-rule-backed
// field (so an index order has something to sort on) plus the matching
// *pbv1.Series to query it back.
func seriesWithIndexedField(t *testing.T, subject string, i int, ruleID uint32, value string) (index.Document, *pbv1.Series) {
	t.Helper()
	var series pbv1.Series
	series.Subject = subject
	series.EntityValues = []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("s%02d", i)}}}}
	require.NoError(t, series.Marshal())
	identity := append([]byte(nil), series.Buffer...)
	f := index.NewStringField(index.FieldKey{IndexRuleID: ruleID}, value)
	f.Store = true
	f.Index = true
	doc := index.Document{Fields: []index.Field{f}, EntityValues: identity, Timestamp: int64(i + 1)}
	var queried pbv1.Series
	queried.Subject = subject
	queried.EntityValues = series.EntityValues
	return doc, &queried
}

// TestSeriesIndex_Search_TimeOrderTakesUnsortedPath is the S1 regression: a
// time order (resolveOrderBy's {Type: OrderByTypeTime, Index: nil}, the
// plan's canonical shape for "no index order requested") must take the same
// unsorted path as no order at all -- hits stay in MatchTermsSet order and
// sortedValues is nil -- not SortHits/ProjectSortValue. Sorting on every
// non-nil Order (the bug: `if opts.Order != nil`) changes index-mode output
// order and flips banyand/measure/query.go's needsSorting decision.
func TestSeriesIndex_Search_TimeOrderTakesUnsortedPath(t *testing.T) {
	ctx := context.Background()
	dir, fn := setUp(require.New(t))
	defer fn()
	si, err := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	defer func() { require.NoError(t, si.Close()) }()

	const ruleID = 77
	var docs index.Documents
	var queries []*pbv1.Series
	for i := 0; i < 5; i++ {
		doc, q := seriesWithIndexedField(t, testSubjectSvc, i, ruleID, fmt.Sprintf("v%d", i))
		docs = append(docs, doc)
		queries = append(queries, q)
	}
	require.NoError(t, si.Insert(docs))

	// No order at all: the baseline unsorted shape.
	baseline, baselineSorted, err := si.Search(ctx, queries, IndexSearchOpts{})
	require.NoError(t, err)
	require.Nil(t, baselineSorted)
	require.Len(t, baseline.SeriesList, 5)

	// Time order: must match the baseline's unsorted shape exactly --
	// sortedValues nil, same hit order -- not a sorted one.
	timeOrdered, timeOrderedSorted, err := si.Search(ctx, queries, IndexSearchOpts{Order: &index.OrderBy{Type: index.OrderByTypeTime}})
	require.NoError(t, err)
	require.Nil(t, timeOrderedSorted, "a time order must take the unsorted path: sortedValues must stay nil")
	require.Len(t, timeOrdered.SeriesList, 5)
	for i := range baseline.SeriesList {
		require.Equal(t, baseline.SeriesList[i].EntityValues[0].GetStr().GetValue(),
			timeOrdered.SeriesList[i].EntityValues[0].GetStr().GetValue(),
			"a time order must not reorder hits relative to the unsorted baseline")
	}

	// Index order: by contrast, must take the sorted path.
	indexOrdered, indexOrderedSorted, err := si.Search(ctx, queries, IndexSearchOpts{Order: &index.OrderBy{
		Type: index.OrderByTypeIndex, Index: &databasev1.IndexRule{Metadata: &commonv1.Metadata{Id: ruleID}},
	}})
	require.NoError(t, err)
	require.NotNil(t, indexOrderedSorted, "an index order must take the sorted path: sortedValues must be populated")
	require.Len(t, indexOrderedSorted, len(indexOrdered.SeriesList))
}

// TestOrderFieldName_IndexTypeWithNilIndexErrorsNotPanics is the other half
// of S1: {Type: OrderByTypeIndex, Index: nil} must report an error, not
// dereference order.Index.Metadata.Id and panic.
func TestOrderFieldName_IndexTypeWithNilIndexErrorsNotPanics(t *testing.T) {
	require.NotPanics(t, func() {
		_, err := orderFieldName(&index.OrderBy{Type: index.OrderByTypeIndex, Index: nil})
		require.Error(t, err)
	})
}

// budgetCapLease is a minimal query.BudgetLease that errors once charged
// bytes exceed cap, for pinning that seriesIndex.search charges the query
// memory budget on the unsorted path (S2).
type budgetCapLease struct {
	cap  uint64
	used uint64
}

func (l *budgetCapLease) Charge(n uint64) error {
	l.used += n
	if l.used > l.cap {
		return fmt.Errorf("budget exceeded: used %d > cap %d", l.used, l.cap)
	}
	return nil
}

func (l *budgetCapLease) Limit() uint64 { return l.cap }
func (l *budgetCapLease) Used() uint64  { return l.used }
func (l *budgetCapLease) Owner() any    { return nil }

// TestSeriesIndex_Search_UnsortedPathChargesQueryBudget is the S2
// regression: the previous release's parseResult/readSeriesDocument charged
// the query memory budget per hit and per projected byte on the unsorted
// path (stream/trace segment.Lookup, unsorted measure search); the native
// rewrite must restore equivalent charging so a budget-exceeded error still
// surfaces instead of search silently ignoring the lease.
func TestSeriesIndex_Search_UnsortedPathChargesQueryBudget(t *testing.T) {
	dir, fn := setUp(require.New(t))
	defer fn()
	si, err := newSeriesIndex(context.Background(), dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	defer func() { require.NoError(t, si.Close()) }()

	var series pbv1.Series
	series.Subject = testSubjectSvc
	series.EntityValues = []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "s00"}}}}
	require.NoError(t, series.Marshal())
	identity := append([]byte(nil), series.Buffer...)
	require.NoError(t, si.Insert(index.Documents{{EntityValues: identity, Timestamp: 1}}))

	// A cap of 1 byte is exhausted by the very first (fixed, 1024-byte
	// floor) charge in search(), regardless of the identifier's actual
	// length -- proving charging happens at all, not any particular amount.
	lease := &budgetCapLease{cap: 1}
	ctx := query.WithBudgetLease(context.Background(), lease)

	var queried pbv1.Series
	queried.Subject = testSubjectSvc
	queried.EntityValues = series.EntityValues
	_, _, searchErr := si.Search(ctx, []*pbv1.Series{&queried}, IndexSearchOpts{})
	require.Error(t, searchErr, "an exhausted query budget must surface as an error from the unsorted search path")
	require.Greater(t, lease.used, uint64(0), "search must have charged the budget before erroring")
}

// TestEncodeSeriesDocument_UnindexedFieldAlwaysStored is the S5 regression:
// an Index=false field must always be stored (the previous release's
// bluge.NewStoredOnlyField, NIDX-03 §6.1), regardless of the caller's Store
// value -- an Index=false, Store=false field must not silently carry
// neither an index entry nor a stored value.
func TestEncodeSeriesDocument_UnindexedFieldAlwaysStored(t *testing.T) {
	f := index.NewStringField(index.FieldKey{TagName: "unindexed"}, "value")
	f.Index = false
	f.Store = false // the caller under-declares Store; EncodeSeriesDocument must correct it.

	var series pbv1.Series
	series.Subject = testSubjectSvc
	series.EntityValues = []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "s00"}}}}
	require.NoError(t, series.Marshal())

	doc := index.Document{Fields: []index.Field{f}, EntityValues: append([]byte(nil), series.Buffer...), Timestamp: 1}
	nd, err := EncodeSeriesDocument(doc)
	require.NoError(t, err)
	require.Len(t, nd.Fields, 1)
	require.True(t, nd.Fields[0].Store, "an Index=false field must always be stored, regardless of the caller's Store value")
	require.False(t, nd.Fields[0].Index)
}

// TestRemoveLegacySeriesIndexArtifacts_TargetsSiblingExternalSegmentTempDir
// is the L3 regression: the previous release opened its bluge store with
// ExternalSegmentTempDir: path.Join(root, ...) where root is the SEGMENT
// directory -- a SIBLING of root/sidx, not a subdirectory of it. A fresh
// newSeriesIndex must remove both the legacy lock file (inside sidx) and the
// legacy external-segment-temp directory at its real, sibling location, and
// must never panic if either happens to be unremovable.
func TestRemoveLegacySeriesIndexArtifacts_TargetsSiblingExternalSegmentTempDir(t *testing.T) {
	ctx := context.Background()
	root, fn := setUp(require.New(t))
	defer fn()

	indexPath := filepath.Join(root, seriesIndexDirName)
	require.NoError(t, os.MkdirAll(indexPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(indexPath, legacyLockFilename), []byte("pid"), 0o600))

	legacyExternalSegmentTempDir := filepath.Join(root, legacyExternalSegmentTempDirName)
	require.NoError(t, os.MkdirAll(legacyExternalSegmentTempDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(legacyExternalSegmentTempDir, "staged.bin"), []byte("x"), 0o600))

	// The wrong (pre-fix) location: root/sidx/external-segment-temp. It must
	// not exist in this fixture, and newSeriesIndex must not need it to
	// exist to clean up the real, sibling one.
	wrongLocation := filepath.Join(indexPath, legacyExternalSegmentTempDirName)
	_, statErr := os.Stat(wrongLocation)
	require.True(t, os.IsNotExist(statErr))

	si, err := newSeriesIndex(ctx, root, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	defer func() { require.NoError(t, si.Close()) }()

	_, lockStatErr := os.Stat(filepath.Join(indexPath, legacyLockFilename))
	require.True(t, os.IsNotExist(lockStatErr), "the legacy lock file must be removed")
	_, dirStatErr := os.Stat(legacyExternalSegmentTempDir)
	require.True(t, os.IsNotExist(dirStatErr), "the legacy external-segment-temp directory at its real, sibling location must be removed")
}

// TestNewOwner_RejectsNilLease documents the L9 contract
// newSegmentController's production path now relies on instead of a
// silently-substituted default lease: pkg/index/native.NewOwner itself
// refuses a nil lease with ErrLeaseUnavailable.
func TestNewOwner_RejectsNilLease(t *testing.T) {
	dir := t.TempDir()
	_, err := native.NewOwner(native.OwnerOptions{Path: filepath.Join(dir, "sidx"), IdentifierDocValues: true})
	require.ErrorIs(t, err, native.ErrLeaseUnavailable)
}
