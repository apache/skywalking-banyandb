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
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// nidx03FixtureDir is the checked-in sidx compatibility-oracle fixture
// directory pair (normal/sidx, indexmode/sidx) the tests below open with the
// native series index to prove it reads a directory written by the previous
// release's index writer identically. The fixture is immutable, hand-sized
// data: it was produced by commit 8c172364 (NIDX-03, #1397) and its tracked
// bytes hash to sha256
// 23e1d6ce5ef5ab704229a1ebca4244ea625734198fc65d4fc45e9a89880c23f0
// (concatenation of normal/sidx/*, indexmode/sidx/*, each path-sorted).
// There is no generator test: regenerating the fixture would require the
// retired third-party index library this repository no longer depends on,
// so the checked-in bytes themselves are the provenance.
const nidx03FixtureDir = "testdata/nidx03_fixture"

// nidx03ScoreRuleID is the index-rule ID the fixture's "score" field is
// keyed by, so the sort test can exercise the real OrderByTypeIndex path
// (FieldKey{IndexRuleID}.Marshal()) the same way a production Measure
// index-rule sort does, instead of a TagName-keyed field no OrderBy path
// addresses.
const nidx03ScoreRuleID = 42

// fixtureResolver implements criteria.FieldResolver for the NIDX-03 fixture:
// "region"/"tag" are TagName-keyed fields (their own name is the engine
// field); "score" is IndexRuleID-keyed (nidx03ScoreRuleID), the same way a
// production Measure index-rule field is, so the sort test below can
// exercise the real OrderByTypeIndex path; "description" carries the simple
// analyzer MATCH needs.
type fixtureResolver struct{}

func (fixtureResolver) Field(tagName string) (string, string, bool) {
	switch tagName {
	case "region", "tag":
		return tagName, "", true
	case "score":
		return index.FieldKey{IndexRuleID: nidx03ScoreRuleID}.Marshal(), "", true
	case "description":
		return tagName, index.AnalyzerSimple, true
	default:
		return "", "", false
	}
}

func fixtureStrTagValue(v string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
}

func fixtureSeriesExact(service, instance string) *pbv1.Series {
	return &pbv1.Series{Subject: testSubjectSvc, EntityValues: []*modelv1.TagValue{fixtureStrTagValue(service), fixtureStrTagValue(instance)}}
}

func fixtureSeriesPrefix(service string) *pbv1.Series {
	return &pbv1.Series{Subject: testSubjectSvc, EntityValues: []*modelv1.TagValue{fixtureStrTagValue(service), pbv1.AnyTagValue}}
}

func fixtureSeriesWildcard(instance string) *pbv1.Series {
	return &pbv1.Series{Subject: testSubjectSvc, EntityValues: []*modelv1.TagValue{pbv1.AnyTagValue, fixtureStrTagValue(instance)}}
}

func fixtureCondition(name string, op modelv1.Condition_BinaryOp, value *modelv1.TagValue) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{Name: name, Op: op, Value: value}}}
}

func fixtureStrArray(values ...string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: values}}}
}

// copyDirForTest recursively byte-copies src into dst (both must be plain
// directories of regular files), so a test can open a writable owner on a
// disposable copy of a checked-in fixture instead of risking any mutation of
// the fixture itself.
func copyDirForTest(t *testing.T, src, dst string) {
	t.Helper()
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(dst, 0o755))
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())
		if entry.IsDir() {
			copyDirForTest(t, srcPath, dstPath)
			continue
		}
		data, readErr := os.ReadFile(srcPath)
		require.NoError(t, readErr)
		require.NoError(t, os.WriteFile(dstPath, data, 0o600))
	}
}

// openNIDX03Fixture copies the checked-in fixture at testdata/nidx03_fixture/
// <name> into a fresh temp dir and opens it with the native series index
// (newSeriesIndex), exactly as the new binary opens a sidx directory the
// previous release wrote (NIDX-03 §1's "Forward" compatibility guarantee).
func openNIDX03Fixture(t *testing.T, name string) *seriesIndex {
	t.Helper()
	fixtureRoot, err := filepath.Abs(nidx03FixtureDir)
	require.NoError(t, err)
	src := filepath.Join(fixtureRoot, name)
	require.DirExists(t, src, "checked-in fixture missing from testdata/nidx03_fixture")

	workDir := t.TempDir()
	copyDirForTest(t, src, workDir)

	si, err := newSeriesIndex(context.Background(), workDir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, si.Close()) })
	return si
}

// searchSeries runs si.Search(series, criteria) with the fixture resolver and
// returns the matched (service, instance) pairs sorted for stable
// comparison.
func searchFixtureSeries(t *testing.T, si *seriesIndex, series []*pbv1.Series, criteria *modelv1.Criteria) []string {
	t.Helper()
	sd, _, err := si.Search(context.Background(), series, IndexSearchOpts{Criteria: criteria, Fields: fixtureResolver{}})
	require.NoError(t, err)
	return sortedFixtureLabels(sd.SeriesList)
}

// fixtureLabels renders sl as "<service>/<instance>" labels, preserving sl's
// own order (which, for a sorted Search, is the result's actual sort order
// and must not be disturbed).
func fixtureLabels(sl pbv1.SeriesList) []string {
	labels := make([]string, 0, len(sl))
	for _, s := range sl {
		service := s.EntityValues[0].GetStr().GetValue()
		instance := ""
		if len(s.EntityValues) > 1 {
			instance = s.EntityValues[1].GetStr().GetValue()
		}
		labels = append(labels, service+"/"+instance)
	}
	return labels
}

// sortedFixtureLabels is fixtureLabels with the labels themselves sorted, for
// comparing an unordered filter result (Search/SearchWithoutSeries with no
// Order) where only set membership, not result order, is pinned.
func sortedFixtureLabels(sl pbv1.SeriesList) []string {
	labels := fixtureLabels(sl)
	sort.Strings(labels)
	return labels
}

// TestSeriesIndex_OpensPreviousReleaseFixture_NormalMode is NIDX-03 §12 item
// 1: the checked-in fixture testdata/nidx03_fixture/normal/sidx (see
// nidx03FixtureDir's provenance comment) is opened by the native series
// index and every canned query -- exact, prefix, wildcard, and every
// criteria.Filter kind -- returns exactly the pinned expected result,
// proving the previous release's on-disk sidx is read identically.
//
// Expected results below (per design §12.1, "literal expectations" from a
// previous-release run) were cross-checked against an independent, direct
// read of the checked-in fixture's raw stored fields with the previous
// release's own reader, computing each scenario's matches programmatically
// from the on-disk data rather than from the fixture's declared Go literal
// content, before being pinned here; every one matched exactly.
func TestSeriesIndex_OpensPreviousReleaseFixture_NormalMode(t *testing.T) {
	si := openNIDX03Fixture(t, "normal")
	all := []*pbv1.Series{
		fixtureSeriesExact("alpha", "one"), fixtureSeriesExact("alpha", "two"),
		fixtureSeriesExact("beta", "one"), fixtureSeriesExact("gamma", "one"),
	}

	t.Run("exact", func(t *testing.T) {
		got := searchFixtureSeries(t, si, []*pbv1.Series{fixtureSeriesExact("alpha", "one")}, nil)
		require.Equal(t, []string{"alpha/one"}, got)
	})
	t.Run("prefix", func(t *testing.T) {
		got := searchFixtureSeries(t, si, []*pbv1.Series{fixtureSeriesPrefix("alpha")}, nil)
		require.Equal(t, []string{"alpha/one", "alpha/two"}, got)
	})
	t.Run("wildcard", func(t *testing.T) {
		got := searchFixtureSeries(t, si, []*pbv1.Series{fixtureSeriesWildcard("one")}, nil)
		require.Equal(t, []string{"alpha/one", "beta/one", "gamma/one"}, got)
	})
	t.Run("EQ", func(t *testing.T) {
		got := searchFixtureSeries(t, si, all, fixtureCondition("region", modelv1.Condition_BINARY_OP_EQ, fixtureStrTagValue("us")))
		require.Equal(t, []string{"alpha/one", "alpha/two", "gamma/one"}, got)
	})
	t.Run("range GT", func(t *testing.T) {
		got := searchFixtureSeries(t, si, all, fixtureCondition("score", modelv1.Condition_BINARY_OP_GT, fixtureStrTagValue("010")))
		require.Equal(t, []string{"alpha/two", "beta/one"}, got)
	})
	t.Run("range LE", func(t *testing.T) {
		got := searchFixtureSeries(t, si, all, fixtureCondition("score", modelv1.Condition_BINARY_OP_LE, fixtureStrTagValue("010")))
		require.Equal(t, []string{"alpha/one", "gamma/one"}, got)
	})
	t.Run("IN", func(t *testing.T) {
		got := searchFixtureSeries(t, si, all, fixtureCondition("region", modelv1.Condition_BINARY_OP_IN, fixtureStrArray("eu", "xx")))
		require.Equal(t, []string{"beta/one"}, got)
	})
	t.Run("NOT_IN", func(t *testing.T) {
		got := searchFixtureSeries(t, si, all, fixtureCondition("region", modelv1.Condition_BINARY_OP_NOT_IN, fixtureStrArray("eu")))
		require.Equal(t, []string{"alpha/one", "alpha/two", "gamma/one"}, got)
	})
	t.Run("HAVING", func(t *testing.T) {
		got := searchFixtureSeries(t, si, all, fixtureCondition("tag", modelv1.Condition_BINARY_OP_HAVING, fixtureStrArray("a", "b")))
		require.Equal(t, []string{"alpha/one"}, got, "only alpha/one carries both tag=a and tag=b")
	})
	t.Run("NOT_HAVING", func(t *testing.T) {
		got := searchFixtureSeries(t, si, all, fixtureCondition("tag", modelv1.Condition_BINARY_OP_NOT_HAVING, fixtureStrArray("a")))
		require.Equal(t, []string{"alpha/two", "gamma/one"}, got, "alpha/one and beta/one carry tag=a; gamma/one carries no tag field at all")
	})
	t.Run("MATCH", func(t *testing.T) {
		got := searchFixtureSeries(t, si, all, fixtureCondition("description", modelv1.Condition_BINARY_OP_MATCH, fixtureStrTagValue("fox")))
		require.Equal(t, []string{"alpha/one", "beta/one"}, got)
	})
	t.Run("sort by score ascending", func(t *testing.T) {
		sd, sortedValues, err := si.Search(context.Background(), all, IndexSearchOpts{
			Order: &index.OrderBy{
				Type:  index.OrderByTypeIndex,
				Index: &databasev1.IndexRule{Metadata: &commonv1.Metadata{Id: nidx03ScoreRuleID}},
				Sort:  modelv1.Sort_SORT_ASC,
			},
		})
		require.NoError(t, err)
		require.Equal(t, []string{"gamma/one", "alpha/one", "alpha/two", "beta/one"}, fixtureLabels(sd.SeriesList))
		require.Len(t, sortedValues, 4)
		for i := 1; i < len(sortedValues); i++ {
			require.LessOrEqual(t, string(sortedValues[i-1]), string(sortedValues[i]), "sorted values must be non-decreasing")
		}
	})
	t.Run("timestamps and versions", func(t *testing.T) {
		sd, _, err := si.Search(context.Background(), all, IndexSearchOpts{})
		require.NoError(t, err)
		gotTimestamps := make(map[string]int64, len(sd.SeriesList))
		gotVersions := make(map[string]int64, len(sd.SeriesList))
		for i, label := range fixtureLabels(sd.SeriesList) {
			require.True(t, sd.TimestampSet[i], "fixture series %s must carry a timestamp", label)
			require.True(t, sd.VersionSet[i], "fixture series %s must carry a version", label)
			gotTimestamps[label] = sd.Timestamps[i]
			gotVersions[label] = sd.Versions[i]
		}
		// From nidx03FixtureNormalDocs' declared content.
		require.Equal(t, map[string]int64{"alpha/one": 1000, "alpha/two": 2000, "beta/one": 3000, "gamma/one": 4000}, gotTimestamps)
		require.Equal(t, map[string]int64{"alpha/one": 1, "alpha/two": 1, "beta/one": 2, "gamma/one": 1}, gotVersions)
	})
}

// TestSeriesIndex_OpensPreviousReleaseFixture_IndexMode is NIDX-03 §12 item 1
// (index-mode Measure): the fixture testdata/nidx03_fixture/indexmode/sidx
// holds two "idx_measure" series and one "other_measure" series;
// SearchWithoutSeries scoped to IndexModeSubject "idx_measure" must return
// exactly the two, never the third (proving the _im_name universe scoping).
func TestSeriesIndex_OpensPreviousReleaseFixture_IndexMode(t *testing.T) {
	si := openNIDX03Fixture(t, "indexmode")

	sd, _, err := si.SearchWithoutSeries(context.Background(), IndexSearchOpts{
		IndexModeSubject: "idx_measure",
		Projection:       []index.FieldKey{{TagName: "region"}},
	})
	require.NoError(t, err)
	labels := make(map[string]string, len(sd.SeriesList))
	for i, s := range sd.SeriesList {
		labels[s.EntityValues[0].GetStr().GetValue()] = string(sd.Fields[i][index.FieldKey{TagName: "region"}.Marshal()])
	}
	require.Equal(t, map[string]string{"svcA": "us", "svcB": "eu"}, labels, "only idx_measure's own series must be returned, with their region values")

	// Index-mode criteria: a region filter within the idx_measure universe
	// must apply on top of (not instead of) the _im_name scoping -- svcC
	// (region=us) belongs to "other_measure" and must stay excluded even
	// though its region matches.
	t.Run("criteria", func(t *testing.T) {
		sd, _, err := si.SearchWithoutSeries(context.Background(), IndexSearchOpts{
			IndexModeSubject: "idx_measure",
			Criteria:         fixtureCondition("region", modelv1.Condition_BINARY_OP_EQ, fixtureStrTagValue("us")),
			Fields:           fixtureResolver{},
		})
		require.NoError(t, err)
		require.Len(t, sd.SeriesList, 1)
		require.Equal(t, "svcA", sd.SeriesList[0].EntityValues[0].GetStr().GetValue())
	})

	// Index-mode sort: svcA (score=001) must sort before svcB (score=002)
	// within the idx_measure universe; svcC (other_measure) must stay
	// excluded from both the result set and the sort.
	t.Run("sort", func(t *testing.T) {
		sd, sortedValues, err := si.SearchWithoutSeries(context.Background(), IndexSearchOpts{
			IndexModeSubject: "idx_measure",
			Order: &index.OrderBy{
				Type:  index.OrderByTypeIndex,
				Index: &databasev1.IndexRule{Metadata: &commonv1.Metadata{Id: nidx03ScoreRuleID}},
				Sort:  modelv1.Sort_SORT_ASC,
			},
		})
		require.NoError(t, err)
		got := make([]string, len(sd.SeriesList))
		for i, s := range sd.SeriesList {
			got[i] = s.EntityValues[0].GetStr().GetValue()
		}
		require.Equal(t, []string{"svcA", "svcB"}, got)
		require.Len(t, sortedValues, 2)
		require.LessOrEqual(t, string(sortedValues[0]), string(sortedValues[1]), "sorted values must be non-decreasing")
	})
}
