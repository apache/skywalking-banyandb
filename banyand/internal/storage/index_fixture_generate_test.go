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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// nidx03FixtureDir is the checked-in sidx fixture directory pair
// (normal/sidx, indexmode/sidx) TestSeriesIndex_OpensPreviousReleaseFixture
// opens with the native series index. See
// nidx03FixtureNormalDocs/nidx03FixtureIndexModeDocs below for the exact,
// hand-authored document set these directories hold, and
// index_fixture_verify_test.go for the pinned expected query results.
const nidx03FixtureDir = "testdata/nidx03_fixture"

// nidx03ScoreRuleID is the index-rule ID the fixture's "score" field is
// keyed by, so the sort test can exercise the real OrderByTypeIndex path
// (FieldKey{IndexRuleID}.Marshal()) the same way a production Measure
// index-rule sort does, instead of a TagName-keyed field no OrderBy path
// addresses.
const nidx03ScoreRuleID = 42

// nidx03FixtureSeries describes one fixture series: a two-tag entity
// (service, instance) plus its tag field values. A nil map value for a
// field name means "this series has no such field" (exercises NOT_HAVING /
// absent-field projection).
//
//nolint:govet // fixture fields are grouped by meaning, not padding.
type nidx03FixtureSeries struct {
	tags        []string
	service     string
	instance    string
	region      string
	score       string // zero-padded decimal so byte order == numeric order.
	description string
	timestamp   int64
	version     int64
}

// nidx03FixtureNormalDocs is the hand-authored fixture content for the
// non-index-mode sidx: four series covering exact/prefix/wildcard identity
// matching (shared "alpha"/"one" components), every criteria.Filter kind
// (EQ/range/IN/NOT_IN/HAVING/NOT_HAVING/MATCH) and a sortable field. Kept
// deliberately tiny (4 series) so the checked-in fixture stays a few KB.
func nidx03FixtureNormalDocs() []nidx03FixtureSeries {
	return []nidx03FixtureSeries{
		{service: "alpha", instance: "one", region: "us", score: "010", tags: []string{"a", "b"}, description: "quick brown fox", timestamp: 1000, version: 1},
		{service: "alpha", instance: "two", region: "us", score: "020", tags: []string{"b", "c"}, description: "lazy dog sleeps", timestamp: 2000, version: 1},
		{service: "beta", instance: "one", region: "eu", score: "030", tags: []string{"a"}, description: "quick fox jumps", timestamp: 3000, version: 2},
		{service: "gamma", instance: "one", region: "us", score: "005", tags: nil, description: "dog barks loud", timestamp: 4000, version: 1},
	}
}

// nidx03FixtureIndexModeDocs is the hand-authored fixture content for the
// index-mode sidx: two series under subject "idx_measure", one under a
// different subject "other_measure" to prove SearchWithoutSeries' _im_name
// universe scoping excludes it.
func nidx03FixtureIndexModeDocs() []nidx03FixtureSeries {
	return []nidx03FixtureSeries{
		{service: "svcA", instance: "", region: "us", score: "001", timestamp: 1000, version: 1},
		{service: "svcB", instance: "", region: "eu", score: "002", timestamp: 2000, version: 1},
	}
}

// nidx03FixtureOtherSubjectDoc is the one "other_measure" document the
// index-mode fixture also carries.
func nidx03FixtureOtherSubjectDoc() nidx03FixtureSeries {
	return nidx03FixtureSeries{service: "svcC", instance: "", region: "us", score: "003", timestamp: 3000, version: 1}
}

// buildNormalFixtureDoc builds the legacy-store index.Document for one
// normal-mode fixture series, matching storage.EncodeSeriesDocument's
// mapping one-for-one so the native reader interprets it identically.
func buildNormalFixtureDoc(t *testing.T, s nidx03FixtureSeries) index.Document {
	t.Helper()
	var series pbv1.Series
	series.Subject = testSubjectSvc
	series.EntityValues = []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.service}}},
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.instance}}},
	}
	require.NoError(t, series.Marshal())

	var fields []index.Field
	region := index.NewBytesField(index.FieldKey{TagName: "region"}, []byte(s.region))
	region.Store, region.Index, region.NoSort = true, true, true
	fields = append(fields, region)

	score := index.NewBytesField(index.FieldKey{IndexRuleID: nidx03ScoreRuleID}, []byte(s.score))
	score.Store, score.Index = true, true // Sort (NoSort=false): score is the fixture's sort-order field.
	fields = append(fields, score)

	for _, tag := range s.tags {
		tf := index.NewBytesField(index.FieldKey{TagName: "tag"}, []byte(tag))
		tf.Store, tf.Index, tf.NoSort = true, true, true
		fields = append(fields, tf)
	}

	desc := index.NewStringField(index.FieldKey{TagName: "description", Analyzer: index.AnalyzerSimple}, s.description)
	desc.Store, desc.Index, desc.NoSort = true, true, true
	fields = append(fields, desc)

	return index.Document{
		Fields:       fields,
		EntityValues: append([]byte(nil), series.Buffer...),
		Timestamp:    s.timestamp,
		Version:      s.version,
	}
}

// buildIndexModeFixtureDoc builds the legacy-store index.Document for one
// index-mode fixture series: an _im_name field (the subject, matching
// appendEntityTagsToIndexFields' production shape) plus a regular "region"/
// "score" tag, keyed the same way buildNormalFixtureDoc does.
func buildIndexModeFixtureDoc(t *testing.T, subject string, s nidx03FixtureSeries) index.Document {
	t.Helper()
	var series pbv1.Series
	series.Subject = subject
	series.EntityValues = []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s.service}}}}
	require.NoError(t, series.Marshal())

	imName := index.NewStringField(index.FieldKey{TagName: index.IndexModeName}, subject)
	imName.Index, imName.NoSort = true, true

	region := index.NewBytesField(index.FieldKey{TagName: "region"}, []byte(s.region))
	region.Store, region.Index, region.NoSort = true, true, true

	score := index.NewBytesField(index.FieldKey{IndexRuleID: nidx03ScoreRuleID}, []byte(s.score))
	score.Store, score.Index = true, true

	return index.Document{
		Fields:       []index.Field{imName, region, score},
		EntityValues: append([]byte(nil), series.Buffer...),
		Timestamp:    s.timestamp,
		Version:      s.version,
	}
}

// TestGenerateNIDX03Fixture is the NIDX-03 §12 item 1 one-off generator. It
// is skipped unless GENERATE_NIDX03_FIXTURE is set, so it never runs in CI;
// invoke it once (GENERATE_NIDX03_FIXTURE=1 go test -run
// TestGenerateNIDX03Fixture ./banyand/internal/storage/ -v) to (re)produce
// the checked-in fixture under testdata/nidx03_fixture using the CURRENT
// legacy store (pkg/index/inverted), exactly as the previous release's
// writer would. It is not kept running in CI; delete it once the fixture is
// pinned and stable, or leave it here, permanently skipped, as the record
// of how the fixture was produced.
func TestGenerateNIDX03Fixture(t *testing.T) {
	if os.Getenv("GENERATE_NIDX03_FIXTURE") == "" {
		t.Skip("one-off fixture generator; set GENERATE_NIDX03_FIXTURE=1 to (re)run it")
	}

	root, err := filepath.Abs(nidx03FixtureDir)
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(root))

	normalDir := filepath.Join(root, "normal", "sidx")
	require.NoError(t, os.MkdirAll(normalDir, 0o755))
	normalStore, err := inverted.NewStore(inverted.StoreOpts{Path: normalDir, BatchWaitSec: 0})
	require.NoError(t, err)
	var normalDocs index.Documents
	for _, s := range nidx03FixtureNormalDocs() {
		normalDocs = append(normalDocs, buildNormalFixtureDoc(t, s))
	}
	require.NoError(t, normalStore.UpdateSeriesBatch(index.Batch{Documents: normalDocs}))
	require.NoError(t, normalStore.Close())

	indexModeDir := filepath.Join(root, "indexmode", "sidx")
	require.NoError(t, os.MkdirAll(indexModeDir, 0o755))
	indexModeStore, err := inverted.NewStore(inverted.StoreOpts{Path: indexModeDir, BatchWaitSec: 0})
	require.NoError(t, err)
	var indexModeDocs index.Documents
	for _, s := range nidx03FixtureIndexModeDocs() {
		indexModeDocs = append(indexModeDocs, buildIndexModeFixtureDoc(t, "idx_measure", s))
	}
	indexModeDocs = append(indexModeDocs, buildIndexModeFixtureDoc(t, "other_measure", nidx03FixtureOtherSubjectDoc()))
	require.NoError(t, indexModeStore.UpdateSeriesBatch(index.Batch{Documents: indexModeDocs}))
	require.NoError(t, indexModeStore.Close())

	t.Logf("fixture written to %s", root)
}
