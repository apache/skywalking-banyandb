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
	"math/rand"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// refModelScoreRuleID is the fixed IndexRuleID the reference model's "score"
// field carries, so the sorted-search check below exercises the real
// OrderByTypeIndex path (the same one dispatch.go's resolveOrderBy
// produces), not the time order.
const refModelScoreRuleID = uint32(901)

// refModelDoc is one reference-model document: the field values a real
// index.Document.Fields list would carry (keyed by marshaled FieldKey name),
// the set of field names it carries (what InsertIfAbsent's coverage check
// compares), its timestamp/version, and its zero-padded "score" value (for
// the index-rule sorted-search check; zero-padding makes lexicographic byte
// order equal numeric order, so SortedValue's raw bytes can be compared
// directly).
type refModelDoc struct {
	fields    map[string][]byte
	fieldSet  map[string]struct{}
	score     string
	timestamp int64
	version   int64
}

// refModel is the plain in-memory model of NIDX-03 §2: a map from _id to
// document, plus the external-receive keep-existing rule (§4.3). It never
// touches the series index itself; TestSeriesIndex_ReferenceModel drives
// both in lockstep and compares.
//
// Insert (§2.1's segment-wide field-set coverage skip) is deliberately NOT
// exercised by this randomized model: pkg/index/native.Owner computes
// InsertIfAbsent's coverage check per SEGMENT (the union of every document's
// field names the segment physically holds), not per identifier, and
// newSeriesIndex's production config runs compaction (CompactionThreshold
// left at its conservative default), which repeatedly merges many series'
// segments into one over the run -- a simple per-identifier model cannot
// predict the resulting segment-wide field set without reimplementing the
// engine's own merge decisions. TestSeriesIndex_Insert_FieldCoverageContract
// below pins Insert's contract directly instead, staying well under the
// compaction threshold so segment-wide and per-identifier coverage coincide.
type refModel struct {
	docs map[string]*refModelDoc
}

func newRefModel() *refModel { return &refModel{docs: map[string]*refModelDoc{}} }

// applyUpdate mirrors Update's contract: full replace, no version check.
func (m *refModel) applyUpdate(identity string, doc *refModelDoc) {
	m.docs[identity] = doc
}

// applyExternalReceive mirrors ExternalDedupKeepExisting: the existing live
// document wins; an incoming duplicate is masked.
func (m *refModel) applyExternalReceive(identity string, doc *refModelDoc) {
	if _, ok := m.docs[identity]; ok {
		return
	}
	m.docs[identity] = doc
}

// refModelFieldNames enumerates the small fixed field catalog the property
// test draws random subsets from.
var refModelFieldNames = []string{"region", "zone", "team", "tier"}

// referenceModelIterations lets REFMODEL_ITERATIONS (unset -> 200) drive a
// longer run out-of-band without touching the default CI budget, matching
// NIDX-03 §12 item 3's "fixed seed in CI, longer behind a flag" requirement.
func referenceModelIterations() int {
	if raw := os.Getenv("REFMODEL_ITERATIONS"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return 200
}

// buildRefModelDoc builds both the index.Document (for the real series
// index) and the refModelDoc (for the model) for one series, drawing a
// random subset of refModelFieldNames with random values from rng, plus a
// zero-padded "score" field every document always carries.
func buildRefModelDoc(rng *rand.Rand, identity []byte, timestamp, version int64) (index.Document, *refModelDoc) {
	names := append([]string(nil), refModelFieldNames...)
	rng.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	chosen := names[:rng.Intn(len(names)+1)]
	sort.Strings(chosen) // deterministic catalog order regardless of shuffle draw.

	fields := make([]index.Field, 0, len(chosen)+1)
	modelFields := make(map[string][]byte, len(chosen)+1)
	fieldSet := make(map[string]struct{}, len(chosen)+1)
	for _, name := range chosen {
		value := []byte(fmt.Sprintf("v%d", rng.Intn(5)))
		key := index.FieldKey{TagName: name}
		f := index.NewBytesField(key, value)
		f.Store = true
		f.Index = true
		fields = append(fields, f)
		modelFields[key.Marshal()] = value
		fieldSet[key.Marshal()] = struct{}{}
	}

	score := fmt.Sprintf("%03d", rng.Intn(1000))
	scoreKey := index.FieldKey{IndexRuleID: refModelScoreRuleID}
	scoreField := index.NewStringField(scoreKey, score)
	scoreField.Store = true
	scoreField.Index = true
	fields = append(fields, scoreField)
	modelFields[scoreKey.Marshal()] = []byte(score)
	fieldSet[scoreKey.Marshal()] = struct{}{}

	doc := index.Document{Fields: fields, EntityValues: identity, Timestamp: timestamp, Version: version}
	return doc, &refModelDoc{fields: modelFields, fieldSet: fieldSet, score: score, timestamp: timestamp, version: version}
}

// refModelResolver implements criteria.FieldResolver for the catalog above,
// so the criteria-filter check can run through the real
// pkg/index/native/criteria.Filter path.
type refModelResolver struct{}

func (refModelResolver) Field(tagName string) (string, string, bool) {
	if tagName == "score" {
		return index.FieldKey{IndexRuleID: refModelScoreRuleID}.Marshal(), "", true
	}
	for _, name := range refModelFieldNames {
		if name == tagName {
			return index.FieldKey{TagName: name}.Marshal(), "", true
		}
	}
	return "", "", false
}

// refModelPopulation is 4 groups x 3 instances: a two-entity-tag-position
// identity scheme (group, instance) so prefix (fixed group, wildcard
// instance) and wildcard (wildcard group, fixed instance) series matchers
// are meaningful, unlike a single-tag-position identity.
const (
	refModelGroups     = 4
	refModelInstances  = 3
	refModelPopulation = refModelGroups * refModelInstances
)

// refModelSeriesIdentity returns the marshaled identity for series index i in
// the (group, instance) population, and the *pbv1.Series to search for it.
func refModelSeriesIdentity(t *testing.T, i int) ([]byte, *pbv1.Series) {
	t.Helper()
	group, instance := i/refModelInstances, i%refModelInstances
	var series pbv1.Series
	series.Subject = "refmodel"
	series.EntityValues = []*modelv1.TagValue{
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("group-%02d", group)}}},
		{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("instance-%02d", instance)}}},
	}
	require.NoError(t, series.Marshal())
	return append([]byte(nil), series.Buffer...), &series
}

// buildNativeExternalSegmentMultiField is buildNativeExternalSegment
// generalized to carry several already-marshaled fields (keyed by FieldKey
// name) plus a version, matching what the reference-model property test
// needs to simulate an external receive of a multi-field, versioned
// document -- the same "_version" stored-only field
// storage.EncodeSeriesDocument writes.
func buildNativeExternalSegmentMultiField(t *testing.T, identity []byte, fields map[string][]byte, timestamp, version int64) []byte {
	t.Helper()
	dir := t.TempDir()
	owner, err := native.NewOwner(native.OwnerOptions{Lease: &testRootLease{}, Path: dir, IdentifierDocValues: true})
	require.NoError(t, err)
	nativeFields := make([]native.Field, 0, len(fields)+1)
	for name, value := range fields {
		// Sort: true matches storage.EncodeSeriesDocument's mapping for an
		// indexed field with no NoSort override -- the real sender-side
		// encoding every product uses, including for data a replication
		// receiver introduces via external segments. Omitting it here would
		// leave this fixture's fields un-sortable, unlike what production
		// ever actually writes.
		nativeFields = append(nativeFields, native.Field{Name: name, Value: value, Store: true, Index: true, Sort: true})
	}
	nativeFields = append(nativeFields, native.Field{Name: versionFieldName, Value: convert.Int64ToBytes(version), Store: true})
	done := make(chan error, 1)
	require.NoError(t, owner.Batch(context.Background(), native.Batch{
		Documents:          []native.Document{{Identifier: identity, Fields: nativeFields, Timestamp: timestamp}},
		PersistentCallback: func(err error) { done <- err },
	}))
	require.NoError(t, <-done)
	require.NoError(t, owner.Close())
	return singleSegFile(t, dir)
}

// receiveExternalSegmentRetryingBusy retries receiveExternalSegment on
// native.ErrPersistenceBusy: introduceExternalSegment legitimately refuses a
// receive while the owner's own background garbage collection is active
// (owner.go's o.collecting guard) -- a real, expected contention a
// production replication receiver hits too, not a test bug -- and
// CompleteSegment discards the streamer's staged bytes on any error, so the
// whole StartSegment/WriteChunk/CompleteSegment cycle must be redone with
// payload, not merely re-invoked.
func receiveExternalSegmentRetryingBusy(t *testing.T, idx IndexDB, payload []byte) {
	t.Helper()
	require.Eventually(t, func() bool {
		err := receiveExternalSegment(t, idx, payload)
		if err == nil {
			return true
		}
		require.ErrorIs(t, err, native.ErrPersistenceBusy, "only transient GC contention may be retried")
		return false
	}, 5*time.Second, time.Millisecond)
}

// verifyRefModel re-reads si (unsorted, then index-rule sorted) and asserts
// its observations against model: live document count and per-document
// fields/timestamp/version (unsorted), plus -- the index-rule sorted search
// -- that the SAME live set comes back and SortedValue is non-decreasing
// and consistent with each row's own "score" field.
func verifyRefModel(t *testing.T, ctx context.Context, si *seriesIndex, seriesByIdentity []*pbv1.Series, model *refModel) {
	t.Helper()
	projection := make([]index.FieldKey, 0, len(refModelFieldNames)+1)
	for _, name := range refModelFieldNames {
		projection = append(projection, index.FieldKey{TagName: name})
	}
	projection = append(projection, index.FieldKey{IndexRuleID: refModelScoreRuleID})

	sd, _, searchErr := si.Search(ctx, seriesByIdentity, IndexSearchOpts{Projection: projection})
	require.NoError(t, searchErr)

	got := make(map[string]*refModelDoc, len(sd.SeriesList))
	for i, s := range sd.SeriesList {
		s.Buffer = nil // Unmarshal leaves Buffer as decode scratch; Marshal appends rather than resetting.
		require.NoError(t, s.Marshal())
		fields := map[string][]byte{}
		for _, name := range refModelFieldNames {
			key := index.FieldKey{TagName: name}.Marshal()
			if v, ok := sd.Fields[i][key]; ok {
				fields[key] = v
			}
		}
		scoreKey := index.FieldKey{IndexRuleID: refModelScoreRuleID}.Marshal()
		var score string
		if v, ok := sd.Fields[i][scoreKey]; ok {
			fields[scoreKey] = v
			score = string(v)
		}
		var version int64
		if sd.VersionSet[i] {
			version = sd.Versions[i]
		}
		var timestamp int64
		if sd.TimestampSet[i] {
			timestamp = sd.Timestamps[i]
		}
		got[string(s.Buffer)] = &refModelDoc{fields: fields, score: score, timestamp: timestamp, version: version}
	}

	require.Len(t, got, len(model.docs), "live document count must match the model")
	for identity, want := range model.docs {
		actual, ok := got[identity]
		require.True(t, ok, "model expects series %q to be live", identity)
		require.Equal(t, want.timestamp, actual.timestamp, "series %q timestamp", identity)
		require.Equal(t, want.version, actual.version, "series %q version", identity)
		require.Equal(t, want.score, actual.score, "series %q score", identity)
		require.Equal(t, len(want.fields), len(actual.fields), "series %q field count", identity)
		for name, value := range want.fields {
			require.Equal(t, value, actual.fields[name], "series %q field %q", identity, name)
		}
	}

	// Index-rule sorted search: the previous release's SeriesSort / NIDX-03
	// §6.2. Membership must be the same live set as the unsorted search
	// above, and SortedValue must be non-decreasing AND must agree with
	// each row's own (zero-padded, so byte order == numeric order) "score"
	// field -- not just internally monotonic, but actually the right field.
	sorted, sortedValues, sortErr := si.Search(ctx, seriesByIdentity, IndexSearchOpts{
		Projection: []index.FieldKey{{IndexRuleID: refModelScoreRuleID}},
		Order: &index.OrderBy{
			Type: index.OrderByTypeIndex, Sort: modelv1.Sort_SORT_ASC,
			Index: &databasev1.IndexRule{Metadata: &commonv1.Metadata{Id: refModelScoreRuleID}},
		},
	})
	require.NoError(t, sortErr)
	require.Len(t, sorted.SeriesList, len(model.docs), "sorted search must return the same live set")
	require.Len(t, sortedValues, len(sorted.SeriesList))
	scoreKey := index.FieldKey{IndexRuleID: refModelScoreRuleID}.Marshal()
	for i := range sorted.SeriesList {
		if i > 0 {
			require.LessOrEqual(t, string(sortedValues[i-1]), string(sortedValues[i]), "SortedValue must be non-decreasing")
		}
		rowScore, ok := sorted.Fields[i][scoreKey]
		require.True(t, ok, "sorted row %d missing its score field", i)
		require.Equal(t, string(rowScore), string(sortedValues[i]), "SortedValue must agree with the row's own score field")
	}
}

// TestSeriesIndex_ReferenceModel is NIDX-03 §12 item 3: a plain in-memory
// model of the series-index contract (§2) runs against the real series
// index -- built through newSeriesIndex, the production constructor,
// exactly as a live segment opens one (presence cache sized like production,
// compaction left at its conservative default, asynchronous persistence via
// a positive flush timeout) -- under seeded random Update and external
// receive, verified via unsorted Search, index-rule sorted Search
// (membership + SortedValue order) and criteria filters; every observation
// must agree. Periodic restarts (Close + reopen via newSeriesIndex) prove
// the model's state survives exactly like a real segment reopening. The
// seed is fixed so CI is deterministic; REFMODEL_ITERATIONS drives a longer
// run out-of-band.
func TestSeriesIndex_ReferenceModel(t *testing.T) {
	ctx := context.Background()
	dir, fn := setUp(require.New(t))
	defer fn()
	newSI := func() *seriesIndex {
		si, err := newSeriesIndex(ctx, dir, 10, 4<<20, nil, &testRootLease{})
		require.NoError(t, err)
		return si
	}
	si := newSI()
	defer func() { require.NoError(t, si.Close()) }()

	rng := rand.New(rand.NewSource(1)) //nolint:gosec // deterministic test fixture, not a security context.
	model := newRefModel()
	identities := make([][]byte, refModelPopulation)
	seriesByIdentity := make([]*pbv1.Series, refModelPopulation)
	for i := 0; i < refModelPopulation; i++ {
		identities[i], seriesByIdentity[i] = refModelSeriesIdentity(t, i)
	}

	verify := func() { verifyRefModel(t, ctx, si, seriesByIdentity, model) }

	verify() // empty state: both sides agree on zero live documents.
	iterations := referenceModelIterations()
	// Restart roughly 4 times over the run (never on iteration 0, so the
	// first restart has real state to survive).
	restartEvery := iterations/4 + 1
	for iteration := 0; iteration < iterations; iteration++ {
		i := rng.Intn(refModelPopulation)
		identity := identities[i]
		timestamp := int64(iteration + 1)
		version := int64(rng.Intn(10) + 1)
		doc, modelDoc := buildRefModelDoc(rng, identity, timestamp, version)

		if rng.Intn(2) == 0 {
			require.NoError(t, si.Update(index.Documents{doc}))
			model.applyUpdate(string(identity), modelDoc)
		} else {
			seg := buildNativeExternalSegmentMultiField(t, identity, modelDoc.fields, timestamp, version)
			receiveExternalSegmentRetryingBusy(t, si, seg)
			model.applyExternalReceive(string(identity), modelDoc)
		}
		verify()

		if iteration > 0 && iteration%restartEvery == 0 {
			// Reopen/restart (L5): a graceful Close drains everything
			// pending (see seriesIndex.Close -> Owner.Close's final
			// drainPersistence), so a fresh newSeriesIndex on the same
			// directory must observe exactly the same live state.
			require.NoError(t, si.Close())
			si = newSI()
			verify()
		}
	}

	// Prefix and wildcard series matchers (L5): query group-00's three
	// instances via a prefix matcher (trailing AnyTagValue) and instance-00
	// across every group via a wildcard matcher (leading AnyTagValue),
	// checking membership against the model's own live set filtered the
	// same way.
	t.Run("prefix matcher", func(t *testing.T) {
		var prefixQuery pbv1.Series
		prefixQuery.Subject = "refmodel"
		prefixQuery.EntityValues = []*modelv1.TagValue{
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "group-00"}}},
			pbv1.AnyTagValue,
		}
		sd, _, err := si.Search(ctx, []*pbv1.Series{&prefixQuery}, IndexSearchOpts{})
		require.NoError(t, err)
		var want int
		for i := 0; i < refModelInstances; i++ {
			identity, _ := refModelSeriesIdentity(t, i) // group-00 is indices 0..refModelInstances-1.
			if _, live := model.docs[string(identity)]; live {
				want++
			}
		}
		require.Len(t, sd.SeriesList, want, "prefix matcher must return exactly group-00's live instances")
		for _, s := range sd.SeriesList {
			require.Equal(t, "group-00", s.EntityValues[0].GetStr().GetValue())
		}
	})
	t.Run("wildcard matcher", func(t *testing.T) {
		var wildcardQuery pbv1.Series
		wildcardQuery.Subject = "refmodel"
		wildcardQuery.EntityValues = []*modelv1.TagValue{
			pbv1.AnyTagValue,
			{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "instance-00"}}},
		}
		sd, _, err := si.Search(ctx, []*pbv1.Series{&wildcardQuery}, IndexSearchOpts{})
		require.NoError(t, err)
		var want int
		for g := 0; g < refModelGroups; g++ {
			identity, _ := refModelSeriesIdentity(t, g*refModelInstances) // instance 0 of group g.
			if _, live := model.docs[string(identity)]; live {
				want++
			}
		}
		require.Len(t, sd.SeriesList, want, "wildcard matcher must return exactly every group's live instance-00")
		for _, s := range sd.SeriesList {
			require.Equal(t, "instance-00", s.EntityValues[1].GetStr().GetValue())
		}
	})

	// Criteria filter (L5): a tag EQ condition through the real
	// pkg/index/native/criteria.Filter path (refModelResolver), checked
	// against the model's own filtered subset.
	t.Run("criteria filter", func(t *testing.T) {
		const filterField, filterValue = "region", "v0"
		sd, _, err := si.Search(ctx, seriesByIdentity, IndexSearchOpts{
			Criteria: &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
				Name: filterField, Op: modelv1.Condition_BINARY_OP_EQ,
				Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: filterValue}}},
			}}},
			Fields: refModelResolver{},
		})
		require.NoError(t, err)
		key := index.FieldKey{TagName: filterField}.Marshal()
		var want int
		for _, doc := range model.docs {
			if string(doc.fields[key]) == filterValue {
				want++
			}
		}
		require.Len(t, sd.SeriesList, want, "criteria filter must match the model's own filtered subset")
	})
}

// TestSeriesIndex_Insert_FieldCoverageContract pins Insert's contract
// (NIDX-03 §2.1) directly: skip a doc if its _id is live AND the segment
// already covers every field name the new doc carries; otherwise replace.
// It runs through the real production constructor (newSeriesIndex) but
// stays well under the default compaction threshold (a handful of
// admissions, default 16), so segment-wide coverage coincides with
// per-identifier coverage and the simple assertions below are exact --
// see TestSeriesIndex_ReferenceModel's comment on why the larger randomized
// model avoids Insert once compaction is in play.
func TestSeriesIndex_Insert_FieldCoverageContract(t *testing.T) {
	ctx := context.Background()
	dir, fn := setUp(require.New(t))
	defer fn()
	si, err := newSeriesIndex(ctx, dir, 0, 0, nil, &testRootLease{})
	require.NoError(t, err)
	defer func() { require.NoError(t, si.Close()) }()

	var series pbv1.Series
	series.Subject = "coverage"
	series.EntityValues = []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "s"}}}}
	require.NoError(t, series.Marshal())
	identity := append([]byte(nil), series.Buffer...)

	regionField := index.NewStringField(index.FieldKey{TagName: "region"}, "us")
	regionField.Store, regionField.Index = true, true
	require.NoError(t, si.Insert(index.Documents{{
		Fields: []index.Field{regionField}, EntityValues: identity, Timestamp: 1,
	}}))

	// A second Insert carrying a field name already covered ("region") must
	// be skipped: the original value survives.
	regionField2 := index.NewStringField(index.FieldKey{TagName: "region"}, "eu")
	regionField2.Store, regionField2.Index = true, true
	require.NoError(t, si.Insert(index.Documents{{
		Fields: []index.Field{regionField2}, EntityValues: identity, Timestamp: 2,
	}}))

	var queried pbv1.Series
	queried.Subject = "coverage"
	queried.EntityValues = series.EntityValues
	sd, _, err := si.Search(ctx, []*pbv1.Series{&queried}, IndexSearchOpts{Projection: []index.FieldKey{{TagName: "region"}}})
	require.NoError(t, err)
	require.Len(t, sd.SeriesList, 1)
	require.Equal(t, int64(1), sd.Timestamps[0], "a covered-field Insert must be skipped, keeping the original document")
	require.Equal(t, []byte("us"), sd.Fields[0][index.FieldKey{TagName: "region"}.Marshal()])

	// A third Insert carrying a NEW field name ("zone", not yet covered)
	// must replace the document.
	zoneField := index.NewStringField(index.FieldKey{TagName: "zone"}, "z1")
	zoneField.Store, zoneField.Index = true, true
	require.NoError(t, si.Insert(index.Documents{{
		Fields: []index.Field{zoneField}, EntityValues: identity, Timestamp: 3,
	}}))

	sd, _, err = si.Search(ctx, []*pbv1.Series{&queried}, IndexSearchOpts{Projection: []index.FieldKey{{TagName: "zone"}}})
	require.NoError(t, err)
	require.Len(t, sd.SeriesList, 1)
	require.Equal(t, int64(3), sd.Timestamps[0], "an uncovered-field Insert must replace the document")
	require.Equal(t, []byte("z1"), sd.Fields[0][index.FieldKey{TagName: "zone"}.Marshal()])
}
