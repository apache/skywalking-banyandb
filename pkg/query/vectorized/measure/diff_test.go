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

package measure

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
)

// diffFixture is a single parity case: a Measure schema, the projection
// applied to it, and the deterministic MeasureResult sequence both paths
// consume.
type diffFixture struct {
	schema  *databasev1.Measure
	name    string
	results []*model.MeasureResult
	opts    model.MeasureQueryOptions
}

// runDiff drives both the row-path serializer and the vectorized adapter on
// fresh copies of fx.results, then returns the two output slices for
// proto-equality comparison. fx.results is *not* shared across paths because
// the vectorized adapter consumes (and may mutate referenced) values; each
// path needs an independent fake MeasureQueryResult.
func runDiff(t *testing.T, fx diffFixture) (rowOut, vecOut []*measurev1.InternalDataPoint) {
	t.Helper()
	rowQR := &fakeMeasureQueryResult{seq: cloneResults(fx.results)}
	rowOut = rowSerialize(rowQR, fx.opts)

	vecQR := &fakeMeasureQueryResult{seq: cloneResults(fx.results)}
	cfg := VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 64}
	it, err := NewMIterator(context.Background(), vecQR, fx.schema, fx.opts, cfg)
	if err != nil {
		t.Fatalf("%s: NewMIterator: %v", fx.name, err)
	}
	defer it.Close()
	for it.Next() {
		// Adapter conforms to the row-path's "one DataPoint per Next" contract:
		// the gRPC collector reads only Current()[0]. Mirror that here.
		current := it.Current()
		if len(current) > 0 {
			vecOut = append(vecOut, cloneIDPs(current[:1])...)
		}
	}
	if itErr := it.Err(); itErr != nil {
		t.Fatalf("%s: vectorized iterator err: %v", fx.name, itErr)
	}
	return rowOut, vecOut
}

// cloneResults returns a fresh slice so the row and vectorized paths
// each own their MeasureResult sequence; both paths advance the cursor
// independently.
func cloneResults(in []*model.MeasureResult) []*model.MeasureResult {
	out := make([]*model.MeasureResult, len(in))
	copy(out, in) // shallow — neither path writes back into MeasureResult.
	return out
}

func cloneIDPs(in []*measurev1.InternalDataPoint) []*measurev1.InternalDataPoint {
	out := make([]*measurev1.InternalDataPoint, len(in))
	for i, dp := range in {
		out[i] = proto.Clone(dp).(*measurev1.InternalDataPoint)
	}
	return out
}

// rowSerialize replays the row-path serializer that pkg/query/logical/measure
// runs in resultMIterator.Next. Replicated here (not imported) because
// pkg/query/logical/measure already imports this package — importing it back
// would form a cycle.
func rowSerialize(qr model.MeasureQueryResult, opts model.MeasureQueryOptions) []*measurev1.InternalDataPoint {
	var dps []*measurev1.InternalDataPoint
	for {
		r := qr.Pull()
		if r == nil {
			return dps
		}
		if r.Error != nil {
			return dps
		}
		tagFamilyMap := make(map[string]*model.TagFamily, len(r.TagFamilies))
		for idx := range r.TagFamilies {
			tagFamilyMap[r.TagFamilies[idx].Name] = &r.TagFamilies[idx]
		}
		for i := range r.Timestamps {
			dp := &measurev1.DataPoint{
				Timestamp: timestamppb.New(time.Unix(0, r.Timestamps[i])),
				Sid:       uint64(r.SID),
				Version:   r.Versions[i],
			}
			for _, proj := range opts.TagProjection {
				tf := &modelv1.TagFamily{Name: proj.Family}
				dp.TagFamilies = append(dp.TagFamilies, tf)
				resultTF := tagFamilyMap[proj.Family]
				for _, tagName := range proj.Names {
					var value *modelv1.TagValue
					if resultTF != nil {
						for _, t := range resultTF.Tags {
							if t.Name == tagName {
								value = t.Values[i]
								break
							}
						}
					}
					if value == nil {
						value = pbv1.NullTagValue
					}
					tf.Tags = append(tf.Tags, &modelv1.Tag{Key: tagName, Value: value})
				}
			}
			for _, pf := range opts.FieldProjection {
				foundIdx := -1
				for idx := range r.Fields {
					if r.Fields[idx].Name == pf {
						foundIdx = idx
						break
					}
				}
				if foundIdx != -1 {
					dp.Fields = append(dp.Fields, &measurev1.DataPoint_Field{
						Name:  r.Fields[foundIdx].Name,
						Value: r.Fields[foundIdx].Values[i],
					})
				} else {
					dp.Fields = append(dp.Fields, &measurev1.DataPoint_Field{
						Name:  pf,
						Value: pbv1.NullFieldValue,
					})
				}
			}
			var shardID common.ShardID
			if len(r.ShardIDs) > i {
				shardID = r.ShardIDs[i]
			}
			dps = append(dps, &measurev1.InternalDataPoint{
				DataPoint: dp,
				ShardId:   uint32(shardID),
			})
		}
	}
}

// assertDPEqual compares two slices of InternalDataPoint structurally up to
// the subset of fields the vectorized path is expected to set. Notes:
//   - Tag/field projection ordering is identical across both paths.
//   - The vectorized path may not populate TagFamilies whose entries do not
//     appear in the source MeasureResult; the row path always emits a
//     family-shaped entry. Tests below pin the input to materialize tags so
//     this difference does not surface.
func assertDPEqual(t *testing.T, name string, want, got []*measurev1.InternalDataPoint) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("%s: count mismatch: row=%d vec=%d", name, len(want), len(got))
	}
	for i := range want {
		if diff := cmp.Diff(want[i], got[i],
			cmp.Comparer(proto.Equal),
		); diff != "" {
			t.Fatalf("%s: dp[%d] mismatch:\n%s", name, i, diff)
		}
	}
}

// Schemas / fixtures.

func diffSchemaTagFieldMix() *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: nil,
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: "default", Tags: []*databasev1.TagSpec{
				{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "env_id", Type: databasev1.TagType_TAG_TYPE_INT},
				{Name: "blob", Type: databasev1.TagType_TAG_TYPE_DATA_BINARY},
				{Name: "ports", Type: databasev1.TagType_TAG_TYPE_INT_ARRAY},
				{Name: "labels", Type: databasev1.TagType_TAG_TYPE_STRING_ARRAY},
			}},
		},
		Fields: []*databasev1.FieldSpec{
			{Name: "v_int", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
			{Name: "v_float", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
			{Name: "v_str", FieldType: databasev1.FieldType_FIELD_TYPE_STRING},
			{Name: "v_bytes", FieldType: databasev1.FieldType_FIELD_TYPE_DATA_BINARY},
		},
	}
}

func diffOptsAllTagsAllFields() model.MeasureQueryOptions {
	return model.MeasureQueryOptions{
		TagProjection: []model.TagProjection{
			{Family: "default", Names: []string{"svc", "env_id", "blob", "ports", "labels"}},
		},
		FieldProjection: []string{"v_int", "v_float", "v_str", "v_bytes"},
	}
}

// rowSet is shorthand for the canonical "all variants set" row used by most
// fixtures.
func rowSet(sid common.SeriesID, ts int64) *model.MeasureResult {
	return &model.MeasureResult{
		SID:        sid,
		Timestamps: []int64{ts},
		Versions:   []int64{1},
		ShardIDs:   []common.ShardID{0},
		TagFamilies: []model.TagFamily{
			{Name: "default", Tags: []model.Tag{
				{Name: "svc", Values: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "alpha"}}},
				}},
				{Name: "env_id", Values: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 7}}},
				}},
				{Name: "blob", Values: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte{0x01, 0x02, 0x03}}},
				}},
				{Name: "ports", Values: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{80, 443}}}},
				}},
				{Name: "labels", Values: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"a", "b"}}}},
				}},
			}},
		},
		Fields: []model.Field{
			{Name: "v_int", Values: []*modelv1.FieldValue{
				{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 42}}},
			}},
			{Name: "v_float", Values: []*modelv1.FieldValue{
				{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: 3.14}}},
			}},
			{Name: "v_str", Values: []*modelv1.FieldValue{
				{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: "ok"}}},
			}},
			{Name: "v_bytes", Values: []*modelv1.FieldValue{
				{Value: &modelv1.FieldValue_BinaryData{BinaryData: []byte{0xde, 0xad}}},
			}},
		},
	}
}

// rowMulti returns one MeasureResult covering n consecutive rows of a single
// series — exercises the bulk fast path.
func rowMulti(sid common.SeriesID, baseTS int64, n int) *model.MeasureResult {
	r := &model.MeasureResult{SID: sid}
	tags := []model.Tag{
		{Name: "svc"},
		{Name: "env_id"},
		{Name: "blob"},
		{Name: "ports"},
		{Name: "labels"},
	}
	fields := []model.Field{
		{Name: "v_int"},
		{Name: "v_float"},
		{Name: "v_str"},
		{Name: "v_bytes"},
	}
	for i := range n {
		r.Timestamps = append(r.Timestamps, baseTS+int64(i))
		r.Versions = append(r.Versions, 1)
		r.ShardIDs = append(r.ShardIDs, 0)
		tags[0].Values = append(tags[0].Values, &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "alpha"}}})
		tags[1].Values = append(tags[1].Values, &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: int64(i)}}})
		tags[2].Values = append(tags[2].Values, &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte{byte(i)}}})
		tags[3].Values = append(tags[3].Values, &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: []int64{int64(i)}}}})
		tags[4].Values = append(tags[4].Values, &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"x"}}}})
		fields[0].Values = append(fields[0].Values, &modelv1.FieldValue{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(i)}}})
		fields[1].Values = append(fields[1].Values, &modelv1.FieldValue{Value: &modelv1.FieldValue_Float{Float: &modelv1.Float{Value: float64(i)}}})
		fields[2].Values = append(fields[2].Values, &modelv1.FieldValue{Value: &modelv1.FieldValue_Str{Str: &modelv1.Str{Value: "s"}}})
		fields[3].Values = append(fields[3].Values, &modelv1.FieldValue{Value: &modelv1.FieldValue_BinaryData{BinaryData: []byte{0xff}}})
	}
	r.TagFamilies = []model.TagFamily{{Name: "default", Tags: tags}}
	r.Fields = fields
	return r
}

// Tests.

func TestDiff_SingleSeries_AllVariants(t *testing.T) {
	fx := diffFixture{
		name:    "single-series-all-variants",
		schema:  diffSchemaTagFieldMix(),
		opts:    diffOptsAllTagsAllFields(),
		results: []*model.MeasureResult{rowSet(1, 100)},
	}
	rowOut, vecOut := runDiff(t, fx)
	assertDPEqual(t, fx.name, rowOut, vecOut)
}

func TestDiff_MultiSeries_TagOnlyProjection(t *testing.T) {
	fx := diffFixture{
		name:   "multi-series-tag-only",
		schema: diffSchemaTagFieldMix(),
		opts: model.MeasureQueryOptions{
			TagProjection: []model.TagProjection{
				{Family: "default", Names: []string{"svc", "env_id"}},
			},
		},
		results: []*model.MeasureResult{rowSet(1, 100), rowSet(2, 200), rowSet(3, 300)},
	}
	rowOut, vecOut := runDiff(t, fx)
	assertDPEqual(t, fx.name, rowOut, vecOut)
}

func TestDiff_MultiSeries_MixedProjection(t *testing.T) {
	fx := diffFixture{
		name:   "multi-series-mixed",
		schema: diffSchemaTagFieldMix(),
		opts: model.MeasureQueryOptions{
			TagProjection: []model.TagProjection{
				{Family: "default", Names: []string{"svc"}},
			},
			FieldProjection: []string{"v_int", "v_float"},
		},
		results: []*model.MeasureResult{rowSet(1, 100), rowSet(2, 200)},
	}
	rowOut, vecOut := runDiff(t, fx)
	assertDPEqual(t, fx.name, rowOut, vecOut)
}

func TestDiff_IndexMode_SingleRowResults(t *testing.T) {
	// indexSortResult.Pull yields single-row MeasureResults; mimic that shape.
	fx := diffFixture{
		name:   "index-mode-single-row",
		schema: diffSchemaTagFieldMix(),
		opts:   diffOptsAllTagsAllFields(),
		results: []*model.MeasureResult{
			rowSet(1, 100), rowSet(1, 200), rowSet(2, 300),
		},
	}
	rowOut, vecOut := runDiff(t, fx)
	assertDPEqual(t, fx.name, rowOut, vecOut)
}

func TestDiff_EmptyResultSet(t *testing.T) {
	fx := diffFixture{
		name:    "empty",
		schema:  diffSchemaTagFieldMix(),
		opts:    diffOptsAllTagsAllFields(),
		results: nil,
	}
	rowOut, vecOut := runDiff(t, fx)
	if len(rowOut) != 0 || len(vecOut) != 0 {
		t.Fatalf("empty fixture produced rows: row=%d vec=%d", len(rowOut), len(vecOut))
	}
}

func TestDiff_CrossesBatchBoundary(t *testing.T) {
	// BatchSize=4 (set in runDiff). Use 9 rows so we cross at least two
	// batch boundaries inside a single series.
	fx := diffFixture{
		name:    "batch-boundary",
		schema:  diffSchemaTagFieldMix(),
		opts:    diffOptsAllTagsAllFields(),
		results: []*model.MeasureResult{rowMulti(1, 1000, 9)},
	}
	rowOut, vecOut := runDiff(t, fx)
	assertDPEqual(t, fx.name, rowOut, vecOut)
}

func TestDiff_NullVariants(t *testing.T) {
	r := &model.MeasureResult{
		SID:        1,
		Timestamps: []int64{100},
		Versions:   []int64{1},
		ShardIDs:   []common.ShardID{0},
		TagFamilies: []model.TagFamily{
			{Name: "default", Tags: []model.Tag{
				{Name: "svc", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
				{Name: "env_id", Values: []*modelv1.TagValue{pbv1.NullTagValue}},
			}},
		},
		Fields: []model.Field{
			{Name: "v_int", Values: []*modelv1.FieldValue{pbv1.NullFieldValue}},
			{Name: "v_float", Values: []*modelv1.FieldValue{pbv1.NullFieldValue}},
		},
	}
	fx := diffFixture{
		name:   "null-variants",
		schema: diffSchemaTagFieldMix(),
		opts: model.MeasureQueryOptions{
			TagProjection:   []model.TagProjection{{Family: "default", Names: []string{"svc", "env_id"}}},
			FieldProjection: []string{"v_int", "v_float"},
		},
		results: []*model.MeasureResult{r},
	}
	rowOut, vecOut := runDiff(t, fx)
	assertDPEqual(t, fx.name, rowOut, vecOut)
}

func TestDiff_CanceledContext(t *testing.T) {
	// Build a result the iterator would normally walk through, then cancel
	// the context before iteration. Both paths must surface the cancellation
	// without producing partial output. The row path doesn't propagate
	// cancellation through the dummy fakeMeasureQueryResult, so this test
	// just asserts the vectorized path's Close is safe under cancel.
	fx := diffFixture{
		name:    "canceled-context",
		schema:  diffSchemaTagFieldMix(),
		opts:    diffOptsAllTagsAllFields(),
		results: []*model.MeasureResult{rowSet(1, 100), rowSet(2, 200)},
	}
	cfg := VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 64}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	qr := &fakeMeasureQueryResult{seq: cloneResults(fx.results)}
	it, err := NewMIterator(ctx, qr, fx.schema, fx.opts, cfg)
	if err != nil {
		t.Fatalf("%s: NewMIterator under canceled ctx: %v", fx.name, err)
	}
	if closeErr := it.Close(); closeErr != nil && !errors.Is(closeErr, context.Canceled) {
		t.Fatalf("%s: Close after cancel: %v", fx.name, closeErr)
	}
}

// rowWithShard mirrors rowSet but pins both Version and ShardID to caller-
// specified values. Used by parity tests that exercise non-zero ShardIDs and
// per-row version variations.
func rowWithShard(sid common.SeriesID, ts, version int64, shard common.ShardID) *model.MeasureResult {
	r := rowSet(sid, ts)
	r.Versions = []int64{version}
	r.ShardIDs = []common.ShardID{shard}
	return r
}

// TestDiff_NonZeroShardIDs pins ShardID column flow through the
// fillMetadata → serializeBatchToProto path: row path emits shard via
// dp.ShardId; vectorized path stores it as an int64 column then casts to
// uint32. Mismatched casts surface here.
func TestDiff_NonZeroShardIDs(t *testing.T) {
	fx := diffFixture{
		name:   "non-zero-shard-ids",
		schema: diffSchemaTagFieldMix(),
		opts:   diffOptsAllTagsAllFields(),
		results: []*model.MeasureResult{
			rowWithShard(1, 100, 1, 7),
			rowWithShard(2, 200, 1, 0),
			rowWithShard(3, 300, 1, 12345),
		},
	}
	rowOut, vecOut := runDiff(t, fx)
	assertDPEqual(t, fx.name, rowOut, vecOut)
}

// TestDiff_MixedShardIDsAcrossBatchBoundary keeps ShardID varying within a
// single MeasureResult so the bulk-fill metadata path copies a non-uniform
// shard column across a batch boundary.
func TestDiff_MixedShardIDsAcrossBatchBoundary(t *testing.T) {
	r := rowMulti(1, 1000, 9)
	for i := range r.ShardIDs {
		r.ShardIDs[i] = common.ShardID(i % 3)
	}
	fx := diffFixture{
		name:    "mixed-shard-ids-batch-boundary",
		schema:  diffSchemaTagFieldMix(),
		opts:    diffOptsAllTagsAllFields(),
		results: []*model.MeasureResult{r},
	}
	rowOut, vecOut := runDiff(t, fx)
	assertDPEqual(t, fx.name, rowOut, vecOut)
}

// TestDiff_VersionVariations advances per-row Version (the field that drives
// the row-path's queryResult.merge dedup, applied upstream of this adapter).
// At the adapter layer both paths must propagate every supplied row verbatim;
// dedup happens inside queryResult.Pull, not the iterator.
func TestDiff_VersionVariations(t *testing.T) {
	fx := diffFixture{
		name:   "version-variations",
		schema: diffSchemaTagFieldMix(),
		opts:   diffOptsAllTagsAllFields(),
		results: []*model.MeasureResult{
			rowWithShard(1, 100, 1, 0),
			rowWithShard(1, 200, 5, 0),
			rowWithShard(1, 300, 99, 0),
		},
	}
	rowOut, vecOut := runDiff(t, fx)
	assertDPEqual(t, fx.name, rowOut, vecOut)
}

// TestDiff_InterleavedSeriesAcrossResults verifies cross-MeasureResult
// ordering: each Pull yields a different series, and the adapter must emit
// rows in the order produced by the source (no internal reordering).
func TestDiff_InterleavedSeriesAcrossResults(t *testing.T) {
	fx := diffFixture{
		name:   "interleaved-multi-result",
		schema: diffSchemaTagFieldMix(),
		opts:   diffOptsAllTagsAllFields(),
		results: []*model.MeasureResult{
			rowWithShard(7, 100, 1, 0),
			rowWithShard(3, 110, 1, 1),
			rowWithShard(7, 120, 1, 0),
			rowWithShard(11, 130, 1, 2),
			rowWithShard(3, 140, 1, 1),
		},
	}
	rowOut, vecOut := runDiff(t, fx)
	assertDPEqual(t, fx.name, rowOut, vecOut)
}

// TestDiff_CanceledMidIteration starts iterating, cancels the context, then
// continues iterating. The vectorized adapter must safely terminate without
// returning partial garbage and Close must be safe.
func TestDiff_CanceledMidIteration(t *testing.T) {
	fx := diffFixture{
		name:    "canceled-mid-iteration",
		schema:  diffSchemaTagFieldMix(),
		opts:    diffOptsAllTagsAllFields(),
		results: []*model.MeasureResult{rowMulti(1, 1000, 9)},
	}
	cfg := VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 64}
	ctx, cancel := context.WithCancel(context.Background())
	qr := &fakeMeasureQueryResult{seq: cloneResults(fx.results)}
	it, err := NewMIterator(ctx, qr, fx.schema, fx.opts, cfg)
	if err != nil {
		t.Fatalf("%s: NewMIterator: %v", fx.name, err)
	}
	if !it.Next() {
		t.Fatalf("%s: first Next must succeed before cancel", fx.name)
	}
	if got := it.Current(); len(got) != 1 {
		t.Fatalf("%s: Current after first Next: want 1, got %d", fx.name, len(got))
	}
	cancel()
	// Subsequent Next calls may return true (the fake doesn't honor ctx) or
	// false; either is acceptable. The contract under test is that Close
	// after cancel cleans up without panicking and returns either nil or a
	// context.Canceled error.
	for it.Next() { //nolint:revive // intentionally drains remaining cached batch
	}
	if closeErr := it.Close(); closeErr != nil && !errors.Is(closeErr, context.Canceled) {
		t.Fatalf("%s: Close after mid-iteration cancel: %v", fx.name, closeErr)
	}
}

// TestDiff_StorageError_PropagatesViaErr confirms a Pull-time error from the
// MeasureQueryResult surfaces via Err() and is joined into Close().
func TestDiff_StorageError_PropagatesViaErr(t *testing.T) {
	boom := errors.New("simulated storage failure")
	fx := diffFixture{
		name:   "storage-error",
		schema: diffSchemaTagFieldMix(),
		opts:   diffOptsAllTagsAllFields(),
		results: []*model.MeasureResult{
			rowSet(1, 100),
			{Error: boom},
		},
	}
	cfg := VectorizedConfig{Enabled: true, BatchSize: 4, QueryMemoryMiB: 64}
	qr := &fakeMeasureQueryResult{seq: cloneResults(fx.results)}
	it, err := NewMIterator(context.Background(), qr, fx.schema, fx.opts, cfg)
	if err != nil {
		t.Fatalf("%s: NewMIterator: %v", fx.name, err)
	}
	for it.Next() { //nolint:revive // drain until error
	}
	if itErr := it.Err(); !errors.Is(itErr, boom) {
		t.Fatalf("%s: Err want %v, got %v", fx.name, boom, itErr)
	}
	if closeErr := it.Close(); !errors.Is(closeErr, boom) {
		t.Fatalf("%s: Close must surface sticky storage error; got %v", fx.name, closeErr)
	}
}
