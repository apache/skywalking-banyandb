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

package lifecycle

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumpmeasure "github.com/apache/skywalking-banyandb/banyand/internal/dump/measure"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// fillTagValue overwrites tv in place, reusing its scalar oneof when the value
// type matches and only rebuilding on a type change. Equivalent to a fresh
// dump.DecodeTagValue.
func fillTagValue(tv *modelv1.TagValue, vt pbv1.ValueType, raw []byte) {
	// A nil raw is a null column entry. Skip the in-place scalar reuse — it would
	// decode nil bytes (convert.BytesToInt64(nil) panics; string(nil) silently
	// empties) — and fall through to dump.DecodeTagValue, which returns
	// NullTagValue for nil exactly as the row/non-pooled paths do.
	if raw != nil {
		switch vt { //nolint:exhaustive // only scalar str/int are reused in place; all other types fall through to a fresh decode.
		case pbv1.ValueTypeStr:
			if s, ok := tv.GetValue().(*modelv1.TagValue_Str); ok && s.Str != nil {
				s.Str.Value = string(raw)
				return
			}
		case pbv1.ValueTypeInt64:
			if i, ok := tv.GetValue().(*modelv1.TagValue_Int); ok && i.Int != nil {
				i.Int.Value = convert.BytesToInt64(raw)
				return
			}
		}
	}
	if v := dump.DecodeTagValue(vt, raw, nil); v != nil {
		tv.Value = v.Value
	}
}

// fillFieldValue is fillTagValue for fields (int/float/str scalars reused).
func fillFieldValue(fv *modelv1.FieldValue, vt pbv1.ValueType, raw []byte) {
	// A nil raw is a null/absent column entry. Skip the in-place scalar reuse — it
	// would decode nil bytes (convert.BytesToInt64/Float64(nil) panics; string(nil)
	// silently empties) — and fall through to dumpmeasure.DecodeFieldValue, which
	// returns NullFieldValue for nil exactly as the row/non-pooled paths do.
	if raw != nil {
		switch vt { //nolint:exhaustive // only scalar int/float/str are reused in place; all other types fall through to a fresh decode.
		case pbv1.ValueTypeInt64:
			if i, ok := fv.GetValue().(*modelv1.FieldValue_Int); ok && i.Int != nil {
				i.Int.Value = convert.BytesToInt64(raw)
				return
			}
		case pbv1.ValueTypeFloat64:
			if f, ok := fv.GetValue().(*modelv1.FieldValue_Float); ok && f.Float != nil {
				f.Float.Value = convert.BytesToFloat64(raw)
				return
			}
		case pbv1.ValueTypeStr:
			if s, ok := fv.GetValue().(*modelv1.FieldValue_Str); ok && s.Str != nil {
				s.Str.Value = string(raw)
				return
			}
		}
	}
	if v := dumpmeasure.DecodeFieldValue(vt, raw); v != nil {
		fv.Value = v.Value
	}
}

// tagFamilySlot is a reusable TagFamilyForWrite plus a per-position flag marking
// whether the current TagValue is a shared (block-cached) pointer that must not
// be mutated in place.
type tagFamilySlot struct {
	pb        *modelv1.TagFamilyForWrite
	ownShared []bool
	// fullNames[ti] is the precomputed "family.tag" column key for tag ti, built
	// once in prepare so the per-row hot path never re-concatenates it.
	fullNames []string
}

// measureProtoBuilder is a single reusable measure write-request tree. Because
// the row-replay sender marshals each request to bytes immediately (and holds
// the bytes, not the proto), one tree can be refilled for every row instead of
// allocating a fresh graph — eliminating the bulk of the build churn.
type measureProtoBuilder struct {
	iwr     *measurev1.InternalWriteRequest
	wr      *measurev1.WriteRequest
	dpv     *measurev1.DataPointValue
	meta    *commonv1.Metadata
	subject string
	fams    []*tagFamilySlot
	famPB   []*modelv1.TagFamilyForWrite
	fields  []*modelv1.FieldValue
}

// shapeMatches reports whether the existing tree already matches the schema's
// tag-family/field layout, so it can be refilled without reallocation.
func (b *measureProtoBuilder) shapeMatches(schema *databasev1.Measure) bool {
	if b.iwr == nil || len(b.fams) != len(schema.TagFamilies) || len(b.fields) != len(schema.Fields) {
		return false
	}
	for i, fam := range schema.TagFamilies {
		if len(b.fams[i].pb.Tags) != len(fam.Tags) {
			return false
		}
	}
	return true
}

// prepare points the tree at subject, reallocating the graph only when the
// schema shape changes (measures with identical layouts share the tree).
func (b *measureProtoBuilder) prepare(group, subject string, schema *databasev1.Measure) {
	b.subject = subject
	if b.shapeMatches(schema) {
		b.meta.Name = subject
		// Same tree shape, but possibly a different measure (shapeMatches only
		// compares tag/field counts, not names): refresh the cached "family.tag"
		// column keys so the per-row fast path looks up THIS measure's columns,
		// not the previously prepared one's. Done once per series, not per row.
		for fi, fam := range schema.TagFamilies {
			for ti, t := range fam.Tags {
				b.fams[fi].fullNames[ti] = fam.Name + "." + t.Name
			}
		}
		return
	}
	b.fams = make([]*tagFamilySlot, len(schema.TagFamilies))
	b.famPB = make([]*modelv1.TagFamilyForWrite, len(schema.TagFamilies))
	for fi, fam := range schema.TagFamilies {
		tf := &modelv1.TagFamilyForWrite{Tags: make([]*modelv1.TagValue, len(fam.Tags))}
		fullNames := make([]string, len(fam.Tags))
		for ti, t := range fam.Tags {
			fullNames[ti] = fam.Name + "." + t.Name
		}
		b.fams[fi] = &tagFamilySlot{pb: tf, ownShared: make([]bool, len(fam.Tags)), fullNames: fullNames}
		b.famPB[fi] = tf
	}
	b.fields = make([]*modelv1.FieldValue, len(schema.Fields))
	b.meta = &commonv1.Metadata{Group: group, Name: subject}
	b.dpv = &measurev1.DataPointValue{TagFamilies: b.famPB, Fields: b.fields, Timestamp: &timestamppb.Timestamp{}}
	b.wr = &measurev1.WriteRequest{Metadata: b.meta, DataPoint: b.dpv}
	b.iwr = &measurev1.InternalWriteRequest{Request: b.wr}
}

// fillTagFamilies overwrites the reused tag-family trees from the columns,
// mirroring buildMeasureTagFamiliesColumnar so the result is identical. Column
// tags own a reusable TagValue (filled in place); indexed/entity tags reuse the
// block-cached shared pointers, which the column branch never mutates.
func (b *measureProtoBuilder) fillTagFamilies(families []*databasev1.TagFamilySpec,
	cb *dumpmeasure.ColumnarBlock, i int, entityIdx, indexedTyped map[string]*modelv1.TagValue,
) {
	for fi, fam := range families {
		tf := b.fams[fi]
		for ti, t := range fam.Tags {
			fullName := tf.fullNames[ti]
			if col, ok := cb.TagCols[fullName]; ok && i < len(col) {
				vt, hasType := cb.TagTypes[fullName]
				if !hasType {
					vt = pbv1.TagValueSpecToValueType(t.Type)
				}
				if tf.pb.Tags[ti] == nil || tf.pb.Tags[ti] == pbv1.NullTagValue || tf.ownShared[ti] {
					tf.pb.Tags[ti] = dump.DecodeTagValue(vt, col[i], nil)
					tf.ownShared[ti] = false
				} else {
					fillTagValue(tf.pb.Tags[ti], vt, col[i])
				}
				continue
			}
			if tv, ok := indexedTyped[fullName]; ok && tv != nil {
				tf.pb.Tags[ti] = tv
				tf.ownShared[ti] = true
				continue
			}
			if tv, ok := entityIdx[t.Name]; ok && tv != nil {
				tf.pb.Tags[ti] = tv
				tf.ownShared[ti] = true
				continue
			}
			tf.pb.Tags[ti] = pbv1.NullTagValue
			tf.ownShared[ti] = true
		}
	}
}

// fillFields overwrites the reused field slice from the columns.
func (b *measureProtoBuilder) fillFields(specs []*databasev1.FieldSpec, cb *dumpmeasure.ColumnarBlock, i int) {
	for fi, spec := range specs {
		col, ok := cb.FieldCols[spec.Name]
		if !ok || i >= len(col) {
			b.fields[fi] = pbv1.NullFieldValue
			continue
		}
		vt, hasType := cb.FieldTypes[spec.Name]
		if !hasType {
			vt = fieldTypeToValueType(spec.FieldType)
		}
		if b.fields[fi] == nil || b.fields[fi] == pbv1.NullFieldValue {
			b.fields[fi] = dumpmeasure.DecodeFieldValue(vt, col[i])
		} else {
			fillFieldValue(b.fields[fi], vt, col[i])
		}
	}
}

// fillWriteRequestColumnar refills the builder's reusable tree for row i of cb
// and returns the request + target node. Output is byte-identical to
// buildWriteRequestColumnar; this is the pooled (allocation-free per row) path.
func (r *measureRowReplayer) fillWriteRequestColumnar(
	ir *dump.IndexResolver, bc *measureBlockCtx, b *measureProtoBuilder, cb *dumpmeasure.ColumnarBlock, i int,
) (*measurev1.InternalWriteRequest, string, error) {
	if blockErr := r.ensureBlock(ir, bc, cb); blockErr != nil {
		return nil, "", blockErr
	}
	if b.subject != bc.subject {
		b.prepare(r.group, bc.subject, bc.cached.schema)
	}
	tt := time.Unix(0, cb.Timestamp(i))
	b.dpv.Timestamp.Seconds = tt.Unix()
	b.dpv.Timestamp.Nanos = int32(tt.Nanosecond())
	b.dpv.Version = cb.Version(i)
	b.wr.MessageId = uint64(time.Now().UnixNano())
	b.fillTagFamilies(bc.cached.schema.TagFamilies, cb, i, bc.entityIdx, bc.indexedTyped)
	b.fillFields(bc.cached.schema.Fields, cb, i)
	shardID, entityEnc, nodeID, err := r.routeColumnar(bc, b.famPB)
	if err != nil {
		return nil, "", err
	}
	b.iwr.ShardId = shardID
	b.iwr.EntityValues = entityEnc
	return b.iwr, nodeID, nil
}
