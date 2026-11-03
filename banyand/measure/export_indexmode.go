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
	"fmt"
	"io/fs"
	"slices"
	"strings"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// IndexModeRow is one row of an index-mode measure read from a segment series index.
type IndexModeRow struct {
	// Tags is keyed "family.tag"; it includes the entity tags decoded from the series id.
	// A tag with no stored value (a null at write time) is absent.
	Tags map[string]*modelv1.TagValue
	// Subject is the measure name.
	Subject   string
	Timestamp int64
	Version   int64
	// UnknownFields counts stored fields that map to no tag of the measure's
	// current schema (e.g. a tag or index rule removed after the data was
	// written, or a multi-tag index rule whose values cannot be told apart) or
	// whose bytes do not decode as the tag's current type; their values are not
	// in Tags.
	UnknownFields int
}

type indexModeTagSpec struct {
	key       string
	valueType pbv1.ValueType
}

type indexModeMeasureSpec struct {
	byTagName map[string]indexModeTagSpec
	byRuleID  map[uint32]indexModeTagSpec
	// multiTagRules are the ids of rules over several tags of the measure; the export
	// reports their values once per measure, so their fields are not unknown.
	multiTagRules map[uint32]struct{}
	entityTags    []indexModeTagSpec
}

// WalkIndexModeRows visits every live row of the given index-mode measures stored in
// sidxDir (a segment's sidx directory, opened read-only); measures without
// index_mode are ignored. Documents whose subject is not one of measures (e.g.
// series documents of normal measures in a mixed group) are skipped. A missing or
// never-committed index is an empty walk, not an error. The row handed to visit
// is owned by the caller.
func WalkIndexModeRows(ctx context.Context, sidxDir string, measures []*databasev1.Measure, rules []*databasev1.IndexRule,
	visit func(*IndexModeRow) error,
) error {
	specs := buildIndexModeSpecs(measures, rules)
	if len(specs) == 0 {
		return nil
	}
	generation, err := native.OpenReadOnlyGeneration(sidxDir)
	if err != nil {
		if errors.Is(err, native.ErrNoSnapshot) || errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open sidx %s: %w", sidxDir, err)
	}
	defer func() { _ = generation.Close() }()
	var doc storedIndexModeDoc
	return generation.VisitLiveDocuments(ctx, func(source native.StoredDocument) error {
		if collectErr := doc.collect(source); collectErr != nil {
			return fmt.Errorf("read document in %s: %w", sidxDir, collectErr)
		}
		row, buildErr := doc.toRow(specs)
		if buildErr != nil {
			return fmt.Errorf("decode document in %s: %w", sidxDir, buildErr)
		}
		if row == nil {
			return nil
		}
		return visit(row)
	})
}

func buildIndexModeSpecs(measures []*databasev1.Measure, rules []*databasev1.IndexRule) map[string]*indexModeMeasureSpec {
	specs := make(map[string]*indexModeMeasureSpec, len(measures))
	for _, m := range measures {
		if !m.GetIndexMode() {
			continue
		}
		spec := &indexModeMeasureSpec{
			byTagName:     map[string]indexModeTagSpec{},
			byRuleID:      map[uint32]indexModeTagSpec{},
			multiTagRules: map[uint32]struct{}{},
		}
		for _, family := range m.GetTagFamilies() {
			for _, tag := range family.GetTags() {
				spec.byTagName[tag.GetName()] = indexModeTagSpec{
					key:       family.GetName() + "." + tag.GetName(),
					valueType: pbv1.TagValueSpecToValueType(tag.GetType()),
				}
			}
		}
		for _, rule := range rules {
			// A multi-tag rule stores the values of all its tags under one rule id, so a
			// stored value cannot be traced back to its tag.
			if len(rule.GetTags()) != 1 {
				if slices.ContainsFunc(rule.GetTags(), func(tag string) bool { _, ok := spec.byTagName[tag]; return ok }) {
					spec.multiTagRules[rule.GetMetadata().GetId()] = struct{}{}
				}
				continue
			}
			if tagSpec, ok := spec.byTagName[rule.GetTags()[0]]; ok {
				spec.byRuleID[rule.GetMetadata().GetId()] = tagSpec
			}
		}
		for _, tagName := range m.GetEntity().GetTagNames() {
			// An entity tag missing from the families cannot be keyed; keep the
			// position with an empty key so offsets stay aligned.
			spec.entityTags = append(spec.entityTags, spec.byTagName[tagName])
		}
		specs[m.GetMetadata().GetName()] = spec
	}
	return specs
}

// storedIndexModeDoc holds one document's stored fields, copied out of the
// borrowed buffers so they survive the visit callback.
type storedIndexModeDoc struct {
	id        []byte
	timestamp []byte
	version   []byte
	names     []string
	values    [][]byte
}

func (d *storedIndexModeDoc) collect(source native.StoredDocument) error {
	d.id, d.timestamp, d.version = nil, nil, nil
	d.names, d.values = d.names[:0], d.values[:0]
	return source.VisitStoredFields(func(name string, value []byte) bool {
		switch name {
		case imDocIDField:
			d.id = append([]byte(nil), value...)
		case imTimestampField:
			d.timestamp = append([]byte(nil), value...)
		case imVersionField:
			d.version = append([]byte(nil), value...)
		default:
			// The index-only fields the write path adds are never stored; ignore
			// them defensively rather than read them as tags.
			if name != index.IndexModeName && !strings.HasPrefix(name, index.IndexModeEntityTagPrefix) {
				d.names = append(d.names, strings.Clone(name))
				// A stored field always has a value: keep an empty one non-nil so it decodes
				// as an empty string, not as null.
				d.values = append(d.values, append([]byte{}, value...))
			}
		}
		return true
	})
}

// toRow decodes the collected document; it returns nil for a document whose
// subject is not one of the requested measures.
func (d *storedIndexModeDoc) toRow(specs map[string]*indexModeMeasureSpec) (*IndexModeRow, error) {
	var series pbv1.Series
	if err := series.Unmarshal(d.id); err != nil {
		return nil, fmt.Errorf("unmarshal series from %s: %w", imDocIDField, err)
	}
	spec, ok := specs[series.Subject]
	if !ok {
		return nil, nil
	}
	if d.timestamp == nil {
		return nil, fmt.Errorf("index-mode document of measure %q has no %s", series.Subject, imTimestampField)
	}
	ts, err := native.DecodeTimestamp(d.timestamp)
	if err != nil {
		return nil, fmt.Errorf("decode %s of measure %q: %w", imTimestampField, series.Subject, err)
	}
	row := &IndexModeRow{
		Subject:   series.Subject,
		Timestamp: ts,
		Tags:      make(map[string]*modelv1.TagValue, len(spec.byTagName)),
	}
	if d.version != nil {
		row.Version = convert.BytesToInt64(d.version)
	}
	for i, tagSpec := range spec.entityTags {
		if i < len(series.EntityValues) && tagSpec.key != "" {
			row.Tags[tagSpec.key] = series.EntityValues[i]
		}
	}
	// Array tags are stored as one field per element under the same name, so
	// group the values by tag before decoding.
	var order []indexModeTagSpec
	grouped := map[string][][]byte{}
	for i, name := range d.names {
		tagSpec, found := lookupStoredField(spec, name)
		if !found {
			if !spec.isMultiTagRuleField(name) {
				row.UnknownFields++
			}
			continue
		}
		if _, seen := grouped[tagSpec.key]; !seen {
			order = append(order, tagSpec)
		}
		grouped[tagSpec.key] = append(grouped[tagSpec.key], d.values[i])
	}
	for _, tagSpec := range order {
		values := grouped[tagSpec.key]
		switch tagSpec.valueType {
		case pbv1.ValueTypeStrArr, pbv1.ValueTypeInt64Arr:
			if !dump.FitsValueType(tagSpec.valueType, nil, values) {
				row.UnknownFields++
				continue
			}
			row.Tags[tagSpec.key] = dump.DecodeTagValue(tagSpec.valueType, nil, values)
		default:
			value := values[len(values)-1]
			if !dump.FitsValueType(tagSpec.valueType, value, nil) {
				row.UnknownFields++
				continue
			}
			row.Tags[tagSpec.key] = dump.DecodeTagValue(tagSpec.valueType, value, nil)
		}
	}
	return row, nil
}

// isMultiTagRuleField reports whether name is the marshaled id of a multi-tag rule over the
// measure's tags.
func (spec *indexModeMeasureSpec) isMultiTagRuleField(name string) bool {
	if len(name) != 4 {
		return false
	}
	_, ok := spec.multiTagRules[convert.BytesToUint32([]byte(name))]
	return ok
}

// lookupStoredField reverses index.FieldKey.Marshal like classifyStoredField: a
// schema tag name first (a 4-character tag name is as wide as a rule id), else a
// marshaled 4-byte IndexRuleID resolved through the index rules.
func lookupStoredField(spec *indexModeMeasureSpec, name string) (indexModeTagSpec, bool) {
	if tagSpec, ok := spec.byTagName[name]; ok {
		return tagSpec, true
	}
	if len(name) == 4 {
		tagSpec, ok := spec.byRuleID[convert.BytesToUint32([]byte(name))]
		return tagSpec, ok
	}
	return indexModeTagSpec{}, false
}

// ExportIndexModeRows lets the export service read index-mode rows without importing this
// package: it walks the rows with WalkIndexModeRows and hands each one to visit.
func (s *standalone) ExportIndexModeRows(ctx context.Context, sidxDir string, measures []*databasev1.Measure, rules []*databasev1.IndexRule,
	visit func(subject string, timestamp, version int64, tags map[string]*modelv1.TagValue, unknownFields int) error,
) error {
	return exportIndexModeRows(ctx, sidxDir, measures, rules, visit)
}

// ExportIndexModeRows is the data node counterpart of (*standalone).ExportIndexModeRows.
func (s *dataSVC) ExportIndexModeRows(ctx context.Context, sidxDir string, measures []*databasev1.Measure, rules []*databasev1.IndexRule,
	visit func(subject string, timestamp, version int64, tags map[string]*modelv1.TagValue, unknownFields int) error,
) error {
	return exportIndexModeRows(ctx, sidxDir, measures, rules, visit)
}

func exportIndexModeRows(ctx context.Context, sidxDir string, measures []*databasev1.Measure, rules []*databasev1.IndexRule,
	visit func(subject string, timestamp, version int64, tags map[string]*modelv1.TagValue, unknownFields int) error,
) error {
	return WalkIndexModeRows(ctx, sidxDir, measures, rules, func(r *IndexModeRow) error {
		return visit(r.Subject, r.Timestamp, r.Version, r.Tags, r.UnknownFields)
	})
}
