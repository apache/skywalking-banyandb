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

package csvfmt

import (
	"fmt"
	"slices"
	"strings"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// ValueType is the cell encoding type of a column.
type ValueType uint8

// Value types of CSV columns.
const (
	ValueString ValueType = iota
	ValueInt
	ValueFloat
	ValueBinary
	ValueStringArray
	ValueIntArray
	ValueTimestamp
)

// String returns the `#datatype` token of the value type.
func (t ValueType) String() string {
	switch t {
	case ValueString:
		return "string"
	case ValueInt:
		return "long"
	case ValueFloat:
		return "double"
	case ValueBinary:
		return "base64Binary"
	case ValueStringArray:
		return "stringArray"
	case ValueIntArray:
		return "longArray"
	case ValueTimestamp:
		return "timestamp"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(t))
	}
}

// ColumnKind tells where the value of a column comes from.
type ColumnKind uint8

// Column kinds. The fixed columns of each model come first in a layout, followed by
// KindTag columns and, for measures, KindField columns.
const (
	KindName ColumnKind = iota
	KindElementID
	KindTimestamp
	KindVersion
	KindTraceID
	KindSpanID
	KindSpan
	KindPropertyID
	KindModRevision
	KindCreateRevision
	KindUpdatedAt
	KindDeleteTime
	KindTag
	KindField
)

// Column describes one CSV column.
type Column struct {
	// Header is the human-readable header cell, e.g. "_name", "searchable.trace_id",
	// "_field.value" or "svc_a::default.foo" for a type-conflict split column.
	Header string
	// Family is the tag family of a stream/measure tag column; empty otherwise.
	Family string
	// Name is the tag or field name; empty for fixed columns.
	Name string
	// Resources lists the resources owning a tag/field column; nil for fixed columns.
	Resources []string
	Kind      ColumnKind
	Type      ValueType
}

type colKey struct {
	family string
	name   string
}

// Layout is the group-level column vector of a CSV stream. It is a pure function of the
// schemas it was built from: columns follow resources in name order, so the same schemas
// in any order always produce the same layout.
type Layout struct {
	tagIdx   map[string]map[colKey]int
	fieldIdx map[string]map[string]int
	Columns  []Column
}

type tagDecl struct {
	family string
	name   string
	typ    ValueType
}

type resourceDecl struct {
	name   string
	tags   []tagDecl
	fields []tagDecl
}

// TopNResultMeasure is the internal measure that stores pre-aggregated Top-N results. Its
// only field is an engine-internal encoding, so CSV skips it and a target recomputes it from
// the source measures.
const TopNResultMeasure = "_top_n_result"

var (
	nameColumn      = Column{Header: "_name", Kind: KindName, Type: ValueString}
	timestampColumn = Column{Header: "_timestamp", Kind: KindTimestamp, Type: ValueTimestamp}
)

// StreamLayout builds the layout of a stream group:
// _name, _element_id, _timestamp, then the union of family.tag columns. Columns follow
// resources in name order, whatever the order of streams.
func StreamLayout(streams []*databasev1.Stream) (*Layout, error) {
	decls := make([]resourceDecl, 0, len(streams))
	for i, s := range streams {
		name := s.GetMetadata().GetName()
		if name == "" {
			return nil, fmt.Errorf("stream #%d has no name", i)
		}
		tags, err := familyTags(name, s.GetTagFamilies())
		if err != nil {
			return nil, err
		}
		decls = append(decls, resourceDecl{name: name, tags: tags})
	}
	return buildLayout([]Column{
		nameColumn,
		{Header: "_element_id", Kind: KindElementID, Type: ValueString},
		timestampColumn,
	}, decls)
}

// MeasureLayout builds the layout of a measure group:
// _name, _timestamp, _version, then the union of family.tag columns and the union of
// _field.* columns. Index-mode measures have no fields and so contribute no field columns.
// Columns follow resources in name order, whatever the order of measures. The internal
// TopNResultMeasure has no rows in CSV and is left out.
func MeasureLayout(measures []*databasev1.Measure) (*Layout, error) {
	decls := make([]resourceDecl, 0, len(measures))
	for i, m := range measures {
		name := m.GetMetadata().GetName()
		if name == "" {
			return nil, fmt.Errorf("measure #%d has no name", i)
		}
		if name == TopNResultMeasure {
			continue
		}
		tags, err := familyTags(name, m.GetTagFamilies())
		if err != nil {
			return nil, err
		}
		fields := make([]tagDecl, 0, len(m.GetFields()))
		for _, f := range m.GetFields() {
			t, err := fieldValueType(f.GetFieldType())
			if err != nil {
				return nil, fmt.Errorf("measure %q field %q: %w", name, f.GetName(), err)
			}
			fields = append(fields, tagDecl{name: f.GetName(), typ: t})
		}
		decls = append(decls, resourceDecl{name: name, tags: tags, fields: fields})
	}
	return buildLayout([]Column{
		nameColumn,
		timestampColumn,
		{Header: "_version", Kind: KindVersion, Type: ValueInt},
	}, decls)
}

// TraceLayout builds the layout of a trace group:
// _name, trace_id, span_id, _span, then the union of tag columns, excluding each trace's
// own trace-id and span-id tags. Columns follow resources in name order, whatever the
// order of traces.
func TraceLayout(traces []*databasev1.Trace) (*Layout, error) {
	decls := make([]resourceDecl, 0, len(traces))
	for i, tr := range traces {
		name := tr.GetMetadata().GetName()
		if name == "" {
			return nil, fmt.Errorf("trace #%d has no name", i)
		}
		tags := make([]tagDecl, 0, len(tr.GetTags()))
		for _, tag := range tr.GetTags() {
			if tag.GetName() == tr.GetTraceIdTagName() || tag.GetName() == tr.GetSpanIdTagName() {
				continue
			}
			t, err := tagValueType(tag.GetType(), true)
			if err != nil {
				return nil, fmt.Errorf("trace %q tag %q: %w", name, tag.GetName(), err)
			}
			tags = append(tags, tagDecl{name: tag.GetName(), typ: t})
		}
		decls = append(decls, resourceDecl{name: name, tags: tags})
	}
	return buildLayout([]Column{
		nameColumn,
		{Header: "trace_id", Kind: KindTraceID, Type: ValueString},
		{Header: "span_id", Kind: KindSpanID, Type: ValueString},
		{Header: "_span", Kind: KindSpan, Type: ValueBinary},
	}, decls)
}

// PropertyLayout builds the layout of a property group:
// _name, _id, _mod_revision, _create_revision, _updated_at, _delete_time, then the union
// of tag columns. Columns follow resources in name order, whatever the order of props.
func PropertyLayout(props []*databasev1.Property) (*Layout, error) {
	decls := make([]resourceDecl, 0, len(props))
	for i, p := range props {
		name := p.GetMetadata().GetName()
		if name == "" {
			return nil, fmt.Errorf("property #%d has no name", i)
		}
		tags := make([]tagDecl, 0, len(p.GetTags()))
		for _, tag := range p.GetTags() {
			t, err := tagValueType(tag.GetType(), true)
			if err != nil {
				return nil, fmt.Errorf("property %q tag %q: %w", name, tag.GetName(), err)
			}
			tags = append(tags, tagDecl{name: tag.GetName(), typ: t})
		}
		decls = append(decls, resourceDecl{name: name, tags: tags})
	}
	return buildLayout([]Column{
		nameColumn,
		{Header: "_id", Kind: KindPropertyID, Type: ValueString},
		{Header: "_mod_revision", Kind: KindModRevision, Type: ValueInt},
		{Header: "_create_revision", Kind: KindCreateRevision, Type: ValueInt},
		{Header: "_updated_at", Kind: KindUpdatedAt, Type: ValueTimestamp},
		{Header: "_delete_time", Kind: KindDeleteTime, Type: ValueTimestamp},
	}, decls)
}

func familyTags(resource string, families []*databasev1.TagFamilySpec) ([]tagDecl, error) {
	var tags []tagDecl
	for _, fam := range families {
		for _, tag := range fam.GetTags() {
			t, err := tagValueType(tag.GetType(), false)
			if err != nil {
				return nil, fmt.Errorf("%q tag %s.%s: %w", resource, fam.GetName(), tag.GetName(), err)
			}
			tags = append(tags, tagDecl{family: fam.GetName(), name: tag.GetName(), typ: t})
		}
	}
	return tags, nil
}

func tagValueType(t databasev1.TagType, allowTimestamp bool) (ValueType, error) {
	switch t {
	case databasev1.TagType_TAG_TYPE_STRING:
		return ValueString, nil
	case databasev1.TagType_TAG_TYPE_INT:
		return ValueInt, nil
	case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		return ValueStringArray, nil
	case databasev1.TagType_TAG_TYPE_INT_ARRAY:
		return ValueIntArray, nil
	case databasev1.TagType_TAG_TYPE_DATA_BINARY:
		return ValueBinary, nil
	case databasev1.TagType_TAG_TYPE_TIMESTAMP:
		if allowTimestamp {
			return ValueTimestamp, nil
		}
		return 0, fmt.Errorf("tag type %s is not supported by this model", t)
	default:
		return 0, fmt.Errorf("unsupported tag type %s", t)
	}
}

func fieldValueType(t databasev1.FieldType) (ValueType, error) {
	switch t {
	case databasev1.FieldType_FIELD_TYPE_STRING:
		return ValueString, nil
	case databasev1.FieldType_FIELD_TYPE_INT:
		return ValueInt, nil
	case databasev1.FieldType_FIELD_TYPE_DATA_BINARY:
		return ValueBinary, nil
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		return ValueFloat, nil
	default:
		return 0, fmt.Errorf("unsupported field type %s", t)
	}
}

func buildLayout(fixed []Column, decls []resourceDecl) (*Layout, error) {
	slices.SortStableFunc(decls, func(a, b resourceDecl) int { return strings.Compare(a.name, b.name) })
	l := &Layout{
		Columns:  append([]Column(nil), fixed...),
		tagIdx:   make(map[string]map[colKey]int, len(decls)),
		fieldIdx: make(map[string]map[string]int, len(decls)),
	}
	for _, d := range decls {
		if _, dup := l.tagIdx[d.name]; dup {
			return nil, fmt.Errorf("duplicate resource %q", d.name)
		}
		l.tagIdx[d.name] = make(map[colKey]int, len(d.tags))
		l.fieldIdx[d.name] = make(map[string]int, len(d.fields))
	}
	tagOf := func(d resourceDecl) []tagDecl { return d.tags }
	fieldOf := func(d resourceDecl) []tagDecl { return d.fields }
	// The "_field.<name>" headers belong to fields: a tag of family "_field" yields to them.
	fieldHeaders := map[string]struct{}{}
	for _, d := range decls {
		for _, f := range d.fields {
			fieldHeaders[header(KindField, "", f.name)] = struct{}{}
		}
	}
	if err := l.union(decls, tagOf, KindTag, fieldHeaders); err != nil {
		return nil, err
	}
	if err := l.union(decls, fieldOf, KindField, nil); err != nil {
		return nil, err
	}
	// A split header can still repeat another one, e.g. one resource declaring family "a"
	// with tag "b.c" and family "a.b" with tag "c"; such a group has no unambiguous header.
	headers := make(map[string]struct{}, len(l.Columns))
	for _, c := range l.Columns {
		if _, dup := headers[c.Header]; dup {
			return nil, fmt.Errorf("two columns share the header %q", c.Header)
		}
		headers[c.Header] = struct{}{}
	}
	return l, nil
}

// union appends the union of the declared columns. Columns sharing a key are merged unless
// two resources declare the key with different types, or the key's header is already taken
// (by a fixed column or an earlier column) or shared with another key, such as family "a"
// with tag "b.c" and family "a.b" with tag "c"; then every resource declaring the key gets
// its own "<resource>::<header>" column, so every header is unique. reserved headers count
// as taken.
func (l *Layout) union(decls []resourceDecl, get func(resourceDecl) []tagDecl, kind ColumnKind, reserved map[string]struct{}) error {
	types := make(map[colKey]map[ValueType]struct{})
	keysOf := make(map[string]map[colKey]struct{})
	for _, d := range decls {
		for _, td := range get(d) {
			h := header(kind, td.family, td.name)
			if keysOf[h] == nil {
				keysOf[h] = make(map[colKey]struct{}, 1)
			}
			keysOf[h][colKey{family: td.family, name: td.name}] = struct{}{}
		}
	}
	taken := make(map[string]struct{}, len(l.Columns)+len(reserved))
	for _, c := range l.Columns {
		taken[c.Header] = struct{}{}
	}
	for h := range reserved {
		taken[h] = struct{}{}
	}
	split := func(k colKey, h string) bool {
		_, isTaken := taken[h]
		return len(types[k]) > 1 || isTaken || len(keysOf[h]) > 1
	}
	for _, d := range decls {
		seen := make(map[colKey]struct{}, len(get(d)))
		for _, td := range get(d) {
			k := colKey{family: td.family, name: td.name}
			if _, dup := seen[k]; dup {
				return fmt.Errorf("resource %q declares %q twice", d.name, header(kind, td.family, td.name))
			}
			seen[k] = struct{}{}
			if types[k] == nil {
				types[k] = make(map[ValueType]struct{}, 1)
			}
			types[k][td.typ] = struct{}{}
		}
	}
	shared := make(map[colKey]int)
	for _, d := range decls {
		for _, td := range get(d) {
			k := colKey{family: td.family, name: td.name}
			h := header(kind, td.family, td.name)
			idx, ok := shared[k]
			switch {
			case split(k, h):
				// A type conflict or a header clash splits the column for every resource
				// declaring the key, including those whose type agrees with another resource.
				idx = l.appendColumn(Column{Header: d.name + "::" + h, Family: td.family, Name: td.name, Kind: kind, Type: td.typ})
			case !ok:
				idx = l.appendColumn(Column{Header: h, Family: td.family, Name: td.name, Kind: kind, Type: td.typ})
				shared[k] = idx
			}
			l.Columns[idx].Resources = append(l.Columns[idx].Resources, d.name)
			if kind == KindField {
				l.fieldIdx[d.name][td.name] = idx
			} else {
				l.tagIdx[d.name][k] = idx
			}
		}
	}
	return nil
}

func (l *Layout) appendColumn(c Column) int {
	l.Columns = append(l.Columns, c)
	return len(l.Columns) - 1
}

func header(kind ColumnKind, family, name string) string {
	if kind == KindField {
		return "_field." + name
	}
	if family == "" {
		return name
	}
	return family + "." + name
}

// TagIndex returns the column index of a resource's tag. family is "" for trace and property.
func (l *Layout) TagIndex(resource, family, tag string) (int, bool) {
	idx, ok := l.tagIdx[resource][colKey{family: family, name: tag}]
	return idx, ok
}

// FieldIndex returns the column index of a measure's field.
func (l *Layout) FieldIndex(resource, field string) (int, bool) {
	idx, ok := l.fieldIdx[resource][field]
	return idx, ok
}

// Header returns the header row.
func (l *Layout) Header() []string {
	h := make([]string, len(l.Columns))
	for i := range l.Columns {
		h[i] = l.Columns[i].Header
	}
	return h
}

// DatatypeLine returns the optional annotation row: "#datatype" followed by one type token
// per column, so its length is len(Columns)+1. Readers of annotated files must therefore
// allow a variable number of fields per record.
func (l *Layout) DatatypeLine() []string {
	line := make([]string, 0, len(l.Columns)+1)
	line = append(line, "#datatype")
	for i := range l.Columns {
		line = append(line, l.Columns[i].Type.String())
	}
	return line
}

// NewRow returns a row with every cell set to NullCell.
func (l *Layout) NewRow() []string {
	row := make([]string, len(l.Columns))
	for i := range row {
		row[i] = NullCell
	}
	return row
}

func (l *Layout) hasResource(resource string) bool {
	_, ok := l.tagIdx[resource]
	return ok
}
