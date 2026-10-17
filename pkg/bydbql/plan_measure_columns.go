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

package bydbql

import (
	"fmt"
	"slices"
	"strings"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/ql"
	"github.com/apache/skywalking-banyandb/pkg/query/vectorized"
)

type measureColumns struct {
	unambiguousTags map[measureColumnKey]bool
	tags            map[string][]vectorized.ColumnDef
	fields          map[string]vectorized.ColumnDef
	tagSpecs        map[string]*tagSpecWithFamily
	ordered         []vectorized.ColumnDef
}

type measureColumnKey struct {
	family, name string
	role         vectorized.ColumnRole
}

func (m *MeasureTemplate) compileRaw(columns *measureColumns) error {
	stmt := m.statement.template.Select
	if stmt.Projection == nil || stmt.Projection.Empty {
		return fmt.Errorf("projection must not be empty")
	}
	if stmt.Projection.TopN != nil || stmt.GroupBy != nil {
		return fmt.Errorf("extended grouped and TOP queries are not supported by typed raw execution")
	}
	for _, column := range stmt.Projection.Columns {
		if column.Aggregate != nil {
			return fmt.Errorf("extended aggregate queries are not supported by typed raw execution")
		}
	}
	var err error
	if containsQuotedMeasureLiteral(stmt.Projection, columns) {
		return m.compileExpressions(columns)
	}
	var slots []ql.Slot
	if stmt.Projection.All {
		for _, column := range columns.ordered {
			slots = append(slots, ql.Slot{ID: ql.SlotID(len(slots)), Source: column, Output: column, Nullable: true})
		}
	} else {
		for _, column := range stmt.Projection.Columns {
			if column.TypeSpec == nil && isBuiltinTime(column.Identifier) {
				if column.Alias == nil {
					continue
				} // DataPoint.timestamp is always preserved.
				alias, aliasErr := column.Alias.ToString(true)
				if aliasErr != nil {
					return aliasErr
				}
				source := vectorized.ColumnDef{Name: "time", Role: vectorized.RoleTimestamp, Type: vectorized.ColumnTypeInt64}
				output := vectorized.ColumnDef{Name: alias, TagFamily: "__ql", Role: vectorized.RoleTag, Type: vectorized.ColumnTypeInt64}
				slots = append(slots, ql.Slot{ID: ql.SlotID(len(slots)), Source: source, Output: output})
				continue
			}
			name, nameErr := column.Identifier.ToString(column.TypeSpec != nil)
			if nameErr != nil {
				return nameErr
			}
			source, sourceErr := columns.resolve(name, column.TypeSpec)
			if sourceErr != nil {
				return sourceErr
			}
			output := source
			if column.Alias != nil {
				alias, aliasErr := column.Alias.ToString(true)
				if aliasErr != nil {
					return aliasErr
				}
				output.Name = alias
			}
			slots = append(slots, ql.Slot{ID: ql.SlotID(len(slots)), Source: source, Output: output, Nullable: true})
		}
	}
	if err = validateRawOutputSlots(slots); err != nil {
		return err
	}
	m.slots = slots
	m.request = baseMeasureRequest(stmt)
	m.request.OrderBy, err = rawOrderBy(stmt.OrderBy, slots, columns)
	if err != nil {
		return err
	}
	m.request.TagProjection, m.request.FieldProjection = rawScanProjection(slots)
	if m.request.TagProjection == nil && m.request.FieldProjection == nil {
		// Native scans require one user column even when SQL selects only time.
		// This hidden anchor is intentionally absent from the visible slot list.
		if len(columns.ordered) == 0 {
			return fmt.Errorf("time projection requires a native source column")
		}
		anchor := columns.ordered[0]
		for _, candidate := range columns.ordered {
			if candidate.Role == vectorized.RoleTag && candidate.Entity {
				anchor = candidate
				break
			}
		}
		m.request.TagProjection, m.request.FieldProjection = rawScanProjection([]ql.Slot{{Source: anchor}})
	}
	if stmt.Where != nil {
		err = m.compileWhere(columns)
		if err != nil {
			return err
		}
	}
	return nil
}

// rawOrderBy resolves visible aliases before the legacy index-rule fallback.
// General column sorting is owned by the complete ORDER BY capability.
func rawOrderBy(orderBy *GrammarSelectOrderByClause, slots []ql.Slot, columns *measureColumns) (*modelv1.QueryOrder, error) {
	native := (&transformRun{}).convertSelectOrderBy(orderBy)
	if orderBy == nil || orderBy.Tail.WithIdent == nil {
		return native, nil
	}
	identifier := orderBy.Tail.WithIdent.Identifier
	name, err := identifier.ToString(true)
	if err != nil {
		return nil, err
	}
	var resolved *ql.Slot
	for i := range slots {
		if slots[i].Output.Name != name {
			continue
		}
		if resolved != nil {
			return nil, fmt.Errorf("ambiguous ORDER BY output name %q", name)
		}
		resolved = &slots[i]
	}
	if resolved != nil {
		if resolved.Source.Role != vectorized.RoleTimestamp {
			return nil, fmt.Errorf("ORDER BY output %q requires column sorting", name)
		}
		native.IndexRuleName = ""
		return native, nil
	}
	if isBuiltinTime(identifier) {
		native.IndexRuleName = ""
		return native, nil
	}
	if len(columns.tags[name]) > 0 {
		return nil, fmt.Errorf("ORDER BY source %q requires column sorting", name)
	}
	if _, exists := columns.fields[name]; exists {
		return nil, fmt.Errorf("ORDER BY source %q requires column sorting", name)
	}
	return native, nil
}

func resolveMeasureColumns(groups []string, schemas map[string]*databasev1.Measure) (*measureColumns, error) {
	columns := &measureColumns{tags: make(map[string][]vectorized.ColumnDef), fields: make(map[string]vectorized.ColumnDef), tagSpecs: make(map[string]*tagSpecWithFamily)}
	seen := make(map[measureColumnKey]vectorized.ColumnDef)
	for _, group := range groups {
		schema := schemas[group]
		for _, family := range schema.TagFamilies {
			for _, tag := range family.Tags {
				columnType, err := measureTagType(tag.Type)
				if err != nil {
					return nil, err
				}
				column := vectorized.ColumnDef{
					Name: tag.Name, TagFamily: family.Name, Role: vectorized.RoleTag, Type: columnType,
					Entity: slices.Contains(schema.GetEntity().GetTagNames(), tag.Name),
				}
				key := measureColumnKey{family: family.Name, name: tag.Name, role: vectorized.RoleTag}
				if prior, exists := seen[key]; exists {
					if prior.Type != column.Type {
						return nil, fmt.Errorf("tag %s.%s has incompatible types across source groups", family.Name, tag.Name)
					}
					continue
				}
				seen[key] = column
				columns.ordered = append(columns.ordered, column)
				columns.tags[tag.Name] = append(columns.tags[tag.Name], column)
				qualified := family.Name + "." + tag.Name
				if qualified != tag.Name {
					columns.tags[qualified] = append(columns.tags[qualified], column)
				}
				columns.tagSpecs[qualified] = &tagSpecWithFamily{family: family.Name, tag: tag}
			}
		}
		for _, field := range schema.Fields {
			columnType, err := measureFieldType(field.FieldType)
			if err != nil {
				return nil, err
			}
			column := vectorized.ColumnDef{Name: field.Name, Role: vectorized.RoleField, Type: columnType}
			if prior, exists := columns.fields[field.Name]; exists {
				if prior.Type != column.Type {
					return nil, fmt.Errorf("field %s has incompatible types across source groups", field.Name)
				}
				continue
			}
			columns.fields[field.Name] = column
			columns.ordered = append(columns.ordered, column)
		}
	}
	columns.unambiguousTags = unambiguousMeasureTags(columns.ordered)
	return columns, nil
}

func unambiguousMeasureTags(columns []vectorized.ColumnDef) map[measureColumnKey]bool {
	names := make(map[string]int)
	for _, source := range columns {
		if source.Role == vectorized.RoleTag {
			names[source.Name]++
		}
	}
	tags := make(map[measureColumnKey]bool)
	for _, source := range columns {
		if source.Role == vectorized.RoleTag && names[source.Name] == 1 {
			tags[columnIdentity(source)] = true
		}
	}
	return tags
}

func (c *measureColumns) resolve(name string, typeSpec *string) (vectorized.ColumnDef, error) {
	kind := columnTypeAuto
	if typeSpec != nil {
		kind = strings.ToUpper(*typeSpec)
	}
	tags := c.tags[name]
	field, hasField := c.fields[name]
	if kind == columnTypeField {
		tags = nil
	}
	if kind == columnTypeTag {
		hasField = false
	}
	if len(tags) > 1 || (len(tags) > 0 && hasField) {
		return vectorized.ColumnDef{}, fmt.Errorf("ambiguous column %q; qualify its tag family or use ::TAG/::FIELD", name)
	}
	if len(tags) == 1 {
		return tags[0], nil
	}
	if hasField {
		return field, nil
	}
	return vectorized.ColumnDef{}, fmt.Errorf("column %q not found in schema", name)
}

func rawScanProjection(slots []ql.Slot) (*modelv1.TagProjection, *measurev1.QueryRequest_FieldProjection) {
	var tags *modelv1.TagProjection
	var fields *measurev1.QueryRequest_FieldProjection
	families := make(map[string]*modelv1.TagProjection_TagFamily)
	seen := make(map[measureColumnKey]bool)
	for _, slot := range slots {
		column := slot.Source
		key := measureColumnKey{family: column.TagFamily, name: column.Name, role: column.Role}
		if seen[key] {
			continue
		}
		seen[key] = true
		switch column.Role {
		case vectorized.RoleTag:
			if tags == nil {
				tags = &modelv1.TagProjection{}
			}
			family := families[column.TagFamily]
			if family == nil {
				family = &modelv1.TagProjection_TagFamily{Name: column.TagFamily}
				families[column.TagFamily] = family
				tags.TagFamilies = append(tags.TagFamilies, family)
			}
			family.Tags = append(family.Tags, column.Name)
		case vectorized.RoleField:
			if fields == nil {
				fields = &measurev1.QueryRequest_FieldProjection{}
			}
			fields.Names = append(fields.Names, column.Name)
		default:
			// Timestamp and other native metadata do not require a user projection.
		}
	}
	return tags, fields
}

func measureTagType(tagType databasev1.TagType) (vectorized.ColumnType, error) {
	switch tagType {
	case databasev1.TagType_TAG_TYPE_STRING:
		return vectorized.ColumnTypeString, nil
	case databasev1.TagType_TAG_TYPE_INT:
		return vectorized.ColumnTypeInt64, nil
	case databasev1.TagType_TAG_TYPE_DATA_BINARY:
		return vectorized.ColumnTypeBytes, nil
	case databasev1.TagType_TAG_TYPE_INT_ARRAY:
		return vectorized.ColumnTypeInt64Array, nil
	case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		return vectorized.ColumnTypeStrArray, nil
	case databasev1.TagType_TAG_TYPE_TIMESTAMP:
		return vectorized.ColumnTypeTagValue, nil
	default:
		return 0, fmt.Errorf("unsupported tag type %s", tagType)
	}
}

func measureFieldType(fieldType databasev1.FieldType) (vectorized.ColumnType, error) {
	switch fieldType {
	case databasev1.FieldType_FIELD_TYPE_STRING:
		return vectorized.ColumnTypeString, nil
	case databasev1.FieldType_FIELD_TYPE_INT:
		return vectorized.ColumnTypeInt64, nil
	case databasev1.FieldType_FIELD_TYPE_DATA_BINARY:
		return vectorized.ColumnTypeBytes, nil
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		return vectorized.ColumnTypeFloat64, nil
	default:
		return 0, fmt.Errorf("unsupported field type %s", fieldType)
	}
}

func validateRawOutputSlots(slots []ql.Slot) error {
	outputs := make(map[measureColumnKey]bool, len(slots))
	for _, slot := range slots {
		key := measureColumnKey{family: slot.Output.TagFamily, name: slot.Output.Name, role: slot.Output.Role}
		if outputs[key] {
			return fmt.Errorf("duplicate output name %q in container %q", key.name, key.family)
		}
		outputs[key] = true
	}
	return nil
}

func containsQuotedMeasureLiteral(projection *GrammarProjection, columns *measureColumns) bool {
	for _, column := range projection.Columns {
		if isQuotedMeasureLiteral(column, columns) {
			return true
		}
	}
	return false
}
