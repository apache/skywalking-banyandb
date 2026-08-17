// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package tools

import (
	"fmt"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// A resource can exist in several groups with different capabilities. Only what every group agrees
// on is exposed, so a query built from the merged snapshot is valid against all of them.

func mergeGroupSchemas(req SchemaRequest, snapshots []session.SchemaSnapshot) (session.SchemaSnapshot, error) {
	if len(snapshots) == 0 {
		return session.SchemaSnapshot{}, fmt.Errorf("schema unavailable for requested groups")
	}
	merged := cloneSchemaSummary(snapshots[0])
	merged.Groups = append([]string(nil), req.Groups...)
	for snapshotIndex := 1; snapshotIndex < len(snapshots); snapshotIndex++ {
		current := snapshots[snapshotIndex]
		if current.Type != merged.Type || current.Name != merged.Name {
			return session.SchemaSnapshot{}, fmt.Errorf("schema identity differs across requested groups")
		}
		if current.SourceMeasure != merged.SourceMeasure || current.FieldValueSort != merged.FieldValueSort {
			return session.SchemaSnapshot{}, fmt.Errorf("TopN schema differs across requested groups")
		}
		if current.SourceMeasureGroup != merged.SourceMeasureGroup {
			merged.SourceMeasureGroup = ""
		}
		if current.TraceIDTag != merged.TraceIDTag {
			merged.TraceIDTag = ""
		}
		if current.TimestampTag != merged.TimestampTag {
			merged.TimestampTag = ""
		}
		merged.Tags = intersectStrings(merged.Tags, current.Tags)
		merged.EntityTags = intersectStrings(merged.EntityTags, current.EntityTags)
		merged.Fields = intersectStrings(merged.Fields, current.Fields)
		merged.Columns = intersectColumns(merged.Columns, current.Columns)
		merged.IndexedFields = intersectStrings(merged.IndexedFields, current.IndexedFields)
		merged.SortableIndexes = intersectSortableIndexes(merged.SortableIndexes, current.SortableIndexes)
		merged.ResourceNames = intersectStrings(merged.ResourceNames, current.ResourceNames)
		if current.UpdatedAt.Before(merged.UpdatedAt) {
			merged.UpdatedAt = current.UpdatedAt
		}
		merged.Loaded = merged.Loaded && current.Loaded
	}
	merged.Fingerprint = ""
	merged.EnsureFingerprint()
	return merged, nil
}

func enrichTopNSchema(topNSnapshot *session.SchemaSnapshot, sourceSnapshot session.SchemaSnapshot) {
	if topNSnapshot == nil {
		return
	}
	topNSnapshot.EntityTags = make([]string, 0, len(topNSnapshot.Tags))
	for _, groupByTag := range topNSnapshot.Tags {
		if column, found := sourceSnapshot.Column(groupByTag); found {
			topNSnapshot.EntityTags = append(topNSnapshot.EntityTags, column.Name)
		}
	}
	columnByName := make(map[string]session.SchemaColumn, len(sourceSnapshot.Columns))
	for _, column := range sourceSnapshot.Columns {
		columnByName[column.Name] = column
	}
	columns := make([]session.SchemaColumn, 0, len(topNSnapshot.Tags)+1+len(topNSnapshot.EntityTags))
	seen := make(map[string]struct{})
	for _, columnName := range append(append([]string(nil), topNSnapshot.Tags...), topNSnapshot.EntityTags...) {
		column, ok := columnByName[columnName]
		if !ok {
			column, ok = sourceSnapshot.Column(columnName)
		}
		if !ok {
			continue
		}
		if _, exists := seen[column.Name]; exists {
			continue
		}
		seen[column.Name] = struct{}{}
		columns = append(columns, column)
	}
	for _, fieldName := range topNSnapshot.Fields {
		column, ok := columnByName[fieldName]
		if !ok {
			column, ok = sourceSnapshot.Column(fieldName)
		}
		if !ok {
			continue
		}
		if _, exists := seen[column.Name]; exists {
			continue
		}
		seen[column.Name] = struct{}{}
		columns = append(columns, column)
	}
	topNSnapshot.Columns = columns
}

func cloneSchemaSummary(snapshot session.SchemaSnapshot) session.SchemaSnapshot {
	cloned := snapshot
	cloned.Groups = append([]string(nil), snapshot.Groups...)
	cloned.Tags = append([]string(nil), snapshot.Tags...)
	cloned.EntityTags = append([]string(nil), snapshot.EntityTags...)
	cloned.Fields = append([]string(nil), snapshot.Fields...)
	cloned.Columns = append([]session.SchemaColumn(nil), snapshot.Columns...)
	cloned.IndexedFields = append([]string(nil), snapshot.IndexedFields...)
	cloned.SortableIndexes = cloneSortableIndexSummary(snapshot.SortableIndexes)
	cloned.ResourceNames = append([]string(nil), snapshot.ResourceNames...)
	return cloned
}

func cloneSortableIndexSummary(indexes []session.SortableIndex) []session.SortableIndex {
	cloned := append([]session.SortableIndex(nil), indexes...)
	for indexPosition := range cloned {
		cloned[indexPosition].Tags = append([]string(nil), indexes[indexPosition].Tags...)
	}
	return cloned
}

func intersectStrings(left, right []string) []string {
	rightValues := make(map[string]struct{}, len(right))
	for _, value := range right {
		rightValues[value] = struct{}{}
	}
	intersection := make([]string, 0, len(left))
	for _, value := range left {
		if _, ok := rightValues[value]; ok {
			intersection = append(intersection, value)
		}
	}
	return intersection
}

func intersectColumns(left, right []session.SchemaColumn) []session.SchemaColumn {
	rightColumns := make(map[string]session.SchemaColumn, len(right))
	for _, column := range right {
		rightColumns[column.Name] = column
	}
	intersection := make([]session.SchemaColumn, 0, len(left))
	for _, column := range left {
		rightColumn, ok := rightColumns[column.Name]
		if !ok || rightColumn.Kind != column.Kind || rightColumn.Type != column.Type {
			continue
		}
		column.Indexed = column.Indexed && rightColumn.Indexed
		intersection = append(intersection, column)
	}
	return intersection
}

func intersectSortableIndexes(left, right []session.SortableIndex) []session.SortableIndex {
	rightIndexes := make(map[string]session.SortableIndex, len(right))
	for _, index := range right {
		rightIndexes[index.RuleName] = index
	}
	intersection := make([]session.SortableIndex, 0, len(left))
	for _, index := range left {
		rightIndex, ok := rightIndexes[index.RuleName]
		if !ok || !sameStrings(index.Tags, rightIndex.Tags) {
			continue
		}
		intersection = append(intersection, session.SortableIndex{
			RuleName: index.RuleName,
			Tags:     append([]string(nil), index.Tags...),
		})
	}
	return intersection
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for valueIndex := range left {
		if left[valueIndex] != right[valueIndex] {
			return false
		}
	}
	return true
}
