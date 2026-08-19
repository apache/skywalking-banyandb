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

// Package tools implements controlled schema discovery and BYDBQL execution for the TUI.
package tools

import (
	"fmt"
	"slices"

	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
)

// A resource can exist in several groups with different capabilities. Only what every group agrees
// on is exposed, so a query built from the merged snapshot is valid against all of them.

func mergeGroupSchemas(req SchemaRequest, snapshots []session.SchemaSnapshot) (session.SchemaSnapshot, error) {
	if len(snapshots) == 0 {
		return session.SchemaSnapshot{}, fmt.Errorf("schema unavailable for requested groups")
	}
	merged := session.CloneSchemaSnapshot(snapshots[0])
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

// enrichTopNSchema gives a TOPN snapshot the typed columns of the measure it aggregates.
//
// A TOPN registration names only its group-by tags and one field, so their types come from the
// source measure; anything the measure does not define is dropped rather than guessed at.
func enrichTopNSchema(topNSnapshot *session.SchemaSnapshot, sourceSnapshot session.SchemaSnapshot) {
	if topNSnapshot == nil {
		return
	}
	columnByName := make(map[string]session.SchemaColumn, len(sourceSnapshot.Columns))
	for _, column := range sourceSnapshot.Columns {
		columnByName[column.Name] = column
	}
	sourceColumn := func(name string) (session.SchemaColumn, bool) {
		if column, found := columnByName[name]; found {
			return column, true
		}
		return sourceSnapshot.Column(name)
	}
	topNSnapshot.EntityTags = make([]string, 0, len(topNSnapshot.Tags))
	for _, groupByTag := range topNSnapshot.Tags {
		if column, found := sourceSnapshot.Column(groupByTag); found {
			topNSnapshot.EntityTags = append(topNSnapshot.EntityTags, column.Name)
		}
	}
	columns := make([]session.SchemaColumn, 0, len(topNSnapshot.Tags)+len(topNSnapshot.EntityTags)+len(topNSnapshot.Fields))
	seen := make(map[string]struct{})
	for _, columnName := range slices.Concat(topNSnapshot.Tags, topNSnapshot.EntityTags, topNSnapshot.Fields) {
		column, found := sourceColumn(columnName)
		if !found {
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

// intersectBy keeps the left items whose key also appears on the right and that reconcile with it.
//
// reconcile returns the merged item to keep, or false to drop a key the two sides define
// incompatibly, so a capability survives the merge only where every group agrees on it.
func intersectBy[T any, K comparable](left, right []T, keyOf func(T) K, reconcile func(leftItem, rightItem T) (T, bool)) []T {
	rightByKey := make(map[K]T, len(right))
	for _, item := range right {
		rightByKey[keyOf(item)] = item
	}
	intersection := make([]T, 0, len(left))
	for _, leftItem := range left {
		rightItem, found := rightByKey[keyOf(leftItem)]
		if !found {
			continue
		}
		merged, keep := reconcile(leftItem, rightItem)
		if !keep {
			continue
		}
		intersection = append(intersection, merged)
	}
	return intersection
}

func intersectStrings(left, right []string) []string {
	return intersectBy(left, right,
		func(value string) string { return value },
		func(leftValue, _ string) (string, bool) { return leftValue, true })
}

func intersectColumns(left, right []session.SchemaColumn) []session.SchemaColumn {
	return intersectBy(left, right,
		func(column session.SchemaColumn) string { return column.Name },
		func(leftColumn, rightColumn session.SchemaColumn) (session.SchemaColumn, bool) {
			if leftColumn.Kind != rightColumn.Kind || leftColumn.Type != rightColumn.Type {
				return session.SchemaColumn{}, false
			}
			leftColumn.Indexed = leftColumn.Indexed && rightColumn.Indexed
			return leftColumn, true
		})
}

func intersectSortableIndexes(left, right []session.SortableIndex) []session.SortableIndex {
	return intersectBy(left, right,
		func(index session.SortableIndex) string { return index.RuleName },
		func(leftIndex, rightIndex session.SortableIndex) (session.SortableIndex, bool) {
			if !slices.Equal(leftIndex.Tags, rightIndex.Tags) {
				return session.SortableIndex{}, false
			}
			return session.SortableIndex{
				RuleName: leftIndex.RuleName,
				Tags:     append([]string(nil), leftIndex.Tags...),
			}, true
		})
}
