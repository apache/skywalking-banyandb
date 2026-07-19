// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import "testing"

func TestCandidateSelectionPreservesManualVersionAsTheCurrentBasis(t *testing.T) {
	querySession := &QuerySession{}
	querySession.AddCandidate(BydbqlCandidate{
		ID:     "agent",
		Query:  "SELECT * FROM MEASURE latency IN production TIME > '-30m' LIMIT 10",
		Source: CandidateSourceAgent,
		Validation: ValidationReport{
			Valid: true,
		},
	})
	querySession.AddCandidate(BydbqlCandidate{
		ID:     "manual",
		Query:  "SELECT endpoint FROM MEASURE latency IN production TIME > '-30m' LIMIT 10",
		Source: CandidateSourceManual,
		Validation: ValidationReport{
			Valid: true,
		},
	})
	if currentCandidate := querySession.CurrentCandidate(); currentCandidate == nil || currentCandidate.ID != "manual" {
		t.Fatalf("expected manual version to be selected, got %+v", currentCandidate)
	}
	if !querySession.SelectCandidate(0) {
		t.Fatal("expected agent version to be selectable")
	}
	if currentCandidate := querySession.CurrentCandidate(); currentCandidate == nil || currentCandidate.ID != "agent" {
		t.Fatalf("expected selected agent version, got %+v", currentCandidate)
	}
	if querySession.SelectCandidate(2) {
		t.Fatal("expected nonexistent version selection to fail")
	}
}

func TestPlannedQueriesAdvanceOnlyAfterTheExactCurrentStatement(t *testing.T) {
	querySession := &QuerySession{}
	querySession.SetPlannedQueries([]PlannedQuery{
		{ID: "first", Query: "SELECT * FROM MEASURE latency IN production TIME > '-30m' LIMIT 10"},
		{ID: "second", Query: "SELECT * FROM STREAM logs IN production TIME > '-30m' LIMIT 10"},
	})
	if querySession.CompletePlannedQuery("SELECT * FROM STREAM logs IN production TIME > '-30m' LIMIT 10") != nil {
		t.Fatal("unexpectedly advanced a plan with the wrong query")
	}
	nextQuery := querySession.CompletePlannedQuery("SELECT * FROM MEASURE latency IN production TIME > '-30m' LIMIT 10")
	if nextQuery == nil || nextQuery.ID != "second" {
		t.Fatalf("expected second query after completing first, got %+v", nextQuery)
	}
	if !querySession.PlannedQueries[0].Completed {
		t.Fatal("expected first planned query to be complete")
	}
}

func TestSchemaStoreKeepsMultipleResourceSnapshots(t *testing.T) {
	querySession := &QuerySession{SchemaSnapshot: SchemaSnapshot{
		AvailableGroups: []string{"production"},
		Catalog: []CatalogEntry{
			{Group: "production", Type: ResourceTypeMeasure, Name: "latency"},
			{Group: "production", Type: ResourceTypeStream, Name: "logs"},
		},
	}}
	measure := SchemaSnapshot{
		Type:    ResourceTypeMeasure,
		Name:    "latency",
		Groups:  []string{"production"},
		Loaded:  true,
		Columns: []SchemaColumn{{Name: "value", Kind: SchemaColumnField, Type: SchemaValueTypeFloat}},
	}
	stream := SchemaSnapshot{
		Type:    ResourceTypeStream,
		Name:    "logs",
		Groups:  []string{"production"},
		Loaded:  true,
		Columns: []SchemaColumn{{Name: "message", Kind: SchemaColumnTag, Type: SchemaValueTypeString}},
	}
	querySession.ActivateSchema(measure)
	querySession.CacheSchema(stream)

	cachedMeasure, measureFound := querySession.CachedSchema(ResourceTypeMeasure, "latency", []string{"production"})
	cachedStream, streamFound := querySession.CachedSchema(ResourceTypeStream, "logs", []string{"production"})
	if !measureFound || !streamFound {
		t.Fatalf("expected both schemas in cache: measure=%t stream=%t", measureFound, streamFound)
	}
	if cachedMeasure.Fingerprint == "" || cachedStream.Fingerprint == "" || cachedMeasure.Fingerprint == cachedStream.Fingerprint {
		t.Fatalf("expected distinct schema fingerprints: measure=%q stream=%q", cachedMeasure.Fingerprint, cachedStream.Fingerprint)
	}
	if querySession.ResourceName != "latency" {
		t.Fatalf("caching a second schema must not replace the active resource: %s", querySession.ResourceName)
	}
	if len(cachedStream.Catalog) != 2 || len(cachedMeasure.AvailableGroups) != 1 {
		t.Fatal("expected discovery context to be preserved in cached schemas")
	}
}

func TestSchemaFingerprintIncludesSortableIndexRules(t *testing.T) {
	first := SchemaSnapshot{
		Type:            ResourceTypeStream,
		Name:            "logs",
		Groups:          []string{"production"},
		Columns:         []SchemaColumn{{Name: "service", Kind: SchemaColumnTag, Type: SchemaValueTypeString, Indexed: true}},
		SortableIndexes: []SortableIndex{{RuleName: "service_sort", Tags: []string{"service"}}},
	}
	second := first
	second.SortableIndexes = []SortableIndex{{RuleName: "service_sort_v2", Tags: []string{"service"}}}
	if first.EnsureFingerprint() == second.EnsureFingerprint() {
		t.Fatal("expected index-rule changes to invalidate the schema fingerprint")
	}
}

func TestSchemaFingerprintIncludesTopNFilterTags(t *testing.T) {
	first := SchemaSnapshot{
		Type:           ResourceTypeTopN,
		Name:           "service_latency_topn",
		Groups:         []string{"production"},
		Tags:           []string{"service"},
		EntityTags:     []string{"service"},
		Columns:        []SchemaColumn{{Name: "service", Kind: SchemaColumnEntityTag, Type: SchemaValueTypeString}},
		FieldValueSort: "SORT_DESC",
	}
	second := first
	second.EntityTags = []string{"endpoint"}
	if first.EnsureFingerprint() == second.EnsureFingerprint() {
		t.Fatal("expected TopN filter-tag changes to invalidate the schema fingerprint")
	}
}
