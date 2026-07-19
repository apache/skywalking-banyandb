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

package tools

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/pkg/auth"
)

func TestHTTPExecutorDiscoverSchema(t *testing.T) {
	var gotPaths []string
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gotPaths = append(gotPaths, request.URL.Path)
		gotAuth = request.Header.Get("Authorization")
		if strings.HasPrefix(request.URL.Path, "/api/v1/index-rule-binding/schema/lists/") {
			listResponse := &databasev1.IndexRuleBindingRegistryServiceListResponse{
				IndexRuleBinding: []*databasev1.IndexRuleBinding{
					{
						Metadata: &commonv1.Metadata{Name: "endpoint-binding", Group: "production"},
						Rules:    []string{"endpoint"},
						Subject: &databasev1.Subject{
							Catalog: commonv1.Catalog_CATALOG_MEASURE,
							Name:    "service_latency",
						},
					},
				},
			}
			body, marshalErr := protojson.Marshal(listResponse)
			if marshalErr != nil {
				t.Fatalf("failed to marshal index rule bindings: %v", marshalErr)
			}
			_, _ = writer.Write(body)
			return
		}
		if strings.HasPrefix(request.URL.Path, "/api/v1/index-rule/schema/lists/") {
			listResponse := &databasev1.IndexRuleRegistryServiceListResponse{
				IndexRule: []*databasev1.IndexRule{
					{
						Metadata: &commonv1.Metadata{Name: "endpoint", Group: "production"},
						Tags:     []string{"endpoint"},
					},
				},
			}
			body, marshalErr := protojson.Marshal(listResponse)
			if marshalErr != nil {
				t.Fatalf("failed to marshal index rules: %v", marshalErr)
			}
			_, _ = writer.Write(body)
			return
		}
		if strings.HasPrefix(request.URL.Path, "/api/v1/measure/schema/lists/") {
			listResponse := &databasev1.MeasureRegistryServiceListResponse{
				Measure: []*databasev1.Measure{
					{Metadata: &commonv1.Metadata{Name: "service_latency", Group: "production"}},
					{Metadata: &commonv1.Metadata{Name: "service_cpm", Group: "production"}},
				},
			}
			body, marshalErr := protojson.Marshal(listResponse)
			if marshalErr != nil {
				t.Fatalf("failed to marshal measure list: %v", marshalErr)
			}
			_, _ = writer.Write(body)
			return
		}
		measure := &databasev1.Measure{
			Metadata: &commonv1.Metadata{
				Group: "production",
				Name:  "service_latency",
			},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "default",
					Tags: []*databasev1.TagSpec{
						{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING},
						{Name: "endpoint", Type: databasev1.TagType_TAG_TYPE_STRING},
					},
				},
			},
			Entity: &databasev1.Entity{TagNames: []string{"service"}},
			Fields: []*databasev1.FieldSpec{
				{Name: "latency", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT},
				{Name: "cpm", FieldType: databasev1.FieldType_FIELD_TYPE_INT},
			},
		}
		body, marshalErr := protojson.Marshal(&databasev1.MeasureRegistryServiceGetResponse{
			Measure: measure,
		})
		if marshalErr != nil {
			t.Fatalf("failed to marshal measure: %v", marshalErr)
		}
		_, _ = writer.Write(body)
	}))
	defer server.Close()
	executor := NewHTTPExecutor(HTTPConfig{
		Addr:     server.URL,
		Username: "user",
		Password: "pass",
	})
	snapshot, discoverErr := executor.DiscoverSchema(context.Background(), SchemaRequest{
		Type:   session.ResourceTypeMeasure,
		Name:   "service_latency",
		Groups: []string{"production"},
	})
	if discoverErr != nil {
		t.Fatalf("DiscoverSchema returned error: %v", discoverErr)
	}
	if !containsPath(gotPaths, "/api/v1/measure/schema/production/service_latency") {
		t.Fatalf("unexpected paths: %v", gotPaths)
	}
	if !containsPath(gotPaths, "/api/v1/index-rule/schema/lists/production") {
		t.Fatalf("unexpected paths: %v", gotPaths)
	}
	if !containsPath(gotPaths, "/api/v1/index-rule-binding/schema/lists/production") {
		t.Fatalf("unexpected paths: %v", gotPaths)
	}
	if gotAuth != auth.GenerateBasicAuthHeader("user", "pass") {
		t.Fatalf("unexpected auth header: %s", gotAuth)
	}
	if !reflect.DeepEqual(snapshot.Tags, []string{"default.service", "default.endpoint"}) {
		t.Fatalf("unexpected tags: %v", snapshot.Tags)
	}
	if !reflect.DeepEqual(snapshot.EntityTags, []string{"service"}) {
		t.Fatalf("unexpected entity tags: %v", snapshot.EntityTags)
	}
	if !snapshot.Loaded {
		t.Fatal("expected loaded schema snapshot")
	}
	if !reflect.DeepEqual(snapshot.Fields, []string{"latency", "cpm"}) {
		t.Fatalf("unexpected fields: %v", snapshot.Fields)
	}
	endpoint, found := snapshot.Column("endpoint")
	if !found || endpoint.Type != session.SchemaValueTypeString || !endpoint.Indexed {
		t.Fatalf("unexpected typed endpoint metadata: %+v", endpoint)
	}
	latency, found := snapshot.Column("latency")
	if !found || latency.Type != session.SchemaValueTypeFloat {
		t.Fatalf("unexpected typed latency metadata: %+v", latency)
	}
	if !containsPath(gotPaths, "/api/v1/measure/schema/lists/production") {
		t.Fatalf("unexpected paths: %v", gotPaths)
	}
	if !reflect.DeepEqual(snapshot.IndexedFields, []string{"endpoint"}) {
		t.Fatalf("unexpected indexed fields: %v", snapshot.IndexedFields)
	}
	if len(snapshot.SortableIndexes) != 1 || snapshot.SortableIndexes[0].RuleName != "endpoint" ||
		!reflect.DeepEqual(snapshot.SortableIndexes[0].Tags, []string{"endpoint"}) {
		t.Fatalf("unexpected sortable indexes: %+v", snapshot.SortableIndexes)
	}
	if snapshot.Fingerprint == "" {
		t.Fatal("expected schema fingerprint")
	}
	if !reflect.DeepEqual(snapshot.ResourceNames, []string{"service_latency", "service_cpm"}) {
		t.Fatalf("unexpected resource names: %v", snapshot.ResourceNames)
	}
}

func TestHTTPExecutorFallsBackWhenSchemaUnavailable(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	executor := NewHTTPExecutor(HTTPConfig{Addr: server.URL})
	snapshot, discoverErr := executor.DiscoverSchema(context.Background(), SchemaRequest{
		Type:   session.ResourceTypeStream,
		Name:   "sw",
		Groups: []string{"default"},
	})
	if discoverErr != nil {
		t.Fatalf("DiscoverSchema returned error: %v", discoverErr)
	}
	if snapshot.Name != "sw" || snapshot.Type != session.ResourceTypeStream {
		t.Fatalf("unexpected fallback snapshot: %+v", snapshot)
	}
	if len(snapshot.Tags) != 0 || len(snapshot.Fields) != 0 {
		t.Fatalf("expected empty fallback schema summary: %+v", snapshot)
	}
}

func TestMergeGroupSchemasKeepsOnlyCompatibleCapabilities(t *testing.T) {
	first := session.SchemaSnapshot{
		Type:   session.ResourceTypeStream,
		Name:   "logs",
		Groups: []string{"production"},
		Loaded: true,
		Tags:   []string{"service", "status"},
		Columns: []session.SchemaColumn{
			{Name: "service", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString, Indexed: true},
			{Name: "status", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeInt},
		},
		SortableIndexes: []session.SortableIndex{
			{RuleName: "service_sort", Tags: []string{"service"}},
			{RuleName: "status_sort", Tags: []string{"status"}},
		},
	}
	second := session.SchemaSnapshot{
		Type:            session.ResourceTypeStream,
		Name:            "logs",
		Groups:          []string{"staging"},
		Loaded:          true,
		Tags:            []string{"service"},
		Columns:         []session.SchemaColumn{{Name: "service", Kind: session.SchemaColumnTag, Type: session.SchemaValueTypeString, Indexed: true}},
		SortableIndexes: []session.SortableIndex{{RuleName: "service_sort", Tags: []string{"service"}}},
	}
	merged, mergeErr := mergeGroupSchemas(SchemaRequest{
		Type:   session.ResourceTypeStream,
		Name:   "logs",
		Groups: []string{"production", "staging"},
	}, []session.SchemaSnapshot{first, second})
	if mergeErr != nil {
		t.Fatalf("mergeGroupSchemas returned error: %v", mergeErr)
	}
	if len(merged.Columns) != 1 || merged.Columns[0].Name != "service" {
		t.Fatalf("expected common column only, got %+v", merged.Columns)
	}
	if len(merged.SortableIndexes) != 1 || merged.SortableIndexes[0].RuleName != "service_sort" {
		t.Fatalf("expected common sortable rule only, got %+v", merged.SortableIndexes)
	}
	if !reflect.DeepEqual(merged.Groups, []string{"production", "staging"}) || merged.Fingerprint == "" {
		t.Fatalf("unexpected merged schema identity: %+v", merged)
	}
}

func TestMergeGroupTopNSchemasAllowsGroupScopedSourceMeasure(t *testing.T) {
	first := session.SchemaSnapshot{
		Type:               session.ResourceTypeTopN,
		Name:               "service_latency_topn",
		Groups:             []string{"production"},
		Loaded:             true,
		SourceMeasure:      "service_latency",
		SourceMeasureGroup: "production",
		FieldValueSort:     "SORT_DESC",
	}
	second := first
	second.Groups = []string{"staging"}
	second.SourceMeasureGroup = "staging"
	merged, mergeErr := mergeGroupSchemas(SchemaRequest{
		Type:   session.ResourceTypeTopN,
		Name:   "service_latency_topn",
		Groups: []string{"production", "staging"},
	}, []session.SchemaSnapshot{first, second})
	if mergeErr != nil {
		t.Fatalf("mergeGroupSchemas returned error: %v", mergeErr)
	}
	if merged.SourceMeasure != "service_latency" || merged.SourceMeasureGroup != "" || merged.Fingerprint == "" {
		t.Fatalf("unexpected merged TopN source identity: %+v", merged)
	}
}

func TestHTTPExecutorEnrichesTopNSchemaFromSourceMeasure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var responseMessage proto.Message
		switch request.URL.Path {
		case "/api/v1/topn-agg/schema/production/service_latency_topn":
			responseMessage = &databasev1.TopNAggregationRegistryServiceGetResponse{
				TopNAggregation: &databasev1.TopNAggregation{
					Metadata:        &commonv1.Metadata{Name: "service_latency_topn", Group: "production"},
					SourceMeasure:   &commonv1.Metadata{Name: "service_latency", Group: "production"},
					FieldName:       "latency",
					FieldValueSort:  modelv1.Sort_SORT_DESC,
					GroupByTagNames: []string{"service"},
				},
			}
		case "/api/v1/measure/schema/production/service_latency":
			responseMessage = &databasev1.MeasureRegistryServiceGetResponse{Measure: &databasev1.Measure{
				Metadata: &commonv1.Metadata{Name: "service_latency", Group: "production"},
				TagFamilies: []*databasev1.TagFamilySpec{{
					Name: "default",
					Tags: []*databasev1.TagSpec{{Name: "service", Type: databasev1.TagType_TAG_TYPE_STRING}},
				}},
				Entity: &databasev1.Entity{TagNames: []string{"service"}},
				Fields: []*databasev1.FieldSpec{{Name: "latency", FieldType: databasev1.FieldType_FIELD_TYPE_FLOAT}},
			}}
		default:
			http.NotFound(writer, request)
			return
		}
		body, marshalErr := protojson.Marshal(responseMessage)
		if marshalErr != nil {
			t.Fatalf("failed to marshal schema response: %v", marshalErr)
		}
		_, _ = writer.Write(body)
	}))
	defer server.Close()
	executor := NewHTTPExecutor(HTTPConfig{Addr: server.URL})
	snapshot, discoverErr := executor.DiscoverSchema(context.Background(), SchemaRequest{
		Type:   session.ResourceTypeTopN,
		Name:   "service_latency_topn",
		Groups: []string{"production"},
	})
	if discoverErr != nil {
		t.Fatalf("DiscoverSchema returned error: %v", discoverErr)
	}
	if !snapshot.Loaded || snapshot.SourceMeasure != "service_latency" || snapshot.FieldValueSort != "SORT_DESC" {
		t.Fatalf("unexpected TopN metadata: %+v", snapshot)
	}
	serviceColumn, serviceFound := snapshot.ExactColumn("service")
	latencyColumn, latencyFound := snapshot.ExactColumn("latency")
	if !serviceFound || serviceColumn.Kind != session.SchemaColumnTag || serviceColumn.Type != session.SchemaValueTypeString {
		t.Fatalf("unexpected TopN entity column: %+v", serviceColumn)
	}
	if !latencyFound || latencyColumn.Kind != session.SchemaColumnField || latencyColumn.Type != session.SchemaValueTypeFloat {
		t.Fatalf("unexpected TopN value column: %+v", latencyColumn)
	}
}

func TestHTTPExecutorDiscoverStreamSchema(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/stream/schema/sw_records/pprof_task" {
			http.NotFound(writer, request)
			return
		}
		stream := &databasev1.Stream{
			Metadata: &commonv1.Metadata{Group: "sw_records", Name: "pprof_task"},
			TagFamilies: []*databasev1.TagFamilySpec{
				{
					Name: "data_binary",
					Tags: []*databasev1.TagSpec{
						{Name: "task_id"},
						{Name: "service_id"},
					},
				},
			},
			Entity: &databasev1.Entity{TagNames: []string{"task_id"}},
		}
		body, marshalErr := protojson.Marshal(&databasev1.StreamRegistryServiceGetResponse{Stream: stream})
		if marshalErr != nil {
			t.Fatalf("failed to marshal stream response: %v", marshalErr)
		}
		_, _ = writer.Write(body)
	}))
	defer server.Close()
	executor := NewHTTPExecutor(HTTPConfig{Addr: server.URL})
	snapshot, discoverErr := executor.DiscoverSchema(context.Background(), SchemaRequest{
		Type:   session.ResourceTypeStream,
		Name:   "pprof_task",
		Groups: []string{"sw_records"},
	})
	if discoverErr != nil {
		t.Fatalf("DiscoverSchema returned error: %v", discoverErr)
	}
	if !snapshot.Loaded {
		t.Fatal("expected loaded stream schema snapshot")
	}
	if !reflect.DeepEqual(snapshot.EntityTags, []string{"task_id"}) {
		t.Fatalf("unexpected entity tags: %v", snapshot.EntityTags)
	}
	if !reflect.DeepEqual(snapshot.Tags, []string{"data_binary.task_id", "data_binary.service_id"}) {
		t.Fatalf("unexpected tags: %v", snapshot.Tags)
	}
}

func TestHTTPExecutorExecuteBydbQL(t *testing.T) {
	var gotPath string
	var gotQuery string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		gotPath = request.URL.Path
		queryRequest := new(bydbqlv1.QueryRequest)
		if decodeErr := protojson.Unmarshal(readRequestBody(t, request), queryRequest); decodeErr != nil {
			t.Fatalf("failed to decode request: %v", decodeErr)
		}
		gotQuery = queryRequest.GetQuery()
		queryResponse := &bydbqlv1.QueryResponse{
			Result: &bydbqlv1.QueryResponse_MeasureResult{
				MeasureResult: &measurev1.QueryResponse{
					DataPoints: []*measurev1.DataPoint{
						{},
						{},
					},
				},
			},
		}
		body, marshalErr := protojson.Marshal(queryResponse)
		if marshalErr != nil {
			t.Fatalf("failed to marshal query response: %v", marshalErr)
		}
		_, _ = writer.Write(body)
	}))
	defer server.Close()
	executor := NewHTTPExecutor(HTTPConfig{Addr: server.URL})
	executionResult, executeErr := executor.Execute(context.Background(), nil, "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10")
	if executeErr != nil {
		t.Fatalf("Execute returned error: %v", executeErr)
	}
	if gotPath != "/api/v1/bydbql/query" {
		t.Fatalf("unexpected path: %s", gotPath)
	}
	if gotQuery != "SELECT * FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10" {
		t.Fatalf("unexpected query: %s", gotQuery)
	}
	if executionResult.Rows != 2 {
		t.Fatalf("unexpected rows: %d", executionResult.Rows)
	}
	if executionResult.Command != "POST /api/v1/bydbql/query" || executionResult.Path != "/api/v1/bydbql/query" {
		t.Fatalf("unexpected command summary: %+v", executionResult)
	}
	if executionResult.Response == "" {
		t.Fatal("expected raw response preview")
	}
	if executionResult.ResourceType != "measure" {
		t.Fatalf("unexpected resource type: %s", executionResult.ResourceType)
	}
	if len(executionResult.Columns) == 0 || len(executionResult.Preview) != 2 {
		t.Fatalf("expected a structured table preview, got columns=%v preview=%v", executionResult.Columns, executionResult.Preview)
	}
}

func TestHTTPExecutorDiscoverCatalog(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/api/v1/group/schema/lists":
			listResponse := &databasev1.GroupRegistryServiceListResponse{
				Group: []*commonv1.Group{
					{Metadata: &commonv1.Metadata{Name: "sw_metrics"}},
				},
			}
			body, marshalErr := protojson.Marshal(listResponse)
			if marshalErr != nil {
				t.Fatalf("failed to marshal groups: %v", marshalErr)
			}
			_, _ = writer.Write(body)
		case request.URL.Path == "/api/v1/measure/schema/lists/sw_metrics":
			listResponse := &databasev1.MeasureRegistryServiceListResponse{
				Measure: []*databasev1.Measure{
					{Metadata: &commonv1.Metadata{Name: "service_endpoint_latency", Group: "sw_metrics"}},
				},
			}
			body, marshalErr := protojson.Marshal(listResponse)
			if marshalErr != nil {
				t.Fatalf("failed to marshal measures: %v", marshalErr)
			}
			_, _ = writer.Write(body)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	executor := NewHTTPExecutor(HTTPConfig{Addr: server.URL})
	catalog, discoverErr := executor.DiscoverCatalog(context.Background())
	if discoverErr != nil {
		t.Fatalf("DiscoverCatalog returned error: %v", discoverErr)
	}
	if len(catalog.Groups) != 1 || catalog.Groups[0] != "sw_metrics" {
		t.Fatalf("unexpected groups: %+v", catalog.Groups)
	}
	found := false
	for _, entry := range catalog.Entries {
		if entry.Group == "sw_metrics" && entry.Name == "service_endpoint_latency" && entry.Type == session.ResourceTypeMeasure {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected measure catalog entry, got %+v", catalog.Entries)
	}
}

func TestHTTPExecutorUsesBydbctlTLSSettings(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/group/schema/lists" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		body, marshalErr := protojson.Marshal(&databasev1.GroupRegistryServiceListResponse{
			Group: []*commonv1.Group{{Metadata: &commonv1.Metadata{Name: "production"}}},
		})
		if marshalErr != nil {
			t.Fatalf("failed to marshal groups: %v", marshalErr)
		}
		_, _ = writer.Write(body)
	}))
	defer server.Close()
	executor := NewHTTPExecutor(HTTPConfig{Addr: server.URL, EnableTLS: true, Insecure: true})
	catalog, catalogErr := executor.DiscoverCatalog(context.Background())
	if catalogErr != nil {
		t.Fatalf("DiscoverCatalog returned error: %v", catalogErr)
	}
	if !reflect.DeepEqual(catalog.Groups, []string{"production"}) {
		t.Fatalf("unexpected TLS catalog groups: %v", catalog.Groups)
	}
}

func readRequestBody(t *testing.T, request *http.Request) []byte {
	t.Helper()
	body, readErr := io.ReadAll(request.Body)
	if readErr != nil {
		t.Fatalf("failed to read request body: %v", readErr)
	}
	return body
}

func containsPath(paths []string, expected string) bool {
	for _, path := range paths {
		if path == expected {
			return true
		}
	}
	return false
}

func TestResponsePreviewFlattensStreamTagFamilies(t *testing.T) {
	body := []byte(`{
		"streamResult": {
			"elements": [
				{
					"elementId": "ace4e1e4b2ec08a0",
					"timestamp": "2026-07-12T14:12:39.279Z",
					"tagFamilies": [
						{
							"name": "searchable",
							"tags": [
								{"key": "trace_id", "value": {"str": {"value": "f3f3ae1f-c973-4ce9-84cd-03cec7ddb903"}}},
								{"key": "content", "value": {"str": {"value": "Listing top songs"}}},
								{"key": "tags_raw_data", "value": {"binaryData": "AAAA"}}
							]
						}
					]
				}
			]
		}
	}`)
	columns, preview, truncated := responsePreview(body, 10)
	if truncated {
		t.Fatal("expected single-row preview without truncation")
	}
	if len(preview) != 1 {
		t.Fatalf("expected one preview row, got %v", preview)
	}
	if !containsString(columns, "trace_id") || !containsString(columns, "content") {
		t.Fatalf("unexpected columns: %v", columns)
	}
	row := map[string]string{}
	for idx, column := range columns {
		if idx < len(preview[0]) {
			row[column] = preview[0][idx]
		}
	}
	if row["trace_id"] != "f3f3ae1f-c973-4ce9-84cd-03cec7ddb903" {
		t.Fatalf("unexpected trace_id: %q", row["trace_id"])
	}
	if !strings.Contains(row["content"], "Listing top songs") {
		t.Fatalf("unexpected content: %q", row["content"])
	}
	if containsString(columns, "tags_raw_data") {
		t.Fatalf("did not expect skipped tag column: %v", columns)
	}
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
