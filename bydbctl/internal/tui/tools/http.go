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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"google.golang.org/protobuf/encoding/protojson"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/pkg/auth"
)

const (
	defaultHTTPTimeout       = 3 * time.Second
	maxPreviewCellRunes      = 120
	defaultPreviewRows       = 50
	groupListPath            = "/api/v1/group/schema/lists"
	measureSchemaPath        = "/api/v1/measure/schema/{group}/{name}"
	streamSchemaPath         = "/api/v1/stream/schema/{group}/{name}"
	traceSchemaPath          = "/api/v1/trace/schema/{group}/{name}"
	propertySchemaPath       = "/api/v1/property/schema/{group}/{name}"
	topnSchemaPath           = "/api/v1/topn-agg/schema/{group}/{name}"
	measureListPath          = "/api/v1/measure/schema/lists/{group}"
	streamListPath           = "/api/v1/stream/schema/lists/{group}"
	traceListPath            = "/api/v1/trace/schema/lists/{group}"
	propertyListPath         = "/api/v1/property/schema/lists/{group}"
	topnListPath             = "/api/v1/topn-agg/schema/lists/{group}"
	indexRuleListPath        = "/api/v1/index-rule/schema/lists/{group}"
	indexRuleBindingListPath = "/api/v1/index-rule-binding/schema/lists/{group}"
	bydbqlQueryPath          = "/api/v1/bydbql/query"
)

// HTTPConfig configures schema discovery through BanyanDB's HTTP API.
type HTTPConfig struct {
	Timeout        time.Duration
	Addr           string
	Username       string
	Password       string
	Cert           string
	EnableTLS      bool
	Insecure       bool
	MaxPreviewRows int
}

// HTTPExecutor discovers schema through BanyanDB's read-only HTTP endpoints.
type HTTPExecutor struct {
	client    *resty.Client
	fallback  *ReadOnlyExecutor
	config    HTTPConfig
	now       func() time.Time
	configErr error
	limits    ExecutionLimits
}

// NewHTTPExecutor creates a read-only HTTP executor.
func NewHTTPExecutor(config HTTPConfig) *HTTPExecutor {
	timeout := config.Timeout
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}
	previewRows := config.MaxPreviewRows
	if previewRows <= 0 {
		previewRows = defaultPreviewRows
	}
	client := resty.New().SetTimeout(timeout)
	executor := &HTTPExecutor{
		client:   client,
		fallback: NewReadOnlyExecutor(),
		config: HTTPConfig{
			Timeout:        timeout,
			Addr:           strings.TrimRight(config.Addr, "/"),
			Username:       config.Username,
			Password:       config.Password,
			Cert:           config.Cert,
			EnableTLS:      config.EnableTLS,
			Insecure:       config.Insecure,
			MaxPreviewRows: previewRows,
		},
		now:    time.Now,
		limits: ExecutionLimits{Timeout: timeout, PreviewRows: previewRows},
	}
	if config.EnableTLS {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
			// #nosec G402 -- this directly preserves bydbctl's --insecure flag semantics.
			InsecureSkipVerify: config.Insecure,
		}
		if strings.TrimSpace(config.Cert) != "" {
			certificate, readErr := os.ReadFile(config.Cert)
			if readErr != nil {
				executor.configErr = fmt.Errorf("failed to read TLS certificate: %w", readErr)
				return executor
			}
			certificatePool := x509.NewCertPool()
			if !certificatePool.AppendCertsFromPEM(certificate) {
				executor.configErr = fmt.Errorf("failed to add server TLS certificate")
				return executor
			}
			tlsConfig.RootCAs = certificatePool
		}
		client.SetTLSClientConfig(tlsConfig)
	}
	return executor
}

// ExecutionLimits returns the executor's effective timeout and preview bound.
func (executor *HTTPExecutor) ExecutionLimits() ExecutionLimits {
	return executor.limits
}

const maxCatalogEntries = 400

// DiscoverCatalog lists groups and resource names across supported resource types.
func (executor *HTTPExecutor) DiscoverCatalog(ctx context.Context) (session.SchemaCatalog, error) {
	catalog := session.SchemaCatalog{UpdatedAt: executor.now()}
	if executor.configErr != nil {
		return catalog, executor.configErr
	}
	if executor.config.Addr == "" {
		return catalog, nil
	}
	groups, groupsErr := executor.listGroups(ctx)
	if groupsErr != nil {
		return catalog, groupsErr
	}
	catalog.Groups = groups
	for _, group := range groups {
		for _, resourceType := range catalogResourceTypes() {
			resourceNames, listErr := executor.listResources(ctx, group, resourceType)
			if listErr != nil {
				continue
			}
			for _, resourceName := range resourceNames {
				catalog.Entries = append(catalog.Entries, session.CatalogEntry{
					Group: group,
					Type:  resourceType,
					Name:  resourceName,
				})
				if len(catalog.Entries) >= maxCatalogEntries {
					return catalog, nil
				}
			}
		}
	}
	return catalog, nil
}

func catalogResourceTypes() []session.ResourceType {
	return []session.ResourceType{
		session.ResourceTypeMeasure,
		session.ResourceTypeStream,
		session.ResourceTypeTrace,
		session.ResourceTypeProperty,
		session.ResourceTypeTopN,
	}
}

func (executor *HTTPExecutor) listGroups(ctx context.Context) ([]string, error) {
	request := executor.client.R().
		SetContext(ctx).
		SetHeader("Accept", "application/json")
	if authHeader := executor.authHeader(); authHeader != "" {
		request.SetHeader("Authorization", authHeader)
	}
	response, requestErr := request.Get(executor.config.Addr + groupListPath)
	if requestErr != nil || response.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("group list unavailable")
	}
	listResponse := new(databasev1.GroupRegistryServiceListResponse)
	if unmarshalErr := protojson.Unmarshal(response.Body(), listResponse); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	return metadataNames(extractGroupMetadata(listResponse.GetGroup())), nil
}

func extractGroupMetadata(groups []*commonv1.Group) []*commonv1.Metadata {
	metadataItems := make([]*commonv1.Metadata, 0, len(groups))
	for _, group := range groups {
		if group == nil || group.GetMetadata() == nil {
			continue
		}
		metadataItems = append(metadataItems, group.GetMetadata())
	}
	return metadataItems
}

// DiscoverSchema fetches and summarizes a resource schema, falling back to a local snapshot when unavailable.
func (executor *HTTPExecutor) DiscoverSchema(ctx context.Context, req SchemaRequest) (session.SchemaSnapshot, error) {
	if executor.configErr != nil {
		return session.SchemaSnapshot{}, executor.configErr
	}
	fallbackSnapshot, fallbackErr := executor.fallback.DiscoverSchema(ctx, req)
	if fallbackErr != nil {
		return session.SchemaSnapshot{}, fallbackErr
	}
	snapshot := fallbackSnapshot
	if executor.config.Addr == "" || req.Name == "" || len(req.Groups) == 0 {
		return snapshot, nil
	}
	path, pathErr := schemaPath(req.Type)
	if pathErr != nil {
		return snapshot, nil
	}
	groupSnapshots := make([]session.SchemaSnapshot, 0, len(req.Groups))
	for _, group := range req.Groups {
		groupSnapshot, discoverErr := executor.discoverGroupSchema(ctx, req, group, path)
		if discoverErr != nil {
			return snapshot, nil
		}
		if !groupSnapshot.Loaded {
			return snapshot, nil
		}
		groupSnapshots = append(groupSnapshots, groupSnapshot)
	}
	mergedSnapshot, mergeErr := mergeGroupSchemas(req, groupSnapshots)
	if mergeErr != nil {
		return session.SchemaSnapshot{}, mergeErr
	}
	mergedSnapshot.EnsureFingerprint()
	return mergedSnapshot, nil
}

func (executor *HTTPExecutor) discoverGroupSchema(ctx context.Context, req SchemaRequest, group, path string) (session.SchemaSnapshot, error) {
	groupRequest := req
	groupRequest.Groups = []string{group}
	fallbackSnapshot, fallbackErr := executor.fallback.DiscoverSchema(ctx, groupRequest)
	if fallbackErr != nil {
		return session.SchemaSnapshot{}, fallbackErr
	}
	request := executor.client.R().
		SetContext(ctx).
		SetPathParam("group", group).
		SetPathParam("name", req.Name).
		SetHeader("Accept", "application/json")
	if authHeader := executor.authHeader(); authHeader != "" {
		request.SetHeader("Authorization", authHeader)
	}
	response, requestErr := request.Get(executor.config.Addr + path)
	if requestErr != nil || response.StatusCode() != http.StatusOK {
		if resourceNames, listErr := executor.listResources(ctx, group, req.Type); listErr == nil {
			fallbackSnapshot.ResourceNames = resourceNames
		}
		return fallbackSnapshot, nil
	}
	schemaSnapshot, summarizeErr := summarizeSchema(groupRequest, response.Body(), executor.now())
	if summarizeErr != nil {
		return fallbackSnapshot, nil
	}
	schemaSnapshot.Loaded = true
	if schemaSnapshot.Type == session.ResourceTypeTopN && schemaSnapshot.SourceMeasure != "" {
		sourceGroup := schemaSnapshot.SourceMeasureGroup
		if sourceGroup == "" {
			sourceGroup = group
		}
		sourceSnapshot, sourceErr := executor.discoverGroupSchema(ctx, SchemaRequest{
			Type:   session.ResourceTypeMeasure,
			Name:   schemaSnapshot.SourceMeasure,
			Groups: []string{sourceGroup},
		}, sourceGroup, measureSchemaPath)
		if sourceErr != nil || !sourceSnapshot.Loaded {
			return fallbackSnapshot, nil
		}
		enrichTopNSchema(&schemaSnapshot, sourceSnapshot)
	}
	if resourceNames, listErr := executor.listResources(ctx, group, req.Type); listErr == nil {
		schemaSnapshot.ResourceNames = resourceNames
	}
	if sortableIndexes, indexErr := executor.discoverResourceSortableIndexes(ctx, group, req.Type, req.Name); indexErr == nil {
		schemaSnapshot.SortableIndexes = sortableIndexes
		indexedTags := sortableIndexTags(sortableIndexes)
		schemaSnapshot.IndexedFields = indexedTags
		schemaSnapshot.Columns = markIndexedColumns(schemaSnapshot.Columns, indexedTags)
	}
	schemaSnapshot.EnsureFingerprint()
	return schemaSnapshot, nil
}

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

func (executor *HTTPExecutor) listResources(ctx context.Context, group string, resourceType session.ResourceType) ([]string, error) {
	listPath, listErr := resourceListPath(resourceType)
	if listErr != nil {
		return nil, listErr
	}
	request := executor.client.R().
		SetContext(ctx).
		SetPathParam("group", group).
		SetHeader("Accept", "application/json")
	if authHeader := executor.authHeader(); authHeader != "" {
		request.SetHeader("Authorization", authHeader)
	}
	response, requestErr := request.Get(executor.config.Addr + listPath)
	if requestErr != nil || response.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("resource list unavailable")
	}
	return resourceNamesFromList(resourceType, response.Body())
}

func (executor *HTTPExecutor) discoverResourceSortableIndexes(
	ctx context.Context,
	group string,
	resourceType session.ResourceType,
	resourceName string,
) ([]session.SortableIndex, error) {
	indexRules, rulesErr := executor.listIndexRules(ctx, group)
	if rulesErr != nil {
		return nil, rulesErr
	}
	bindings, bindingsErr := executor.listIndexRuleBindings(ctx, group)
	if bindingsErr != nil {
		return nil, bindingsErr
	}
	boundRuleNames := boundRuleNamesForResource(bindings, resourceType, resourceName)
	var sortableIndexes []session.SortableIndex
	for _, indexRule := range indexRules {
		ruleName := strings.TrimSpace(indexRule.GetMetadata().GetName())
		if ruleName == "" {
			continue
		}
		if _, ok := boundRuleNames[ruleName]; !ok {
			continue
		}
		if indexRule.GetNoSort() {
			continue
		}
		sortableIndexes = append(sortableIndexes, session.SortableIndex{
			RuleName: ruleName,
			Tags:     compactStrings(indexRule.GetTags()),
		})
	}
	sort.Slice(sortableIndexes, func(leftIndex, rightIndex int) bool {
		return sortableIndexes[leftIndex].RuleName < sortableIndexes[rightIndex].RuleName
	})
	return sortableIndexes, nil
}

func sortableIndexTags(indexes []session.SortableIndex) []string {
	var tags []string
	for _, index := range indexes {
		tags = append(tags, index.Tags...)
	}
	return compactStrings(tags)
}

func boundRuleNamesForResource(
	bindings []*databasev1.IndexRuleBinding,
	resourceType session.ResourceType,
	resourceName string,
) map[string]struct{} {
	expectedCatalog := resourceCatalog(resourceType)
	boundRuleNames := make(map[string]struct{})
	for _, binding := range bindings {
		subject := binding.GetSubject()
		if subject == nil {
			continue
		}
		if strings.TrimSpace(subject.GetName()) != resourceName {
			continue
		}
		if expectedCatalog != commonv1.Catalog_CATALOG_UNSPECIFIED && subject.GetCatalog() != expectedCatalog {
			continue
		}
		for _, ruleName := range binding.GetRules() {
			trimmedRuleName := strings.TrimSpace(ruleName)
			if trimmedRuleName != "" {
				boundRuleNames[trimmedRuleName] = struct{}{}
			}
		}
	}
	return boundRuleNames
}

func resourceCatalog(resourceType session.ResourceType) commonv1.Catalog {
	switch resourceType {
	case session.ResourceTypeStream:
		return commonv1.Catalog_CATALOG_STREAM
	case session.ResourceTypeMeasure:
		return commonv1.Catalog_CATALOG_MEASURE
	case session.ResourceTypeProperty:
		return commonv1.Catalog_CATALOG_PROPERTY
	case session.ResourceTypeTrace:
		return commonv1.Catalog_CATALOG_TRACE
	default:
		return commonv1.Catalog_CATALOG_UNSPECIFIED
	}
}

func (executor *HTTPExecutor) listIndexRules(ctx context.Context, group string) ([]*databasev1.IndexRule, error) {
	request := executor.client.R().
		SetContext(ctx).
		SetPathParam("group", group).
		SetHeader("Accept", "application/json")
	if authHeader := executor.authHeader(); authHeader != "" {
		request.SetHeader("Authorization", authHeader)
	}
	response, requestErr := request.Get(executor.config.Addr + indexRuleListPath)
	if requestErr != nil || response.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("index rule list unavailable")
	}
	listResponse := new(databasev1.IndexRuleRegistryServiceListResponse)
	if unmarshalErr := protojson.Unmarshal(response.Body(), listResponse); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	return listResponse.GetIndexRule(), nil
}

func (executor *HTTPExecutor) listIndexRuleBindings(ctx context.Context, group string) ([]*databasev1.IndexRuleBinding, error) {
	request := executor.client.R().
		SetContext(ctx).
		SetPathParam("group", group).
		SetHeader("Accept", "application/json")
	if authHeader := executor.authHeader(); authHeader != "" {
		request.SetHeader("Authorization", authHeader)
	}
	response, requestErr := request.Get(executor.config.Addr + indexRuleBindingListPath)
	if requestErr != nil || response.StatusCode() != http.StatusOK {
		return nil, fmt.Errorf("index rule binding list unavailable")
	}
	listResponse := new(databasev1.IndexRuleBindingRegistryServiceListResponse)
	if unmarshalErr := protojson.Unmarshal(response.Body(), listResponse); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	return listResponse.GetIndexRuleBinding(), nil
}

// Execute runs a read-only BYDBQL query through the BanyanDB HTTP gateway.
func (executor *HTTPExecutor) Execute(ctx context.Context, querySession *session.QuerySession, query string) (session.ExecutionResult, error) {
	if executor.configErr != nil {
		return session.ExecutionResult{}, executor.configErr
	}
	if executor.config.Addr == "" {
		return executor.fallback.Execute(ctx, querySession, query)
	}
	trimmedQuery := strings.TrimSpace(query)
	if trimmedQuery == "" {
		return session.ExecutionResult{}, fmt.Errorf("BYDBQL query is required")
	}
	requestStartedAt := time.Now()
	requestBody, marshalErr := protojson.Marshal(&bydbqlv1.QueryRequest{Query: trimmedQuery})
	if marshalErr != nil {
		return session.ExecutionResult{}, fmt.Errorf("failed to marshal BYDBQL request: %w", marshalErr)
	}
	request := executor.client.R().
		SetContext(ctx).
		SetHeader("Accept", "application/json").
		SetHeader("Content-Type", "application/json").
		SetBody(requestBody)
	if authHeader := executor.authHeader(); authHeader != "" {
		request.SetHeader("Authorization", authHeader)
	}
	response, requestErr := request.Post(executor.config.Addr + bydbqlQueryPath)
	if requestErr != nil {
		executionResult := session.ExecutionResult{
			CheckedAt: executor.now(),
			Duration:  time.Since(requestStartedAt),
			Query:     trimmedQuery,
			Command:   "POST " + bydbqlQueryPath,
			Path:      bydbqlQueryPath,
			Error:     requestErr.Error(),
		}
		return executionResult, fmt.Errorf("failed to execute BYDBQL query: %w", requestErr)
	}
	rawResponse := strings.TrimSpace(string(response.Body()))
	executionResult := session.ExecutionResult{
		CheckedAt: executor.now(),
		Duration:  time.Since(requestStartedAt),
		Query:     trimmedQuery,
		Command:   "POST " + bydbqlQueryPath,
		Path:      bydbqlQueryPath,
		Response:  rawResponse,
	}
	if response.StatusCode() != http.StatusOK {
		executionResult.Error = truncateBody(rawResponse)
		return executionResult, fmt.Errorf("BYDBQL query returned HTTP %d: %s", response.StatusCode(), executionResult.Error)
	}
	queryResponse := new(bydbqlv1.QueryResponse)
	if unmarshalErr := protojson.Unmarshal(response.Body(), queryResponse); unmarshalErr != nil {
		executionResult.Error = unmarshalErr.Error()
		return executionResult, fmt.Errorf("failed to decode BYDBQL response: %w", unmarshalErr)
	}
	rows, resultType := responseRows(queryResponse)
	executionResult.Rows = rows
	executionResult.ResourceType = resultType
	executionResult.Columns, executionResult.Preview, executionResult.Truncated = responsePreview(response.Body(), executor.limits.PreviewRows)
	executionResult.Summary = fmt.Sprintf("executed %s BYDBQL query through %s; rows=%d", resultType, bydbqlQueryPath, rows)
	if rows == 0 {
		executionResult.Hint = "query returned zero rows; consider widening the TIME range or verifying resource name, group, and filters"
	}
	return executionResult, nil
}

func (executor *HTTPExecutor) authHeader() string {
	if executor.config.Username == "" && executor.config.Password == "" {
		return ""
	}
	return auth.GenerateBasicAuthHeader(executor.config.Username, executor.config.Password)
}

func schemaPath(resourceType session.ResourceType) (string, error) {
	switch resourceType {
	case session.ResourceTypeMeasure:
		return measureSchemaPath, nil
	case session.ResourceTypeStream:
		return streamSchemaPath, nil
	case session.ResourceTypeTrace:
		return traceSchemaPath, nil
	case session.ResourceTypeProperty:
		return propertySchemaPath, nil
	case session.ResourceTypeTopN:
		return topnSchemaPath, nil
	default:
		return "", fmt.Errorf("unsupported resource type: %s", resourceType)
	}
}

func resourceListPath(resourceType session.ResourceType) (string, error) {
	switch resourceType {
	case session.ResourceTypeMeasure:
		return measureListPath, nil
	case session.ResourceTypeStream:
		return streamListPath, nil
	case session.ResourceTypeTrace:
		return traceListPath, nil
	case session.ResourceTypeProperty:
		return propertyListPath, nil
	case session.ResourceTypeTopN:
		return topnListPath, nil
	default:
		return "", fmt.Errorf("unsupported resource type: %s", resourceType)
	}
}

func resourceNamesFromList(resourceType session.ResourceType, body []byte) ([]string, error) {
	switch resourceType {
	case session.ResourceTypeMeasure:
		listResponse := new(databasev1.MeasureRegistryServiceListResponse)
		if unmarshalErr := protojson.Unmarshal(body, listResponse); unmarshalErr != nil {
			return nil, unmarshalErr
		}
		return metadataNames(extractMeasureMetadata(listResponse.GetMeasure())), nil
	case session.ResourceTypeStream:
		listResponse := new(databasev1.StreamRegistryServiceListResponse)
		if unmarshalErr := protojson.Unmarshal(body, listResponse); unmarshalErr != nil {
			return nil, unmarshalErr
		}
		return metadataNames(extractStreamMetadata(listResponse.GetStream())), nil
	case session.ResourceTypeTrace:
		listResponse := new(databasev1.TraceRegistryServiceListResponse)
		if unmarshalErr := protojson.Unmarshal(body, listResponse); unmarshalErr != nil {
			return nil, unmarshalErr
		}
		return metadataNames(extractTraceMetadata(listResponse.GetTrace())), nil
	case session.ResourceTypeProperty:
		listResponse := new(databasev1.PropertyRegistryServiceListResponse)
		if unmarshalErr := protojson.Unmarshal(body, listResponse); unmarshalErr != nil {
			return nil, unmarshalErr
		}
		return metadataNames(extractPropertyMetadata(listResponse.GetProperties())), nil
	case session.ResourceTypeTopN:
		listResponse := new(databasev1.TopNAggregationRegistryServiceListResponse)
		if unmarshalErr := protojson.Unmarshal(body, listResponse); unmarshalErr != nil {
			return nil, unmarshalErr
		}
		return metadataNames(extractTopNMetadata(listResponse.GetTopNAggregation())), nil
	default:
		return nil, fmt.Errorf("unsupported resource type: %s", resourceType)
	}
}

func extractMeasureMetadata(measures []*databasev1.Measure) []*commonv1.Metadata {
	return extractMetadata(len(measures), func(idx int) *commonv1.Metadata {
		if measures[idx] == nil {
			return nil
		}
		return measures[idx].GetMetadata()
	})
}

func extractStreamMetadata(streams []*databasev1.Stream) []*commonv1.Metadata {
	return extractMetadata(len(streams), func(idx int) *commonv1.Metadata {
		if streams[idx] == nil {
			return nil
		}
		return streams[idx].GetMetadata()
	})
}

func extractTraceMetadata(traces []*databasev1.Trace) []*commonv1.Metadata {
	return extractMetadata(len(traces), func(idx int) *commonv1.Metadata {
		if traces[idx] == nil {
			return nil
		}
		return traces[idx].GetMetadata()
	})
}

func extractPropertyMetadata(properties []*databasev1.Property) []*commonv1.Metadata {
	return extractMetadata(len(properties), func(idx int) *commonv1.Metadata {
		if properties[idx] == nil {
			return nil
		}
		return properties[idx].GetMetadata()
	})
}

func extractTopNMetadata(topNItems []*databasev1.TopNAggregation) []*commonv1.Metadata {
	return extractMetadata(len(topNItems), func(idx int) *commonv1.Metadata {
		if topNItems[idx] == nil {
			return nil
		}
		return topNItems[idx].GetMetadata()
	})
}

func extractMetadata(count int, at func(int) *commonv1.Metadata) []*commonv1.Metadata {
	metadataItems := make([]*commonv1.Metadata, 0, count)
	for idx := 0; idx < count; idx++ {
		metadataItems = append(metadataItems, at(idx))
	}
	return metadataItems
}

func metadataNames(metadataItems []*commonv1.Metadata) []string {
	var names []string
	for _, metadata := range metadataItems {
		if metadata == nil {
			continue
		}
		if name := strings.TrimSpace(metadata.GetName()); name != "" {
			names = append(names, name)
		}
	}
	return compactStrings(names)
}

func summarizeSchema(req SchemaRequest, body []byte, updatedAt time.Time) (session.SchemaSnapshot, error) {
	base := session.SchemaSnapshot{
		UpdatedAt: updatedAt,
		Type:      req.Type,
		Name:      req.Name,
		Groups:    append([]string(nil), req.Groups...),
	}
	switch req.Type {
	case session.ResourceTypeMeasure:
		measure, parseErr := parseMeasureSchema(body)
		if parseErr != nil {
			return session.SchemaSnapshot{}, parseErr
		}
		base.Tags = tagFamilies(measure.GetTagFamilies())
		base.EntityTags = entityTagNames(measure.GetEntity())
		base.Fields = fieldNames(measure.GetFields())
		base.Columns = append(tagFamilyColumns(measure.GetTagFamilies()), fieldColumns(measure.GetFields())...)
		return base, nil
	case session.ResourceTypeStream:
		stream, parseErr := parseStreamSchema(body)
		if parseErr != nil {
			return session.SchemaSnapshot{}, parseErr
		}
		base.Tags = tagFamilies(stream.GetTagFamilies())
		base.EntityTags = entityTagNames(stream.GetEntity())
		base.Columns = tagFamilyColumns(stream.GetTagFamilies())
		return base, nil
	case session.ResourceTypeTrace:
		trace, parseErr := parseTraceSchema(body)
		if parseErr != nil {
			return session.SchemaSnapshot{}, parseErr
		}
		base.Tags = traceTagNames(trace.GetTags())
		base.Columns = traceTagColumns(trace.GetTags())
		return base, nil
	case session.ResourceTypeProperty:
		property, parseErr := parsePropertySchema(body)
		if parseErr != nil {
			return session.SchemaSnapshot{}, parseErr
		}
		base.Tags = tagNames(property.GetTags())
		base.Columns = tagColumns(property.GetTags(), session.SchemaColumnTag)
		return base, nil
	case session.ResourceTypeTopN:
		topN, parseErr := parseTopNSchema(body)
		if parseErr != nil {
			return session.SchemaSnapshot{}, parseErr
		}
		base.Tags = append([]string(nil), topN.GetGroupByTagNames()...)
		base.Fields = compactStrings([]string{topN.GetFieldName()})
		base.SourceMeasure = strings.TrimSpace(topN.GetSourceMeasure().GetName())
		base.SourceMeasureGroup = strings.TrimSpace(topN.GetSourceMeasure().GetGroup())
		base.FieldValueSort = topN.GetFieldValueSort().String()
		for _, tagName := range base.Tags {
			base.Columns = append(base.Columns, session.SchemaColumn{Name: tagName, Kind: session.SchemaColumnTag})
		}
		if fieldName := strings.TrimSpace(topN.GetFieldName()); fieldName != "" {
			base.Columns = append(base.Columns, session.SchemaColumn{Name: fieldName, Kind: session.SchemaColumnField})
		}
		return base, nil
	default:
		return session.SchemaSnapshot{}, fmt.Errorf("unsupported resource type: %s", req.Type)
	}
}

func parseMeasureSchema(body []byte) (*databasev1.Measure, error) {
	wrapped := new(databasev1.MeasureRegistryServiceGetResponse)
	if unmarshalErr := protojson.Unmarshal(body, wrapped); unmarshalErr == nil {
		if measure := wrapped.GetMeasure(); measure != nil {
			return measure, nil
		}
	}
	measure := new(databasev1.Measure)
	if unmarshalErr := protojson.Unmarshal(body, measure); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	if measure.GetMetadata() == nil {
		return nil, fmt.Errorf("measure schema missing in response")
	}
	return measure, nil
}

func parseStreamSchema(body []byte) (*databasev1.Stream, error) {
	wrapped := new(databasev1.StreamRegistryServiceGetResponse)
	if unmarshalErr := protojson.Unmarshal(body, wrapped); unmarshalErr == nil {
		if stream := wrapped.GetStream(); stream != nil {
			return stream, nil
		}
	}
	stream := new(databasev1.Stream)
	if unmarshalErr := protojson.Unmarshal(body, stream); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	if stream.GetMetadata() == nil {
		return nil, fmt.Errorf("stream schema missing in response")
	}
	return stream, nil
}

func parseTraceSchema(body []byte) (*databasev1.Trace, error) {
	wrapped := new(databasev1.TraceRegistryServiceGetResponse)
	if unmarshalErr := protojson.Unmarshal(body, wrapped); unmarshalErr == nil {
		if trace := wrapped.GetTrace(); trace != nil {
			return trace, nil
		}
	}
	trace := new(databasev1.Trace)
	if unmarshalErr := protojson.Unmarshal(body, trace); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	if trace.GetMetadata() == nil {
		return nil, fmt.Errorf("trace schema missing in response")
	}
	return trace, nil
}

func parsePropertySchema(body []byte) (*databasev1.Property, error) {
	wrapped := new(databasev1.PropertyRegistryServiceGetResponse)
	if unmarshalErr := protojson.Unmarshal(body, wrapped); unmarshalErr == nil {
		if property := wrapped.GetProperty(); property != nil {
			return property, nil
		}
	}
	property := new(databasev1.Property)
	if unmarshalErr := protojson.Unmarshal(body, property); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	if property.GetMetadata() == nil {
		return nil, fmt.Errorf("property schema missing in response")
	}
	return property, nil
}

func parseTopNSchema(body []byte) (*databasev1.TopNAggregation, error) {
	wrapped := new(databasev1.TopNAggregationRegistryServiceGetResponse)
	if unmarshalErr := protojson.Unmarshal(body, wrapped); unmarshalErr == nil {
		if topN := wrapped.GetTopNAggregation(); topN != nil {
			return topN, nil
		}
	}
	topN := new(databasev1.TopNAggregation)
	if unmarshalErr := protojson.Unmarshal(body, topN); unmarshalErr != nil {
		return nil, unmarshalErr
	}
	if topN.GetMetadata() == nil {
		return nil, fmt.Errorf("topn schema missing in response")
	}
	return topN, nil
}

func tagFamilies(families []*databasev1.TagFamilySpec) []string {
	var tags []string
	for _, family := range families {
		familyName := strings.TrimSpace(family.GetName())
		for _, tag := range family.GetTags() {
			tagName := strings.TrimSpace(tag.GetName())
			if tagName == "" {
				continue
			}
			if familyName != "" {
				tags = append(tags, familyName+"."+tagName)
				continue
			}
			tags = append(tags, tagName)
		}
	}
	return compactStrings(tags)
}

func tagFamilyColumns(families []*databasev1.TagFamilySpec) []session.SchemaColumn {
	var columns []session.SchemaColumn
	for _, family := range families {
		familyName := strings.TrimSpace(family.GetName())
		for _, tag := range family.GetTags() {
			tagName := strings.TrimSpace(tag.GetName())
			if tagName == "" {
				continue
			}
			if familyName != "" {
				tagName = familyName + "." + tagName
			}
			columns = append(columns, session.SchemaColumn{
				Name: tagName,
				Kind: session.SchemaColumnTag,
				Type: tagValueType(tag.GetType()),
			})
		}
	}
	return columns
}

func entityTagNames(entity *databasev1.Entity) []string {
	if entity == nil {
		return nil
	}
	return compactStrings(entity.GetTagNames())
}

func tagNames(tags []*databasev1.TagSpec) []string {
	var names []string
	for _, tag := range tags {
		names = append(names, tag.GetName())
	}
	return compactStrings(names)
}

func tagColumns(tags []*databasev1.TagSpec, kind session.SchemaColumnKind) []session.SchemaColumn {
	columns := make([]session.SchemaColumn, 0, len(tags))
	for _, tag := range tags {
		tagName := strings.TrimSpace(tag.GetName())
		if tagName == "" {
			continue
		}
		columns = append(columns, session.SchemaColumn{Name: tagName, Kind: kind, Type: tagValueType(tag.GetType())})
	}
	return columns
}

func traceTagNames(tags []*databasev1.TraceTagSpec) []string {
	var names []string
	for _, tag := range tags {
		names = append(names, tag.GetName())
	}
	return compactStrings(names)
}

func traceTagColumns(tags []*databasev1.TraceTagSpec) []session.SchemaColumn {
	columns := make([]session.SchemaColumn, 0, len(tags))
	for _, tag := range tags {
		tagName := strings.TrimSpace(tag.GetName())
		if tagName == "" {
			continue
		}
		columns = append(columns, session.SchemaColumn{
			Name: tagName,
			Kind: session.SchemaColumnTag,
			Type: tagValueType(tag.GetType()),
		})
	}
	return columns
}

func fieldNames(fields []*databasev1.FieldSpec) []string {
	var names []string
	for _, field := range fields {
		names = append(names, field.GetName())
	}
	return compactStrings(names)
}

func fieldColumns(fields []*databasev1.FieldSpec) []session.SchemaColumn {
	columns := make([]session.SchemaColumn, 0, len(fields))
	for _, field := range fields {
		fieldName := strings.TrimSpace(field.GetName())
		if fieldName == "" {
			continue
		}
		columns = append(columns, session.SchemaColumn{
			Name: fieldName,
			Kind: session.SchemaColumnField,
			Type: fieldValueType(field.GetFieldType()),
		})
	}
	return columns
}

func markIndexedColumns(columns []session.SchemaColumn, indexedFields []string) []session.SchemaColumn {
	indexedColumns := append([]session.SchemaColumn(nil), columns...)
	for columnIndex := range indexedColumns {
		for _, indexedField := range indexedFields {
			if matchesColumnName(indexedColumns[columnIndex].Name, indexedField) {
				indexedColumns[columnIndex].Indexed = true
				break
			}
		}
	}
	return indexedColumns
}

func matchesColumnName(columnName, requestedName string) bool {
	if strings.EqualFold(strings.TrimSpace(columnName), strings.TrimSpace(requestedName)) {
		return true
	}
	lastDot := strings.LastIndex(columnName, ".")
	return lastDot >= 0 && strings.EqualFold(columnName[lastDot+1:], strings.TrimSpace(requestedName))
}

func tagValueType(tagType databasev1.TagType) session.SchemaValueType {
	switch tagType {
	case databasev1.TagType_TAG_TYPE_STRING:
		return session.SchemaValueTypeString
	case databasev1.TagType_TAG_TYPE_INT:
		return session.SchemaValueTypeInt
	case databasev1.TagType_TAG_TYPE_STRING_ARRAY:
		return session.SchemaValueTypeStringArray
	case databasev1.TagType_TAG_TYPE_INT_ARRAY:
		return session.SchemaValueTypeIntArray
	case databasev1.TagType_TAG_TYPE_TIMESTAMP:
		return session.SchemaValueTypeTimestamp
	case databasev1.TagType_TAG_TYPE_DATA_BINARY:
		return session.SchemaValueTypeBinary
	default:
		return session.SchemaValueTypeUnknown
	}
}

func fieldValueType(fieldType databasev1.FieldType) session.SchemaValueType {
	switch fieldType {
	case databasev1.FieldType_FIELD_TYPE_STRING:
		return session.SchemaValueTypeString
	case databasev1.FieldType_FIELD_TYPE_INT:
		return session.SchemaValueTypeInt
	case databasev1.FieldType_FIELD_TYPE_FLOAT:
		return session.SchemaValueTypeFloat
	case databasev1.FieldType_FIELD_TYPE_DATA_BINARY:
		return session.SchemaValueTypeBinary
	default:
		return session.SchemaValueTypeUnknown
	}
}

func compactStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	var compactedValues []string
	for _, value := range values {
		trimmedValue := strings.TrimSpace(value)
		if trimmedValue == "" {
			continue
		}
		if _, ok := seen[trimmedValue]; ok {
			continue
		}
		seen[trimmedValue] = struct{}{}
		compactedValues = append(compactedValues, trimmedValue)
	}
	return compactedValues
}

func responseRows(response *bydbqlv1.QueryResponse) (int, string) {
	if measureResult := response.GetMeasureResult(); measureResult != nil {
		return len(measureResult.GetDataPoints()), "measure"
	}
	if streamResult := response.GetStreamResult(); streamResult != nil {
		return len(streamResult.GetElements()), "stream"
	}
	if propertyResult := response.GetPropertyResult(); propertyResult != nil {
		return len(propertyResult.GetProperties()), "property"
	}
	if traceResult := response.GetTraceResult(); traceResult != nil {
		if len(traceResult.GetTraces()) > 0 {
			return len(traceResult.GetTraces()), "trace"
		}
		if traceResult.GetTraceQueryResult() != nil {
			return 1, "trace"
		}
		return 0, "trace"
	}
	if topNResult := response.GetTopnResult(); topNResult != nil {
		rows := 0
		for _, topNList := range topNResult.GetLists() {
			rows += len(topNList.GetItems())
		}
		return rows, "topn"
	}
	return 0, "unknown"
}

func responsePreview(body []byte, maxRows int) ([]string, [][]string, bool) {
	var value any
	if unmarshalErr := json.Unmarshal(body, &value); unmarshalErr != nil {
		return nil, nil, false
	}
	items := firstArray(value)
	if len(items) == 0 {
		return nil, nil, false
	}
	columns := previewColumns(items, maxRows)
	if len(columns) == 0 {
		columns = []string{"value"}
	}
	previewLength := minimum(len(items), maxRows)
	preview := make([][]string, 0, previewLength)
	for _, item := range items[:previewLength] {
		preview = append(preview, previewRow(item, columns))
	}
	return columns, preview, len(items) > previewLength
}

func firstArray(value any) []any {
	switch typedValue := value.(type) {
	case map[string]any:
		preferredKeys := []string{"dataPoints", "elements", "properties", "traces", "items", "lists"}
		for _, key := range preferredKeys {
			if items := firstArray(typedValue[key]); len(items) > 0 {
				return items
			}
		}
		keys := make([]string, 0, len(typedValue))
		for key := range typedValue {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if items := firstArray(typedValue[key]); len(items) > 0 {
				return items
			}
		}
	case []any:
		return typedValue
	}
	return nil
}

var (
	previewSkipTagKeys = map[string]struct{}{
		"tags_raw_data": {},
	}
	preferredPreviewColumns = []string{
		"timestamp",
		"trace_id",
		"endpoint_id",
		"content",
		"service_id",
		"service_instance_id",
		"span_id",
		"tags",
		"elementId",
		"unique_id",
		"trace_segment_id",
	}
)

func previewColumns(items []any, maxRows int) []string {
	if len(items) > 0 {
		if flat := flattenPreviewItem(items[0]); len(flat) > 0 {
			return orderedPreviewColumns(flat)
		}
	}
	columnSet := make(map[string]struct{})
	for _, item := range items[:minimum(len(items), maxRows)] {
		object, ok := item.(map[string]any)
		if !ok {
			continue
		}
		for key := range object {
			columnSet[key] = struct{}{}
		}
	}
	columns := make([]string, 0, len(columnSet))
	for column := range columnSet {
		columns = append(columns, column)
	}
	sort.Strings(columns)
	return columns
}

func previewRow(item any, columns []string) []string {
	if flat := flattenPreviewItem(item); len(flat) > 0 {
		row := make([]string, 0, len(columns))
		for _, column := range columns {
			row = append(row, flat[column])
		}
		return row
	}
	row := make([]string, 0, len(columns))
	object, objectOK := item.(map[string]any)
	for _, column := range columns {
		if !objectOK {
			row = append(row, previewValue(item))
			continue
		}
		row = append(row, previewValue(object[column]))
	}
	return row
}

func flattenPreviewItem(item any) map[string]string {
	object, ok := item.(map[string]any)
	if !ok {
		return nil
	}
	tagFamilies, tagFamiliesOK := object["tagFamilies"].([]any)
	if !tagFamiliesOK || len(tagFamilies) == 0 {
		return nil
	}
	flat := make(map[string]string)
	for _, topKey := range []string{"elementId", "timestamp", "traceId"} {
		if value, exists := object[topKey]; exists {
			flat[topKey] = previewValue(value)
		}
	}
	for _, familyValue := range tagFamilies {
		family, familyOK := familyValue.(map[string]any)
		if !familyOK {
			continue
		}
		tags, _ := family["tags"].([]any)
		for _, tagValue := range tags {
			tag, tagOK := tagValue.(map[string]any)
			if !tagOK {
				continue
			}
			tagKey, _ := tag["key"].(string)
			if tagKey == "" {
				continue
			}
			if _, skip := previewSkipTagKeys[tagKey]; skip {
				continue
			}
			flat[tagKey] = previewTagValue(tag["value"])
		}
	}
	if len(flat) == 0 {
		return nil
	}
	return flat
}

func orderedPreviewColumns(flat map[string]string) []string {
	columns := make([]string, 0, len(flat))
	seen := make(map[string]struct{}, len(flat))
	for _, column := range preferredPreviewColumns {
		if _, exists := flat[column]; !exists {
			continue
		}
		columns = append(columns, column)
		seen[column] = struct{}{}
	}
	rest := make([]string, 0, len(flat))
	for column := range flat {
		if _, alreadyUsed := seen[column]; alreadyUsed {
			continue
		}
		rest = append(rest, column)
	}
	sort.Strings(rest)
	return append(columns, rest...)
}

func previewTagValue(value any) string {
	valueMap, ok := value.(map[string]any)
	if !ok {
		return previewValue(value)
	}
	if _, hasBinary := valueMap["binaryData"]; hasBinary {
		return "<binary>"
	}
	if stringWrap, stringOK := valueMap["str"].(map[string]any); stringOK {
		return truncatePreviewValue(fmt.Sprint(stringWrap["value"]))
	}
	if intWrap, intOK := valueMap["int"].(map[string]any); intOK {
		return fmt.Sprint(intWrap["value"])
	}
	if strArrayWrap, strArrayOK := valueMap["strArray"].(map[string]any); strArrayOK {
		arrayValue, arrayOK := strArrayWrap["value"].([]any)
		if !arrayOK {
			return previewValue(value)
		}
		parts := make([]string, 0, len(arrayValue))
		for _, element := range arrayValue {
			parts = append(parts, fmt.Sprint(element))
		}
		return truncatePreviewValue(strings.Join(parts, ","))
	}
	return previewValue(value)
}

func previewValue(value any) string {
	if stringValue, ok := value.(string); ok {
		return truncatePreviewValue(stringValue)
	}
	encodedValue, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return "<unavailable>"
	}
	return truncatePreviewValue(string(encodedValue))
}

func truncatePreviewValue(value string) string {
	runes := []rune(value)
	if len(runes) <= maxPreviewCellRunes {
		return value
	}
	return string(runes[:maxPreviewCellRunes-3]) + "..."
}

func minimum(left, right int) int {
	if left < right {
		return left
	}
	return right
}

func truncateBody(value string) string {
	const maxBodyLength = 300
	if len(value) <= maxBodyLength {
		return value
	}
	return value[:maxBodyLength] + "..."
}
