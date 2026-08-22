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
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/session"
	"github.com/apache/skywalking-banyandb/bydbctl/internal/tui/tuitext"
	"github.com/apache/skywalking-banyandb/pkg/auth"
)

const (
	defaultHTTPTimeout       = 3 * time.Second
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
	Addr           string
	Username       string
	Password       string
	Cert           string
	Timeout        time.Duration
	MaxPreviewRows int
	EnableTLS      bool
	Insecure       bool
}

// HTTPExecutor discovers schema through BanyanDB's read-only HTTP endpoints.
type HTTPExecutor struct {
	configErr error
	client    *resty.Client
	fallback  *ReadOnlyExecutor
	now       func() time.Time
	config    HTTPConfig
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

const (
	maxCatalogEntries = 10000
	// catalogDiscoveryConcurrency bounds in-flight schema list requests so discovery stays fast on a
	// server with many groups without opening an unbounded number of connections to it.
	catalogDiscoveryConcurrency = 8
)

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
	entries, discoveryErr := executor.discoverCatalogEntries(ctx, groups)
	if discoveryErr != nil {
		return catalog, fmt.Errorf("failed to discover catalog resources: %w", discoveryErr)
	}
	catalog.Entries = entries
	return catalog, nil
}

// discoverCatalogEntries lists resources per group and interleaves them fairly so one large
// group cannot starve later groups under the global entry limit.
//
// One list request is issued per group and resource type. On a server with many groups that is
// hundreds of independent round trips, so they run concurrently under a bounded worker count and
// are collected into fixed slots, which keeps the result identical to a sequential walk.
func (executor *HTTPExecutor) discoverCatalogEntries(ctx context.Context, groups []string) ([]session.CatalogEntry, error) {
	resourceTypes := catalogResourceTypes()
	perGroupType := make([][]session.CatalogEntry, len(groups)*len(resourceTypes))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(catalogDiscoveryConcurrency)
	for groupIndex, groupName := range groups {
		for typeIndex, resourceType := range resourceTypes {
			slot := groupIndex*len(resourceTypes) + typeIndex
			group.Go(func() error {
				resourceNames, listErr := executor.listResources(groupCtx, groupName, resourceType)
				if listErr != nil {
					if groupCtx.Err() != nil {
						return fmt.Errorf("catalog discovery for group %q and type %s canceled: %w", groupName, resourceType, groupCtx.Err())
					}
					// A resource type absent from a group is normal, so it narrows the catalog
					// rather than failing the whole discovery pass.
					return nil
				}
				groupResources := make([]session.CatalogEntry, 0, len(resourceNames))
				for _, resourceName := range resourceNames {
					groupResources = append(groupResources, session.CatalogEntry{
						Group: groupName,
						Type:  resourceType,
						Name:  resourceName,
					})
				}
				perGroupType[slot] = groupResources
				return nil
			})
		}
	}
	waitErr := group.Wait()
	if waitErr != nil {
		return nil, fmt.Errorf("catalog discovery did not complete: %w", waitErr)
	}
	resourcesByGroup := make([][]session.CatalogEntry, len(groups))
	for groupIndex := range groups {
		for typeIndex := range resourceTypes {
			resourcesByGroup[groupIndex] = append(resourcesByGroup[groupIndex], perGroupType[groupIndex*len(resourceTypes)+typeIndex]...)
		}
	}
	return interleaveCatalogEntries(resourcesByGroup, maxCatalogEntries), nil
}

// interleaveCatalogEntries fills the catalog fairly by taking one entry per non-empty group in
// each round until the entry limit is reached, so every group stays represented under the cap.
func interleaveCatalogEntries(resourcesByGroup [][]session.CatalogEntry, limit int) []session.CatalogEntry {
	if limit <= 0 {
		return nil
	}
	entries := make([]session.CatalogEntry, 0, limit)
	cursor := make([]int, len(resourcesByGroup))
	for len(entries) < limit {
		addedInRound := 0
		for groupIndex := range resourcesByGroup {
			if cursor[groupIndex] >= len(resourcesByGroup[groupIndex]) {
				continue
			}
			entries = append(entries, resourcesByGroup[groupIndex][cursor[groupIndex]])
			cursor[groupIndex]++
			addedInRound++
			if len(entries) >= limit {
				break
			}
		}
		if addedInRound == 0 {
			break
		}
	}
	return entries
}

// newRequest builds an authenticated read-only JSON request against the configured BanyanDB address.
func (executor *HTTPExecutor) newRequest(ctx context.Context) *resty.Request {
	request := executor.client.R().
		SetContext(ctx).
		SetHeader("Accept", "application/json")
	if authHeader := executor.authHeader(); authHeader != "" {
		request.SetHeader("Authorization", authHeader)
	}
	return request
}

// registryList names one schema registry listing in the errors reported for it.
type registryList struct {
	// plural names what was being listed, as in "failed to list BanyanDB groups".
	plural string
	// listing names the endpoint, as in "BanyanDB group list returned 503".
	listing string
}

var (
	groupRegistry            = registryList{plural: "groups", listing: "group list"}
	indexRuleRegistry        = registryList{plural: "index rules", listing: "index rule list"}
	indexRuleBindingRegistry = registryList{plural: "index rule bindings", listing: "index rule binding list"}
)

// getRegistryList performs one read-only registry GET and decodes its protojson body into target.
//
// A non-OK status is reported by naming the endpoint rather than by returning the response body, so
// a discovery failure never forwards server text into an agent prompt.
func (executor *HTTPExecutor) getRegistryList(ctx context.Context, path, group string, registry registryList, target proto.Message) error {
	request := executor.newRequest(ctx)
	if group != "" {
		request.SetPathParam("group", group)
	}
	response, requestErr := request.Get(executor.config.Addr + path)
	if requestErr != nil {
		return fmt.Errorf("failed to list BanyanDB %s from %s: %w", registry.plural, executor.config.Addr, requestErr)
	}
	if response.StatusCode() != http.StatusOK {
		return fmt.Errorf("BanyanDB %s returned %s", registry.listing, response.Status())
	}
	if unmarshalErr := protojson.Unmarshal(response.Body(), target); unmarshalErr != nil {
		return fmt.Errorf("failed to decode BanyanDB %s: %w", registry.listing, unmarshalErr)
	}
	return nil
}

func (executor *HTTPExecutor) listGroups(ctx context.Context) ([]string, error) {
	listResponse := new(databasev1.GroupRegistryServiceListResponse)
	if getErr := executor.getRegistryList(ctx, groupListPath, "", groupRegistry, listResponse); getErr != nil {
		return nil, getErr
	}
	return metadataNames(collectMetadata(listResponse.GetGroup(), (*commonv1.Group).GetMetadata)), nil
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
	request := executor.newRequest(ctx).
		SetPathParam("group", group).
		SetPathParam("name", req.Name)
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

func (executor *HTTPExecutor) listResources(ctx context.Context, group string, resourceType session.ResourceType) ([]string, error) {
	listPath, listErr := resourceListPath(resourceType)
	if listErr != nil {
		return nil, listErr
	}
	response, requestErr := executor.newRequest(ctx).SetPathParam("group", group).Get(executor.config.Addr + listPath)
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
			Tags:     tuitext.Compact(indexRule.GetTags()),
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
	return tuitext.Compact(tags)
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

func (executor *HTTPExecutor) listIndexRules(ctx context.Context, group string) ([]*databasev1.IndexRule, error) {
	listResponse := new(databasev1.IndexRuleRegistryServiceListResponse)
	if getErr := executor.getRegistryList(ctx, indexRuleListPath, group, indexRuleRegistry, listResponse); getErr != nil {
		return nil, getErr
	}
	return listResponse.GetIndexRule(), nil
}

func (executor *HTTPExecutor) listIndexRuleBindings(ctx context.Context, group string) ([]*databasev1.IndexRuleBinding, error) {
	listResponse := new(databasev1.IndexRuleBindingRegistryServiceListResponse)
	if getErr := executor.getRegistryList(ctx, indexRuleBindingListPath, group, indexRuleBindingRegistry, listResponse); getErr != nil {
		return nil, getErr
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
	request := executor.newRequest(ctx).
		SetHeader("Content-Type", "application/json").
		SetBody(requestBody)
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

// collectMetadata reads the metadata of every non-nil resource in a registry list response.
func collectMetadata[T any](resources []*T, metadataOf func(*T) *commonv1.Metadata) []*commonv1.Metadata {
	metadataItems := make([]*commonv1.Metadata, 0, len(resources))
	for _, resource := range resources {
		if resource == nil {
			continue
		}
		metadataItems = append(metadataItems, metadataOf(resource))
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
	return tuitext.Compact(names)
}
