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

package property

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/api/validate"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/run"
)

type clientMetrics struct {
	totalSyncStarted  meter.Counter
	totalSyncFinished meter.Counter
	totalSyncErr      meter.Counter
	totalSyncLatency  meter.Counter
	totalSyncUpdates  meter.Counter

	totalQueryStarted  meter.Counter
	totalQueryFinished meter.Counter
	totalQueryErr      meter.Counter
	totalQueryLatency  meter.Counter

	totalRepairStarted  meter.Counter
	totalRepairFinished meter.Counter
	totalRepairNodes    meter.Counter

	totalWriteStarted  meter.Counter
	totalWriteFinished meter.Counter
	totalWriteErr      meter.Counter
	totalWriteLatency  meter.Counter

	totalDeleteStarted  meter.Counter
	totalDeleteFinished meter.Counter
	totalDeleteErr      meter.Counter
	totalDeleteLatency  meter.Counter

	cacheSize meter.Gauge
}

func newClientMetrics(factory observability.Factory) *clientMetrics {
	return &clientMetrics{
		totalSyncStarted:  factory.NewCounter("property_sync_started"),
		totalSyncFinished: factory.NewCounter("property_sync_finished"),
		totalSyncErr:      factory.NewCounter("property_sync_err"),
		totalSyncLatency:  factory.NewCounter("property_sync_latency"),
		totalSyncUpdates:  factory.NewCounter("property_sync_updates"),

		totalQueryStarted:  factory.NewCounter("property_query_started", "method"),
		totalQueryFinished: factory.NewCounter("property_query_finished", "method"),
		totalQueryErr:      factory.NewCounter("property_query_err", "method"),
		totalQueryLatency:  factory.NewCounter("property_query_latency", "method"),

		totalRepairStarted:  factory.NewCounter("property_repair_started"),
		totalRepairFinished: factory.NewCounter("property_repair_finished"),
		totalRepairNodes:    factory.NewCounter("property_repair_nodes"),

		totalWriteStarted:  factory.NewCounter("property_write_started", "kind", "group"),
		totalWriteFinished: factory.NewCounter("property_write_finished", "kind", "group"),
		totalWriteErr:      factory.NewCounter("property_write_err", "kind", "group"),
		totalWriteLatency:  factory.NewCounter("property_write_latency", "kind", "group"),

		totalDeleteStarted:  factory.NewCounter("property_delete_started", "kind", "group"),
		totalDeleteFinished: factory.NewCounter("property_delete_finished", "kind", "group"),
		totalDeleteErr:      factory.NewCounter("property_delete_err", "kind", "group"),
		totalDeleteLatency:  factory.NewCounter("property_delete_latency", "kind", "group"),

		cacheSize: factory.NewGauge("property_cache_size"),
	}
}

// SchemaRegistry implements schema.Registry interface using property-based storage.
type SchemaRegistry struct {
	nodesClient       queue.Client
	localUpdateClient schemav1.SchemaUpdateServiceClient
	localMgrClient    schemav1.SchemaManagementServiceClient
	l                 *logger.Logger
	closer            *run.Closer
	syncCloser        *run.Closer
	cache             *schemaCache
	metrics           *clientMetrics
	handlers          map[schema.Kind][]schema.EventHandler
	grpcTimeout       time.Duration
	syncInterval      time.Duration
	mu                sync.RWMutex
}

// NodeInfo holds information about a node and its schema clients.
type NodeInfo struct {
	Node               *databasev1.Node
	SchemaMgrClient    schemav1.SchemaManagementServiceClient
	SchemaUpdateClient schemav1.SchemaUpdateServiceClient
}

func (i *NodeInfo) canBeUse() bool {
	return i.Node != nil && i.SchemaMgrClient != nil && i.SchemaUpdateClient != nil
}

// ClientConfig holds configuration for construct SchemaRegistry client.
type ClientConfig struct {
	OMR          observability.MetricsRegistry
	NodeSchema   schema.Node
	Node         *NodeInfo
	DialProvider pub.DialOptionsProvider
	metrics      *clientMetrics
	GRPCTimeout  time.Duration
	SyncInterval time.Duration
}

// NewSchemaRegistryClient creates a new property schema registry client.
// When node is non-nil and has ROLE_META, the registry connects to itself synchronously
// during construction, ensuring metadata is available before subsequent services start.
func NewSchemaRegistryClient(cfg *ClientConfig) (*SchemaRegistry, error) {
	r := &SchemaRegistry{
		handlers:     make(map[schema.Kind][]schema.EventHandler),
		cache:        newSchemaCache(),
		closer:       run.NewCloser(1),
		l:            logger.GetLogger("property-schema-registry"),
		grpcTimeout:  cfg.GRPCTimeout,
		syncInterval: cfg.SyncInterval,
	}
	r.nodesClient = pub.NewWithoutMetadataAndFactory(schemaClientFactory(r), cfg.DialProvider)
	if cfg.OMR != nil {
		if cfg.metrics == nil {
			clientScope := metadataScope.SubScope("schema_property_client")
			cfg.metrics = newClientMetrics(cfg.OMR.With(clientScope))
		}
		r.metrics = cfg.metrics
	}
	// init current node with local clients
	if cfg.Node != nil && cfg.Node.canBeUse() {
		hasMetadataRole := false
		for _, role := range cfg.Node.Node.Roles {
			if role == databasev1.Role_ROLE_META {
				hasMetadataRole = true
				break
			}
		}
		if !hasMetadataRole {
			r.l.Info().Msg("metadata role not found in node, so skip bootstrapping from local meta node")
			return r, nil
		}
		// Store local clients for self-node operations
		r.localMgrClient = cfg.Node.SchemaMgrClient
		r.localUpdateClient = cfg.Node.SchemaUpdateClient
		cfg.Node.Node.GrpcAddress = pub.SelfGrpcAddress
		md := schema.Metadata{
			TypeMeta: schema.TypeMeta{
				Kind: schema.KindNode,
			},
			Spec: cfg.Node.Node,
		}
		r.nodesClient.OnAddOrUpdate(md)
		r.l.Info().Str("address", cfg.Node.Node.GetGrpcAddress()).Msg("registered local metadata node")
	} else {
		r.l.Info().Msg("local node info not provided or incomplete, " +
			"so skip bootstrapping from local meta node, and lookup other meta nodes from node registry")
		if cfg.NodeSchema == nil {
			return r, nil
		}
		listNode, listErr := cfg.NodeSchema.ListNode(context.Background(), databasev1.Role_ROLE_META)
		if listErr != nil {
			return nil, fmt.Errorf("failed to list metadata nodes: %w", listErr)
		}
		if len(listNode) == 0 {
			return nil, fmt.Errorf("no metadata nodes found in node registry")
		}
		for _, n := range listNode {
			md := schema.Metadata{
				TypeMeta: schema.TypeMeta{
					Kind: schema.KindNode,
				},
				Spec: n,
			}
			r.nodesClient.OnAddOrUpdate(md)
		}
	}
	return r, nil
}

// OnInit implements schema.EventHandler.
func (r *SchemaRegistry) OnInit(_ []schema.Kind) (bool, []int64) {
	return false, nil
}

// OnAddOrUpdate implements schema.EventHandler for getting the all metadata nodes.
func (r *SchemaRegistry) OnAddOrUpdate(m schema.Metadata) {
	if m.Kind != schema.KindNode {
		return
	}
	node, ok := m.Spec.(*databasev1.Node)
	if !ok {
		return
	}
	containsMetadata := false
	for _, role := range node.Roles {
		if role == databasev1.Role_ROLE_META {
			containsMetadata = true
			break
		}
	}
	if !containsMetadata {
		return
	}
	// Delegate to pub client for connection management
	r.nodesClient.OnAddOrUpdate(m)
	r.l.Info().Str("node", node.GetMetadata().GetName()).
		Str("address", node.GetGrpcAddress()).Msg("metadata node added via pub")
}

// OnDelete implements schema.EventHandler for getting which metadata node has been deleted.
func (r *SchemaRegistry) OnDelete(m schema.Metadata) {
	if m.Kind != schema.KindNode {
		return
	}
	// Delegate to pub client for connection management
	if handler, ok := r.nodesClient.(schema.EventHandler); ok {
		handler.OnDelete(m)
	}
}

// Close closes the registry.
func (r *SchemaRegistry) Close() error {
	r.closer.Done()
	r.closer.CloseThenWait()
	if r.syncCloser != nil {
		r.syncCloser.Done()
		r.syncCloser.CloseThenWait()
	}
	// Close pub client which manages all connections
	r.nodesClient.GracefulStop()
	return nil
}

// RegisterHandler registers an event handler for a schema kind.
func (r *SchemaRegistry) RegisterHandler(name string, kind schema.Kind, handler schema.EventHandler) {
	// Validate kind
	if kind&schema.KindMask != kind {
		panic(fmt.Sprintf("invalid kind %d", kind))
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var kinds []schema.Kind
	for i := 0; i < schema.KindSize; i++ {
		ki := schema.Kind(1 << i)
		if kind&ki > 0 {
			kinds = append(kinds, ki)
		}
	}
	r.l.Info().Str("name", name).Interface("kinds", kinds).Msg("registering handler")
	handler.OnInit(kinds)
	for _, ki := range kinds {
		r.addHandler(ki, handler)
	}
}

// Register registers a metadata entry.
func (r *SchemaRegistry) Register(context.Context, schema.Metadata, bool) error {
	panic("property based schema registry not support register")
}

// Compact is not supported in property mode.
func (r *SchemaRegistry) Compact(context.Context, int64) error {
	return nil
}

// StartWatcher starts the global sync mechanism.
func (r *SchemaRegistry) StartWatcher() {
	r.syncCloser = run.NewCloser(1)
	// init all metadata from connections
	go func() {
		// Use BroadcastWithExecutor to initialize from first responding node
		var initialized bool
		_ = r.nodesClient.BroadcastWithExecutor(func(nodeName string, c queue.PubClient) error {
			if initialized {
				return nil
			}
			sc, ok := c.(*schemaClient)
			if !ok {
				return errors.Errorf("client for node %s is not a schemaClient", nodeName)
			}
			if initErr := r.initializeFromSchemaClient(nodeName, sc); initErr != nil {
				r.l.Error().Err(initErr).Str("node", nodeName).Msg("failed to initialize metadata from node")
				return initErr
			}
			initialized = true
			return nil
		})
	}()

	go r.globalSync()
}

// Stream methods.

// GetStream retrieves a stream by metadata.
func (r *SchemaRegistry) GetStream(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Stream, error) {
	return getResource[*databasev1.Stream](ctx, r, schema.KindStream, metadata.GetGroup(), metadata.GetName())
}

// ListStream lists streams in a group.
func (r *SchemaRegistry) ListStream(ctx context.Context, opt schema.ListOpt) ([]*databasev1.Stream, error) {
	return listResources[*databasev1.Stream](ctx, r, schema.KindStream, opt.Group, true)
}

// CreateStream creates a new stream.
func (r *SchemaRegistry) CreateStream(ctx context.Context, stream *databasev1.Stream) (int64, error) {
	if validateErr := validate.Stream(stream); validateErr != nil {
		return 0, validateErr
	}
	now := time.Now().UnixNano()
	stream.Metadata.ModRevision = now
	stream.UpdatedAt = timestamppb.Now()
	return now, createResource(ctx, r, schema.KindStream, stream)
}

// UpdateStream updates an existing stream.
func (r *SchemaRegistry) UpdateStream(ctx context.Context, stream *databasev1.Stream) (int64, error) {
	if validateErr := validate.Stream(stream); validateErr != nil {
		return 0, validateErr
	}
	now := time.Now().UnixNano()
	stream.Metadata.ModRevision = now
	stream.UpdatedAt = timestamppb.Now()
	return now, updateResource(ctx, r, schema.KindStream, stream)
}

// DeleteStream deletes a stream.
func (r *SchemaRegistry) DeleteStream(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindStream, metadata.GetGroup(), metadata.GetName())
}

// Measure methods.

// GetMeasure retrieves a measure by metadata.
func (r *SchemaRegistry) GetMeasure(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Measure, error) {
	return getResource[*databasev1.Measure](ctx, r, schema.KindMeasure, metadata.GetGroup(), metadata.GetName())
}

// ListMeasure lists measures in a group.
func (r *SchemaRegistry) ListMeasure(ctx context.Context, opt schema.ListOpt) ([]*databasev1.Measure, error) {
	return listResources[*databasev1.Measure](ctx, r, schema.KindMeasure, opt.Group, true)
}

// CreateMeasure creates a new measure.
func (r *SchemaRegistry) CreateMeasure(ctx context.Context, measure *databasev1.Measure) (int64, error) {
	if validateErr := validate.Measure(measure); validateErr != nil {
		return 0, validateErr
	}
	now := time.Now().UnixNano()
	measure.Metadata.ModRevision = now
	measure.UpdatedAt = timestamppb.Now()
	return now, createResource(ctx, r, schema.KindMeasure, measure)
}

// UpdateMeasure updates an existing measure.
func (r *SchemaRegistry) UpdateMeasure(ctx context.Context, measure *databasev1.Measure) (int64, error) {
	if validateErr := validate.Measure(measure); validateErr != nil {
		return 0, validateErr
	}
	now := time.Now().UnixNano()
	measure.Metadata.ModRevision = now
	measure.UpdatedAt = timestamppb.Now()
	return now, updateResource(ctx, r, schema.KindMeasure, measure)
}

// DeleteMeasure deletes a measure.
func (r *SchemaRegistry) DeleteMeasure(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindMeasure, metadata.GetGroup(), metadata.GetName())
}

// TopNAggregations returns top-N aggregations for a measure.
func (r *SchemaRegistry) TopNAggregations(ctx context.Context, metadata *commonv1.Metadata) ([]*databasev1.TopNAggregation, error) {
	aggregations, listErr := r.ListTopNAggregation(ctx, schema.ListOpt{Group: metadata.GetGroup()})
	if listErr != nil {
		return nil, listErr
	}
	var result []*databasev1.TopNAggregation
	for _, aggrDef := range aggregations {
		if aggrDef.GetSourceMeasure().GetName() == metadata.GetName() {
			result = append(result, aggrDef)
		}
	}
	return result, nil
}

// Trace methods.

// GetTrace retrieves a trace by metadata.
func (r *SchemaRegistry) GetTrace(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Trace, error) {
	return getResource[*databasev1.Trace](ctx, r, schema.KindTrace, metadata.GetGroup(), metadata.GetName())
}

// ListTrace lists traces in a group.
func (r *SchemaRegistry) ListTrace(ctx context.Context, opt schema.ListOpt) ([]*databasev1.Trace, error) {
	return listResources[*databasev1.Trace](ctx, r, schema.KindTrace, opt.Group, true)
}

// CreateTrace creates a new trace.
func (r *SchemaRegistry) CreateTrace(ctx context.Context, trace *databasev1.Trace) (int64, error) {
	if validateErr := validate.Trace(trace); validateErr != nil {
		return 0, validateErr
	}
	now := time.Now().UnixNano()
	trace.Metadata.ModRevision = now
	trace.UpdatedAt = timestamppb.Now()
	return now, createResource(ctx, r, schema.KindTrace, trace)
}

// UpdateTrace updates an existing trace.
func (r *SchemaRegistry) UpdateTrace(ctx context.Context, trace *databasev1.Trace) (int64, error) {
	if validateErr := validate.Trace(trace); validateErr != nil {
		return 0, validateErr
	}
	now := time.Now().UnixNano()
	trace.Metadata.ModRevision = now
	trace.UpdatedAt = timestamppb.Now()
	return now, updateResource(ctx, r, schema.KindTrace, trace)
}

// DeleteTrace deletes a trace.
func (r *SchemaRegistry) DeleteTrace(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindTrace, metadata.GetGroup(), metadata.GetName())
}

// Group methods.

// GetGroup retrieves a group by name.
func (r *SchemaRegistry) GetGroup(ctx context.Context, group string) (*commonv1.Group, error) {
	return getResource[*commonv1.Group](ctx, r, schema.KindGroup, "", group)
}

// ListGroup lists all groups.
func (r *SchemaRegistry) ListGroup(ctx context.Context) ([]*commonv1.Group, error) {
	return listResources[*commonv1.Group](ctx, r, schema.KindGroup, "", false)
}

// CreateGroup creates a new group.
func (r *SchemaRegistry) CreateGroup(ctx context.Context, group *commonv1.Group) error {
	if validateErr := validate.Group(group); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	group.Metadata.ModRevision = now
	group.UpdatedAt = timestamppb.Now()
	return createResource(ctx, r, schema.KindGroup, group)
}

// UpdateGroup updates an existing group.
func (r *SchemaRegistry) UpdateGroup(ctx context.Context, group *commonv1.Group) error {
	if validateErr := validate.Group(group); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	group.Metadata.ModRevision = now
	group.UpdatedAt = timestamppb.Now()
	return updateResource(ctx, r, schema.KindGroup, group)
}

// DeleteGroup deletes a group.
func (r *SchemaRegistry) DeleteGroup(ctx context.Context, group string) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindGroup, "", group)
}

// IndexRule methods.

// GetIndexRule retrieves an index rule by metadata.
func (r *SchemaRegistry) GetIndexRule(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRule, error) {
	return getResource[*databasev1.IndexRule](ctx, r, schema.KindIndexRule, metadata.GetGroup(), metadata.GetName())
}

// ListIndexRule lists index rules in a group.
func (r *SchemaRegistry) ListIndexRule(ctx context.Context, opt schema.ListOpt) ([]*databasev1.IndexRule, error) {
	return listResources[*databasev1.IndexRule](ctx, r, schema.KindIndexRule, opt.Group, true)
}

// CreateIndexRule creates a new index rule.
func (r *SchemaRegistry) CreateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error {
	if indexRule.GetMetadata().GetId() == 0 {
		buf := []byte(indexRule.Metadata.Group)
		buf = append(buf, indexRule.Metadata.Name...)
		indexRule.Metadata.Id = crc32.ChecksumIEEE(buf)
	}
	if validateErr := validate.IndexRule(indexRule); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	indexRule.Metadata.ModRevision = now
	indexRule.UpdatedAt = timestamppb.Now()
	return createResource(ctx, r, schema.KindIndexRule, indexRule)
}

// UpdateIndexRule updates an existing index rule.
func (r *SchemaRegistry) UpdateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error {
	if indexRule.GetMetadata().GetId() == 0 {
		existingIndexRule, getErr := r.GetIndexRule(ctx, indexRule.Metadata)
		if getErr != nil {
			return getErr
		}
		indexRule.Metadata.Id = existingIndexRule.Metadata.Id
	}
	if validateErr := validate.IndexRule(indexRule); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	indexRule.Metadata.ModRevision = now
	indexRule.UpdatedAt = timestamppb.Now()
	return updateResource(ctx, r, schema.KindIndexRule, indexRule)
}

// DeleteIndexRule deletes an index rule.
func (r *SchemaRegistry) DeleteIndexRule(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindIndexRule, metadata.GetGroup(), metadata.GetName())
}

// IndexRuleBinding methods.

// GetIndexRuleBinding retrieves an index rule binding by metadata.
func (r *SchemaRegistry) GetIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRuleBinding, error) {
	return getResource[*databasev1.IndexRuleBinding](ctx, r, schema.KindIndexRuleBinding, metadata.GetGroup(), metadata.GetName())
}

// ListIndexRuleBinding lists index rule bindings in a group.
func (r *SchemaRegistry) ListIndexRuleBinding(ctx context.Context, opt schema.ListOpt) ([]*databasev1.IndexRuleBinding, error) {
	return listResources[*databasev1.IndexRuleBinding](ctx, r, schema.KindIndexRuleBinding, opt.Group, true)
}

// CreateIndexRuleBinding creates a new index rule binding.
func (r *SchemaRegistry) CreateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error {
	if validateErr := validate.IndexRuleBinding(indexRuleBinding); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	indexRuleBinding.Metadata.ModRevision = now
	indexRuleBinding.UpdatedAt = timestamppb.Now()
	return createResource(ctx, r, schema.KindIndexRuleBinding, indexRuleBinding)
}

// UpdateIndexRuleBinding updates an existing index rule binding.
func (r *SchemaRegistry) UpdateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error {
	if validateErr := validate.IndexRuleBinding(indexRuleBinding); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	indexRuleBinding.Metadata.ModRevision = now
	indexRuleBinding.UpdatedAt = timestamppb.Now()
	return updateResource(ctx, r, schema.KindIndexRuleBinding, indexRuleBinding)
}

// DeleteIndexRuleBinding deletes an index rule binding.
func (r *SchemaRegistry) DeleteIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindIndexRuleBinding, metadata.GetGroup(), metadata.GetName())
}

// TopNAggregation methods.

// GetTopNAggregation retrieves a top-N aggregation by metadata.
func (r *SchemaRegistry) GetTopNAggregation(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.TopNAggregation, error) {
	return getResource[*databasev1.TopNAggregation](ctx, r, schema.KindTopNAggregation, metadata.GetGroup(), metadata.GetName())
}

// ListTopNAggregation lists top-N aggregations in a group.
func (r *SchemaRegistry) ListTopNAggregation(ctx context.Context, opt schema.ListOpt) ([]*databasev1.TopNAggregation, error) {
	return listResources[*databasev1.TopNAggregation](ctx, r, schema.KindTopNAggregation, opt.Group, true)
}

// CreateTopNAggregation creates a new top-N aggregation.
func (r *SchemaRegistry) CreateTopNAggregation(ctx context.Context, topN *databasev1.TopNAggregation) error {
	if validateErr := validate.TopNAggregation(topN); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	topN.Metadata.ModRevision = now
	topN.UpdatedAt = timestamppb.Now()
	return createResource(ctx, r, schema.KindTopNAggregation, topN)
}

// UpdateTopNAggregation updates an existing top-N aggregation.
func (r *SchemaRegistry) UpdateTopNAggregation(ctx context.Context, topN *databasev1.TopNAggregation) error {
	if validateErr := validate.TopNAggregation(topN); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	topN.Metadata.ModRevision = now
	topN.UpdatedAt = timestamppb.Now()
	return updateResource(ctx, r, schema.KindTopNAggregation, topN)
}

// DeleteTopNAggregation deletes a top-N aggregation.
func (r *SchemaRegistry) DeleteTopNAggregation(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindTopNAggregation, metadata.GetGroup(), metadata.GetName())
}

// Node methods.

// ListNode lists nodes by role.
func (r *SchemaRegistry) ListNode(context.Context, databasev1.Role) ([]*databasev1.Node, error) {
	panic("property based schema registry does not support list node")
}

// RegisterNode registers a node.
func (r *SchemaRegistry) RegisterNode(context.Context, *databasev1.Node, bool) error {
	panic("property based schema registry does not support register node")
}

// GetNode retrieves a node by name.
func (r *SchemaRegistry) GetNode(context.Context, string) (*databasev1.Node, error) {
	panic("property based schema registry does not support get node")
}

// UpdateNode updates a node.
func (r *SchemaRegistry) UpdateNode(context.Context, *databasev1.Node) error {
	panic("property based schema registry does not support update node")
}

// Property methods.

// GetProperty retrieves a property by metadata.
func (r *SchemaRegistry) GetProperty(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Property, error) {
	return getResource[*databasev1.Property](ctx, r, schema.KindProperty, metadata.GetGroup(), metadata.GetName())
}

// ListProperty lists properties in a group.
func (r *SchemaRegistry) ListProperty(ctx context.Context, opt schema.ListOpt) ([]*databasev1.Property, error) {
	return listResources[*databasev1.Property](ctx, r, schema.KindProperty, opt.Group, true)
}

// CreateProperty creates a new property.
func (r *SchemaRegistry) CreateProperty(ctx context.Context, property *databasev1.Property) error {
	now := time.Now().UnixNano()
	property.Metadata.ModRevision = now
	property.UpdatedAt = timestamppb.Now()
	return createResource(ctx, r, schema.KindProperty, property)
}

// UpdateProperty updates an existing property.
func (r *SchemaRegistry) UpdateProperty(ctx context.Context, property *databasev1.Property) error {
	now := time.Now().UnixNano()
	property.Metadata.ModRevision = now
	property.UpdatedAt = timestamppb.Now()
	return updateResource(ctx, r, schema.KindProperty, property)
}

// DeleteProperty deletes a property.
func (r *SchemaRegistry) DeleteProperty(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.deleteFromAllServers(ctx, schema.KindProperty, metadata.GetGroup(), metadata.GetName())
}

func getResource[T proto.Message](ctx context.Context, r *SchemaRegistry, kind schema.Kind, group, name string) (T, error) {
	var zero T
	prop, getErr := r.getSchema(ctx, kind, group, name)
	if getErr != nil {
		return zero, getErr
	}
	if prop == nil {
		return zero, schema.ErrGRPCResourceNotFound
	}
	md, convErr := ToSchema(kind, prop)
	if convErr != nil {
		return zero, convErr
	}
	result, ok := md.Spec.(T)
	if !ok {
		return zero, errors.Errorf("unexpected spec type for kind %s", kind)
	}
	return result, nil
}

func listResources[T proto.Message](ctx context.Context, r *SchemaRegistry, kind schema.Kind, group string, requireGroup bool) ([]T, error) {
	if requireGroup && group == "" {
		return nil, schema.BadRequest("group", "group should not be empty")
	}
	props, listErr := r.listSchemas(ctx, kind, group)
	if listErr != nil {
		return nil, listErr
	}
	results := make([]T, 0, len(props))
	for _, prop := range props {
		md, convErr := ToSchema(kind, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Stringer("kind", kind).Msg("failed to convert property")
			continue
		}
		result, ok := md.Spec.(T)
		if !ok {
			r.l.Warn().Stringer("kind", kind).Msg("unexpected spec type")
			continue
		}
		results = append(results, result)
	}
	return results, nil
}

func initResourceFromClient[T proto.Message](ctx context.Context, r *SchemaRegistry, client schemav1.SchemaManagementServiceClient,
	kind schema.Kind, group string, maxRevision *int64,
) {
	props, listErr := r.listSchemasFromClient(ctx, client, kind, group)
	if listErr != nil {
		r.l.Warn().Err(listErr).Str("group", group).Msg("failed to list streams")
		return
	}
	for _, prop := range props {
		md, convErr := ToSchema(kind, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msgf("failed to convert property to %s", kind)
			continue
		}
		*maxRevision = r.processInitialResource(kind, md.Spec.(T), *maxRevision)
	}
}

func createResource[T proto.Message](ctx context.Context, r *SchemaRegistry, kind schema.Kind, spec T) error {
	metadata, err := getMetadataFromSpec(kind, spec)
	if err != nil {
		return err
	}
	exists, existErr := r.existSchema(ctx, kind, metadata.Group, metadata.Name)
	if existErr != nil {
		return existErr
	}
	if exists {
		return schema.ErrGRPCAlreadyExists
	}
	prop, convErr := SchemaToProperty(kind, spec)
	if convErr != nil {
		return convErr
	}
	return r.insertToAllServers(ctx, prop)
}

func updateResource[T proto.Message](ctx context.Context, r *SchemaRegistry, kind schema.Kind, spec T) error {
	metadata, err := getMetadataFromSpec(kind, spec)
	if err != nil {
		return err
	}
	originalSchema, err := r.getSchema(ctx, kind, metadata.Group, metadata.Name)
	if err != nil {
		return err
	}
	if originalSchema == nil {
		return fmt.Errorf("schema %s/%s not exist", metadata.Group, metadata.Name)
	}
	prop, convErr := SchemaToProperty(kind, spec)
	if convErr != nil {
		return convErr
	}
	return r.updateToAllServers(ctx, prop)
}

func (r *SchemaRegistry) getSchema(ctx context.Context, kind schema.Kind, group, name string) (*propertyv1.Property, error) {
	propID := BuildPropertyID(kind, &commonv1.Metadata{Group: group, Name: name})
	query := buildSchemaQuery(kind, group, []string{propID})
	propMap, queryErr := r.queryAndRepairSchemas(ctx, query)
	if queryErr != nil {
		return nil, queryErr
	}
	info := propMap[propID]
	if info == nil || info.best == nil || info.best.deleteTime > 0 {
		return nil, nil
	}
	return info.best.property, nil
}

func (r *SchemaRegistry) existSchema(ctx context.Context, kind schema.Kind, group, name string) (bool, error) {
	propID := BuildPropertyID(kind, &commonv1.Metadata{Group: group, Name: name})
	query := buildSchemaQuery(kind, group, []string{propID})

	var hasSchema bool
	var mu sync.Mutex
	broadcastErr := r.nodesClient.BroadcastWithExecutor(func(nodeName string, c queue.PubClient) error {
		sc, ok := c.(*schemaClient)
		if !ok {
			return errors.Errorf("client for node %s is not a schemaClient", nodeName)
		}
		resp, callErr := sc.MgrClient().ExistSchema(ctx, &schemav1.ExistSchemaRequest{Query: query})
		if callErr != nil {
			return callErr
		}
		if resp.GetHasSchema() {
			mu.Lock()
			hasSchema = true
			mu.Unlock()
		}
		return nil
	})
	if hasSchema {
		return true, nil
	}
	return false, broadcastErr
}

type schemaWithDeleteTime struct {
	property   *propertyv1.Property
	deleteTime int64
}

type propInfo struct {
	nodeRev     map[string]int64
	nodeDelTime map[string]int64
	best        *schemaWithDeleteTime
}

func (r *SchemaRegistry) listSchemas(ctx context.Context, kind schema.Kind, group string) ([]*propertyv1.Property, error) {
	query := buildSchemaQuery(kind, group, nil)
	propMap, queryErr := r.queryAndRepairSchemas(ctx, query)
	if queryErr != nil {
		return nil, queryErr
	}
	result := make([]*propertyv1.Property, 0, len(propMap))
	for _, info := range propMap {
		if info.best != nil && info.best.deleteTime == 0 {
			result = append(result, info.best.property)
		}
	}
	return result, nil
}

func (r *SchemaRegistry) querySchemasFromClient(ctx context.Context, client schemav1.SchemaManagementServiceClient,
	query *propertyv1.QueryRequest,
) ([]*schemaWithDeleteTime, error) {
	stream, streamErr := client.ListSchemas(ctx, &schemav1.ListSchemasRequest{Query: query})
	if streamErr != nil {
		return nil, streamErr
	}
	var results []*schemaWithDeleteTime
	for {
		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return nil, recvErr
		}
		for idx, prop := range resp.Properties {
			var deleteTime int64
			if idx < len(resp.DeleteTimes) {
				deleteTime = resp.DeleteTimes[idx]
			}
			results = append(results, &schemaWithDeleteTime{
				property:   prop,
				deleteTime: deleteTime,
			})
		}
	}
	return results, nil
}

func buildSchemaQuery(kind schema.Kind, group string, ids []string) *propertyv1.QueryRequest {
	query := &propertyv1.QueryRequest{
		Groups: []string{SchemaGroup},
		Name:   kind.String(),
		Ids:    ids,
		Limit:  10000,
	}
	if group != "" {
		query.Criteria = &modelv1.Criteria{
			Exp: &modelv1.Criteria_Condition{
				Condition: &modelv1.Condition{
					Name: TagKeyGroup,
					Op:   modelv1.Condition_BINARY_OP_EQ,
					Value: &modelv1.TagValue{
						Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: group}},
					},
				},
			},
		}
	}
	return query
}

func (r *SchemaRegistry) queryAndRepairSchemas(ctx context.Context, query *propertyv1.QueryRequest) (map[string]*propInfo, error) {
	if r.metrics != nil {
		r.metrics.totalQueryStarted.Inc(1, "list")
	}
	start := time.Now()
	defer func() {
		if r.metrics != nil {
			r.metrics.totalQueryFinished.Inc(1, "list")
			r.metrics.totalQueryLatency.Inc(time.Since(start).Seconds(), "list")
		}
	}()

	propMap := make(map[string]*propInfo)
	respondedNodes := make(map[string]bool)
	var mu sync.Mutex

	broadcastErr := r.nodesClient.BroadcastWithExecutor(func(nodeName string, c queue.PubClient) error {
		sc, ok := c.(*schemaClient)
		if !ok {
			return errors.Errorf("client for node %s is not a schemaClient", nodeName)
		}
		schemas, queryErr := r.querySchemasFromClient(ctx, sc.MgrClient(), query)
		if queryErr != nil {
			r.l.Warn().Err(queryErr).Str("node", nodeName).Msg("failed to query node")
			return queryErr
		}
		mu.Lock()
		defer mu.Unlock()
		respondedNodes[nodeName] = true
		for _, s := range schemas {
			propID := s.property.Id
			rev := s.property.UpdatedAt.AsTime().UnixNano()
			info, exists := propMap[propID]
			if !exists {
				info = &propInfo{
					nodeRev:     make(map[string]int64),
					nodeDelTime: make(map[string]int64),
				}
				propMap[propID] = info
			}
			if existingRev, prevExists := info.nodeRev[nodeName]; !prevExists || rev > existingRev {
				info.nodeRev[nodeName] = rev
				info.nodeDelTime[nodeName] = s.deleteTime
			}
			if info.best == nil || info.best.property.UpdatedAt.AsTime().UnixNano() < rev {
				info.best = s
			}
		}
		return nil
	})

	// Repair inconsistent nodes using broadcast
	r.repairInconsistentNodesWithBroadcast(ctx, respondedNodes, propMap)

	if broadcastErr != nil && len(propMap) == 0 {
		if r.metrics != nil {
			r.metrics.totalQueryErr.Inc(1, "list")
		}
		return nil, broadcastErr
	}
	return propMap, nil
}

func (r *SchemaRegistry) repairInconsistentNodesWithBroadcast(ctx context.Context, respondedNodes map[string]bool, propMap map[string]*propInfo) {
	if r.metrics != nil {
		r.metrics.totalRepairStarted.Inc(1)
	}
	defer func() {
		if r.metrics != nil {
			r.metrics.totalRepairFinished.Inc(1)
		}
	}()
	for propID, info := range propMap {
		if info.best == nil {
			continue
		}
		bestRev := info.best.property.UpdatedAt.AsTime().UnixNano()
		var nodesToRepair []string
		for nodeName := range respondedNodes {
			nodeRev, exists := info.nodeRev[nodeName]
			if !exists || nodeRev < bestRev {
				nodesToRepair = append(nodesToRepair, nodeName)
			}
		}
		if len(nodesToRepair) == 0 {
			continue
		}
		if r.metrics != nil {
			r.metrics.totalRepairNodes.Inc(float64(len(nodesToRepair)))
		}
		r.l.Info().Str("propID", propID).Int64("bestRev", bestRev).
			Int64("deleteTime", info.best.deleteTime).
			Strs("nodesToRepair", nodesToRepair).Msg("repairing schema inconsistency")

		nodesToRepairSet := make(map[string]bool)
		for _, n := range nodesToRepair {
			nodesToRepairSet[n] = true
		}
		bestProp := info.best.property
		bestDelTime := info.best.deleteTime

		_ = r.nodesClient.BroadcastWithExecutor(func(nodeName string, c queue.PubClient) error {
			if !nodesToRepairSet[nodeName] {
				return nil
			}
			sc, ok := c.(*schemaClient)
			if !ok {
				return errors.Errorf("client for node %s is not a schemaClient", nodeName)
			}
			_, repairErr := sc.MgrClient().RepairSchema(ctx, &schemav1.RepairSchemaRequest{
				Property:   bestProp,
				DeleteTime: bestDelTime,
			})
			if repairErr != nil {
				r.l.Warn().Err(repairErr).Str("propID", propID).Str("node", nodeName).Msg("repair failed")
				return repairErr
			}
			r.l.Info().Str("propID", propID).Str("node", nodeName).Msg("repaired successfully")
			return nil
		})
	}
}

func (r *SchemaRegistry) insertToAllServers(ctx context.Context, prop *propertyv1.Property) error {
	kind, group := extractKindAndGroupFromProperty(prop)
	if r.metrics != nil {
		r.metrics.totalWriteStarted.Inc(1, kind, group)
	}
	start := time.Now()
	defer func() {
		if r.metrics != nil {
			r.metrics.totalWriteFinished.Inc(1, kind, group)
			r.metrics.totalWriteLatency.Inc(time.Since(start).Seconds(), kind, group)
		}
	}()
	broadcastErr := r.nodesClient.BroadcastWithExecutor(func(nodeName string, c queue.PubClient) error {
		sc, ok := c.(*schemaClient)
		if !ok {
			return errors.Errorf("client for node %s is not a schemaClient", nodeName)
		}
		_, callErr := sc.MgrClient().InsertSchema(ctx, &schemav1.InsertSchemaRequest{Property: prop})
		return callErr
	})
	if broadcastErr != nil && r.metrics != nil {
		r.metrics.totalWriteErr.Inc(1, kind, group)
	}
	return broadcastErr
}

func (r *SchemaRegistry) updateToAllServers(ctx context.Context, prop *propertyv1.Property) error {
	kind, group := extractKindAndGroupFromProperty(prop)
	if r.metrics != nil {
		r.metrics.totalWriteStarted.Inc(1, kind, group)
	}
	start := time.Now()
	defer func() {
		if r.metrics != nil {
			r.metrics.totalWriteFinished.Inc(1, kind, group)
			r.metrics.totalWriteLatency.Inc(time.Since(start).Seconds(), kind, group)
		}
	}()
	broadcastErr := r.nodesClient.BroadcastWithExecutor(func(nodeName string, c queue.PubClient) error {
		sc, ok := c.(*schemaClient)
		if !ok {
			return errors.Errorf("client for node %s is not a schemaClient", nodeName)
		}
		_, callErr := sc.MgrClient().UpdateSchema(ctx, &schemav1.UpdateSchemaRequest{Property: prop})
		return callErr
	})
	if broadcastErr != nil && r.metrics != nil {
		r.metrics.totalWriteErr.Inc(1, kind, group)
	}
	return broadcastErr
}

func (r *SchemaRegistry) deleteFromAllServers(ctx context.Context, kind schema.Kind, group, name string) (bool, error) {
	kindStr := kind.String()
	if r.metrics != nil {
		r.metrics.totalDeleteStarted.Inc(1, kindStr, group)
	}
	start := time.Now()
	defer func() {
		if r.metrics != nil {
			r.metrics.totalDeleteFinished.Inc(1, kindStr, group)
			r.metrics.totalDeleteLatency.Inc(time.Since(start).Seconds(), kindStr, group)
		}
	}()
	propID := BuildPropertyID(kind, &commonv1.Metadata{Group: group, Name: name})
	var found bool
	var mu sync.Mutex

	broadcastErr := r.nodesClient.BroadcastWithExecutor(func(nodeName string, c queue.PubClient) error {
		sc, ok := c.(*schemaClient)
		if !ok {
			return errors.Errorf("client for node %s is not a schemaClient", nodeName)
		}
		resp, callErr := sc.MgrClient().DeleteSchema(ctx, &schemav1.DeleteSchemaRequest{
			Delete: &propertyv1.DeleteRequest{
				Group: SchemaGroup,
				Name:  kind.String(),
				Id:    propID,
			},
			UpdateAt: timestamppb.Now(),
		})
		if callErr != nil {
			return callErr
		}
		if resp.GetFound() {
			mu.Lock()
			found = true
			mu.Unlock()
		}
		return nil
	})
	if broadcastErr != nil && r.metrics != nil {
		r.metrics.totalDeleteErr.Inc(1, kindStr, group)
	}
	return found, broadcastErr
}

func (r *SchemaRegistry) initializeFromSchemaClient(nodeName string, sc *schemaClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	r.l.Info().Str("node", nodeName).Msg("initializing resources from metadata node")
	groups, listErr := r.listGroupFromClient(ctx, sc.MgrClient())
	if listErr != nil {
		r.l.Warn().Err(listErr).Msg("failed to list groups during initialization")
		return listErr
	}
	var maxRevision int64
	for _, group := range groups {
		// init group
		maxRevision = r.processInitialResource(schema.KindGroup, group, maxRevision)

		// init related resources
		groupName := group.GetMetadata().GetName()
		catalog := group.GetCatalog()
		switch catalog {
		case commonv1.Catalog_CATALOG_STREAM:
			initResourceFromClient[*databasev1.Stream](ctx, r, sc.MgrClient(), schema.KindStream, groupName, &maxRevision)
		case commonv1.Catalog_CATALOG_MEASURE:
			initResourceFromClient[*databasev1.Measure](ctx, r, sc.MgrClient(), schema.KindMeasure, groupName, &maxRevision)
			initResourceFromClient[*databasev1.TopNAggregation](ctx, r, sc.MgrClient(), schema.KindTopNAggregation, groupName, &maxRevision)
		case commonv1.Catalog_CATALOG_TRACE:
			initResourceFromClient[*databasev1.Trace](ctx, r, sc.MgrClient(), schema.KindTrace, groupName, &maxRevision)
		case commonv1.Catalog_CATALOG_PROPERTY:
			initResourceFromClient[*databasev1.Property](ctx, r, sc.MgrClient(), schema.KindProperty, groupName, &maxRevision)
		}
		if catalog != commonv1.Catalog_CATALOG_PROPERTY {
			initResourceFromClient[*databasev1.IndexRule](ctx, r, sc.MgrClient(), schema.KindIndexRule, groupName, &maxRevision)
			initResourceFromClient[*databasev1.IndexRuleBinding](ctx, r, sc.MgrClient(), schema.KindIndexRuleBinding, groupName, &maxRevision)
		}
	}
	r.l.Info().Str("node", nodeName).
		Int64("latestUpdateAt", maxRevision).Msg("completed resource initialization")
	return nil
}

func (r *SchemaRegistry) processInitialResource(kind schema.Kind, spec proto.Message, currentMax int64) int64 {
	prop, convErr := SchemaToProperty(kind, spec)
	if convErr != nil {
		r.l.Warn().Err(convErr).Stringer("kind", kind).Msg("failed to convert to property")
		return currentMax
	}
	return r.processInitialResourceFromProperty(kind, prop, spec, currentMax)
}

func (r *SchemaRegistry) processInitialResourceFromProperty(kind schema.Kind, prop *propertyv1.Property, spec proto.Message, currentMax int64) int64 {
	revision := prop.UpdatedAt.AsTime().UnixNano()
	entry := &cacheEntry{
		latestUpdateAt: revision,
		kind:           kind,
		group:          getGroupFromTags(prop.Tags),
		name:           getNameFromTags(prop.Tags),
	}
	if r.cache.Update(prop.Id, entry) {
		md := schema.Metadata{
			TypeMeta: schema.TypeMeta{
				Kind:        kind,
				Name:        entry.name,
				Group:       entry.group,
				ModRevision: prop.Metadata.ModRevision,
			},
			Spec: spec,
		}
		r.notifyHandlers(kind, md, false)
	}
	if r.metrics != nil {
		r.metrics.cacheSize.Set(float64(r.cache.Size()))
	}
	if revision > currentMax {
		return revision
	}
	return currentMax
}

func getGroupFromTags(tags []*modelv1.Tag) string {
	for _, tag := range tags {
		if tag.Key == TagKeyGroup {
			return tag.Value.GetStr().GetValue()
		}
	}
	return ""
}

func getNameFromTags(tags []*modelv1.Tag) string {
	for _, tag := range tags {
		if tag.Key == TagKeyName {
			return tag.Value.GetStr().GetValue()
		}
	}
	return ""
}

func (r *SchemaRegistry) notifyHandlers(kind schema.Kind, md schema.Metadata, isDelete bool) {
	r.mu.RLock()
	handlers := r.handlers[kind]
	r.mu.RUnlock()
	for _, h := range handlers {
		if isDelete {
			h.OnDelete(md)
		} else {
			h.OnAddOrUpdate(md)
		}
	}
}

func (r *SchemaRegistry) addHandler(kind schema.Kind, handler schema.EventHandler) {
	if r.handlers[kind] == nil {
		r.handlers[kind] = make([]schema.EventHandler, 0)
	}
	r.handlers[kind] = append(r.handlers[kind], handler)
}

// parsePropertyID parses property ID to get kind, group, name.
// Format: "kind_group/name" or "kind_name" (for Group/Node).
func parsePropertyID(propID string) (schema.Kind, string, string) {
	underscoreIdx := strings.Index(propID, "_")
	if underscoreIdx == -1 {
		return 0, "", ""
	}
	kindStr := propID[:underscoreIdx]
	rest := propID[underscoreIdx+1:]
	kind, kindErr := KindFromString(kindStr)
	if kindErr != nil {
		return 0, "", ""
	}
	if kind == schema.KindGroup {
		return kind, "", rest
	}
	slashIdx := strings.Index(rest, "/")
	if slashIdx == -1 {
		return kind, "", rest
	}
	return kind, rest[:slashIdx], rest[slashIdx+1:]
}

func (r *SchemaRegistry) globalSync() {
	if !r.syncCloser.AddRunning() {
		return
	}
	defer r.syncCloser.Done()
	ticker := time.NewTicker(r.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.syncCloser.CloseNotify():
			return
		case <-ticker.C:
			r.performSync()
		}
	}
}

func (r *SchemaRegistry) performSync() {
	if r.metrics != nil {
		r.metrics.totalSyncStarted.Inc(1)
	}
	start := time.Now()
	defer func() {
		if r.metrics != nil {
			r.metrics.totalSyncFinished.Inc(1)
			r.metrics.totalSyncLatency.Inc(time.Since(start).Seconds())
		}
	}()
	r.l.Debug().Msg("performing global schema sync")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_ = r.nodesClient.BroadcastWithExecutor(func(nodeName string, c queue.PubClient) error {
		sc, ok := c.(*schemaClient)
		if !ok {
			return errors.Errorf("client for node %s is not a schemaClient", nodeName)
		}
		sinceRevision := r.cache.GetMaxRevision()
		updatedNames := r.queryUpdatedSchemas(ctx, sc.UpdateClient(), sinceRevision)
		syncedKinds := make(map[schema.Kind]bool)
		if len(updatedNames) > 0 {
			if r.metrics != nil {
				r.metrics.totalSyncUpdates.Inc(float64(len(updatedNames)))
			}
			r.l.Info().Str("node", nodeName).
				Strs("schema_names", updatedNames).
				Int64("sinceRevision", sinceRevision).
				Msg("detected schema updates")
			for _, name := range updatedNames {
				kind, kindErr := KindFromString(name)
				if kindErr != nil {
					r.l.Warn().Str("kind_name", name).Err(kindErr).Msg("failed to parse kind from name")
					continue
				}
				syncedKinds[kind] = true
				r.syncAllResourcesOfKind(ctx, sc.MgrClient(), kind)
			}
		}
		return nil
	})
}

func (r *SchemaRegistry) queryUpdatedSchemas(ctx context.Context, c schemav1.SchemaUpdateServiceClient, reversion int64) []string {
	req := &schemav1.AggregateSchemaUpdatesRequest{
		Query: &propertyv1.QueryRequest{
			Groups:   []string{SchemaGroup},
			Criteria: buildUpdatedAtCriteria(reversion),
			Limit:    10000,
		},
	}
	resp, queryErr := c.AggregateSchemaUpdates(ctx, req)
	if queryErr != nil {
		r.l.Warn().Err(queryErr).Msg("failed to query schema updates")
		return nil
	}
	return resp.Names
}

func (r *SchemaRegistry) syncAllResourcesOfKind(ctx context.Context, client schemav1.SchemaManagementServiceClient,
	kind schema.Kind,
) {
	cachedEntries := r.cache.GetEntriesByKind(kind)
	propsWithDeleteTime, listErr := r.querySchemasFromClient(ctx, client, buildSchemaQuery(kind, "", nil))
	if listErr != nil {
		r.l.Warn().Err(listErr).Stringer("kind", kind).Msg("failed to list resources during sync")
		return
	}
	var maxRevision int64
	currentPropIDs := make(map[string]*schemaWithDeleteTime, len(propsWithDeleteTime))
	for _, propWithDT := range propsWithDeleteTime {
		if s, exist := currentPropIDs[propWithDT.property.Id]; exist {
			if propWithDT.property.UpdatedAt.AsTime().After(s.property.UpdatedAt.AsTime()) {
				currentPropIDs[propWithDT.property.Id] = propWithDT
			}
			continue
		}
		currentPropIDs[propWithDT.property.Id] = propWithDT
	}
	for _, s := range currentPropIDs {
		// Track max revision for all resources, including deleted ones
		revision := s.property.UpdatedAt.AsTime().UnixNano()
		if revision > maxRevision {
			maxRevision = revision
		}
		if s.deleteTime > 0 {
			continue
		}
		md, convErr := ToSchema(kind, s.property)
		if convErr != nil {
			r.l.Warn().Err(convErr).Stringer("kind", kind).Msg("failed to convert property to schema")
			continue
		}
		spec, ok := md.Spec.(proto.Message)
		if !ok {
			r.l.Warn().Stringer("kind", kind).Msg("spec does not implement proto.Message")
			continue
		}
		maxRevision = r.processInitialResourceFromProperty(kind, s.property, spec, maxRevision)
	}
	for cachedPropID, cachedEntry := range cachedEntries {
		if p, exists := currentPropIDs[cachedPropID]; !exists || p.deleteTime > 0 {
			r.handleDeletion(kind, cachedPropID, cachedEntry, p.property.UpdatedAt.AsTime().UnixNano())
		}
	}
}

func (r *SchemaRegistry) handleDeletion(kind schema.Kind, propID string, entry *cacheEntry, maxRevision int64) {
	if !r.cache.Delete(propID, maxRevision) {
		return
	}
	md := schema.Metadata{
		TypeMeta: schema.TypeMeta{
			Kind:        kind,
			Name:        entry.name,
			Group:       entry.group,
			ModRevision: maxRevision,
		},
		Spec: nil,
	}
	r.l.Info().Stringer("kind", kind).Str("group", entry.group).Str("name", entry.name).Msg("detected resource deletion during sync")
	r.notifyHandlers(kind, md, true)
}

func buildUpdatedAtCriteria(sinceRevision int64) *modelv1.Criteria {
	if sinceRevision <= 0 {
		return nil
	}
	return &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: &modelv1.Condition{
				Name: TagKeyUpdatedAt,
				Op:   modelv1.Condition_BINARY_OP_GT,
				Value: &modelv1.TagValue{
					Value: &modelv1.TagValue_Int{
						Int: &modelv1.Int{Value: sinceRevision},
					},
				},
			},
		},
	}
}

func (r *SchemaRegistry) listSchemasFromClient(ctx context.Context, client schemav1.SchemaManagementServiceClient,
	kind schema.Kind, group string,
) ([]*propertyv1.Property, error) {
	stream, streamErr := client.ListSchemas(ctx, &schemav1.ListSchemasRequest{Query: buildSchemaQuery(kind, group, nil)})
	if streamErr != nil {
		return nil, streamErr
	}
	var props []*propertyv1.Property
	for {
		resp, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return nil, recvErr
		}
		props = append(props, resp.Properties...)
	}
	return props, nil
}

func (r *SchemaRegistry) listGroupFromClient(ctx context.Context, client schemav1.SchemaManagementServiceClient) ([]*commonv1.Group, error) {
	props, listErr := r.listSchemasFromClient(ctx, client, schema.KindGroup, "")
	if listErr != nil {
		return nil, listErr
	}
	groups := make([]*commonv1.Group, 0, len(props))
	for _, prop := range props {
		md, convErr := ToSchema(schema.KindGroup, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert property to group")
			continue
		}
		groups = append(groups, md.Spec.(*commonv1.Group))
	}
	return groups, nil
}

// SetLocalSchemaClients sets the local schema management and update clients.
func (r *SchemaRegistry) SetLocalSchemaClients(mgrClient schemav1.SchemaManagementServiceClient, updateClient schemav1.SchemaUpdateServiceClient) {
	r.localMgrClient = mgrClient
	r.localUpdateClient = updateClient
}
