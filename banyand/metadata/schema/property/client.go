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
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/multierr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	"github.com/apache/skywalking-banyandb/api/validate"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/run"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	pkgtls "github.com/apache/skywalking-banyandb/pkg/tls"
)

const (
	// DefaultGRPCTimeout is the default timeout for gRPC calls to schema servers.
	DefaultGRPCTimeout = 5 * time.Second
	// DefaultSyncInterval is the default polling interval for property-based schema sync.
	DefaultSyncInterval = 30 * time.Second
	// DefaultInitWaitTime is the default maximum time to wait for at least one schema server to become active during initialization.
	DefaultInitWaitTime = time.Minute
)

var _ schema.Registry = (*SchemaRegistry)(nil)

var errNoActiveServers = fmt.Errorf("no active schema servers available")

// ClientConfig holds configuration for the property-based schema registry client.
type ClientConfig struct {
	OMR                observability.MetricsRegistry
	NodeRegistry       schema.Node
	CurNode            *databasev1.Node
	CACertPath         string
	GRPCTimeout        time.Duration
	SyncInterval       time.Duration
	InitWaitTime       time.Duration
	FullReconcileEvery uint64
	MaxRecvMsgSize     int
	TLSEnabled         bool
}

// SchemaRegistry implements schema.Registry using property-based schema servers.
type SchemaRegistry struct {
	connMgr            *grpchelper.ConnManager[*schemaClient]
	closer             *run.Closer
	l                  *logger.Logger
	cache              *schemaCache
	caCertReloader     *pkgtls.Reloader
	handlers           map[schema.Kind][]schema.EventHandler
	timeout            time.Duration
	syncInterval       time.Duration
	fullReconcileEvery uint64
	syncRound          uint64
	mux                sync.RWMutex
}

// NewSchemaRegistryClient creates a new property-based schema registry client.
func NewSchemaRegistryClient(cfg *ClientConfig) (*SchemaRegistry, error) {
	l := logger.GetLogger("property-schema-registry")
	var caCertReloader *pkgtls.Reloader
	if cfg.TLSEnabled && cfg.CACertPath != "" {
		var reloaderErr error
		caCertReloader, reloaderErr = pkgtls.NewClientCertReloader(cfg.CACertPath, l)
		if reloaderErr != nil {
			return nil, fmt.Errorf("failed to initialize CA certificate reloader: %w", reloaderErr)
		}
	}
	handler := &connectionHandler{
		l:              l,
		caCertReloader: caCertReloader,
		tlsEnabled:     cfg.TLSEnabled,
	}
	connMgr := grpchelper.NewConnManager[*schemaClient](grpchelper.ConnManagerConfig[*schemaClient]{
		Handler:        handler,
		Logger:         l,
		MaxRecvMsgSize: cfg.MaxRecvMsgSize,
	})
	timeout := cfg.GRPCTimeout
	if timeout == 0 {
		timeout = DefaultGRPCTimeout
	}
	syncInterval := cfg.SyncInterval
	if syncInterval == 0 {
		syncInterval = DefaultSyncInterval
	}
	initWaitTime := cfg.InitWaitTime
	if initWaitTime == 0 {
		initWaitTime = DefaultInitWaitTime
	}
	fullReconcileEvery := cfg.FullReconcileEvery
	if fullReconcileEvery == 0 {
		fullReconcileEvery = 5
	}
	reg := &SchemaRegistry{
		connMgr:            connMgr,
		closer:             run.NewCloser(1),
		l:                  l,
		cache:              newSchemaCache(),
		caCertReloader:     caCertReloader,
		handlers:           make(map[schema.Kind][]schema.EventHandler),
		timeout:            timeout,
		syncInterval:       syncInterval,
		fullReconcileEvery: fullReconcileEvery,
	}

	if cfg.CurNode != nil && isPropertySchemaNode(cfg.CurNode) {
		connMgr.OnAddOrUpdate(cfg.CurNode)
	} else if cfg.NodeRegistry != nil {
		var nodesAdded bool
		nodes, listErr := cfg.NodeRegistry.ListNode(context.Background(), databasev1.Role_ROLE_META)
		if listErr != nil {
			return nil, fmt.Errorf("failed to list meta nodes: %w", listErr)
		}
		for _, node := range nodes {
			if isPropertySchemaNode(node) {
				connMgr.OnAddOrUpdate(node)
				nodesAdded = true
			}
		}
		if !nodesAdded {
			_ = reg.Close()
			return nil, fmt.Errorf("no property schema nodes found among %d meta nodes", len(nodes))
		}
	}

	// Wait for at least one schema server to become active.
	// OnAddOrUpdate health-checks synchronously; if it fails, a background
	// retry goroutine retries backoff. Poll until active or timeout.
	if connMgr.ActiveCount() == 0 {
		waitDeadline := time.Now().Add(initWaitTime)
		for connMgr.ActiveCount() == 0 && time.Now().Before(waitDeadline) {
			time.Sleep(500 * time.Millisecond)
		}
		if connMgr.ActiveCount() == 0 {
			_ = reg.Close()
			return nil, fmt.Errorf("no schema servers reachable after %s", initWaitTime)
		}
	}
	return reg, nil
}

// OnAddOrUpdate handles node add/update events for dynamic node discovery.
func (r *SchemaRegistry) OnAddOrUpdate(m schema.Metadata) {
	node, ok := m.Spec.(*databasev1.Node)
	if !ok || !isPropertySchemaNode(node) {
		return
	}
	r.connMgr.OnAddOrUpdate(node)
}

// OnDelete handles node delete events.
func (r *SchemaRegistry) OnDelete(m schema.Metadata) {
	node, ok := m.Spec.(*databasev1.Node)
	if !ok {
		return
	}
	r.connMgr.OnDelete(node)
}

// OnInit handles initial schema load for KindNode events.
func (r *SchemaRegistry) OnInit(_ []schema.Kind) (bool, []int64) {
	return false, nil
}

func (r *SchemaRegistry) broadcastAll(fn func(nodeName string, c *schemaClient) error) error {
	names := r.connMgr.ActiveNames()
	if len(names) == 0 {
		return errNoActiveServers
	}
	var broadcastErr error
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, nodeName := range names {
		currentNode := nodeName
		wg.Add(1)
		go func() {
			defer wg.Done()
			execErr := r.connMgr.Execute(currentNode, func(c *schemaClient) error {
				return fn(currentNode, c)
			})
			if execErr != nil {
				mu.Lock()
				broadcastErr = multierr.Append(broadcastErr, fmt.Errorf("node %s: %w", currentNode, execErr))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return broadcastErr
}

func (r *SchemaRegistry) broadcastInsert(ctx context.Context, prop *propertyv1.Property) error {
	names := r.connMgr.ActiveNames()
	if len(names) == 0 {
		return errNoActiveServers
	}
	var mu sync.Mutex
	var realErrors error
	var alreadyExistsCount atomic.Int32
	var wg sync.WaitGroup
	for _, nodeName := range names {
		currentNode := nodeName
		wg.Add(1)
		go func() {
			defer wg.Done()
			execErr := r.connMgr.Execute(currentNode, func(c *schemaClient) error {
				_, rpcErr := c.management.InsertSchema(ctx, &schemav1.InsertSchemaRequest{Property: prop})
				return rpcErr
			})
			if execErr != nil {
				st, ok := status.FromError(execErr)
				if ok && st.Code() == codes.AlreadyExists {
					alreadyExistsCount.Add(1)
					return
				}
				mu.Lock()
				realErrors = multierr.Append(realErrors, fmt.Errorf("node %s: %w", currentNode, execErr))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if realErrors != nil {
		return realErrors
	}
	if int(alreadyExistsCount.Load()) == len(names) {
		return schema.ErrGRPCAlreadyExists
	}
	return nil
}

func (r *SchemaRegistry) forEachActiveNode(fn func(nodeName string, c *schemaClient) error, errMsg string) {
	names := r.connMgr.ActiveNames()
	for _, nodeName := range names {
		currentNode := nodeName
		execErr := r.connMgr.Execute(currentNode, func(c *schemaClient) error {
			return fn(currentNode, c)
		})
		if execErr != nil {
			r.l.Warn().Err(execErr).Str("node", currentNode).Msg(errMsg)
		}
	}
}

type schemaWithDeleteTime struct {
	property   *propertyv1.Property
	deleteTime int64
}

type syncCollector struct {
	entries        map[schema.Kind]map[string]*schemaWithDeleteTime
	queriedServers map[schema.Kind]map[string]bool
	totalServers   int
	mu             sync.Mutex
}

func newSyncCollector(totalServers int) *syncCollector {
	return &syncCollector{
		entries:        make(map[schema.Kind]map[string]*schemaWithDeleteTime),
		queriedServers: make(map[schema.Kind]map[string]bool),
		totalServers:   totalServers,
	}
}

func (c *syncCollector) add(serverName string, kind schema.Kind, schemas []*schemaWithDeleteTime) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries[kind] == nil {
		c.entries[kind] = make(map[string]*schemaWithDeleteTime)
	}
	if c.queriedServers[kind] == nil {
		c.queriedServers[kind] = make(map[string]bool)
	}
	c.queriedServers[kind][serverName] = true
	for _, s := range schemas {
		propID := s.property.GetId()
		existing, exists := c.entries[kind][propID]
		if !exists {
			c.entries[kind][propID] = s
			continue
		}
		existingRev := ParseTags(existing.property.GetTags()).UpdatedAt
		newRev := ParseTags(s.property.GetTags()).UpdatedAt
		if newRev > existingRev {
			c.entries[kind][propID] = s
		}
	}
}

func (c *syncCollector) isFullyQueriedForKind(kind schema.Kind) bool {
	return len(c.queriedServers[kind]) == c.totalServers
}

type propInfo struct {
	nodeRev     map[string]int64
	nodeDelTime map[string]int64
	best        *schemaWithDeleteTime
	bestRev     int64
}

func (r *SchemaRegistry) querySchemasFromClient(ctx context.Context,
	client schemav1.SchemaManagementServiceClient, query *propertyv1.QueryRequest,
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
		for idx, prop := range resp.GetProperties() {
			var deleteTime int64
			if idx < len(resp.GetDeleteTimes()) {
				deleteTime = resp.GetDeleteTimes()[idx]
			}
			results = append(results, &schemaWithDeleteTime{
				property:   prop,
				deleteTime: deleteTime,
			})
		}
	}
	return results, nil
}

func (r *SchemaRegistry) queryAndRepairSchemas(ctx context.Context,
	query *propertyv1.QueryRequest,
) (map[string]*propInfo, error) {
	propMap := make(map[string]*propInfo)
	respondedNodes := make(map[string]bool)
	var mu sync.Mutex
	broadcastErr := r.broadcastAll(func(currentNode string, c *schemaClient) error {
		schemas, queryErr := r.querySchemasFromClient(ctx, c.management, query)
		if queryErr != nil {
			return queryErr
		}
		mu.Lock()
		defer mu.Unlock()
		respondedNodes[currentNode] = true
		for _, s := range schemas {
			propID := s.property.GetId()
			rev := ParseTags(s.property.GetTags()).UpdatedAt
			info, exists := propMap[propID]
			if !exists {
				info = &propInfo{
					nodeRev:     make(map[string]int64),
					nodeDelTime: make(map[string]int64),
				}
				propMap[propID] = info
			}
			if existingRev, prevExists := info.nodeRev[currentNode]; !prevExists || rev > existingRev {
				info.nodeRev[currentNode] = rev
				info.nodeDelTime[currentNode] = s.deleteTime
			}
			if info.best == nil || info.bestRev < rev {
				info.best = s
				info.bestRev = rev
			}
		}
		return nil
	})
	r.repairInconsistentNodes(ctx, respondedNodes, propMap)
	if broadcastErr != nil && len(propMap) == 0 {
		return nil, broadcastErr
	}
	return propMap, nil
}

func (r *SchemaRegistry) repairInconsistentNodes(ctx context.Context,
	respondedNodes map[string]bool, propMap map[string]*propInfo,
) {
	for propID, info := range propMap {
		if info.best == nil {
			continue
		}
		var nodesToRepair []string
		for nodeName := range respondedNodes {
			nodeRev, exists := info.nodeRev[nodeName]
			if !exists || nodeRev < info.bestRev {
				nodesToRepair = append(nodesToRepair, nodeName)
			}
		}
		if len(nodesToRepair) == 0 {
			continue
		}
		r.l.Info().Str("propID", propID).Int64("bestRev", info.bestRev).
			Int64("deleteTime", info.best.deleteTime).
			Strs("nodesToRepair", nodesToRepair).Msg("repairing schema inconsistency")
		bestProp := info.best.property
		bestDelTime := info.best.deleteTime
		for _, nodeName := range nodesToRepair {
			repairErr := r.connMgr.Execute(nodeName, func(c *schemaClient) error {
				_, rpcErr := c.management.RepairSchema(ctx, &schemav1.RepairSchemaRequest{
					Property:   bestProp,
					DeleteTime: bestDelTime,
				})
				return rpcErr
			})
			if repairErr != nil {
				r.l.Warn().Err(repairErr).Str("propID", propID).Str("node", nodeName).Msg("repair failed")
			}
		}
	}
}

func (r *SchemaRegistry) getSchema(ctx context.Context, kind schema.Kind,
	group, name string,
) (*propertyv1.Property, error) {
	query := buildSchemaQuery(kind, group, name)
	propMap, queryErr := r.queryAndRepairSchemas(ctx, query)
	if queryErr != nil {
		return nil, queryErr
	}
	propID := query.Ids[0]
	info := propMap[propID]
	if info == nil || info.best == nil || info.best.deleteTime > 0 {
		return nil, nil
	}
	return info.best.property, nil
}

func (r *SchemaRegistry) listSchemas(ctx context.Context, kind schema.Kind,
	group string,
) ([]*propertyv1.Property, error) {
	query := buildSchemaQuery(kind, group, "")
	propMap, queryErr := r.queryAndRepairSchemas(ctx, query)
	if queryErr != nil {
		return nil, queryErr
	}
	result := make([]*propertyv1.Property, 0, len(propMap))
	for _, info := range propMap {
		if info.best == nil || info.best.deleteTime != 0 {
			continue
		}
		result = append(result, info.best.property)
	}
	return result, nil
}

func (r *SchemaRegistry) broadcastDelete(ctx context.Context, kind schema.Kind, group, name string) (bool, error) {
	req := buildDeleteRequest(kind, group, name)
	var found atomic.Bool
	writeErr := r.broadcastAll(func(_ string, c *schemaClient) error {
		resp, rpcErr := c.management.DeleteSchema(ctx, req)
		if rpcErr != nil {
			return rpcErr
		}
		if resp.GetFound() {
			found.Store(true)
		}
		return nil
	})
	return found.Load(), writeErr
}

func getResource[T proto.Message](ctx context.Context, r *SchemaRegistry,
	kind schema.Kind, group, name string,
) (T, error) {
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
		return zero, fmt.Errorf("unexpected spec type for kind %s", kind)
	}
	return result, nil
}

func listResources[T proto.Message](ctx context.Context, r *SchemaRegistry,
	kind schema.Kind, group string, requireGroup bool,
) ([]T, error) {
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

func (r *SchemaRegistry) validateGroup(ctx context.Context, kind schema.Kind, groupName string) error {
	switch kind {
	case schema.KindStream, schema.KindMeasure, schema.KindTrace:
		g, groupErr := r.GetGroup(ctx, groupName)
		if groupErr != nil {
			return groupErr
		}
		return validate.GroupForNonProperty(g)
	case schema.KindProperty:
		g, groupErr := r.GetGroup(ctx, groupName)
		if groupErr != nil {
			return groupErr
		}
		return validate.Group(g)
	default:
		return nil
	}
}

func createResource[T proto.Message](ctx context.Context, r *SchemaRegistry,
	kind schema.Kind, spec T,
) error {
	metadata, metaErr := getMetadataFromSpec(kind, spec)
	if metaErr != nil {
		return metaErr
	}
	if validateErr := r.validateGroup(ctx, kind, metadata.GetGroup()); validateErr != nil {
		return validateErr
	}
	prop, convErr := SchemaToProperty(kind, spec)
	if convErr != nil {
		return convErr
	}
	return r.broadcastInsert(ctx, prop)
}

func updateResource[T proto.Message](ctx context.Context, r *SchemaRegistry,
	kind schema.Kind, spec T,
) error {
	metadata, metaErr := getMetadataFromSpec(kind, spec)
	if metaErr != nil {
		return metaErr
	}
	if validateErr := r.validateGroup(ctx, kind, metadata.GetGroup()); validateErr != nil {
		return validateErr
	}
	originalProp, getErr := r.getSchema(ctx, kind, metadata.GetGroup(), metadata.GetName())
	if getErr != nil {
		return getErr
	}
	if originalProp == nil {
		return fmt.Errorf("schema %s/%s not exist", metadata.GetGroup(), metadata.GetName())
	}
	prop, convErr := SchemaToProperty(kind, spec)
	if convErr != nil {
		return convErr
	}
	return r.broadcastAll(func(_ string, c *schemaClient) error {
		_, rpcErr := c.management.UpdateSchema(ctx, &schemav1.UpdateSchemaRequest{Property: prop})
		return rpcErr
	})
}

// GetStream retrieves a stream schema.
func (r *SchemaRegistry) GetStream(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Stream, error) {
	return getResource[*databasev1.Stream](ctx, r, schema.KindStream, metadata.GetGroup(), metadata.GetName())
}

// ListStream lists stream schemas in a group.
func (r *SchemaRegistry) ListStream(ctx context.Context, opt schema.ListOpt) ([]*databasev1.Stream, error) {
	return listResources[*databasev1.Stream](ctx, r, schema.KindStream, opt.Group, true)
}

// CreateStream creates a stream schema.
func (r *SchemaRegistry) CreateStream(ctx context.Context, stream *databasev1.Stream) (int64, error) {
	if validateErr := validate.Stream(stream); validateErr != nil {
		return 0, validateErr
	}
	now := time.Now().UnixNano()
	stream.Metadata.ModRevision = now
	stream.UpdatedAt = timestamppb.Now()
	return now, createResource(ctx, r, schema.KindStream, stream)
}

// UpdateStream updates a stream schema.
func (r *SchemaRegistry) UpdateStream(ctx context.Context, stream *databasev1.Stream) (int64, error) {
	if validateErr := validate.Stream(stream); validateErr != nil {
		return 0, validateErr
	}
	now := time.Now().UnixNano()
	stream.Metadata.ModRevision = now
	stream.UpdatedAt = timestamppb.Now()
	return now, updateResource(ctx, r, schema.KindStream, stream)
}

// DeleteStream deletes a stream schema.
func (r *SchemaRegistry) DeleteStream(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.broadcastDelete(ctx, schema.KindStream, metadata.GetGroup(), metadata.GetName())
}

// GetMeasure retrieves a measure schema.
func (r *SchemaRegistry) GetMeasure(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Measure, error) {
	return getResource[*databasev1.Measure](ctx, r, schema.KindMeasure, metadata.GetGroup(), metadata.GetName())
}

// ListMeasure lists measure schemas in a group.
func (r *SchemaRegistry) ListMeasure(ctx context.Context, opt schema.ListOpt) ([]*databasev1.Measure, error) {
	return listResources[*databasev1.Measure](ctx, r, schema.KindMeasure, opt.Group, true)
}

// CreateMeasure creates a measure schema.
func (r *SchemaRegistry) CreateMeasure(ctx context.Context, measure *databasev1.Measure) (int64, error) {
	if measure.GetInterval() != "" {
		if _, parseErr := timestamp.ParseDuration(measure.GetInterval()); parseErr != nil {
			return 0, fmt.Errorf("interval is malformed: %w", parseErr)
		}
	}
	if validateErr := validate.Measure(measure); validateErr != nil {
		return 0, validateErr
	}
	now := time.Now().UnixNano()
	measure.Metadata.ModRevision = now
	measure.UpdatedAt = timestamppb.Now()
	return now, createResource(ctx, r, schema.KindMeasure, measure)
}

// UpdateMeasure updates a measure schema.
func (r *SchemaRegistry) UpdateMeasure(ctx context.Context, measure *databasev1.Measure) (int64, error) {
	if measure.GetInterval() != "" {
		if _, parseErr := timestamp.ParseDuration(measure.GetInterval()); parseErr != nil {
			return 0, fmt.Errorf("interval is malformed: %w", parseErr)
		}
	}
	if validateErr := validate.Measure(measure); validateErr != nil {
		return 0, validateErr
	}
	now := time.Now().UnixNano()
	measure.Metadata.ModRevision = now
	measure.UpdatedAt = timestamppb.Now()
	return now, updateResource(ctx, r, schema.KindMeasure, measure)
}

// DeleteMeasure deletes a measure schema.
func (r *SchemaRegistry) DeleteMeasure(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.broadcastDelete(ctx, schema.KindMeasure, metadata.GetGroup(), metadata.GetName())
}

// TopNAggregations returns TopN aggregations for a measure.
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

// GetTrace retrieves a trace schema.
func (r *SchemaRegistry) GetTrace(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Trace, error) {
	return getResource[*databasev1.Trace](ctx, r, schema.KindTrace, metadata.GetGroup(), metadata.GetName())
}

// ListTrace lists trace schemas in a group.
func (r *SchemaRegistry) ListTrace(ctx context.Context, opt schema.ListOpt) ([]*databasev1.Trace, error) {
	return listResources[*databasev1.Trace](ctx, r, schema.KindTrace, opt.Group, true)
}

// CreateTrace creates a trace schema.
func (r *SchemaRegistry) CreateTrace(ctx context.Context, trace *databasev1.Trace) (int64, error) {
	if validateErr := validate.Trace(trace); validateErr != nil {
		return 0, validateErr
	}
	now := time.Now().UnixNano()
	trace.Metadata.ModRevision = now
	trace.UpdatedAt = timestamppb.Now()
	return now, createResource(ctx, r, schema.KindTrace, trace)
}

// UpdateTrace updates a trace schema.
func (r *SchemaRegistry) UpdateTrace(ctx context.Context, trace *databasev1.Trace) (int64, error) {
	if validateErr := validate.Trace(trace); validateErr != nil {
		return 0, validateErr
	}
	now := time.Now().UnixNano()
	trace.Metadata.ModRevision = now
	trace.UpdatedAt = timestamppb.Now()
	return now, updateResource(ctx, r, schema.KindTrace, trace)
}

// DeleteTrace deletes a trace schema.
func (r *SchemaRegistry) DeleteTrace(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.broadcastDelete(ctx, schema.KindTrace, metadata.GetGroup(), metadata.GetName())
}

// GetGroup retrieves a group schema.
func (r *SchemaRegistry) GetGroup(ctx context.Context, group string) (*commonv1.Group, error) {
	return getResource[*commonv1.Group](ctx, r, schema.KindGroup, "", group)
}

// ListGroup lists all groups.
func (r *SchemaRegistry) ListGroup(ctx context.Context) ([]*commonv1.Group, error) {
	return listResources[*commonv1.Group](ctx, r, schema.KindGroup, "", false)
}

// CreateGroup creates a group schema.
func (r *SchemaRegistry) CreateGroup(ctx context.Context, group *commonv1.Group) error {
	if validateErr := validate.Group(group); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	group.Metadata.ModRevision = now
	group.UpdatedAt = timestamppb.Now()
	return createResource(ctx, r, schema.KindGroup, group)
}

// UpdateGroup updates a group schema.
func (r *SchemaRegistry) UpdateGroup(ctx context.Context, group *commonv1.Group) error {
	if validateErr := validate.Group(group); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	group.Metadata.ModRevision = now
	group.UpdatedAt = timestamppb.Now()
	return updateResource(ctx, r, schema.KindGroup, group)
}

// DeleteGroup deletes a group and all its resources.
func (r *SchemaRegistry) DeleteGroup(ctx context.Context, group string) (bool, error) {
	_, groupErr := r.GetGroup(ctx, group)
	if groupErr != nil {
		return false, fmt.Errorf("%s: %w", group, groupErr)
	}
	var deleteErr error
	for _, kind := range schema.AllKinds() {
		if kind == schema.KindGroup || kind == schema.KindNode {
			continue
		}
		props, listErr := r.listSchemas(ctx, kind, group)
		if listErr != nil {
			deleteErr = multierr.Append(deleteErr, listErr)
			continue
		}
		for _, prop := range props {
			parsed := ParseTags(prop.GetTags())
			if parsed.Name == "" {
				continue
			}
			_, broadcastErr := r.broadcastDelete(ctx, kind, group, parsed.Name)
			if broadcastErr != nil {
				deleteErr = multierr.Append(deleteErr, broadcastErr)
			}
		}
	}
	_, broadcastErr := r.broadcastDelete(ctx, schema.KindGroup, "", group)
	if broadcastErr != nil {
		deleteErr = multierr.Append(deleteErr, broadcastErr)
	}
	return deleteErr == nil, deleteErr
}

// GetIndexRule retrieves an index rule schema.
func (r *SchemaRegistry) GetIndexRule(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRule, error) {
	return getResource[*databasev1.IndexRule](ctx, r, schema.KindIndexRule, metadata.GetGroup(), metadata.GetName())
}

// ListIndexRule lists index rule schemas in a group.
func (r *SchemaRegistry) ListIndexRule(ctx context.Context, opt schema.ListOpt) ([]*databasev1.IndexRule, error) {
	return listResources[*databasev1.IndexRule](ctx, r, schema.KindIndexRule, opt.Group, true)
}

// CreateIndexRule creates an index rule schema.
func (r *SchemaRegistry) CreateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error {
	if indexRule.Metadata.Id == 0 {
		indexRule.Metadata.Id = generateCRC32ID(indexRule.Metadata.Group, indexRule.Metadata.Name)
	}
	if validateErr := validate.IndexRule(indexRule); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	indexRule.Metadata.ModRevision = now
	indexRule.UpdatedAt = timestamppb.Now()
	return createResource(ctx, r, schema.KindIndexRule, indexRule)
}

// UpdateIndexRule updates an index rule schema.
func (r *SchemaRegistry) UpdateIndexRule(ctx context.Context, indexRule *databasev1.IndexRule) error {
	if indexRule.Metadata.Id == 0 {
		existing, getErr := r.GetIndexRule(ctx, indexRule.Metadata)
		if getErr != nil {
			return getErr
		}
		indexRule.Metadata.Id = existing.Metadata.Id
	}
	if validateErr := validate.IndexRule(indexRule); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	indexRule.Metadata.ModRevision = now
	indexRule.UpdatedAt = timestamppb.Now()
	return updateResource(ctx, r, schema.KindIndexRule, indexRule)
}

// DeleteIndexRule deletes an index rule schema.
func (r *SchemaRegistry) DeleteIndexRule(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.broadcastDelete(ctx, schema.KindIndexRule, metadata.GetGroup(), metadata.GetName())
}

// GetIndexRuleBinding retrieves an index rule binding schema.
func (r *SchemaRegistry) GetIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.IndexRuleBinding, error) {
	return getResource[*databasev1.IndexRuleBinding](ctx, r, schema.KindIndexRuleBinding, metadata.GetGroup(), metadata.GetName())
}

// ListIndexRuleBinding lists index rule binding schemas in a group.
func (r *SchemaRegistry) ListIndexRuleBinding(ctx context.Context, opt schema.ListOpt) ([]*databasev1.IndexRuleBinding, error) {
	return listResources[*databasev1.IndexRuleBinding](ctx, r, schema.KindIndexRuleBinding, opt.Group, true)
}

// CreateIndexRuleBinding creates an index rule binding schema.
func (r *SchemaRegistry) CreateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error {
	if validateErr := validate.IndexRuleBinding(indexRuleBinding); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	indexRuleBinding.Metadata.ModRevision = now
	indexRuleBinding.UpdatedAt = timestamppb.Now()
	return createResource(ctx, r, schema.KindIndexRuleBinding, indexRuleBinding)
}

// UpdateIndexRuleBinding updates an index rule binding schema.
func (r *SchemaRegistry) UpdateIndexRuleBinding(ctx context.Context, indexRuleBinding *databasev1.IndexRuleBinding) error {
	if validateErr := validate.IndexRuleBinding(indexRuleBinding); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	indexRuleBinding.Metadata.ModRevision = now
	indexRuleBinding.UpdatedAt = timestamppb.Now()
	return updateResource(ctx, r, schema.KindIndexRuleBinding, indexRuleBinding)
}

// DeleteIndexRuleBinding deletes an index rule binding schema.
func (r *SchemaRegistry) DeleteIndexRuleBinding(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.broadcastDelete(ctx, schema.KindIndexRuleBinding, metadata.GetGroup(), metadata.GetName())
}

// GetTopNAggregation retrieves a TopN aggregation schema.
func (r *SchemaRegistry) GetTopNAggregation(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.TopNAggregation, error) {
	return getResource[*databasev1.TopNAggregation](ctx, r, schema.KindTopNAggregation, metadata.GetGroup(), metadata.GetName())
}

// ListTopNAggregation lists TopN aggregation schemas in a group.
func (r *SchemaRegistry) ListTopNAggregation(ctx context.Context, opt schema.ListOpt) ([]*databasev1.TopNAggregation, error) {
	return listResources[*databasev1.TopNAggregation](ctx, r, schema.KindTopNAggregation, opt.Group, true)
}

// CreateTopNAggregation creates a TopN aggregation schema.
func (r *SchemaRegistry) CreateTopNAggregation(ctx context.Context, topN *databasev1.TopNAggregation) error {
	if validateErr := validate.TopNAggregation(topN); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	topN.Metadata.ModRevision = now
	topN.UpdatedAt = timestamppb.Now()
	return createResource(ctx, r, schema.KindTopNAggregation, topN)
}

// UpdateTopNAggregation updates a TopN aggregation schema.
func (r *SchemaRegistry) UpdateTopNAggregation(ctx context.Context, topN *databasev1.TopNAggregation) error {
	if validateErr := validate.TopNAggregation(topN); validateErr != nil {
		return validateErr
	}
	now := time.Now().UnixNano()
	topN.Metadata.ModRevision = now
	topN.UpdatedAt = timestamppb.Now()
	return updateResource(ctx, r, schema.KindTopNAggregation, topN)
}

// DeleteTopNAggregation deletes a TopN aggregation schema.
func (r *SchemaRegistry) DeleteTopNAggregation(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.broadcastDelete(ctx, schema.KindTopNAggregation, metadata.GetGroup(), metadata.GetName())
}

// GetProperty retrieves a property schema.
func (r *SchemaRegistry) GetProperty(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Property, error) {
	return getResource[*databasev1.Property](ctx, r, schema.KindProperty, metadata.GetGroup(), metadata.GetName())
}

// ListProperty lists property schemas in a group.
func (r *SchemaRegistry) ListProperty(ctx context.Context, opt schema.ListOpt) ([]*databasev1.Property, error) {
	return listResources[*databasev1.Property](ctx, r, schema.KindProperty, opt.Group, true)
}

// CreateProperty creates a property schema.
func (r *SchemaRegistry) CreateProperty(ctx context.Context, property *databasev1.Property) error {
	if property.UpdatedAt == nil {
		property.UpdatedAt = timestamppb.Now()
	}
	now := time.Now().UnixNano()
	property.Metadata.ModRevision = now
	return createResource(ctx, r, schema.KindProperty, property)
}

// UpdateProperty updates a property schema.
func (r *SchemaRegistry) UpdateProperty(ctx context.Context, property *databasev1.Property) error {
	if property.UpdatedAt == nil {
		property.UpdatedAt = timestamppb.Now()
	}
	now := time.Now().UnixNano()
	property.Metadata.ModRevision = now
	return updateResource(ctx, r, schema.KindProperty, property)
}

// DeleteProperty deletes a property schema.
func (r *SchemaRegistry) DeleteProperty(ctx context.Context, metadata *commonv1.Metadata) (bool, error) {
	return r.broadcastDelete(ctx, schema.KindProperty, metadata.GetGroup(), metadata.GetName())
}

// RegisterHandler registers an event handler for the given kinds.
func (r *SchemaRegistry) RegisterHandler(name string, kind schema.Kind, handler schema.EventHandler) {
	if kind&schema.KindMask != kind {
		panic(fmt.Sprintf("invalid kind %d", kind))
	}
	r.mux.Lock()
	defer r.mux.Unlock()
	var kinds []schema.Kind
	for idx := 0; idx < schema.KindSize; idx++ {
		ki := schema.Kind(1 << idx)
		if kind&ki > 0 {
			kinds = append(kinds, ki)
		}
	}
	r.l.Info().Str("name", name).Interface("kinds", kinds).Msg("registering property schema handler")
	handler.OnInit(kinds)
	for _, ki := range kinds {
		r.handlers[ki] = append(r.handlers[ki], handler)
	}
}

// Start starts a single goroutine that periodically syncs schemas.
func (r *SchemaRegistry) Start(ctx context.Context) error {
	if r.caCertReloader != nil {
		if startErr := r.caCertReloader.Start(); startErr != nil {
			r.l.Error().Err(startErr).Msg("failed to start CA certificate reloader")
		} else {
			certUpdateCh := r.caCertReloader.GetUpdateChannel()
			if r.closer.AddRunning() {
				go func() {
					defer r.closer.Done()
					for {
						select {
						case <-certUpdateCh:
							r.l.Info().Msg("CA certificate updated, reconnecting clients")
							r.connMgr.ReconnectAll()
						case <-ctx.Done():
							return
						case <-r.closer.CloseNotify():
							return
						}
					}
				}()
			}
		}
	}
	r.forEachActiveNode(func(_ string, sc *schemaClient) error {
		return r.initializeFromSchemaClient(ctx, sc)
	}, "failed to initialize from schema client at startup")
	go r.syncLoop(ctx)
	return nil
}

func (r *SchemaRegistry) syncLoop(ctx context.Context) {
	if !r.closer.AddRunning() {
		return
	}
	defer r.closer.Done()
	ticker := time.NewTicker(r.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.closer.CloseNotify():
			return
		case <-ticker.C:
			r.performSync(ctx)
		}
	}
}

func (r *SchemaRegistry) performSync(ctx context.Context) {
	round := atomic.AddUint64(&r.syncRound, 1)
	isFullReconcile := r.shouldFullReconcile(round)
	names := r.connMgr.ActiveNames()
	if len(names) == 0 {
		return
	}
	collector := newSyncCollector(len(names))
	if isFullReconcile {
		r.collectFullSync(ctx, collector)
	} else {
		r.collectIncrementalSync(ctx, collector)
	}
	r.reconcileFromCollector(collector)
}

func (r *SchemaRegistry) collectFullSync(ctx context.Context, collector *syncCollector) {
	kindsToSync := r.getKindsToSync()
	if len(kindsToSync) == 0 {
		return
	}
	broadcastErr := r.broadcastAll(func(nodeName string, sc *schemaClient) error {
		for _, kind := range kindsToSync {
			query := buildSchemaQuery(kind, "", "")
			schemas, queryErr := r.querySchemasFromClient(ctx, sc.management, query)
			if queryErr != nil {
				r.l.Warn().Err(queryErr).Stringer("kind", kind).Str("node", nodeName).
					Msg("failed to query schemas for full sync")
				continue
			}
			collector.add(nodeName, kind, schemas)
		}
		return nil
	})
	if broadcastErr != nil {
		r.l.Error().Err(broadcastErr).Msg("failed to collect full sync from some nodes")
	}
}

func (r *SchemaRegistry) collectIncrementalSync(ctx context.Context, collector *syncCollector) {
	broadcastErr := r.broadcastAll(func(nodeName string, sc *schemaClient) error {
		updatedKindNames, queryErr := r.queryUpdatedSchemas(ctx, sc.update, r.cache.GetMaxRevision())
		if queryErr != nil {
			return queryErr
		}
		for _, kindName := range updatedKindNames {
			kind, kindErr := KindFromString(kindName)
			if kindErr != nil {
				r.l.Warn().Str("kindName", kindName).Msg("unknown kind from aggregate update")
				continue
			}
			query := buildSchemaQuery(kind, "", "")
			schemas, schemasErr := r.querySchemasFromClient(ctx, sc.management, query)
			if schemasErr != nil {
				r.l.Warn().Err(schemasErr).Stringer("kind", kind).Str("node", nodeName).
					Msg("failed to query schemas for incremental sync")
				continue
			}
			collector.add(nodeName, kind, schemas)
		}
		return nil
	})
	if broadcastErr != nil {
		r.l.Error().Err(broadcastErr).Msg("failed to collect incremental sync from some nodes")
	}
}

func (r *SchemaRegistry) reconcileFromCollector(collector *syncCollector) {
	for kind, propMap := range collector.entries {
		cachedEntries := r.cache.GetEntriesByKind(kind)
		seen := make(map[string]bool, len(propMap))
		for propID, s := range propMap {
			seen[propID] = true
			if s.deleteTime > 0 {
				if entry, exists := cachedEntries[propID]; exists {
					r.handleDeletion(kind, propID, entry, s.deleteTime)
				}
				continue
			}
			md, convErr := ToSchema(kind, s.property)
			if convErr != nil {
				r.l.Warn().Err(convErr).Str("propID", propID).Msg("failed to convert property for reconcile")
				continue
			}
			r.processInitialResourceFromProperty(kind, s.property, md.Spec.(proto.Message))
		}
		if collector.isFullyQueriedForKind(kind) {
			for propID, entry := range cachedEntries {
				if !seen[propID] {
					r.handleDeletion(kind, propID, entry, time.Now().UnixNano())
				}
			}
		}
	}
}

func (r *SchemaRegistry) getKindsToSync() []schema.Kind {
	r.mux.RLock()
	handlerKindSet := make(map[schema.Kind]struct{}, len(r.handlers))
	for kind := range r.handlers {
		handlerKindSet[kind] = struct{}{}
	}
	r.mux.RUnlock()
	for _, kind := range r.cache.GetCachedKinds() {
		handlerKindSet[kind] = struct{}{}
	}
	kinds := make([]schema.Kind, 0, len(handlerKindSet))
	for kind := range handlerKindSet {
		kinds = append(kinds, kind)
	}
	return kinds
}

func (r *SchemaRegistry) queryUpdatedSchemas(ctx context.Context, client schemav1.SchemaUpdateServiceClient, sinceRevision int64) ([]string, error) {
	query := buildUpdatedSchemasQuery(sinceRevision)
	resp, rpcErr := client.AggregateSchemaUpdates(ctx, &schemav1.AggregateSchemaUpdatesRequest{Query: query})
	if rpcErr != nil {
		return nil, rpcErr
	}
	return resp.GetNames(), nil
}

func (r *SchemaRegistry) shouldFullReconcile(round uint64) bool {
	return r.fullReconcileEvery > 0 && round%r.fullReconcileEvery == 0
}

type kindGroupPair struct {
	groupName string
	kind      schema.Kind
}

func (r *SchemaRegistry) initializeFromSchemaClient(ctx context.Context, sc *schemaClient) error {
	groups, listErr := r.listGroupFromClient(ctx, sc.management)
	if listErr != nil {
		return listErr
	}
	var pairs []kindGroupPair
	for _, group := range groups {
		r.processInitialResource(schema.KindGroup, group)
		catalog := group.GetCatalog()
		groupName := group.GetMetadata().GetName()
		switch catalog {
		case commonv1.Catalog_CATALOG_STREAM:
			pairs = append(pairs,
				kindGroupPair{groupName: groupName, kind: schema.KindStream},
				kindGroupPair{groupName: groupName, kind: schema.KindIndexRule},
				kindGroupPair{groupName: groupName, kind: schema.KindIndexRuleBinding},
			)
		case commonv1.Catalog_CATALOG_MEASURE:
			pairs = append(pairs,
				kindGroupPair{groupName: groupName, kind: schema.KindMeasure},
				kindGroupPair{groupName: groupName, kind: schema.KindTopNAggregation},
				kindGroupPair{groupName: groupName, kind: schema.KindIndexRule},
				kindGroupPair{groupName: groupName, kind: schema.KindIndexRuleBinding},
			)
		case commonv1.Catalog_CATALOG_TRACE:
			pairs = append(pairs,
				kindGroupPair{groupName: groupName, kind: schema.KindTrace},
				kindGroupPair{groupName: groupName, kind: schema.KindIndexRule},
				kindGroupPair{groupName: groupName, kind: schema.KindIndexRuleBinding},
			)
		case commonv1.Catalog_CATALOG_PROPERTY:
			pairs = append(pairs, kindGroupPair{groupName: groupName, kind: schema.KindProperty})
		default:
			r.l.Error().Stringer("catalog", catalog).Str("group", groupName).Msg("unknown catalog type during initialization")
		}
	}
	var wg sync.WaitGroup
	for _, pair := range pairs {
		currentPair := pair
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.initResourcesFromClient(ctx, sc.management, currentPair.kind, currentPair.groupName)
		}()
	}
	wg.Wait()
	return nil
}

func (r *SchemaRegistry) listGroupFromClient(ctx context.Context, client schemav1.SchemaManagementServiceClient) ([]*commonv1.Group, error) {
	query := buildSchemaQuery(schema.KindGroup, "", "")
	results, queryErr := r.querySchemasFromClient(ctx, client, query)
	if queryErr != nil {
		return nil, queryErr
	}
	var groups []*commonv1.Group
	for _, s := range results {
		if s.deleteTime > 0 {
			continue
		}
		md, convErr := ToSchema(schema.KindGroup, s.property)
		if convErr != nil {
			r.l.Warn().Err(convErr).Msg("failed to convert group property")
			continue
		}
		g, ok := md.Spec.(*commonv1.Group)
		if !ok {
			continue
		}
		groups = append(groups, g)
	}
	return groups, nil
}

func (r *SchemaRegistry) initResourcesFromClient(ctx context.Context,
	client schemav1.SchemaManagementServiceClient, kind schema.Kind, groupName string,
) {
	query := buildSchemaQuery(kind, groupName, "")
	results, queryErr := r.querySchemasFromClient(ctx, client, query)
	if queryErr != nil {
		r.l.Warn().Err(queryErr).Stringer("kind", kind).Str("group", groupName).Msg("failed to list resources for init")
		return
	}
	for _, s := range results {
		if s.deleteTime > 0 {
			continue
		}
		md, convErr := ToSchema(kind, s.property)
		if convErr != nil {
			r.l.Warn().Err(convErr).Stringer("kind", kind).Msg("failed to convert property for init")
			continue
		}
		r.processInitialResourceFromProperty(kind, s.property, md.Spec.(proto.Message))
	}
}

func (r *SchemaRegistry) processInitialResource(kind schema.Kind, spec proto.Message) {
	prop, convErr := SchemaToProperty(kind, spec)
	if convErr != nil {
		r.l.Warn().Err(convErr).Stringer("kind", kind).Msg("failed to convert spec to property for initial resource")
		return
	}
	r.processInitialResourceFromProperty(kind, prop, spec)
}

func (r *SchemaRegistry) processInitialResourceFromProperty(kind schema.Kind, prop *propertyv1.Property, spec proto.Message) {
	propID := prop.GetId()
	parsed := ParseTags(prop.GetTags())
	entry := &cacheEntry{
		spec:           spec,
		group:          parsed.Group,
		name:           parsed.Name,
		latestUpdateAt: parsed.UpdatedAt,
		kind:           kind,
	}
	if r.cache.Update(propID, entry) {
		r.notifyHandlers(kind, schema.Metadata{
			TypeMeta: schema.TypeMeta{
				Kind:  kind,
				Name:  parsed.Name,
				Group: parsed.Group,
			},
			Spec: spec,
		}, false)
	}
}

func (r *SchemaRegistry) handleDeletion(kind schema.Kind, propID string, entry *cacheEntry, revision int64) {
	if r.cache.Delete(propID, revision) {
		r.notifyHandlers(kind, schema.Metadata{
			TypeMeta: schema.TypeMeta{
				Kind:  kind,
				Name:  entry.name,
				Group: entry.group,
			},
			Spec: entry.spec,
		}, true)
	}
}

func (r *SchemaRegistry) notifyHandlers(kind schema.Kind, md schema.Metadata, isDelete bool) {
	r.mux.RLock()
	handlers := r.handlers[kind]
	r.mux.RUnlock()
	for _, handler := range handlers {
		if isDelete {
			handler.OnDelete(md)
		} else {
			handler.OnAddOrUpdate(md)
		}
	}
}

// ActiveNodeNames returns the names of all active schema server nodes.
func (r *SchemaRegistry) ActiveNodeNames() []string {
	return r.connMgr.ActiveNames()
}

// Close stops all watchers and gracefully stops ConnManager.
func (r *SchemaRegistry) Close() error {
	if r.caCertReloader != nil {
		r.caCertReloader.Stop()
	}
	r.closer.Done()
	r.closer.CloseThenWait()
	r.connMgr.GracefulStop()
	return nil
}

func isPropertySchemaNode(node *databasev1.Node) bool {
	if node.GetPropertySchemaGrpcAddress() == "" {
		return false
	}
	for _, role := range node.GetRoles() {
		if role == databasev1.Role_ROLE_META {
			return true
		}
	}
	return false
}
