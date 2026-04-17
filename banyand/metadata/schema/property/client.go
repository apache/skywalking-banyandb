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
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
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
	// DefaultHealthCheckInterval is the default interval for periodic connection health checks.
	DefaultHealthCheckInterval = 10 * time.Second
	// defaultSyncTimeout is the default timeout for waiting for a sync response from a single node.
	defaultSyncTimeout = 30 * time.Second
)

var _ schema.Registry = (*SchemaRegistry)(nil)

var errNoActiveServers = fmt.Errorf("no active schema servers available")

// ClientConfig holds configuration for the property-based schema registry client.
type ClientConfig struct {
	OMR                 observability.MetricsRegistry
	NodeRegistry        schema.Node
	CurNode             *databasev1.Node
	CACertPath          string
	GRPCTimeout         time.Duration
	SyncInterval        time.Duration
	SyncTimeout         time.Duration
	WatchMaxBackoff     time.Duration
	InitWaitTime        time.Duration
	HealthCheckInterval time.Duration
	FullReconcileEvery  uint64
	MaxRecvMsgSize      int
	TLSEnabled          bool
}

type syncRequest struct {
	criteria      *modelv1.Criteria
	tagProjection []string
}

type syncMessage struct {
	responseCh chan []*digestEntry
	syncRequest
}

type watchSession struct {
	cancelFn  context.CancelFunc
	syncReqCh chan *syncMessage
}

type digestEntry struct {
	propID     string
	kind       string
	group      string
	name       string
	revision   int64
	deleteTime int64
}

// DefaultFullReconcileEvery is the default number of sync rounds between full reconciliations.
const DefaultFullReconcileEvery = 5

// SchemaRegistry implements schema.Registry using property-based schema servers.
type SchemaRegistry struct {
	connMgr            *grpchelper.ConnManager[*schemaClient]
	closer             *run.Closer
	l                  *logger.Logger
	cache              *schemaCache
	caCertReloader     *pkgtls.Reloader
	handlers           map[schema.Kind][]schema.EventHandler
	watchSessions      map[string]*watchSession
	syncInterval       time.Duration
	syncTimeout        time.Duration
	watchMaxBackoff    time.Duration
	fullReconcileEvery uint64
	syncRound          uint64
	mux                sync.RWMutex
	watchMu            sync.Mutex
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
	healthCheckInterval := cfg.HealthCheckInterval
	if healthCheckInterval == 0 {
		healthCheckInterval = DefaultHealthCheckInterval
	} else if healthCheckInterval < 0 {
		healthCheckInterval = 0 // negative means disabled
	}
	connMgr := grpchelper.NewConnManager[*schemaClient](grpchelper.ConnManagerConfig[*schemaClient]{
		Handler:             handler,
		Logger:              l,
		MaxRecvMsgSize:      cfg.MaxRecvMsgSize,
		HealthCheckInterval: healthCheckInterval,
	})
	syncInterval := cfg.SyncInterval
	if syncInterval == 0 {
		syncInterval = DefaultSyncInterval
	}
	initWaitTime := cfg.InitWaitTime
	if initWaitTime == 0 {
		initWaitTime = DefaultInitWaitTime
	}
	fullReconcileEvery := cfg.FullReconcileEvery
	if fullReconcileEvery <= 0 {
		fullReconcileEvery = DefaultFullReconcileEvery
	}
	syncTimeout := cfg.SyncTimeout
	if syncTimeout == 0 {
		syncTimeout = defaultSyncTimeout
	}
	watchMaxBackoff := cfg.WatchMaxBackoff
	if watchMaxBackoff == 0 {
		watchMaxBackoff = defaultWatchMaxBackoff
	}
	reg := &SchemaRegistry{
		connMgr:            connMgr,
		closer:             run.NewCloser(1),
		l:                  l,
		cache:              newSchemaCache(),
		caCertReloader:     caCertReloader,
		handlers:           make(map[schema.Kind][]schema.EventHandler),
		watchSessions:      make(map[string]*watchSession),
		syncInterval:       syncInterval,
		syncTimeout:        syncTimeout,
		watchMaxBackoff:    watchMaxBackoff,
		fullReconcileEvery: fullReconcileEvery,
	}
	handler.registry = reg

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

type schemaWithDeleteTime struct {
	property   *propertyv1.Property
	deleteTime int64
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
	query := buildSchemaQuery(kind, group, name, 0)
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
	query := buildSchemaQuery(kind, group, "", 0)
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
	kind schema.Kind, spec T, validators ...func(prev T) error,
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
	prevMd, convErr := ToSchema(kind, originalProp)
	if convErr != nil {
		return convErr
	}
	prev, ok := prevMd.Spec.(T)
	if !ok {
		return fmt.Errorf("unexpected spec type for kind %s", kind)
	}
	for _, v := range validators {
		if validateErr := v(prev); validateErr != nil {
			return validateErr
		}
	}
	if checker, checkerOk := schema.CheckerMap[kind]; checkerOk && checker(prev, spec) {
		return nil
	}
	prop, propErr := SchemaToProperty(kind, spec)
	if propErr != nil {
		return propErr
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
	return now, updateResource(ctx, r, schema.KindStream, stream, func(prev *databasev1.Stream) error {
		if err := validateStreamUpdate(prev, stream); err != nil {
			return fmt.Errorf("validation failed: %w", err)
		}
		return nil
	})
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
	return now, updateResource(ctx, r, schema.KindMeasure, measure, func(prev *databasev1.Measure) error {
		if err := validateMeasureUpdate(prev, measure); err != nil {
			return fmt.Errorf("validation failed: %w", err)
		}
		return nil
	})
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
	return now, updateResource(ctx, r, schema.KindTrace, trace, func(prev *databasev1.Trace) error {
		if err := validate.TraceUpdate(prev, trace); err != nil {
			return fmt.Errorf("validation failed: %w", err)
		}
		return nil
	})
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
	var kinds []schema.Kind
	for idx := 0; idx < schema.KindSize; idx++ {
		ki := schema.Kind(1 << idx)
		if kind&ki > 0 {
			kinds = append(kinds, ki)
		}
	}
	r.l.Info().Str("name", name).Interface("kinds", kinds).Msg("registering property schema handler")
	r.initHandlerWithRetry(name, handler, kinds)
	r.mux.Lock()
	defer r.mux.Unlock()
	for _, ki := range kinds {
		r.handlers[ki] = append(r.handlers[ki], handler)
	}
}

func (r *SchemaRegistry) initHandlerWithRetry(name string, handler schema.EventHandler, kinds []schema.Kind) {
	deadline := time.Now().Add(time.Minute)
	var lastErr interface{}
	for {
		if tryCallOnInit(handler, kinds, &lastErr) {
			return
		}
		if time.Now().After(deadline) {
			r.l.Panic().Str("name", name).Interface("error", lastErr).
				Msg("handler OnInit failed after 1m, giving up")
		}
		r.l.Warn().Str("name", name).Interface("error", lastErr).
			Msg("handler OnInit panicked due to transient error, retrying in 1s")
		time.Sleep(time.Second)
	}
}

func tryCallOnInit(handler schema.EventHandler, kinds []schema.Kind, lastErr *interface{}) (ok bool) {
	defer func() {
		if rec := recover(); rec != nil {
			*lastErr = rec
			ok = false
		}
	}()
	handler.OnInit(kinds)
	return true
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
	// Replay all cached entries to handlers. Watch replay (launched during
	// OnActive in PreRun) may have populated the cache before handlers were
	// registered. This explicit replay ensures handlers see every entry.
	r.notifyHandlersFromCache()
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
			r.syncRound++
			if r.syncRound%r.fullReconcileEvery == 0 {
				r.l.Debug().Uint64("round", r.syncRound).Msg("syncLoop: starting full reconcile")
				r.performFullSync(ctx)
			} else {
				r.l.Debug().Uint64("round", r.syncRound).Msg("syncLoop: starting incremental sync")
				r.performIncrementalSync(ctx)
			}
		}
	}
}

func (r *SchemaRegistry) launchWatch(nodeName string, client *schemaClient) {
	r.watchMu.Lock()
	defer r.watchMu.Unlock()
	if old, exists := r.watchSessions[nodeName]; exists {
		r.l.Debug().Str("node", nodeName).Msg("launchWatch: closing existing watch session")
		old.cancelFn()
		delete(r.watchSessions, nodeName)
	}
	ctx, cancel := context.WithCancel(r.closer.Ctx())
	session := &watchSession{
		cancelFn:  cancel,
		syncReqCh: make(chan *syncMessage, 1),
	}
	r.watchSessions[nodeName] = session
	r.l.Debug().Str("node", nodeName).Msg("launchWatch: starting new watch session")
	if r.closer.AddRunning() {
		go func() {
			defer r.closer.Done()
			r.watchLoop(ctx, nodeName, client.update, session)
		}()
	} else {
		cancel()
		delete(r.watchSessions, nodeName)
	}
}

func (r *SchemaRegistry) stopWatch(nodeName string) {
	r.watchMu.Lock()
	defer r.watchMu.Unlock()
	if session, exists := r.watchSessions[nodeName]; exists {
		r.l.Debug().Str("node", nodeName).Msg("stopWatch: canceling watch session")
		session.cancelFn()
		delete(r.watchSessions, nodeName)
	}
}

const (
	watchInitialBackoff = time.Second
	// defaultWatchMaxBackoff is the default maximum backoff for watch stream reconnection.
	defaultWatchMaxBackoff = 30 * time.Second
)

func (r *SchemaRegistry) watchLoop(ctx context.Context, nodeName string,
	updateClient schemav1.SchemaUpdateServiceClient, session *watchSession,
) {
	backoff := watchInitialBackoff
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		err := r.processWatchSession(ctx, updateClient, session)
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.Unimplemented {
				r.l.Debug().Str("node", nodeName).Msg("watch: server does not support WatchSchemas, stopping watch")
				return
			}
			r.l.Warn().Err(err).Str("node", nodeName).Dur("backoff", backoff).Msg("watch stream disconnected, will retry")
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > r.watchMaxBackoff {
			backoff = r.watchMaxBackoff
		}
	}
}

func (r *SchemaRegistry) processWatchSession(ctx context.Context,
	updateClient schemav1.SchemaUpdateServiceClient, session *watchSession,
) error {
	stream, err := updateClient.WatchSchemas(ctx)
	if err != nil {
		return err
	}

	maxRev := r.cache.GetMaxRevision()
	r.l.Debug().Int64("maxRevision", maxRev).Msg("processWatchSession: sending initial replay request")
	initReq := &schemav1.WatchSchemasRequest{
		Criteria: buildRevisionCriteria(maxRev),
	}
	if sendErr := stream.Send(initReq); sendErr != nil {
		return sendErr
	}

	recvCh := make(chan *schemav1.WatchSchemasResponse, 64)
	recvErrCh := make(chan error, 1)
	go func() {
		for {
			resp, recvErr := stream.Recv()
			if recvErr != nil {
				recvErrCh <- recvErr
				close(recvCh)
				return
			}
			recvCh <- resp
		}
	}()

	var inSync bool
	var metadataOnly bool
	var digests []*digestEntry
	var currentResponseCh chan []*digestEntry

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-recvErrCh:
			return err
		case req := <-session.syncReqCh:
			r.l.Debug().Bool("metadataOnly", len(req.tagProjection) > 0).
				Bool("hasCriteria", req.criteria != nil).Msg("processWatchSession: sending sync request")
			if sendErr := stream.Send(&schemav1.WatchSchemasRequest{
				Criteria:      req.criteria,
				TagProjection: req.tagProjection,
			}); sendErr != nil {
				return sendErr
			}
			inSync = true
			metadataOnly = len(req.tagProjection) > 0
			digests = nil
			currentResponseCh = req.responseCh
		case resp, ok := <-recvCh:
			if !ok {
				return <-recvErrCh
			}
			if resp.EventType == schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_REPLAY_DONE {
				r.l.Debug().Bool("inSync", inSync).Msg("processWatchSession: received REPLAY_DONE")
				if inSync && currentResponseCh != nil {
					r.l.Debug().Int("digestCount", len(digests)).Msg("processWatchSession: sync replay completed, sending digests")
					currentResponseCh <- digests
					digests = nil
					inSync = false
					metadataOnly = false
					currentResponseCh = nil
				}
				continue
			}
			if inSync && metadataOnly {
				digests = append(digests, r.parseDigest(resp))
			} else {
				r.handleWatchEvent(resp)
			}
		}
	}
}

func (r *SchemaRegistry) parseDigest(resp *schemav1.WatchSchemasResponse) *digestEntry {
	prop := resp.GetProperty()
	parsed := ParseTags(prop.GetTags())
	return &digestEntry{
		propID:     prop.GetId(),
		kind:       parsed.Kind,
		group:      parsed.Group,
		name:       parsed.Name,
		revision:   parsed.UpdatedAt,
		deleteTime: resp.GetDeleteTime(),
	}
}

func (r *SchemaRegistry) handleWatchEvent(resp *schemav1.WatchSchemasResponse) {
	prop := resp.GetProperty()
	if prop == nil {
		return
	}
	parsed := ParseTags(prop.GetTags())
	kindStr := parsed.Kind
	if kindStr == "" {
		kindStr = prop.GetMetadata().GetName()
	}
	kind, kindErr := KindFromString(kindStr)
	if kindErr != nil {
		r.l.Warn().Str("kind", kindStr).Msg("watch: unknown kind in event")
		return
	}
	r.l.Debug().Stringer("eventType", resp.GetEventType()).Stringer("kind", kind).
		Str("propID", prop.GetId()).Msg("watch: received event")
	switch resp.GetEventType() {
	case schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_INSERT, schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_UPDATE:
		md, convErr := ToSchema(kind, prop)
		if convErr != nil {
			r.l.Warn().Err(convErr).Stringer("kind", kind).Msg("watch: failed to convert property")
			return
		}
		r.processInitialResourceFromProperty(kind, prop, md.Spec.(proto.Message))
	case schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_DELETE:
		propID := prop.GetId()
		if r.cache.Delete(propID, resp.GetDeleteTime()) {
			md, convErr := ToSchema(kind, prop)
			if convErr != nil {
				r.l.Warn().Err(convErr).Stringer("kind", kind).Msg("watch: failed to convert deleted property")
				return
			}
			r.notifyHandlers(kind, md, true)
		}
	case schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_REPLAY_DONE, schemav1.SchemaEventType_SCHEMA_EVENT_TYPE_UNSPECIFIED:
		// handled by processWatchSession, not expected here
	}
}

func (r *SchemaRegistry) performIncrementalSync(ctx context.Context) {
	sessions := r.getActiveSessions()
	if len(sessions) == 0 {
		return
	}
	maxRev := r.cache.GetMaxRevision()
	req := &syncRequest{
		criteria: buildRevisionCriteria(maxRev),
	}
	r.l.Debug().Int64("maxRevision", maxRev).Msg("incremental sync: requesting changes")
	if _, err := r.sendSyncRequest(ctx, sessions, req); err != nil {
		r.l.Warn().Err(err).Msg("incremental sync: sync request partially failed")
	}
}

func (r *SchemaRegistry) getActiveSessions() map[string]*watchSession {
	r.watchMu.Lock()
	defer r.watchMu.Unlock()
	sessions := make(map[string]*watchSession, len(r.watchSessions))
	for name, s := range r.watchSessions {
		sessions[name] = s
	}
	return sessions
}

func (r *SchemaRegistry) sendSyncRequest(ctx context.Context,
	sessions map[string]*watchSession, req *syncRequest,
) (map[string][]*digestEntry, error) {
	r.l.Debug().Int("sessionCount", len(sessions)).Msg("sendSyncRequest: dispatching to watch sessions")
	sentChs := make(map[string]chan []*digestEntry, len(sessions))
	var skipped int
	for name, s := range sessions {
		msg := &syncMessage{
			syncRequest: *req,
			responseCh:  make(chan []*digestEntry, 1),
		}
		select {
		case s.syncReqCh <- msg:
			sentChs[name] = msg.responseCh
		default:
			skipped++
			r.l.Warn().Str("node", name).Msg("sendSyncRequest: channel full, skipping session")
		}
	}
	var timedOut int
	allDigests := make(map[string][]*digestEntry, len(sentChs))
	for name, ch := range sentChs {
		select {
		case digests := <-ch:
			allDigests[name] = digests
			r.l.Debug().Str("node", name).Int("digestCount", len(digests)).Msg("sendSyncRequest: received digests")
		case <-time.After(r.syncTimeout):
			timedOut++
			r.l.Warn().Str("node", name).Msg("sync timeout")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	r.l.Debug().Int("totalSessions", len(sessions)).Int("sent", len(sentChs)).Int("responded", len(allDigests)).Msg("sendSyncRequest: completed")
	if skipped > 0 || timedOut > 0 {
		return allDigests, fmt.Errorf("sync partially failed: %d skipped, %d timed out out of %d sessions",
			skipped, timedOut, len(sessions))
	}
	return allDigests, nil
}

func (r *SchemaRegistry) performFullSync(ctx context.Context) {
	sessions := r.getActiveSessions()
	if len(sessions) == 0 {
		r.l.Debug().Msg("performFullSync: no active watch sessions, skipping")
		return
	}
	req := &syncRequest{
		tagProjection: basicTagKeys,
	}
	allDigests, err := r.sendSyncRequest(ctx, sessions, req)
	if allDigests == nil {
		return
	}
	if err != nil {
		// Partial failure: some sessions timed out or were skipped.
		// Use whatever digests were received for the add/update path,
		// but skip the deletion reconciliation to avoid removing entries
		// that may exist on unreachable nodes.
		r.l.Warn().Err(err).Msg("performFullSync: sync request partially failed, skipping deletion reconciliation")
	}

	merged := r.mergeDigests(allDigests)
	cachedEntries := r.cache.GetAllEntries()
	r.l.Debug().Int("respondedNodes", len(allDigests)).Int("mergedCount", len(merged)).Int("cachedCount", len(cachedEntries)).
		Msg("performFullSync: comparing server digests with local cache")

	for propID, d := range merged {
		if d.deleteTime > 0 {
			if entry, exists := cachedEntries[propID]; exists {
				kind, kindErr := KindFromString(d.kind)
				if kindErr != nil {
					r.l.Warn().Str("kind", d.kind).Msg("full sync: unknown kind")
					continue
				}
				r.handleDeletion(kind, propID, entry, d.revision)
			}
			continue
		}
		localEntry := cachedEntries[propID]
		if localEntry == nil || localEntry.latestUpdateAt < d.revision {
			kind, kindErr := KindFromString(d.kind)
			if kindErr != nil {
				r.l.Warn().Str("kind", d.kind).Msg("full sync: unknown kind")
				continue
			}
			r.l.Info().Str("propID", propID).Stringer("kind", kind).
				Int64("serverRevision", d.revision).
				Bool("localMissing", localEntry == nil).
				Msg("performFullSync: fetching updated schema from server")
			prop, getErr := r.getSchema(ctx, kind, d.group, d.name)
			if getErr != nil {
				r.l.Warn().Err(getErr).Str("propID", propID).Msg("full sync: failed to get schema")
				continue
			}
			if prop == nil {
				continue
			}
			md, convertErr := ToSchema(kind, prop)
			if convertErr != nil {
				r.l.Warn().Err(convertErr).Str("propID", propID).Msg("full sync: failed to convert property")
				continue
			}
			r.processInitialResourceFromProperty(kind, prop, md.Spec.(proto.Message))
		}
	}

	if err == nil {
		for propID, entry := range cachedEntries {
			if _, exists := merged[propID]; !exists {
				r.l.Warn().Str("propID", propID).Stringer("kind", entry.kind).
					Str("group", entry.group).Str("name", entry.name).
					Msg("performFullSync: cached entry not found on server, deleting")
				r.handleDeletion(entry.kind, propID, entry, entry.latestUpdateAt)
			}
		}
	}
}

func (r *SchemaRegistry) mergeDigests(allDigests map[string][]*digestEntry) map[string]*digestEntry {
	merged := make(map[string]*digestEntry)
	for _, digests := range allDigests {
		for _, d := range digests {
			existing, exists := merged[d.propID]
			if !exists || d.revision > existing.revision ||
				(d.revision == existing.revision && d.deleteTime > existing.deleteTime) {
				merged[d.propID] = d
			}
		}
	}
	return merged
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
	updated := r.cache.Update(propID, entry)
	r.l.Debug().Stringer("kind", kind).Str("group", parsed.Group).Str("name", parsed.Name).
		Str("propID", propID).Bool("cacheUpdated", updated).Msg("processing initial resource from property")
	if updated {
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
	r.l.Debug().Stringer("kind", kind).Str("propID", propID).Int64("revision", revision).
		Msg("handleDeletion: attempting to delete from cache")
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
	r.l.Debug().Stringer("kind", kind).Str("group", md.Group).Str("name", md.Name).
		Bool("isDelete", isDelete).Int("handlerCount", len(handlers)).Msg("notifying schema event handlers")
	for _, handler := range handlers {
		if isDelete {
			handler.OnDelete(md)
		} else {
			handler.OnAddOrUpdate(md)
		}
	}
}

func (r *SchemaRegistry) notifyHandlersFromCache() {
	entries := r.cache.GetAllEntries()
	r.l.Debug().Int("entryCount", len(entries)).Msg("notifyHandlersFromCache: replaying cached entries to handlers")
	for _, entry := range entries {
		r.notifyHandlers(entry.kind, schema.Metadata{
			TypeMeta: schema.TypeMeta{
				Kind:  entry.kind,
				Name:  entry.name,
				Group: entry.group,
			},
			Spec: entry.spec,
		}, false)
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
	r.watchMu.Lock()
	for nodeName, session := range r.watchSessions {
		session.cancelFn()
		delete(r.watchSessions, nodeName)
	}
	r.watchMu.Unlock()
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
