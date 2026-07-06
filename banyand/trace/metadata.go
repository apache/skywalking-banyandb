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

package trace

import (
	"context"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/apache/skywalking-banyandb/api/common"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/api/validate"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/internal/wqueue"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/observability"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue/pub"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/meter"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	resourceSchema "github.com/apache/skywalking-banyandb/pkg/schema"
	"github.com/apache/skywalking-banyandb/pkg/schema/registry"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// traceRegistryKinds is the kind set the trace schemaRepo registers with the
// per-node NodeRepoRegistry.
const traceRegistryKinds = schema.KindGroup | schema.KindTrace | schema.KindIndexRule | schema.KindIndexRuleBinding

var (
	metadataScope = traceScope.SubScope("metadata")
	// ErrTraceNotFound is returned when a trace is not found.
	ErrTraceNotFound = errors.New("trace not found")
)

// SchemaService allows querying schema information.
type SchemaService interface {
	Query
	Close()
}

type schemaRepo struct {
	resourceSchema.Repository
	onGroupDelete          func(groupName string)
	l                      *logger.Logger
	metadata               metadata.Repo
	samplerMeter           *samplerMetrics
	pluginTelemetryFactory observability.Factory
	path                   string
	nodeID                 string
	trustedPluginDir       string
	finalizeGraceDefault   time.Duration
	role                   databasev1.Role
	nativePipelineEnabled  bool
}

func newSchemaRepo(path string, svc *standalone, nodeLabels map[string]string, nodeID string) schemaRepo {
	pipelineFactory := svc.omr.With(pipelineScope)
	sr := schemaRepo{
		l:                      svc.l,
		path:                   path,
		metadata:               svc.metadata,
		nodeID:                 nodeID,
		role:                   databasev1.Role_ROLE_DATA,
		nativePipelineEnabled:  svc.option.nativePipelineEnabled,
		trustedPluginDir:       svc.option.trustedPluginDir,
		finalizeGraceDefault:   svc.option.finalizeGraceDefault,
		samplerMeter:           newSamplerMetrics(pipelineFactory),
		pluginTelemetryFactory: pipelineFactory,
		Repository: resourceSchema.NewRepository(
			svc.metadata,
			svc.l,
			newSupplier(path, svc, nodeLabels),
			resourceSchema.NewMetrics(svc.omr.With(metadataScope)),
		),
	}
	sr.start()
	sr.registerWithNodeRepo()
	return sr
}

func newLiaisonSchemaRepo(path string, svc *liaison, traceDataNodeRegistry grpc.NodeRegistry) schemaRepo {
	pipelineFactory := svc.omr.With(pipelineScope)
	sr := schemaRepo{
		l:                      svc.l,
		path:                   path,
		metadata:               svc.metadata,
		role:                   databasev1.Role_ROLE_LIAISON,
		samplerMeter:           newSamplerMetrics(pipelineFactory),
		pluginTelemetryFactory: pipelineFactory,
		Repository: resourceSchema.NewRepository(
			svc.metadata,
			svc.l,
			newQueueSupplier(path, svc, traceDataNodeRegistry),
			resourceSchema.NewMetrics(svc.omr.With(metadataScope)),
		),
	}
	if svc.handoffCtrl != nil {
		sr.onGroupDelete = svc.handoffCtrl.deletePartsByGroup
	}
	sr.start()
	sr.registerWithNodeRepo()
	return sr
}

func (sr *schemaRepo) start() {
	sr.Watcher()
	sr.metadata.
		RegisterHandler("trace", schema.KindGroup|schema.KindTrace|schema.KindIndexRuleBinding|schema.KindIndexRule,
			sr)
}

// registerWithNodeRepo joins this schemaRepo to the per-node aggregator so the
// cluster barrier and NodeSchemaStatusService route Group/Trace/IndexRule/
// IndexRuleBinding lookups through the same cache the executor consults via
// LoadGroup / LoadResource.
func (sr *schemaRepo) registerWithNodeRepo() {
	metaSvc, ok := sr.metadata.(metadata.Service)
	if !ok {
		return
	}
	registry.MaybeRegister(metaSvc.NodeRepoRegistry(), traceRegistryKinds, sr.Repository)
}

func (sr *schemaRepo) Trace(metadata *commonv1.Metadata) (*trace, bool) {
	sm, ok := sr.Repository.LoadResource(metadata)
	if !ok {
		return nil, false
	}
	t, ok := sm.Delegated().(*trace)
	return t, ok
}

func (sr *schemaRepo) GetRemovalSegmentsTimeRange(group string) *timestamp.TimeRange {
	g, ok := sr.LoadGroup(group)
	if !ok {
		return nil
	}
	db := g.SupplyTSDB()
	if db == nil {
		return nil
	}
	return db.(storage.TSDB[*tsTable, option]).GetExpiredSegmentsTimeRange()
}

func (sr *schemaRepo) OnInit(kinds []schema.Kind) (bool, []int64) {
	if len(kinds) != 4 {
		logger.Panicf("invalid kinds: %v", kinds)
		return false, nil
	}
	_, revs := sr.Repository.Init(schema.KindTrace)
	return true, revs
}

func (sr *schemaRepo) OnAddOrUpdate(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindGroup:
		g := metadata.Spec.(*commonv1.Group)
		if g.Catalog != commonv1.Catalog_CATALOG_TRACE {
			return
		}
		if err := validate.GroupForNonProperty(g); err != nil {
			sr.l.Warn().Err(err).Msg("group is ignored")
			return
		}
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventAddOrUpdate,
			Kind:     resourceSchema.EventKindGroup,
			Metadata: g,
		})
		if sr.role == databasev1.Role_ROLE_DATA && sr.nativePipelineEnabled {
			sr.reconcilePipeline(g.Metadata.Name, g.GetPipeline())
		}
	case schema.KindTrace:
		if err := validate.Trace(metadata.Spec.(*databasev1.Trace)); err != nil {
			sr.l.Warn().Err(err).Msg("trace is ignored")
			return
		}
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventAddOrUpdate,
			Kind:     resourceSchema.EventKindResource,
			Metadata: metadata.Spec.(*databasev1.Trace),
		})
	case schema.KindIndexRuleBinding:
		if irb, ok := metadata.Spec.(*databasev1.IndexRuleBinding); ok {
			if err := validate.IndexRuleBinding(irb); err != nil {
				sr.l.Warn().Err(err).Msg("index rule binding is ignored")
				return
			}
			if irb.GetSubject().Catalog == commonv1.Catalog_CATALOG_TRACE {
				sr.SendMetadataEvent(resourceSchema.MetadataEvent{
					Typ:      resourceSchema.EventAddOrUpdate,
					Kind:     resourceSchema.EventKindIndexRuleBinding,
					Metadata: irb,
				})
			}
		}
	case schema.KindIndexRule:
		if ir, ok := metadata.Spec.(*databasev1.IndexRule); ok {
			if err := validate.IndexRule(metadata.Spec.(*databasev1.IndexRule)); err != nil {
				sr.l.Warn().Err(err).Msg("index rule is ignored")
				return
			}
			sr.SendMetadataEvent(resourceSchema.MetadataEvent{
				Typ:      resourceSchema.EventAddOrUpdate,
				Kind:     resourceSchema.EventKindIndexRule,
				Metadata: ir,
			})
		}
	default:
	}
}

func (sr *schemaRepo) OnDelete(metadata schema.Metadata) {
	switch metadata.Kind {
	case schema.KindGroup:
		g := metadata.Spec.(*commonv1.Group)
		if g.Catalog != commonv1.Catalog_CATALOG_TRACE {
			return
		}
		if sr.onGroupDelete != nil {
			sr.onGroupDelete(g.Metadata.Name)
		}
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:      resourceSchema.EventDelete,
			Kind:     resourceSchema.EventKindGroup,
			Metadata: g,
		})
		if sr.role == databasev1.Role_ROLE_DATA && sr.nativePipelineEnabled {
			removeSamplersForGroup(g.Metadata.Name)
			teardownGroupTelemetry(g.Metadata.Name)
			setMergeGraceForGroup(g.Metadata.Name, 0)
			setFinalizeGraceForGroup(g.Metadata.Name, 0)
			setFinalizeConfigForGroup(g.Metadata.Name, nil)
			sr.samplerMeter.setActiveCount(g.Metadata.Name, 0)
			sr.samplerMeter.incRemoveTotal(g.Metadata.Name)
		}
	case schema.KindTrace:
		traceSpec := metadata.Spec.(*databasev1.Trace)
		sr.SendMetadataEvent(resourceSchema.MetadataEvent{
			Typ:            resourceSchema.EventDelete,
			Kind:           resourceSchema.EventKindResource,
			DeleteRevision: traceSpec.GetMetadata().GetModRevision(),
			Metadata:       traceSpec,
		})
	case schema.KindIndexRuleBinding:
		if binding, ok := metadata.Spec.(*databasev1.IndexRuleBinding); ok {
			if binding.GetSubject().Catalog == commonv1.Catalog_CATALOG_TRACE {
				sr.SendMetadataEvent(resourceSchema.MetadataEvent{
					Typ:      resourceSchema.EventDelete,
					Kind:     resourceSchema.EventKindIndexRuleBinding,
					Metadata: metadata.Spec.(*databasev1.IndexRuleBinding),
				})
			}
		}
	case schema.KindIndexRule:
		if rule, ok := metadata.Spec.(*databasev1.IndexRule); ok {
			sr.SendMetadataEvent(resourceSchema.MetadataEvent{
				Typ:      resourceSchema.EventDelete,
				Kind:     resourceSchema.EventKindIndexRule,
				Metadata: rule,
			})
		}
	default:
	}
}

// mergeEventEnabled reports whether cfg applies to the merge-time pipeline event.
// v1 only implements the in-merge filter, so a config that does not enable
// PIPELINE_EVENT_MERGE installs no samplers. An empty enabled_events list defaults
// to MERGE for backward compatibility.
func mergeEventEnabled(cfg *commonv1.TracePipelineConfig) bool {
	events := cfg.GetEnabledEvents()
	if len(events) == 0 {
		return true
	}
	return slices.Contains(events, commonv1.PipelineEvent_PIPELINE_EVENT_MERGE)
}

// finalizeEventEnabled reports whether cfg applies to the scheduled finalization
// pass. Unlike MERGE, an empty enabled_events list does NOT default finalize on:
// finalization is an extra background procedure and must be opted into explicitly
// (empty list keeps only the backward-compatible MERGE default).
func finalizeEventEnabled(cfg *commonv1.TracePipelineConfig) bool {
	return slices.Contains(cfg.GetEnabledEvents(), commonv1.PipelineEvent_PIPELINE_EVENT_FINALIZE)
}

// samplerLoadFailReason maps a plugin load error to a small, stable set of reason
// codes for the sampler_load_failed metric label, keeping label cardinality bounded
// (the full error is logged separately). Unrecognized errors fall back to "load_error".
func samplerLoadFailReason(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "panic"):
		return "panic"
	case strings.Contains(msg, "escapes trusted directory"):
		return "path_escape"
	case strings.Contains(msg, "ABI version") || strings.Contains(msg, "ABIVersion"):
		return "abi_mismatch"
	case strings.Contains(msg, "constructor"):
		return "ctor_failed"
	case strings.Contains(msg, "missing symbol") || strings.Contains(msg, "wrong type"):
		return "symbol_missing"
	case strings.Contains(msg, "cannot open plugin"):
		return "open_failed"
	case strings.Contains(msg, "marshal plugin config"):
		return "config_marshal"
	case strings.Contains(msg, "plugin path is empty") || strings.Contains(msg, "trusted plugin dir not configured"):
		return "config_invalid"
	default:
		return "load_error"
	}
}

// reconcilePipeline rebuilds the sampler registry for group from cfg.
// If cfg is nil (or the group is being deleted), it removes the group's entry.
// On full load success it atomically replaces the slice and stores the per-group
// merge_grace. If any plugin fails to load, the previous good set is kept intact
// (fail-open) and an ERROR log is emitted.
func (sr *schemaRepo) reconcilePipeline(group string, cfg *commonv1.TracePipelineConfig) {
	// A nil/disabled config, or one that enables NEITHER the merge nor the finalize
	// event, clears the group's samplers (retain all). The sampler set is shared by
	// both events (DD11: finalize reuses the group's registered samplers), so it is
	// cleared only when both are off. enabled_events is honored here so filtering can
	// be disabled without removing the config.
	if cfg == nil || !cfg.GetEnabled() || (!mergeEventEnabled(cfg) && !finalizeEventEnabled(cfg)) {
		hadSamplers := len(lookupSamplers(group)) > 0
		removeSamplersForGroup(group)
		teardownGroupTelemetry(group)
		setMergeGraceForGroup(group, 0)
		setFinalizeGraceForGroup(group, 0)
		setFinalizeConfigForGroup(group, nil)
		sr.samplerMeter.setActiveCount(group, 0)
		if hadSamplers {
			sr.samplerMeter.incRemoveTotal(group)
		}
		return
	}
	// Idempotent skip: if the desired identity list (name+configHash in order) equals
	// the currently registered one, this is a redundant watch-replay re-apply. Return
	// early with no reload, no UseHost, no teardown, no swap, and no metrics churn.
	// This is defense-in-depth for the same-group re-reconcile race on top of the
	// once-only-UseHost invariant enforced inside loadSamplerPlugin.
	desired := make([]nameHash, 0, len(cfg.GetPlugins()))
	for _, p := range cfg.GetPlugins() {
		sp := p.GetSampler()
		if sp == nil {
			continue
		}
		desired = append(desired, nameHash{
			name:       p.GetName(),
			configHash: computeConfigHash(sp),
			path:       sp.GetPath(),
			symbol:     sp.GetSymbol(),
			abiVersion: sp.GetAbiVersion(),
		})
	}
	current := currentSamplerIdentity(group)
	if len(desired) == len(current) {
		identical := true
		for idx := range desired {
			if desired[idx] != current[idx] {
				identical = false
				break
			}
		}
		if identical {
			return
		}
	}
	// Config actually changed: tear down the old adapters' series before rebuilding
	// them for the new instances.
	teardownGroupTelemetry(group)
	// Distinguish register (no previous set) from update (previous set exists).
	isUpdate := len(current) > 0
	var newSet []namedSampler
	for _, p := range cfg.GetPlugins() {
		sp := p.GetSampler()
		if sp == nil {
			continue
		}
		pluginName := p.GetName()
		bindHost := func(s sdk.Sampler) {
			ha, ok := s.(sdk.HostAware)
			if !ok {
				return
			}
			tel := newPluginTelemetry(sr.pluginTelemetryFactory, logger.GetLogger("trace"), sr.samplerMeter, group, pluginName)
			func() {
				defer func() {
					if r := recover(); r != nil {
						sr.samplerMeter.incTelemetryPanic(group, pluginName)
						sr.l.Warn().Str("group", group).Str("plugin", pluginName).Interface("panic", r).
							Msg("plugin UseHost panicked; telemetry disabled for this plugin")
					}
				}()
				ha.UseHost(tel)
			}()
			registerGroupTelemetry(group, tel)
		}
		var sampler sdk.Sampler
		var loadErr error
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					loadErr = fmt.Errorf("panic loading plugin %q: %v", pluginName, rec)
				}
			}()
			sampler, loadErr = loadSamplerPlugin(sp, sr.trustedPluginDir, group, bindHost)
		}()
		if loadErr != nil {
			sr.samplerMeter.incLoadFailed(group, p.GetName(), samplerLoadFailReason(loadErr))
			if isUpdate {
				sr.samplerMeter.incUpdateTotal(group, "rejected")
			} else {
				sr.samplerMeter.incRegisterTotal(group, "rejected")
			}
			sr.l.Error().
				Err(loadErr).
				Str("group", group).
				Str("plugin", p.GetName()).
				Msg("sampler plugin load failed; keeping previous good set (fail-open)")
			return
		}
		newSet = append(newSet, namedSampler{
			name:       p.GetName(),
			configHash: computeConfigHash(sp),
			sampler:    sampler,
			path:       sp.GetPath(),
			symbol:     sp.GetSymbol(),
			abiVersion: sp.GetAbiVersion(),
		})
	}
	replaceSamplersForGroup(group, newSet)
	var graceNs int64
	if gd := cfg.GetMergeGrace(); gd != nil {
		graceNs = gd.AsDuration().Nanoseconds()
	}
	setMergeGraceForGroup(group, graceNs)
	// Store finalize_grace + threshold config ONLY when the FINALIZE event is enabled;
	// a finalize config entry is what marks a group for the background finalize scanner
	// (a merge-only group must not be scanned). The proto carries no threshold-override
	// fields in v1, so the config registry holds defaults (filled by lookupFinalizeConfig).
	// finalize_grace falls back to option.finalizeGraceDefault at lookup time when unset.
	if finalizeEventEnabled(cfg) {
		var finalizeGraceNs int64
		if fg := cfg.GetFinalizeGrace(); fg != nil {
			finalizeGraceNs = fg.AsDuration().Nanoseconds()
		}
		setFinalizeGraceForGroup(group, finalizeGraceNs)
		setFinalizeConfigForGroup(group, &finalizeConfig{})
	} else {
		setFinalizeGraceForGroup(group, 0)
		setFinalizeConfigForGroup(group, nil)
	}
	sr.samplerMeter.setActiveCount(group, len(newSet))
	if isUpdate {
		sr.samplerMeter.incUpdateTotal(group, "success")
	} else {
		sr.samplerMeter.incRegisterTotal(group, "success")
	}
}

func (sr *schemaRepo) loadTrace(metadata *commonv1.Metadata) (*trace, bool) {
	r, ok := sr.LoadResource(metadata)
	if !ok {
		return nil, false
	}
	s, ok := r.Delegated().(*trace)
	return s, ok
}

func (sr *schemaRepo) loadTSDB(groupName string) (storage.TSDB[*tsTable, option], error) {
	if sr == nil {
		return nil, fmt.Errorf("schemaRepo is nil")
	}
	g, ok := sr.LoadGroup(groupName)
	if !ok {
		return nil, fmt.Errorf("group %s not found", groupName)
	}
	db := g.SupplyTSDB()
	if db == nil {
		return nil, fmt.Errorf("group %s not found", groupName)
	}
	return db.(storage.TSDB[*tsTable, option]), nil
}

// CollectDataInfo collects data info for a specific group.
func (sr *schemaRepo) CollectDataInfo(ctx context.Context, group string) (*databasev1.DataInfo, error) {
	if sr.nodeID == "" {
		return nil, fmt.Errorf("node ID is empty")
	}
	node, nodeErr := sr.metadata.NodeRegistry().GetNode(ctx, sr.nodeID)
	if nodeErr != nil {
		return nil, fmt.Errorf("failed to get current node info: %w", nodeErr)
	}
	tsdb, tsdbErr := sr.loadTSDB(group)
	if tsdbErr != nil {
		return nil, tsdbErr
	}
	if tsdb == nil {
		return nil, nil
	}
	segments, segmentsErr := tsdb.SelectSegments(timestamp.TimeRange{
		Start: time.Unix(0, 0),
		End:   time.Unix(0, timestamp.MaxNanoTime),
	}, false)
	if segmentsErr != nil {
		return nil, segmentsErr
	}
	var segmentInfoList []*databasev1.SegmentInfo
	var totalDataSize int64
	for _, segment := range segments {
		timeRange := segment.GetTimeRange()
		tables, _ := segment.Tables()
		var shardInfoList []*databasev1.ShardInfo
		if len(tables) > 0 {
			for shardIdx, table := range tables {
				shardInfo := sr.collectShardInfo(ctx, table, uint32(shardIdx))
				shardInfoList = append(shardInfoList, shardInfo)
				totalDataSize += shardInfo.DataSizeBytes
			}
		} else {
			// No live shard tables (a closed segment, or a brand-new open one
			// with no data yet): read shard part stats from disk without
			// reopening. The per-shard SidxInfo is reported empty here: reading
			// it would reopen the shard's sidx/bluge index (the exclusive-lock
			// churn this change exists to avoid), so it is populated only for
			// open segments.
			closedShards, closedSize := storage.CollectClosedShardInfo(segment.Location())
			shardInfoList = closedShards
			totalDataSize += closedSize
		}
		seriesIndexInfo := sr.collectSeriesIndexInfo(segment)
		totalDataSize += seriesIndexInfo.DataSizeBytes
		segmentInfo := &databasev1.SegmentInfo{
			SegmentId:       fmt.Sprintf("%d-%d", timeRange.Start.UnixNano(), timeRange.End.UnixNano()),
			TimeRangeStart:  timeRange.Start.Format(time.RFC3339Nano),
			TimeRangeEnd:    timeRange.End.Format(time.RFC3339Nano),
			ShardInfo:       shardInfoList,
			SeriesIndexInfo: seriesIndexInfo,
		}
		segmentInfoList = append(segmentInfoList, segmentInfo)
		segment.DecRef()
	}
	dataInfo := &databasev1.DataInfo{
		Node:          node,
		SegmentInfo:   segmentInfoList,
		DataSizeBytes: totalDataSize,
	}
	return dataInfo, nil
}

func (sr *schemaRepo) collectSeriesIndexInfo(segment storage.Segment[*tsTable, option]) *databasev1.SeriesIndexInfo {
	// SeriesIndexStats reads from the live index when the segment is open and
	// from disk (read-only) when it is closed, so inspecting a cold segment
	// never reopens its writable index.
	dataCount, dataSizeBytes := segment.SeriesIndexStats()
	return &databasev1.SeriesIndexInfo{
		DataCount:     dataCount,
		DataSizeBytes: dataSizeBytes,
	}
}

func (sr *schemaRepo) collectShardInfo(ctx context.Context, table any, shardID uint32) *databasev1.ShardInfo {
	tst, ok := table.(*tsTable)
	if !ok {
		return &databasev1.ShardInfo{
			ShardId:       shardID,
			DataCount:     0,
			DataSizeBytes: 0,
			PartCount:     0,
		}
	}
	snapshot := tst.currentSnapshot()
	if snapshot == nil {
		return &databasev1.ShardInfo{
			ShardId:       shardID,
			DataCount:     0,
			DataSizeBytes: 0,
			PartCount:     0,
		}
	}
	defer snapshot.decRef()
	var totalCount, compressedSize, uncompressedSize, partCount, filePartCount uint64
	for _, pw := range snapshot.parts {
		if pw.p != nil {
			totalCount += pw.p.partMetadata.TotalCount
			compressedSize += pw.p.partMetadata.CompressedSizeBytes
			uncompressedSize += pw.p.partMetadata.UncompressedSpanSizeBytes
			partCount++
			filePartCount++
		} else if pw.mp != nil {
			totalCount += pw.mp.partMetadata.TotalCount
			compressedSize += pw.mp.partMetadata.CompressedSizeBytes
			uncompressedSize += pw.mp.partMetadata.UncompressedSpanSizeBytes
			partCount++
		}
	}
	sidxInfo := sr.collectSidxInfo(ctx, tst)
	return &databasev1.ShardInfo{
		ShardId:           shardID,
		DataCount:         int64(totalCount),
		DataSizeBytes:     int64(compressedSize),
		PartCount:         int64(partCount),
		InvertedIndexInfo: &databasev1.InvertedIndexInfo{},
		SidxInfo:          sidxInfo,
		FilePartCount:     int64(filePartCount),
	}
}

func (sr *schemaRepo) collectSidxInfo(ctx context.Context, tst *tsTable) *databasev1.SIDXInfo {
	sidxMap := tst.sidxMap
	if len(sidxMap) == 0 {
		return &databasev1.SIDXInfo{
			DataCount:     0,
			DataSizeBytes: 0,
			PartCount:     0,
		}
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var totalDataCount, totalDataSize, totalPartCount int64
	for _, sidxInstance := range sidxMap {
		stats, statsErr := sidxInstance.Stats(timeoutCtx)
		if statsErr != nil {
			continue
		}
		if stats != nil {
			totalDataCount += stats.ElementCount
			totalDataSize += stats.DiskUsageBytes
			totalPartCount += stats.PartCount
		}
	}
	return &databasev1.SIDXInfo{
		DataCount:     totalDataCount,
		DataSizeBytes: totalDataSize,
		PartCount:     totalPartCount,
	}
}

func (sr *schemaRepo) collectPendingWriteInfo(groupName string) (int64, error) {
	if sr == nil || sr.Repository == nil {
		return 0, fmt.Errorf("schema repository is not initialized")
	}
	if sr.role == databasev1.Role_ROLE_LIAISON {
		queue, queueErr := sr.loadQueue(groupName)
		if queueErr != nil {
			return 0, fmt.Errorf("failed to load queue: %w", queueErr)
		}
		if queue == nil {
			return 0, nil
		}
		var pendingWriteCount int64
		for _, sq := range queue.SubQueues() {
			if sq != nil {
				pendingWriteCount += sq.getPendingDataCount()
			}
		}
		return pendingWriteCount, nil
	}
	// Standalone mode
	tsdb, tsdbErr := sr.loadTSDB(groupName)
	if tsdbErr != nil {
		return 0, fmt.Errorf("failed to load TSDB: %w", tsdbErr)
	}
	if tsdb == nil {
		return 0, fmt.Errorf("TSDB is nil for group %s", groupName)
	}
	segments, segmentsErr := tsdb.SelectSegments(timestamp.TimeRange{
		Start: time.Unix(0, 0),
		End:   time.Unix(0, timestamp.MaxNanoTime),
	}, false)
	if segmentsErr != nil {
		return 0, fmt.Errorf("failed to select segments: %w", segmentsErr)
	}
	var pendingWriteCount int64
	for _, segment := range segments {
		tables, _ := segment.Tables()
		for _, tst := range tables {
			pendingWriteCount += tst.getPendingDataCount()
		}
		segment.DecRef()
	}
	return pendingWriteCount, nil
}

func (sr *schemaRepo) collectPendingSyncInfo(groupName string) (partCount int64, totalSizeBytes int64, err error) {
	if sr == nil || sr.Repository == nil {
		return 0, 0, fmt.Errorf("schema repository is not initialized")
	}
	// Only liaison nodes collect pending sync info
	if sr.role != databasev1.Role_ROLE_LIAISON {
		return 0, 0, nil
	}
	queue, queueErr := sr.loadQueue(groupName)
	if queueErr != nil {
		return 0, 0, fmt.Errorf("failed to load queue: %w", queueErr)
	}
	if queue == nil {
		return 0, 0, nil
	}
	for _, sq := range queue.SubQueues() {
		if sq != nil {
			snapshot := sq.currentSnapshot()
			if snapshot != nil {
				for _, pw := range snapshot.parts {
					if pw.mp == nil && pw.p != nil && pw.p.partMetadata.TotalCount > 0 {
						partCount++
						totalSizeBytes += int64(pw.p.partMetadata.CompressedSizeBytes)
					}
				}
				snapshot.decRef()
			}
		}
	}
	return partCount, totalSizeBytes, nil
}

func (sr *schemaRepo) loadQueue(groupName string) (*wqueue.Queue[*tsTable, option], error) {
	g, ok := sr.LoadGroup(groupName)
	if !ok {
		return nil, fmt.Errorf("group %s not found", groupName)
	}
	db := g.SupplyTSDB()
	if db == nil {
		return nil, fmt.Errorf("queue for group %s not found", groupName)
	}
	return db.(*wqueue.Queue[*tsTable, option]), nil
}

var _ resourceSchema.ResourceSupplier = (*supplier)(nil)

// supplier is the supplier for standalone service.
type supplier struct {
	metadata   metadata.Repo
	omr        observability.MetricsRegistry
	pm         protector.Memory
	l          *logger.Logger
	schemaRepo *schemaRepo
	nodeLabels map[string]string
	path       string
	option     option
}

func newSupplier(path string, svc *standalone, nodeLabels map[string]string) *supplier {
	if svc.pm == nil {
		svc.l.Panic().Msg("CRITICAL: svc.pm is nil in newSupplier")
	}
	opt := svc.option
	opt.protector = svc.pm

	if opt.protector == nil {
		svc.l.Panic().Msg("CRITICAL: opt.protector is still nil after assignment")
	}
	return &supplier{
		metadata:   svc.metadata,
		omr:        svc.omr,
		pm:         svc.pm,
		l:          svc.l,
		nodeLabels: nodeLabels,
		schemaRepo: &svc.schemaRepo,
		path:       path,
		option:     opt,
	}
}

func (s *supplier) OpenResource(spec resourceSchema.Resource) (resourceSchema.IndexListener, error) {
	traceSchema := spec.Schema().(*databasev1.Trace)
	return openTrace(traceSchema, s.l, s.pm, s.schemaRepo, s.option.vectorized), nil
}

func (s *supplier) ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.metadata.TraceRegistry().GetTrace(ctx, md)
}

func (s *supplier) OpenDB(groupSchema *commonv1.Group) (resourceSchema.DB, error) {
	name := groupSchema.Metadata.Name
	p := common.Position{
		Module:   "trace",
		Database: name,
	}
	ro := groupSchema.ResourceOpts
	if ro == nil {
		return nil, fmt.Errorf("no resource opts in group %s", name)
	}
	shardNum := ro.ShardNum
	ttl := ro.Ttl
	segInterval := ro.SegmentInterval
	// Non-zero default so the idle-segment reclaimer ticker actually starts
	// (storage/rotation.go gates it on >=1s). Staged Close paths override below.
	segmentIdleTimeout := time.Hour
	disableRetention := false
	disableRotation := false
	foundMatched := false
	if len(ro.Stages) > 0 && len(s.nodeLabels) > 0 {
		var ttlNum uint32
		for i, st := range ro.Stages {
			if st.Ttl.Unit != ro.Ttl.Unit {
				return nil, fmt.Errorf("ttl unit %s is not consistent with stage %s", ro.Ttl.Unit, st.Ttl.Unit)
			}
			selector, err := pub.ParseLabelSelector(st.NodeSelector)
			if err != nil {
				return nil, errors.WithMessagef(err, "failed to parse node selector %s", st.NodeSelector)
			}
			ttlNum += st.Ttl.Num
			if !selector.Matches(s.nodeLabels) {
				continue
			}
			foundMatched = true
			ttl.Num += ttlNum
			shardNum = st.ShardNum
			segInterval = st.SegmentInterval
			if st.Close {
				segmentIdleTimeout = 5 * time.Minute
			}
			disableRetention = i+1 < len(ro.Stages)
			disableRotation = true
			break
		}
		if !foundMatched {
			disableRetention = true
			disableRotation = true
		}
	}
	// isHot marks the Hot stage: a group with no staging at all, a node with no
	// labels (so staging cannot apply), or a node that matched no stage selector.
	isHot := len(ro.Stages) == 0 || len(s.nodeLabels) == 0 || !foundMatched
	opt := s.option
	opt.isHot = isHot
	group := groupSchema.Metadata.Name
	opts := storage.TSDBOpts[*tsTable, option]{
		ShardNum:                       shardNum,
		Location:                       path.Join(s.path, group),
		TSTableCreator:                 newTSTable,
		TableMetrics:                   s.newMetrics(p),
		SegmentInterval:                storage.MustToIntervalRule(segInterval),
		TTL:                            storage.MustToIntervalRule(ttl),
		Option:                         opt,
		SeriesIndexFlushTimeoutSeconds: s.option.flushTimeout.Nanoseconds() / int64(time.Second),
		SeriesIndexCacheMaxBytes:       int(s.option.seriesCacheMaxSize),
		StorageMetricsFactory:          s.omr.With(storageScope.ConstLabels(meter.ToLabelPairs(common.DBLabelNames(), p.DBLabelValues()))),
		SegmentIdleTimeout:             segmentIdleTimeout,
		DisableRetention:               disableRetention,
		DisableRotation:                disableRotation,
		MemoryLimit:                    s.pm.GetLimit(),
	}
	return storage.OpenTSDB(
		common.SetPosition(context.Background(), func(_ common.Position) common.Position {
			return p
		}),
		opts, nil, group,
	)
}

// queueSupplier is the supplier for liaison service.
type queueSupplier struct {
	metadata              metadata.Repo
	omr                   observability.MetricsRegistry
	pm                    protector.Memory
	traceDataNodeRegistry grpc.NodeRegistry
	l                     *logger.Logger
	schemaRepo            *schemaRepo
	handoffCtrl           *handoffController
	path                  string
	option                option
}

func newQueueSupplier(path string, svc *liaison, traceDataNodeRegistry grpc.NodeRegistry) *queueSupplier {
	if svc.pm == nil {
		svc.l.Panic().Msg("CRITICAL: svc.pm is nil in newSupplier")
	}
	opt := svc.option
	opt.protector = svc.pm

	if opt.protector == nil {
		svc.l.Panic().Msg("CRITICAL: opt.protector is still nil after assignment")
	}
	return &queueSupplier{
		metadata:              svc.metadata,
		omr:                   svc.omr,
		pm:                    svc.pm,
		traceDataNodeRegistry: traceDataNodeRegistry,
		l:                     svc.l,
		path:                  path,
		option:                opt,
		schemaRepo:            &svc.schemaRepo,
		handoffCtrl:           svc.handoffCtrl,
	}
}

func (qs *queueSupplier) OpenResource(spec resourceSchema.Resource) (resourceSchema.IndexListener, error) {
	traceSchema := spec.Schema().(*databasev1.Trace)
	return openTrace(traceSchema, qs.l, qs.pm, qs.schemaRepo, qs.option.vectorized), nil
}

func (qs *queueSupplier) ResourceSchema(md *commonv1.Metadata) (resourceSchema.ResourceSchema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return qs.metadata.TraceRegistry().GetTrace(ctx, md)
}

func (qs *queueSupplier) OpenDB(groupSchema *commonv1.Group) (resourceSchema.DB, error) {
	name := groupSchema.Metadata.Name
	p := common.Position{
		Module:   "trace",
		Database: name,
	}
	ro := groupSchema.ResourceOpts
	if ro == nil {
		return nil, fmt.Errorf("no resource opts in group %s", name)
	}
	shardNum := ro.ShardNum
	group := groupSchema.Metadata.Name
	metrics, metricsFactory := qs.newMetrics(p)
	// The liaison write queue has no node labels and performs no staging, so it
	// is always the Hot stage by the same isHot rule used in supplier.OpenDB.
	opt := qs.option
	opt.isHot = true
	opts := wqueue.Opts[*tsTable, option]{
		Group:           group,
		ShardNum:        shardNum,
		SegmentInterval: storage.MustToIntervalRule(ro.SegmentInterval),
		Location:        path.Join(qs.path, group),
		Option:          opt,
		Metrics:         metrics,
		MetricsFactory:  metricsFactory,
		SubQueueCreator: func(fileSystem fs.FileSystem, root string, position common.Position,
			l *logger.Logger, option option, metrics any, group string, shardID common.ShardID, getNodes func() []string,
		) (*tsTable, error) {
			return newWriteQueue(fileSystem, root, position, l, option, metrics, group, shardID, getNodes, qs.handoffCtrl)
		},
		GetNodes: func(shardID common.ShardID) []string {
			copies := ro.Replicas + 1
			nodes, err := qs.traceDataNodeRegistry.LocateAll(group, uint32(shardID), int(copies))
			if err != nil {
				qs.l.Error().Err(err).Str("group", group).Uint32("shard", uint32(shardID)).Msg("failed to locate nodes")
				return nil
			}
			return nodes
		},
	}
	return wqueue.Open(
		common.SetPosition(context.Background(), func(_ common.Position) common.Position {
			return p
		}),
		opts, group,
	)
}
