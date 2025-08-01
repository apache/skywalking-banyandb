# File-Based Measure Migration Design

## Overview

This document outlines the design for refactoring measure migration from a query-based approach to a file-based streaming approach, following the successful pattern established in stream migration.

## Current Architecture Analysis

### Current Query-Based Approach
- **File**: `steps.go:230-296` (`migrateMeasure` function)
- **Method**: Individual record querying via `MeasureQueryOptions`
- **Transfer**: Message-based publishing via `queue.BatchPublisher`
- **Granularity**: Per-record processing
- **Performance**: High overhead due to individual record serialization

### Target File-Based Approach
- **Pattern**: Similar to `file_migration_visitor.go` stream migration
- **Method**: Direct file streaming of measure parts
- **Transfer**: Chunked streaming via `queue.ChunkedSyncClient`
- **Granularity**: Per-part file processing
- **Performance**: Bulk file transfer optimization

## Measure Storage Architecture

Based on analysis of `measure/metadata.go` and `measure/tstable.go`, measures are stored at **group level**, not per-measure:

### Group-Level Storage Structure
```
measure_data/
└── <group_name>/           # Group directory (e.g., "sw_metric")
    ├── 000000000000abc1/   # Part directory (epoch-based ID)
    │   ├── metadata.json   # Part metadata 
    │   ├── primary.bin     # Primary index
    │   ├── meta.bin        # Block metadata (compressed)
    │   ├── timestamps.bin  # Timestamp data
    │   ├── fv.bin          # Field values  
    │   ├── <tagfamily>.tf  # Tag family data
    │   └── <tagfamily>.tfm # Tag family metadata
    ├── 000000000000abc2/   # Another part directory
    └── snapshot_xyz.snp    # Snapshot files
```

### Key Architecture Points
1. **Group-Level Storage**: All measures in a group share the same TSDB storage (`path.Join(s.path, group)`)
2. **Single TSDB per Group**: `g.SupplyTSDB()` returns one TSDB instance per group
3. **Part-Based Organization**: Data stored in epoch-based part directories
4. **Mixed Data**: Each part contains data from **all measures** in the group
5. **No Per-Measure Separation**: Cannot distinguish individual measures at file level

## Proposed Design

### 1. File Structure: `measure_migration_visitor.go`

Create a new visitor implementing measure-specific file migration, following the exact pattern from `streamMigrationVisitor.VisitPart`:

```go
// measureMigrationVisitor implements file-based migration for measure data
type measureMigrationVisitor struct {
    selector       node.Selector                      // Node selector
    client         queue.Client                       // Queue client
    chunkedClients map[string]queue.ChunkedSyncClient // Per-node chunked sync clients
    logger         *logger.Logger
    progress       *Progress                          // Progress tracker
    lfs            fs.FileSystem
    group          string
    targetShardNum uint32                             // Target shard count
    replicas       uint32                             // Replica count
    chunkSize      int                                // Chunk size for streaming
}

// Key methods based on streamMigrationVisitor pattern:
func (mv *measureMigrationVisitor) VisitSeries(segmentTR *timestamp.TimeRange, seriesIndexPath string, shardIDs []common.ShardID) error // Not used for measures
func (mv *measureMigrationVisitor) VisitPart(segmentTR *timestamp.TimeRange, sourceShardID common.ShardID, partPath string) error // Core migration logic
```

### 2. Integration Function: `measure_migration_integration.go`

```go
// migrateMeasureWithFileBasedAndProgress performs file-based measure migration
// NOTE: measures parameter is removed since all measures in a group share the same parts
func migrateMeasureWithFileBasedAndProgress(
    tsdbRootPath string,
    timeRange timestamp.TimeRange,
    group *commonv1.Group,
    nodeLabels map[string]string,
    nodes []*databasev1.Node,
    metadata metadata.Repo,
    logger *logger.Logger,
    progress *Progress,
    chunkSize int,
) error
```

### 3. Core VisitPart Implementation

The key difference from stream migration is that measures use the existing visitor pattern with `storage.VisitSegmentsInTimeRange` which calls `VisitPart` for each part. We adapt `streamMigrationVisitor.VisitPart` for measure data:

#### VisitPart Method (adapted from streamMigrationVisitor.VisitPart:189-242)
```go
func (mv *measureMigrationVisitor) VisitPart(segmentTR *timestamp.TimeRange, sourceShardID common.ShardID, partPath string) error {
    // 1. Parse part ID from directory name (similar to stream.ParsePartMetadata but for measures)
    partID, err := mv.parsePartIDFromPath(partPath)
    if err != nil {
        return fmt.Errorf("failed to parse part ID from path %s: %w", partPath, err)
    }

    // 2. Check if this part has already been completed
    if mv.progress.IsMeasurePartCompleted(mv.group, partID) {
        mv.logger.Debug().Uint64("part_id", partID).Str("group", mv.group).Msg("part already completed, skipping")
        return nil
    }

    // 3. Calculate target shard ID based on source shard ID mapping
    targetShardID := mv.calculateTargetShardID(uint32(sourceShardID))

    // 4. Log migration start
    mv.logger.Info().
        Uint64("part_id", partID).
        Uint32("source_shard", uint32(sourceShardID)).
        Uint32("target_shard", targetShardID).
        Str("part_path", partPath).
        Str("group", mv.group).
        Msg("migrating measure part")

    // 5. Create file readers (adapted from stream.CreatePartFileReaderFromPath)
    files, release := mv.createMeasurePartFileReaders(partPath)
    defer release()

    // 6. Create streaming part data
    partData := mv.createStreamingPartData(partID, targetShardID, files, segmentTR)

    // 7. Stream entire part to target shard replicas
    if err := mv.streamPartToTargetShard(partData); err != nil {
        errorMsg := fmt.Sprintf("failed to stream part to target shard: %v", err)
        mv.progress.MarkMeasurePartError(mv.group, partID, errorMsg)
        return fmt.Errorf("failed to stream part to target shard: %w", err)
    }

    // 8. Mark part as completed in progress tracker
    mv.progress.MarkMeasurePartCompleted(mv.group, partID)
    
    // 9. Log completion
    mv.logger.Info().
        Uint64("part_id", partID).
        Str("group", mv.group).
        Int("completed_parts", mv.progress.GetMeasurePartProgress(mv.group)).
        Int("total_parts", mv.progress.GetMeasurePartCount(mv.group)).
        Msg("measure part migration completed successfully")

    return nil
}
```

#### Supporting Methods
```go
// parsePartIDFromPath extracts part ID from the part directory name
func (mv *measureMigrationVisitor) parsePartIDFromPath(partPath string) (uint64, error) {
    partDirName := filepath.Base(partPath)
    return strconv.ParseUint(partDirName, 16, 64)
}

// calculateTargetShardID maps source shard ID to target shard ID (same as stream)
func (mv *measureMigrationVisitor) calculateTargetShardID(sourceShardID uint32) uint32 {
    return sourceShardID % mv.targetShardNum
}

// createMeasurePartFileReaders creates file readers for all files in a measure part
// Based on measure/part.go structure: metadata.json, primary.bin, meta.bin, timestamps.bin, fv.bin, *.tf, *.tfm
func (mv *measureMigrationVisitor) createMeasurePartFileReaders(partPath string) ([]queue.FileInfo, func()) {
    var files []queue.FileInfo
    var readers []fs.SequentialReader

    // Core measure files
    coreFiles := []string{"metadata.json", "primary.bin", "meta.bin", "timestamps.bin", "fv.bin"}
    for _, filename := range coreFiles {
        if reader := mv.openFileReader(partPath, filename); reader != nil {
            readers = append(readers, reader)
            files = append(files, queue.FileInfo{Name: filename, Reader: reader})
        }
    }

    // Tag family files (*.tf and *.tfm)
    entries := mv.lfs.ReadDir(partPath)
    for _, entry := range entries {
        if !entry.IsDir() && (strings.HasSuffix(entry.Name(), ".tf") || strings.HasSuffix(entry.Name(), ".tfm")) {
            if reader := mv.openFileReader(partPath, entry.Name()); reader != nil {
                readers = append(readers, reader)
                files = append(files, queue.FileInfo{Name: entry.Name(), Reader: reader})
            }
        }
    }

    // Return cleanup function
    release := func() {
        for _, reader := range readers {
            reader.Close()
        }
    }
    
    return files, release
}
```

### 4. Integration with Existing Visitor Pattern

The measure migration leverages the existing `storage.VisitSegmentsInTimeRange` infrastructure:

#### Integration Function
```go
// migrateMeasureWithFileBasedAndProgress performs file-based measure migration
func migrateMeasureWithFileBasedAndProgress(
    tsdbRootPath string,
    timeRange timestamp.TimeRange,
    group *commonv1.Group,
    nodeLabels map[string]string,
    nodes []*databasev1.Node,
    metadata metadata.Repo,
    logger *logger.Logger,
    progress *Progress,
    chunkSize int,
) error {
    // Parse group configuration
    shardNum, replicas, ttl, selector, client, err := parseGroup(group, nodeLabels, nodes, logger, metadata)
    if err != nil {
        return err
    }
    defer client.GracefulStop()

    // Convert TTL to IntervalRule
    intervalRule := storage.MustToIntervalRule(ttl)

    // Create measure migration visitor
    visitor := newMeasureMigrationVisitor(group, shardNum, replicas, selector, client, logger, progress, chunkSize)
    defer visitor.Close()

    // Use existing segment visitor infrastructure
    return storage.VisitSegmentsInTimeRange(tsdbRootPath, timeRange, visitor, intervalRule)
}
```

### 5. Service Integration

The measure migration integrates seamlessly with the existing lifecycle service by replacing the query-based approach:

#### Update `service.go`
```go
func (l *lifecycleService) processMeasureGroup(ctx context.Context, g *commonv1.Group, measureSVC measure.Service,
    nodes []*databasev1.Node, labels map[string]string, progress *Progress,
) {
    // Check if file-based migration is enabled
    if l.useFileBasedMigration {
        l.processMeasureGroupFileBased(ctx, g, measureSVC, nodes, labels, progress)
    } else {
        l.processMeasureGroupQueryBased(ctx, g, measureSVC, nodes, labels, progress) // Keep existing logic
    }
}

func (l *lifecycleService) processMeasureGroupFileBased(ctx context.Context, g *commonv1.Group, measureSVC measure.Service,
    nodes []*databasev1.Node, labels map[string]string, progress *Progress,
) {
    tr := measureSVC.GetRemovalSegmentsTimeRange(g.Metadata.Name)
    if tr.Start.IsZero() && tr.End.IsZero() {
        l.l.Info().Msgf("no removal segments time range for group %s, skipping measure migration", g.Metadata.Name)
        progress.MarkGroupCompleted(g.Metadata.Name)
        return
    }

    // Get measure snapshot directory
    measureDir := progress.SnapshotMeasureDir
    if measureDir == "" {
        l.l.Error().Msgf("no measure snapshot directory available for group: %s", g.Metadata.Name)
        return
    }

    // Perform file-based migration (no need for measure list since all measures share same parts)
    err = migrateMeasureWithFileBasedAndProgress(
        filepath.Join(measureDir, g.Metadata.Name),
        *tr,
        g,
        labels,
        nodes,
        l.metadata,
        l.l,
        progress,
        int(l.chunkSize),
    )
    if err != nil {
        l.l.Error().Err(err).Msgf("file-based measure migration failed for group: %s", g.Metadata.Name)
        return
    }

    l.l.Info().Msgf("completed file-based measure migration for group: %s", g.Metadata.Name)
    l.deleteExpiredMeasureSegments(ctx, g, tr, progress)
    progress.MarkGroupCompleted(g.Metadata.Name)
}
```

### 6. Progress Tracking Enhancement

#### Extend `progress.go`
```go
// Add measure part-specific progress tracking
type Progress struct {
    // ... existing fields ...
    
    // Measure part progress tracking
    MeasurePartCounts    map[string]int            `json:"measure_part_counts"`
    MeasurePartProgress  map[string]int            `json:"measure_part_progress"`
    MeasurePartCompleted map[string]map[uint64]bool `json:"measure_part_completed"`
    MeasurePartErrors    map[string]map[uint64]string `json:"measure_part_errors"`
}

func (p *Progress) IsMeasurePartCompleted(group string, partID uint64) bool
func (p *Progress) MarkMeasurePartCompleted(group string, partID uint64)
func (p *Progress) MarkMeasurePartError(group string, partID uint64, error string)
func (p *Progress) SetMeasurePartCount(group string, totalParts int)
func (p *Progress) GetMeasurePartProgress(group string) int
```

### 7. Queue Topic Extension

#### Add New Topic in `api/data/topics.go`
```go
const (
    // ... existing topics ...
    TopicMeasurePartSync = Topic("measure-part-sync")
)
```

### 8. Configuration Flag

#### Add File-Based Migration Toggle
```go
func (l *lifecycleService) FlagSet() *run.FlagSet {
    flagS := run.NewFlagSet(l.Name())
    // ... existing flags ...
    flagS.BoolVar(&l.useFileBasedMigration, "file-based-migration", true, 
        "Use file-based migration for measures (default: true, fallback to query-based)")
    return flagS
}
```

## Implementation Benefits

### Corrected Understanding Impact
With the correct understanding that **all measures in a group share the same parts**, the file-based approach becomes even more beneficial:

- **Massive efficiency gain**: Instead of querying each measure individually and reconstructing write requests, we stream the entire part containing **all measures at once**
- **~100-1000x performance improvement** expected since we eliminate per-measure processing overhead entirely
- **Single part = All measures**: Each part migration covers all measures in the group simultaneously
- **Reduced complexity**: No need to track per-measure progress, only per-part progress
- **Better resource utilization**: Stream complete data files rather than rebuilding data point by point

### Performance Advantages
1. **Bulk Transfer**: Stream entire part files containing all measures instead of individual records
2. **Eliminated Per-Measure Processing**: No need to query and process each measure separately
3. **Parallel Processing**: Multiple parts can be streamed concurrently
4. **Atomic Group Operations**: Single part transfer covers entire group's measures

### Reliability Improvements
1. **Atomic Operations**: Either entire part (with all measures) succeeds or fails
2. **Resume Capability**: Can restart from failed parts
3. **Error Isolation**: Part-level error handling covers all measures
4. **Consistent State**: No partial measure migrations within a part

### Scalability Benefits
1. **Memory Efficiency**: Stream files directly without data reconstruction
2. **Network Optimization**: Bulk transfer vs thousands of individual messages
3. **Resource Management**: Better control over concurrent transfers
4. **Storage Efficiency**: Direct file-to-file transfers

### Real-World Impact
For a group with 10 measures and 1000 parts:
- **Query-based**: 10,000 individual measure queries + 1,000,000 write requests  
- **File-based**: 1,000 part file transfers (covers all measures automatically)

This represents a **1000x reduction** in operations and network round-trips.

## Migration Strategy

### Phase 1: Implementation
1. Create `measure_migration_visitor.go`
2. Create `measure_migration_integration.go`
3. Extend progress tracking for measure parts
4. Add new queue topic for measure part sync

### Phase 2: Integration
1. Update `service.go` with file-based measure processing
2. Add configuration flag for migration method selection
3. Implement fallback to query-based approach

### Phase 3: Testing & Validation
1. Unit tests for measure part discovery and streaming
2. Integration tests comparing query-based vs file-based results
3. Performance benchmarks and memory usage analysis
4. End-to-end lifecycle migration testing

### Phase 4: Rollout
1. Default to file-based migration with query-based fallback
2. Monitor performance improvements and error rates
3. Deprecate query-based approach after validation period

## Risk Mitigation

### Compatibility
- Maintain query-based fallback for compatibility
- Validate file-based results match query-based results
- Support rollback via configuration flag

### Error Handling
- Implement robust error recovery at part level
- Maintain detailed error logging and progress tracking
- Support manual intervention for failed parts

### Performance
- Implement streaming throttling to prevent resource exhaustion
- Monitor memory usage during large part transfers
- Add metrics for transfer rates and success rates

This design provides a comprehensive approach to migrating measure data using the proven file-based streaming pattern while maintaining compatibility and reliability.