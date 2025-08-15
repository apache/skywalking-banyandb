# Secondary Index File System (sidx) Design

## Overview

The Secondary Index File System (sidx) is a production-ready, high-performance file system abstraction inspired by BanyanDB's stream module that uses user-provided int64 keys as ordering values instead of timestamps. This enables efficient secondary indexing for various data types and use cases beyond time-series data.

**Key Production Features:**
- **Memory Management**: Comprehensive object pooling and memory optimization
- **Thread Safety**: Atomic operations and reference counting for concurrent access
- **Resource Management**: Disk space reservation and backpressure control
- **Fault Tolerance**: Corruption detection, recovery procedures, and graceful degradation
- **Operational Excellence**: Complete observability, metrics, and administrative APIs

## Core Design Principle

**The int64 ordering key is PROVIDED BY THE USER/CLIENT**, not generated or interpreted by sidx. The sidx system treats it as an opaque ordering value and only performs numerical comparisons for storage organization and query processing.

### Key Characteristics

- **User-Controlled**: Client decides what the int64 represents
- **Opaque to sidx**: System doesn't interpret meaning, only orders by numerical value
- **Monotonic Ordering**: sidx only cares about numerical comparison (`<`, `>`, `==`)
- **No Semantic Validation**: sidx doesn't validate what the key represents
- **No Key Generation**: sidx never creates or transforms keys

## Architecture

### Replacing Timestamp-Specific Logic

#### Original Stream Module (Timestamp-based)
```go
// Stream knows these are timestamps and interprets them
type partMetadata struct {
    MinTimestamp int64  // System knows this is time
    MaxTimestamp int64  // System knows this is time
    // ... time-specific logic
}

// Stream might validate time ranges, format for display, etc.
func (s *snapshot) getParts(minTime, maxTime int64) []*part {
    // Time-aware filtering logic
}
```

#### New sidx (User-Key-based)
```go
// sidx treats as opaque ordering values
type partMetadata struct {
    MinKey int64  // Just a number to sidx - no interpretation
    MaxKey int64  // Just a number to sidx - no interpretation
    // ... generic ordering logic
}

// Pure numerical comparison - no time semantics
func (s *snapshot) getParts(minKey, maxKey int64) []*part {
    // Generic key-range filtering
}
```

### Core Components

#### 1. Enhanced Part Structure with Reference Counting

```go
// partState represents the lifecycle state of a part
type partState int32

const (
    partStateActive partState = iota
    partStateRemoving
    partStateRemoved
)

// partWrapper provides thread-safe reference counting for parts
type partWrapper struct {
    p         *part
    refCount  int32     // Atomic reference counter
    state     int32     // Atomic state (partState)
    createdAt int64     // Creation timestamp for debugging
    size      uint64    // Cached size for resource management
}

// Thread-safe reference counting methods
func (pw *partWrapper) acquire() bool {
    for {
        refs := atomic.LoadInt32(&pw.refCount)
        if refs <= 0 {
            return false // Part being destroyed
        }
        if atomic.CompareAndSwapInt32(&pw.refCount, refs, refs+1) {
            return true
        }
    }
}

func (pw *partWrapper) release() {
    if atomic.AddInt32(&pw.refCount, -1) == 0 {
        pw.destroy()
    }
}

func (pw *partWrapper) markRemovable() {
    atomic.StoreInt32(&pw.state, int32(partStateRemoving))
}

func (pw *partWrapper) isRemovable() bool {
    return atomic.LoadInt32(&pw.state) == int32(partStateRemoving)
}

// Enhanced part structure with resource management
type part struct {
    // File readers with standardized naming
    primary     fs.Reader    // primary.bin - Block metadata
    data        fs.Reader    // data.bin - User data payloads  
    userKeys    fs.Reader    // keys.bin - User-provided int64 keys
    meta        fs.Reader    // meta.bin - Part metadata
    
    // File system and path
    fileSystem  fs.FileSystem
    path        string
    
    // Tag storage with individual tag files
    tagMetadata map[string]fs.Reader  // tag_<name>.tm files
    tags        map[string]fs.Reader  // tag_<name>.td files 
    tagFilters  map[string]fs.Reader  // tag_<name>.tf files
    
    // Cached metadata for performance
    primaryBlockMetadata []primaryBlockMetadata
    partMetadata         partMetadata
    
}

// Enhanced metadata with integrity and versioning
type partMetadata struct {
    // Size information
    CompressedSizeBytes   uint64
    UncompressedSizeBytes uint64
    TotalCount            uint64
    BlocksCount           uint64
    
    // Key range (replaces timestamp range)
    MinKey      int64     // Minimum user key in part
    MaxKey      int64     // Maximum user key in part
    
    // Identity
    ID          uint64    // Unique part identifier
}
```

#### 2. Enhanced Element Structure with Object Pooling

```go
// Enhanced element structure with reset capability for pooling
type element struct {
    seriesID  common.SeriesID
    userKey   int64      // The ordering key from user (replaces timestamp)
    elementID uint64     // Internal element identifier  
    data      []byte     // User payload data (pooled slice)
    tags      []tag      // Individual tags (pooled slice)
    
    // Internal fields for pooling
    pooled    bool       // Whether this element came from pool
}

// Reset clears element for reuse in object pool
func (e *element) reset() {
    e.seriesID = 0
    e.userKey = 0
    e.elementID = 0
    if cap(e.data) <= maxPooledSliceSize {
        e.data = e.data[:0]  // Reuse slice if not too large
    } else {
        e.data = nil  // Release oversized slice
    }
    if cap(e.tags) <= maxPooledTagCount {
        // Reset each tag for reuse
        for i := range e.tags {
            e.tags[i].reset()
        }
        e.tags = e.tags[:0]
    } else {
        e.tags = nil
    }
    e.pooled = false
}

// Enhanced elements collection with pooling
type elements struct {
    seriesIDs []common.SeriesID  // Pooled slice
    userKeys  []int64           // Pooled slice
    elementIDs []uint64         // Pooled slice
    data      [][]byte          // Pooled slice of slices
    tags      [][]tag           // Pooled slice of tag slices
    
    // Pool management
    pooled    bool              // Whether from pool
    capacity  int               // Original capacity for pool return
}

// Reset elements collection for pooling
func (e *elements) reset() {
    e.seriesIDs = e.seriesIDs[:0]
    e.userKeys = e.userKeys[:0] 
    e.elementIDs = e.elementIDs[:0]
    e.data = e.data[:0]
    e.tags = e.tags[:0]
    e.pooled = false
}

// Elements are sorted by: seriesID first, then userKey
func (e *elements) Less(i, j int) bool {
    if e.seriesIDs[i] != e.seriesIDs[j] {
        return e.seriesIDs[i] < e.seriesIDs[j]
    }
    return e.userKeys[i] < e.userKeys[j]  // Pure numerical comparison
}
```

#### 3. Memory Management and Object Pooling

**Key Components:**
- **Object Pools**: sync.Pool for elements, tags, requests, and I/O buffers
- **Memory Tracking**: Atomic counters for usage monitoring and leak detection
- **Reset Methods**: All pooled objects implement reset() for safe reuse
- **Size Limits**: Prevents pooling of oversized objects (>1MB)

**Pooled Objects:**
- Elements and element collections
- Tag structures and value slices
- Write/Query requests and responses
- Read/Write buffers for I/O operations

**Benefits:**
- Reduced GC pressure under high load
- Consistent memory usage patterns
- Prevention of memory leaks through proper reset procedures

#### 4. Resource Management and Backpressure Control

**Key Components:**
- **Disk Space Reservation**: Pre-allocates space for flush/merge operations to prevent out-of-space failures
- **Memory Limiting**: Enforces memory usage limits with configurable warning and backpressure thresholds  
- **Concurrency Control**: Limits concurrent operations (flush, merge, query) using semaphores
- **Adaptive Backpressure**: Four-level system (None/Moderate/Severe/Critical) based on resource usage

**Resource Limits:**
- Memory: Maximum usage, part count, warning ratios
- Disk: Maximum usage, minimum free space, warning thresholds
- Concurrency: Maximum concurrent operations per type
- Rate: Maximum writes/queries per second

**Backpressure Behavior:**
- **80-90%**: Minor delays in processing
- **90-95%**: Significant delays and throttling
- **95%+**: Drop new writes, emergency mode

**Benefits:**
- Prevents resource exhaustion failures
- Maintains system stability under load
- Provides graceful degradation

#### 5. Error Handling and Recovery

**Error Categories:**
- **Corruption**: Data integrity violations, format errors
- **Resource**: Memory/disk exhaustion, I/O failures  
- **Validation**: Key ordering violations, format errors
- **Concurrency**: Race conditions, deadlocks

**Recovery Mechanisms:**
- **Detection**: Continuous health monitoring and integrity validation
- **Isolation**: Quarantine corrupted parts to prevent spread
- **Repair**: Automatic recovery for repairable corruption
- **Graceful Degradation**: Continue operation with reduced functionality

**Key Components:**
- **Structured Errors**: Detailed error context for diagnosis
- **Health Checker**: Continuous system monitoring
- **Validator**: File integrity and ordering verification
- **Quarantine System**: Isolation of corrupted data
- **Transactional Operations**: Atomic multi-file updates

#### 6. Tag Storage Architecture

sidx uses a **tag-based file design** where each tag is stored in its own set of files, unlike the stream module's tag-family grouping approach. This provides better isolation, modularity, and performance characteristics.

##### File Organization per Tag
Each tag gets three separate files within a part directory:
- **`tag_<name>.td`** - Tag data file containing encoded tag values
- **`tag_<name>.tm`** - Tag metadata file with encoding info, value type, and block offsets
- **`tag_<name>.tf`** - Tag filter file containing bloom filters for fast lookups

##### Tag Data Structures
```go
// Runtime tag representation
type tag struct {
    name      string
    values    [][]byte
    valueType pbv1.ValueType
    filter    *filter.BloomFilter  // For indexed tags
    min       []byte               // For int64 tags
    max       []byte               // For int64 tags
}

// Persistent tag metadata
type tagMetadata struct {
    name        string
    valueType   pbv1.ValueType
    dataBlock   dataBlock       // Offset/size in .td file
    filterBlock dataBlock       // Offset/size in .tf file
    min         []byte
    max         []byte
}
```

##### Standardized File Format

**Core Files (per part):**
- `primary.bin` - Block metadata with version headers
- `data.bin` - User data payloads with compression
- `keys.bin` - User-provided int64 ordering keys
- `meta.bin` - Part metadata
- `manifest.json` - Human-readable part manifest

**Tag Files (per tag per part):**
- `tag_<name>.td` - Tag data with encoding optimizations
- `tag_<name>.tm` - Tag metadata and block offsets
- `tag_<name>.tf` - Bloom filters for fast lookups

**File Format Features:**
- **Version Control**: Format versioning for backward compatibility
- **Version Control**: Format versioning for backward compatibility
- **Format Validation**: File format and structure verification
- **Atomic Updates**: Transactional multi-file operations
- **Compression Support**: Configurable compression algorithms

##### Benefits of Tag-Based Design
- **Isolation**: Each tag is completely independent, reducing coupling
- **Selective Loading**: Can load only the tags needed for a query
- **Parallel Processing**: Tags can be processed independently for better concurrency
- **Schema Evolution**: Adding/removing tags doesn't affect existing tags
- **Cache Efficiency**: Related data for a single tag is stored contiguously
- **Debugging**: Each tag's data is in separate, identifiable files

#### 4. Data Storage Architecture

sidx separates user data payloads from metadata to enable efficient querying and data organization.

##### File Organization
- **`data.bin`** - Contains the actual user data payloads (compressed)
- **`primary.bin`** - Contains metadata for all data files including:
  - Data block metadata (offset/size in data.bin)
  - Key block metadata (offset/size in keys.bin)  
  - Tag block metadata (references to tag files)

##### Data Block Structure
```go
// Metadata for a block of user data in data.bin
type dataBlockMetadata struct {
    offset uint64  // Offset in data.bin file
    size   uint64  // Compressed size in bytes
}

// Enhanced primary block metadata with data references
type primaryBlockMetadata struct {
    seriesID    common.SeriesID
    minKey      int64                      // Minimum user key in block
    maxKey      int64                      // Maximum user key in block
    dataBlock   dataBlockMetadata          // Reference to data in data.bin
    keysBlock   dataBlockMetadata          // Reference to keys in keys.bin
    tagsBlocks  map[string]dataBlockMetadata // References to tag files
}
```

##### Benefits of Separate Data Storage
- **Efficient Metadata Scanning**: Can read block metadata without loading actual data
- **Selective Data Loading**: Load only the data blocks needed for a query
- **Independent Compression**: Optimize compression strategy per data type
- **Clean Separation**: Metadata operations don't require data I/O
- **Better Cache Utilization**: Metadata fits better in memory caches

#### 5. Snapshot Management
```go
type snapshot struct {
    parts   []*partWrapper
    epoch   uint64
    creator snapshotCreator
    ref     int32
}

// Generic key-range based part filtering
func (s *snapshot) getParts(dst []*part, minKey, maxKey int64) ([]*part, int) {
    var count int
    for _, p := range s.parts {
        pm := p.p.partMetadata
        // Pure numerical comparison - no time interpretation
        if maxKey < pm.MinKey || minKey > pm.MaxKey {
            continue
        }
        dst = append(dst, p.p)
        count++
    }
    return dst, count
}
```

#### 6. Enhanced API Design

**Write Interface:**
- Context support for cancellation and timeouts
- Batch operations for improved performance
- Validation options for data integrity
- Request tracking and tracing support

**Query Interface:**
- Range queries with inclusive/exclusive bounds
- Tag filtering with multiple operators (equals, in, regex)
- Iterator pattern for large result sets
- Memory usage limits and query timeouts
- Comprehensive result metadata

**Administrative Interface:**
- Health checks and system status monitoring
- Manual flush/merge/compaction triggers
- Integrity validation and corruption repair
- Configuration management
- Part information and debugging tools

**Core Features:**
- Full context.Context integration
- Structured error responses with detailed context
- Comprehensive metrics and observability hooks
- Request/response pooling for performance
- Atomic batch operations

### Implementation Details

#### Write Path
1. **Receive user key**: Accept int64 key from client without modification
2. **Element accumulation**: Group elements by seriesID and sort by user key
3. **Part creation**: Create memory parts when thresholds reached
4. **Block organization**: Organize elements into blocks within parts
5. **Flushing**: Persist memory parts to disk based on configurable policies
6. **Merging**: Combine multiple small parts into larger ones

#### Read Path
1. **Query validation**: Validate query parameters (not key semantics)
2. **Snapshot access**: Get current snapshot with reference counting
3. **Part filtering**: Select parts that overlap with query key range
4. **Block scanning**: Scan blocks within selected parts
5. **Result assembly**: Combine and order results by user keys

#### Part Organization
- **Part naming**: Hex-encoded epoch numbers (generation-based)
- **Key ranges**: Each part covers a range of user keys
- **Directory structure**: Similar to stream module (`000000001234abcd/`)
- **File structure**: Core files plus individual tag files

##### Part Directory Example
```
000000001234abcd/                    # Part directory (epoch-based name)
├── manifest.json                    # Part metadata and statistics
├── primary.bin                      # Block metadata (references to all data files)
├── data.bin                        # User data payloads (compressed)
├── meta.bin                        # Part-level metadata (compressed)
├── keys.bin                        # User-provided int64 ordering keys
├── tag_service_id.td               # Service ID tag data
├── tag_service_id.tm               # Service ID tag metadata
├── tag_service_id.tf               # Service ID tag filter (bloom)
├── tag_endpoint.td                 # Endpoint tag data
├── tag_endpoint.tm                 # Endpoint tag metadata
├── tag_endpoint.tf                 # Endpoint tag filter
├── tag_latency.td                  # Latency tag data
├── tag_latency.tm                  # Latency tag metadata
└── tag_latency.tf                  # Latency tag filter
```

#### Block Metadata Validation
```go
// Validation ensures monotonic ordering of blocks
func validatePrimaryBlockMetadata(blocks []primaryBlockMetadata) error {
    for i := 1; i < len(blocks); i++ {
        if blocks[i].seriesID < blocks[i-1].seriesID {
            return fmt.Errorf("unexpected block with smaller seriesID")
        }
        if blocks[i].seriesID == blocks[i-1].seriesID && blocks[i].minKey < blocks[i-1].minKey {
            return fmt.Errorf("unexpected block with smaller key")
        }
    }
    return nil
}
```

## Component Architecture: Introducer, Flusher, and Merger

The sidx design implements a **hybrid approach** that combines user-controlled timing with centralized snapshot management. Users decide when to trigger storage operations, but a background introducer loop ensures snapshot consistency and coordinates all updates.

### Core Design Philosophy

**User-Controlled Timing with Centralized Coordination**: Users control when storage operations occur, but the introducer loop manages snapshot updates to ensure consistency. This provides the benefits of both user control and reliable snapshot management.

### Component Relationships

#### 1. Introducer Loop (Snapshot Coordinator)
The introducer runs as a background goroutine that coordinates all snapshot updates through channel-based communication:

```go
// Introduction types for different operations
type memIntroduction struct {
    memPart *PartWrapper
    applied chan struct{}
}

type flusherIntroduction struct {
    flushed map[uint64]*PartWrapper
    applied chan struct{}
}

type mergerIntroduction struct {
    merged  map[uint64]struct{}
    newPart *PartWrapper
    applied chan struct{}
}

// Introducer loop coordinates all snapshot updates
func (sidx *SIDX) introducerLoop(
    flushCh chan *flusherIntroduction,
    mergeCh chan *mergerIntroduction,
    epoch uint64,
) {
    for {
        select {
        case <-sidx.closeCh:
            return
        case next := <-sidx.introductions:
            // Introduce new memory part
            sidx.introduceMemPart(next, epoch)
            epoch++
        case next := <-flushCh:
            // Introduce flushed parts, replacing memory parts
            sidx.introduceFlushed(next, epoch)
            epoch++
        case next := <-mergeCh:
            // Introduce merged part, replacing old parts
            sidx.introduceMerged(next, epoch)
            epoch++
        }
    }
}
```

**Key Characteristics**:
- **Background Goroutine**: Runs continuously to manage snapshot updates
- **Channel-based Communication**: Receives updates via channels for thread safety
- **Epoch Management**: Maintains ordering and consistency through epochs
- **Single Source of Truth**: Only the introducer can modify snapshots

#### 2. Flusher (User-Triggered Persistence)
The flusher provides a simple interface for user-controlled persistence:

```go
// Flusher provides user-triggered flush operations
type Flusher interface {
    // Flush triggers persistence of memory parts to disk
    // Returns error if flush operation fails
    Flush() error
}

// Internal implementation coordinates with introducer
func (f *flusher) Flush() error {
    // Determine which memory parts to flush
    memParts := f.sidx.getMemPartsToFlush()
    if len(memParts) == 0 {
        return nil
    }
    
    // Persist parts to disk
    flushedParts := make(map[uint64]*PartWrapper)
    for _, mp := range memParts {
        part, err := f.flushMemPartToDisk(mp)
        if err != nil {
            return err
        }
        flushedParts[mp.ID()] = part
    }
    
    // Send introduction to coordinator
    intro := &flusherIntroduction{
        flushed: flushedParts,
        applied: make(chan struct{}),
    }
    
    f.sidx.flushCh <- intro
    <-intro.applied // Wait for introduction to complete
    return nil
}
```

**User Control**:
- **When to Flush**: User calls `Flush()` when needed (memory pressure, durability, etc.)
- **Simple Interface**: Single function without parameters
- **Internal Logic**: Flusher decides what to flush based on current state

#### 3. Merger (User-Triggered Compaction)
The merger provides a simple interface for user-controlled compaction:

```go
// Merger provides user-triggered merge operations
type Merger interface {
    // Merge triggers compaction of parts to optimize storage
    // Returns error if merge operation fails
    Merge() error
}

// Internal implementation coordinates with introducer
func (m *merger) Merge() error {
    // Determine which parts to merge
    parts := m.sidx.getPartsToMerge()
    if len(parts) < 2 {
        return nil // No merge needed
    }
    
    // Perform merge operation
    mergedPart, err := m.mergePartsToDisk(parts)
    if err != nil {
        return err
    }
    
    // Track which parts were merged
    mergedIDs := make(map[uint64]struct{})
    for _, part := range parts {
        mergedIDs[part.ID()] = struct{}{}
    }
    
    // Send introduction to coordinator
    intro := &mergerIntroduction{
        merged:  mergedIDs,
        newPart: mergedPart,
        applied: make(chan struct{}),
    }
    
    m.sidx.mergeCh <- intro
    <-intro.applied // Wait for introduction to complete
    return nil
}
```

**User Control**:
- **When to Merge**: User calls `Merge()` when optimization is needed
- **Simple Interface**: Single function without parameters
- **Internal Logic**: Merger decides what to merge based on current state

### Operational Flow

#### Initialization and Background Loop

```go
// SIDX initialization starts the introducer loop
func NewSIDX(options Options) *SIDX {
    sidx := &SIDX{
        introductions: make(chan *memIntroduction),
        flushCh:       make(chan *flusherIntroduction),
        mergeCh:       make(chan *mergerIntroduction),
        closeCh:       make(chan struct{}),
        flusher:       newFlusher(),
        merger:        newMerger(),
    }
    
    // Start the introducer loop as background goroutine
    go sidx.introducerLoop(sidx.flushCh, sidx.mergeCh, 0)
    
    return sidx
}
```

#### Write → Automatic Introduction

```go
// Write path automatically handles memory part introduction
func (sidx *SIDX) Write(req WriteRequest) error {
    // 1. Add element to current memory part
    memPart, created := sidx.addToMemPart(req)
    
    // 2. If new memory part was created, introduce it automatically
    if created {
        intro := &memIntroduction{
            memPart: memPart,
            applied: make(chan struct{}),
        }
        
        // Send to introducer loop
        sidx.introductions <- intro
        <-intro.applied // Wait for introduction to complete
    }
    
    return nil
}
```

#### User-Triggered Flush Operations

```go
// Example: User decides when to flush based on application needs
func (app *Application) manageStorage() {
    // User monitors memory usage and triggers flush
    if app.sidx.memoryUsage() > app.maxMemory {
        if err := app.sidx.flusher.Flush(); err != nil {
            app.logger.Error("flush failed", err)
        }
    }
    
    // User can also flush on shutdown for durability
    defer func() {
        app.sidx.flusher.Flush() // Ensure all data is persisted
    }()
}
```

#### User-Triggered Merge Operations

```go
// Example: User optimizes storage during maintenance windows
func (app *Application) optimizeStorage() error {
    // User decides when to merge based on query performance
    if app.sidx.partCount() > app.maxParts {
        return app.sidx.merger.Merge()
    }
    
    // User can schedule merges during low-load periods
    if app.isMaintenanceWindow() {
        return app.sidx.merger.Merge()
    }
    
    return nil
}
```

### Key Differences from Stream Module

| Aspect | Stream Module | SIDX Module |
|--------|---------------|-------------|
| **Execution Model** | Background loops for introducer, flusher, merger | Single introducer loop + user-triggered flush/merge |
| **Decision Making** | Automatic flush/merge based on timers/thresholds | User decides when to flush/merge |
| **Coordination** | Full async communication via channels | Hybrid: introducer uses channels, user calls are sync |
| **Lifecycle Management** | System-managed epochs, watchers, and policies | User-managed timing with system-managed epochs |
| **Resource Control** | System decides when to use CPU/I/O | User decides when to use CPU/I/O |
| **Interface Complexity** | Internal implementation details | Simple `Flush()` and `Merge()` interfaces |

### Benefits of Hybrid Design

1. **User Control with Safety**: Users control timing while introducer ensures snapshot consistency
2. **Simple Interface**: Single-function interfaces (`Flush()`, `Merge()`) without parameters
3. **Predictable Performance**: No unexpected background I/O, but reliable snapshot management
4. **Application Integration**: Operations can be coordinated with application lifecycle
5. **Efficient Coordination**: Channel-based introducer prevents race conditions
6. **Flexible Policies**: Users implement custom flush/merge policies while system handles coordination

### Implementation Considerations

```go
// Example user policy implementation
type StorageManager struct {
    sidx   *SIDX
    policy StoragePolicy
}

type StoragePolicy struct {
    // Flush triggers
    MaxMemoryParts    int
    MaxMemorySize     uint64
    FlushOnShutdown   bool
    
    // Merge triggers
    MaxPartCount      int
    MaintenanceWindow time.Duration
}

// User implements custom flush logic
func (sm *StorageManager) CheckAndFlush() error {
    if sm.sidx.memoryPartCount() >= sm.policy.MaxMemoryParts ||
       sm.sidx.memorySize() >= sm.policy.MaxMemorySize {
        return sm.sidx.flusher.Flush()
    }
    return nil
}

// User implements custom merge logic  
func (sm *StorageManager) CheckAndMerge() error {
    if sm.sidx.partCount() >= sm.policy.MaxPartCount ||
       sm.isMaintenanceWindow() {
        return sm.sidx.merger.Merge()
    }
    return nil
}

// Internal flusher implementation
type flusher struct {
    sidx *SIDX
}

func (f *flusher) getMemPartsToFlush() []*PartWrapper {
    // Internal logic to select memory parts for flushing
    // Could be all memory parts, or based on size/age criteria
}

// Internal merger implementation  
type merger struct {
    sidx *SIDX
}

func (m *merger) getPartsToMerge() []*PartWrapper {
    // Internal logic to select parts for merging
    // Could be based on size ratios, part count, key ranges, etc.
}
```

### Architecture Summary

This hybrid architecture provides:

1. **Centralized Coordination**: Single introducer loop manages all snapshot updates
2. **User Control**: Simple `Flush()` and `Merge()` interfaces give users timing control
3. **Thread Safety**: Channel-based communication prevents race conditions
4. **Simplicity**: No complex parameters or configuration - internal logic handles details
5. **Flexibility**: Users can implement any policy for when to trigger operations

The design ensures that sidx remains focused on efficient storage while providing users with predictable, controllable storage operations that integrate cleanly with application lifecycles.

### Configuration Options
```go
type Options struct {
    // Part configuration
    MaxKeysPerPart     int    // Maximum keys in memory part
    KeyRangeSize       int64  // Target size of key ranges per part
    
    // Flush configuration
    FlushTimeout       time.Duration
    FlushKeyThreshold  int64  // Flush when key range exceeds this
    
    // Merge configuration
    MergeMinParts      int
    MergeMaxParts      int
    
    // Query configuration
    DefaultOrder       Order  // Default ordering for queries
    MaxQueryRange      int64  // Maximum allowed query key range
}
```

## User Responsibility

The USER/CLIENT is responsible for:

1. **Key Generation**: Creating meaningful int64 values
2. **Key Semantics**: Understanding what keys represent
3. **Key Consistency**: Ensuring keys are comparable within same series
4. **Range Queries**: Providing meaningful min/max values
5. **Key Distribution**: Ensuring reasonable distribution for performance

## Use Case Examples

### 1. Timestamp-based Secondary Index
```go
// User converts timestamp to int64
func writeEvent(event Event) {
    req := WriteRequest{
        SeriesID: event.ServiceID,
        Key:      event.Timestamp.UnixNano(),  // User generates
        Data:     event.Payload,
        Tags:     event.Tags,
    }
    sidx.Write(req)
}

// User queries with timestamp range
func queryByTime(serviceID string, start, end time.Time) []Element {
    req := QueryRequest{
        SeriesID: serviceID,
        KeyRange: KeyRange{
            Min: start.UnixNano(),  // User provides range
            Max: end.UnixNano(),
        },
        Order: ASC,
    }
    return sidx.Query(req)
}
```

### 2. Latency-based Secondary Index
```go
// User converts latency to int64 (microseconds)
func writeLatency(requestID string, latency time.Duration) {
    req := WriteRequest{
        SeriesID: "latency-index",
        Key:      latency.Microseconds(),  // User scales to int64
        Data:     []byte(requestID),
    }
    sidx.Write(req)
}

// User queries latency range
func queryByLatency(minMs, maxMs int) []Element {
    req := QueryRequest{
        SeriesID: "latency-index",
        KeyRange: KeyRange{
            Min: int64(minMs * 1000),  // User converts to microseconds
            Max: int64(maxMs * 1000),
        },
        Order: DESC,  // Highest latency first
    }
    return sidx.Query(req)
}
```

### 3. Sequence Number Index
```go
// User maintains sequence counter
var seqCounter int64

func writeMessage(msg Message) {
    seq := atomic.AddInt64(&seqCounter, 1)
    req := WriteRequest{
        SeriesID: msg.TopicID,
        Key:      seq,  // User-generated sequence
        Data:     msg.Content,
    }
    sidx.Write(req)
}

// User queries sequence range
func queryBySequence(topicID string, fromSeq, toSeq int64) []Element {
    req := QueryRequest{
        SeriesID: topicID,
        KeyRange: KeyRange{Min: fromSeq, Max: toSeq},
        Order:    ASC,
    }
    return sidx.Query(req)
}
```

### 4. Score-based Index
```go
// User scales float score to int64
func writeScore(userID string, score float64) {
    req := WriteRequest{
        SeriesID: "user-scores",
        Key:      int64(score * 1000000),  // User scales to int64
        Data:     []byte(userID),
    }
    sidx.Write(req)
}

// User queries score range
func queryByScore(minScore, maxScore float64) []Element {
    req := QueryRequest{
        SeriesID: "user-scores",
        KeyRange: KeyRange{
            Min: int64(minScore * 1000000),  // User scales back
            Max: int64(maxScore * 1000000),
        },
        Order: DESC,  // Highest scores first
    }
    return sidx.Query(req)
}
```

## What sidx DOES and DOESN'T Do

### sidx DOES:
- Store user-provided keys efficiently
- Maintain ordering by numerical comparison
- Support range queries on keys
- Optimize storage with LSM-tree structure
- Handle concurrent access safely
- Provide snapshot consistency
- Compress and encode data efficiently
- Support part merging and compaction

### sidx DOESN'T:
- Generate keys
- Interpret key meaning
- Validate key semantics
- Transform keys
- Assume key represents time
- Perform time-based operations
- Convert between key formats
- Validate business logic

## File Structure

```
banyand/internal/sidx/
├── DESIGN.md           # This design document
├── sidx.go             # Main interface and types
├── part.go             # Part structures and operations
├── snapshot.go         # Snapshot management with generic keys
├── metadata.go         # Metadata structures (MinKey/MaxKey)
├── tag.go              # Individual tag handling and encoding
├── tag_metadata.go     # Tag metadata management and persistence
├── tag_filter.go       # Tag filtering logic and bloom filters
├── introducer.go       # Introduction system
├── flusher.go          # Flush operations
├── merger.go           # Merge operations
├── query.go            # Query execution with key ranges
├── writer.go           # Write path implementation
├── reader.go           # Read path implementation
├── block.go            # Block organization
├── options.go          # Configuration options
└── sidx_test.go        # Comprehensive tests
```

## Test Strategy

**Test Categories:**
- **Unit Tests**: Object pooling, reference counting, resource management
- **Integration Tests**: Write-read consistency, flush-merge workflows  
- **Concurrency Tests**: Concurrent read/write operations with race detection
- **Failure Tests**: Corruption detection, recovery procedures, disk full scenarios
- **Performance Tests**: Benchmark throughput, memory usage, latency
- **Property Tests**: Key ordering invariants, data consistency

**Coverage Requirements:**
- Unit Tests: >90% line coverage
- Integration Tests: All major workflows
- Concurrency Tests: Race detection enabled
- Property Tests: Key invariants verification
- Benchmark Tests: Performance regression tracking

## Benefits

1. **Production Readiness**: Comprehensive error handling, resource management, and monitoring
2. **Performance**: Object pooling, atomic operations, and efficient memory management
3. **Reliability**: Reference counting, corruption detection, and automatic recovery
4. **Scalability**: Backpressure control, concurrency limiting, and resource reservation
5. **Observability**: Complete metrics, health checks, and administrative interfaces
6. **Flexibility**: Users can implement any int64-based ordering scheme
2. **Simplicity**: sidx doesn't need domain knowledge about keys
3. **Performance**: No key generation or transformation overhead
4. **Extensibility**: New use cases without sidx changes
5. **Clear Separation**: User controls semantics, sidx handles storage
6. **Reusability**: One system for multiple index types
7. **Compatibility**: Can replicate stream behavior exactly with timestamp keys

## Migration and Integration

### Relationship to Stream Module
- Stream module continues using timestamp-specific logic
- sidx provides generic alternative for non-time indices
- Both can coexist and complement each other
- Future possibility: Stream could migrate to use sidx internally

### Integration Points
- Uses same filesystem abstractions (`pkg/fs`)
- Uses same compression and encoding (`pkg/compress`, `pkg/encoding`)
- Uses same storage patterns (LSM-tree, parts, snapshots)
- Compatible with existing infrastructure

This design creates a clean, general-purpose secondary index system where users have full control over the ordering semantics while sidx focuses purely on efficient storage and retrieval based on numerical key comparisons.