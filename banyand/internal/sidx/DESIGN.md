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
    blockMetadata []blockMetadata
    partMetadata  partMetadata
    
}

// Enhanced metadata with integrity
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
    data      []byte     // User payload data (pooled slice)
    tags      []tag      // Individual tags (pooled slice)
    
    // Internal fields for pooling
    pooled    bool       // Whether this element came from pool
}

// Reset clears element for reuse in object pool
func (e *element) reset() {
    e.seriesID = 0
    e.userKey = 0
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
- `primary.bin` - Block metadata
- `data.bin` - User data payloads with compression
- `keys.bin` - User-provided int64 ordering keys
- `meta.bin` - Part metadata
- `manifest.json` - Human-readable part manifest

**Tag Files (per tag per part):**
- `tag_<name>.td` - Tag data with encoding optimizations
- `tag_<name>.tm` - Tag metadata and block offsets
- `tag_<name>.tf` - Bloom filters for fast lookups

**File Format Features:**
- **No Version Control**: Do not support format versioning 
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
// Enhanced block metadata with data references
type blockMetadata struct {
    seriesID    common.SeriesID
    minKey      int64                      // Minimum user key in block
    maxKey      int64                      // Maximum user key in block
    dataBlock   dataBlock          // Reference to data in data.bin
    keysBlock   dataBlock          // Reference to keys in keys.bin
    tagsBlocks  map[string]dataBlock // References to tag files
    
    // Additional metadata for query processing
    tagProjection    []string // Tags to load for queries
    uncompressedSize uint64   // Uncompressed size of block
    count           uint64   // Number of elements in block
}
```

##### Benefits of Separate Data Storage
- **Efficient Metadata Scanning**: Can read block metadata without loading actual data
- **Selective Data Loading**: Load only the data blocks needed for a query
- **Independent Compression**: Optimize compression strategy per data type
- **Clean Separation**: Metadata operations don't require data I/O
- **Better Cache Utilization**: Metadata fits better in memory caches

#### 5. Block Architecture

The block design in sidx provides a comprehensive system for organizing, reading, writing, and scanning data blocks within parts. This architecture is inspired by the stream module but adapted for user-provided int64 keys instead of timestamps.

##### Block Architecture Overview

Blocks are the fundamental units of data organization within parts, providing:
- **Efficient Storage**: Elements are organized into blocks for optimal compression and access
- **Key-based Organization**: Sorted by seriesID first, then by user-provided int64 keys
- **Memory Management**: Object pooling and reference counting for production use
- **I/O Optimization**: Separate files for different data types enable selective loading

##### Core Components

###### A. Block Structure (`block`)

The core block structure organizes elements for efficient storage and retrieval:

```go
// block represents a collection of elements organized for storage
type block struct {
    // Core data arrays (all same length)
    userKeys   []int64           // User-provided ordering keys
    elementIDs []uint64          // Unique element identifiers
    data       [][]byte          // User payload data
    
    // Tag data organized by tag name
    tags       map[string]*tagData  // Runtime tag data with filtering
    
    // Internal state
    pooled     bool   // Whether this block came from pool
}

// Pool management for memory efficiency
var blockPool = pool.Register[*block]("sidx-block")

func generateBlock() *block {
    v := blockPool.Get()
    if v == nil {
        return &block{
            tags: make(map[string]*tagData),
        }
    }
    return v
}

func releaseBlock(b *block) {
    // Release tag filters back to pool
    for _, tag := range b.tags {
        if tag.filter != nil {
            releaseBloomFilter(tag.filter)
        }
        releaseTagData(tag)
    }
    b.reset()
    blockPool.Put(b)
}

// reset clears block for reuse in object pool
func (b *block) reset() {
    b.userKeys = b.userKeys[:0]
    b.elementIDs = b.elementIDs[:0]
    
    // Reset data slices if not too large
    if cap(b.data) <= maxPooledSliceCount {
        for i := range b.data {
            if cap(b.data[i]) <= maxPooledSliceSize {
                b.data[i] = b.data[i][:0] // Reuse slice
            }
        }
        b.data = b.data[:0]
    } else {
        b.data = nil // Release oversized slice
    }
    
    // Clear tag map but keep the map itself
    for k := range b.tags {
        delete(b.tags, k)
    }
    
    b.pooled = false
}

// mustInitFromElements initializes block from sorted elements
func (b *block) mustInitFromElements(elements *elements) {
    b.reset()
    if elements.len() == 0 {
        return
    }
    
    // Verify elements are sorted
    elements.assertSorted()
    
    // Copy core data
    b.userKeys = append(b.userKeys, elements.userKeys...)
    b.elementIDs = append(b.elementIDs, elements.elementIDs...)
    b.data = append(b.data, elements.data...)
    
    // Process tags
    b.mustInitFromTags(elements.tags)
}

// mustInitFromTags processes tag data for the block
func (b *block) mustInitFromTags(elementTags [][]tag) {
    if len(elementTags) == 0 {
        return
    }
    
    // Collect all unique tag names
    tagNames := make(map[string]struct{})
    for _, tags := range elementTags {
        for _, tag := range tags {
            tagNames[tag.name] = struct{}{}
        }
    }
    
    // Process each tag
    for tagName := range tagNames {
        b.processTag(tagName, elementTags)
    }
}

// processTag creates tag data structure for a specific tag
func (b *block) processTag(tagName string, elementTags [][]tag) {
    td := generateTagData()
    td.name = tagName
    td.values = make([][]byte, len(b.userKeys))
    
    var valueType pbv1.ValueType
    var indexed bool
    
    // Collect values for this tag across all elements
    for i, tags := range elementTags {
        found := false
        for _, tag := range tags {
            if tag.name == tagName {
                td.values[i] = tag.value
                valueType = tag.valueType
                indexed = tag.indexed
                found = true
                break
            }
        }
        if !found {
            td.values[i] = nil // Missing tag value
        }
    }
    
    td.valueType = valueType
    td.indexed = indexed
    
    // Create bloom filter for indexed tags
    if indexed {
        td.filter = generateBloomFilter()
        td.filter.SetN(len(b.userKeys))
        td.filter.ResizeBits((len(b.userKeys)*filter.B + 63) / 64)
        
        for _, value := range td.values {
            if value != nil {
                td.filter.Add(value)
            }
        }
    }
    
    // Track min/max for int64 tags
    if valueType == pbv1.ValueTypeInt64 {
        for _, value := range td.values {
            if value == nil {
                continue
            }
            if len(td.min) == 0 || bytes.Compare(value, td.min) < 0 {
                td.min = value
            }
            if len(td.max) == 0 || bytes.Compare(value, td.max) > 0 {
                td.max = value
            }
        }
    }
    
    b.tags[tagName] = td
}

// validate ensures block data consistency
func (b *block) validate() error {
    count := len(b.userKeys)
    if count != len(b.elementIDs) || count != len(b.data) {
        return fmt.Errorf("inconsistent block arrays: keys=%d, ids=%d, data=%d",
            len(b.userKeys), len(b.elementIDs), len(b.data))
    }
    
    // Verify sorting
    for i := 1; i < count; i++ {
        if b.userKeys[i] < b.userKeys[i-1] {
            return fmt.Errorf("block not sorted by userKey at index %d", i)
        }
    }
    
    // Verify tag consistency
    for tagName, tagData := range b.tags {
        if len(tagData.values) != count {
            return fmt.Errorf("tag %s has %d values but block has %d elements", 
                tagName, len(tagData.values), count)
        }
    }
    
    return nil
}

// uncompressedSizeBytes calculates the uncompressed size of the block
func (b *block) uncompressedSizeBytes() uint64 {
    count := uint64(len(b.userKeys))
    size := count * (8 + 8) // userKey + elementID
    
    // Add data payload sizes
    for _, payload := range b.data {
        size += uint64(len(payload))
    }
    
    // Add tag data sizes
    for tagName, tagData := range b.tags {
        nameSize := uint64(len(tagName))
        for _, value := range tagData.values {
            if value != nil {
                size += nameSize + uint64(len(value))
            }
        }
    }
    
    return size
}
```

###### B. Block Metadata (`blockMetadata`)

Block metadata provides references to data stored in files:

```go
// blockMetadata contains metadata for a block within a part
type blockMetadata struct {
    // Block references to files
    tagsBlocks map[string]dataBlock // References to tag files
    dataBlock  dataBlock            // Reference to data in data.bin
    keysBlock  dataBlock            // Reference to keys in keys.bin

    // Block identification
    seriesID common.SeriesID

    // Key range within block
    minKey int64 // Minimum user key in block
    maxKey int64 // Maximum user key in block
    
    // Additional metadata for query processing
    tagProjection    []string // Tags to load for queries
    uncompressedSize uint64   // Uncompressed size of block
    count           uint64   // Number of elements in block
}

// copyFrom creates a deep copy of block metadata
func (bm *blockMetadata) copyFrom(src *blockMetadata) {
    bm.seriesID = src.seriesID
    bm.minKey = src.minKey
    bm.maxKey = src.maxKey
    bm.dataBlock = src.dataBlock
    bm.keysBlock = src.keysBlock
    bm.uncompressedSize = src.uncompressedSize
    bm.count = src.count
    
    // Deep copy tag blocks
    if bm.tagsBlocks == nil {
        bm.tagsBlocks = make(map[string]dataBlock)
    }
    for k, v := range src.tagsBlocks {
        bm.tagsBlocks[k] = v
    }
    
    // Deep copy tag projection
    bm.tagProjection = make([]string, len(src.tagProjection))
    copy(bm.tagProjection, src.tagProjection)
}

// reset clears blockMetadata for reuse in object pool
func (bm *blockMetadata) reset() {
    bm.seriesID = 0
    bm.minKey = 0
    bm.maxKey = 0
    bm.dataBlock = dataBlock{}
    bm.keysBlock = dataBlock{}
    bm.uncompressedSize = 0
    bm.count = 0
    
    // Clear maps but keep them allocated
    for k := range bm.tagsBlocks {
        delete(bm.tagsBlocks, k)
    }
    bm.tagProjection = bm.tagProjection[:0]
}

// overlapsKeyRange checks if block overlaps with query key range
func (bm *blockMetadata) overlapsKeyRange(minKey, maxKey int64) bool {
    return !(maxKey < bm.minKey || minKey > bm.maxKey)
}

// overlapsSeriesID checks if block contains the specified series
func (bm *blockMetadata) overlapsSeriesID(seriesID common.SeriesID) bool {
    return bm.seriesID == seriesID
}

var blockMetadataPool = pool.Register[*blockMetadata]("sidx-blockMetadata")

func generateBlockMetadata() *blockMetadata {
    v := blockMetadataPool.Get()
    if v == nil {
        return &blockMetadata{
            tagsBlocks: make(map[string]dataBlock),
        }
    }
    return v
}

func releaseBlockMetadata(bm *blockMetadata) {
    bm.reset()
    blockMetadataPool.Put(bm)
}
```

###### C. Block Reader (`block_reader`)

The block reader provides efficient reading of blocks from disk:

```go
// blockReader reads blocks from parts with memory optimization
type blockReader struct {
    // File readers
    dataReader fs.Reader      // Reads from data.bin
    keysReader fs.Reader      // Reads from keys.bin
    tagReaders map[string]fs.Reader // Reads from *.td files
    
    // Decoders for efficient processing
    decoder    *encoding.BytesBlockDecoder
    
    // Pool management
    pooled     bool
}

var blockReaderPool = pool.Register[*blockReader]("sidx-blockReader")

func generateBlockReader() *blockReader {
    v := blockReaderPool.Get()
    if v == nil {
        return &blockReader{
            tagReaders: make(map[string]fs.Reader),
            decoder:    &encoding.BytesBlockDecoder{},
        }
    }
    return v
}

func releaseBlockReader(br *blockReader) {
    br.reset()
    blockReaderPool.Put(br)
}

// reset clears reader for reuse
func (br *blockReader) reset() {
    br.dataReader = nil
    br.keysReader = nil
    
    // Clear tag readers map
    for k := range br.tagReaders {
        delete(br.tagReaders, k)
    }
    
    br.decoder.Reset()
    br.pooled = false
}

// init initializes reader with part files
func (br *blockReader) init(p *part, tagProjection []string) error {
    br.reset()
    
    br.dataReader = p.data
    br.keysReader = p.userKeys
    
    // Initialize tag readers for projected tags only
    for _, tagName := range tagProjection {
        if tagReader, ok := p.tags[tagName]; ok {
            br.tagReaders[tagName] = tagReader
        }
    }
    
    return nil
}

// mustReadFrom loads block data from files
func (br *blockReader) mustReadFrom(bm *blockMetadata, dst *block) error {
    dst.reset()
    
    // Read user keys
    if err := br.readUserKeys(bm, dst); err != nil {
        return fmt.Errorf("failed to read user keys: %w", err)
    }
    
    // Read data payloads
    if err := br.readData(bm, dst); err != nil {
        return fmt.Errorf("failed to read data: %w", err)
    }
    
    // Read tag data
    if err := br.readTags(bm, dst); err != nil {
        return fmt.Errorf("failed to read tags: %w", err)
    }
    
    // Validate loaded block
    if err := dst.validate(); err != nil {
        return fmt.Errorf("loaded block validation failed: %w", err)
    }
    
    return nil
}

// readUserKeys reads user keys from keys.bin
func (br *blockReader) readUserKeys(bm *blockMetadata, dst *block) error {
    bb := bigValuePool.Generate()
    defer bigValuePool.Release(bb)
    
    // Read keys block
    bb.Buf = pkgbytes.ResizeExact(bb.Buf, int(bm.keysBlock.size))
    fs.MustReadData(br.keysReader, int64(bm.keysBlock.offset), bb.Buf)
    
    // Decode keys
    dst.userKeys = encoding.ExtendListCapacity(dst.userKeys, int(bm.count))
    dst.userKeys = dst.userKeys[:bm.count]
    
    _, err := encoding.BytesToInt64List(dst.userKeys, bb.Buf)
    if err != nil {
        return fmt.Errorf("failed to decode user keys: %w", err)
    }
    
    return nil
}

// readData reads data payloads from data.bin
func (br *blockReader) readData(bm *blockMetadata, dst *block) error {
    bb := bigValuePool.Generate()
    defer bigValuePool.Release(bb)
    
    // Read data block
    bb.Buf = pkgbytes.ResizeExact(bb.Buf, int(bm.dataBlock.size))
    fs.MustReadData(br.dataReader, int64(bm.dataBlock.offset), bb.Buf)
    
    // Decompress if necessary
    decompressed, err := zstd.DecompressBytes(bb.Buf)
    if err != nil {
        return fmt.Errorf("failed to decompress data block: %w", err)
    }
    
    // Decode data payloads
    dst.data = encoding.ExtendSliceCapacity(dst.data, int(bm.count))
    dst.data = dst.data[:bm.count]
    
    if err := br.decoder.DecodeBytesList(dst.data, decompressed); err != nil {
        return fmt.Errorf("failed to decode data payloads: %w", err)
    }
    
    return nil
}

// readTags reads tag data from tag files
func (br *blockReader) readTags(bm *blockMetadata, dst *block) error {
    for tagName, tagBlock := range bm.tagsBlocks {
        tagReader, ok := br.tagReaders[tagName]
        if !ok {
            continue // Tag not in projection
        }
        
        if err := br.readTag(tagName, tagBlock, tagReader, dst, int(bm.count)); err != nil {
            return fmt.Errorf("failed to read tag %s: %w", tagName, err)
        }
    }
    
    return nil
}

// readTag reads a single tag's data
func (br *blockReader) readTag(tagName string, tagBlock dataBlock, 
                              tagReader fs.Reader, dst *block, count int) error {
    bb := bigValuePool.Generate()
    defer bigValuePool.Release(bb)
    
    // Read tag data block
    bb.Buf = pkgbytes.ResizeExact(bb.Buf, int(tagBlock.size))
    fs.MustReadData(tagReader, int64(tagBlock.offset), bb.Buf)
    
    // Create tag data
    td := generateTagData()
    td.name = tagName
    td.values = make([][]byte, count)
    
    // Decode tag values
    if err := br.decoder.DecodeBytesList(td.values, bb.Buf); err != nil {
        releaseTagData(td)
        return fmt.Errorf("failed to decode tag values: %w", err)
    }
    
    dst.tags[tagName] = td
    return nil
}
```

###### D. Block Scanner (`block_scanner`)

The block scanner provides efficient scanning for queries:

```go
// blockScanner scans blocks for query processing
type blockScanner struct {
    // Query parameters
    minKey        int64
    maxKey        int64
    seriesFilter  map[common.SeriesID]struct{}
    tagFilters    map[string][]byte // Tag filters for optimization
    
    // Current scan state  
    currentBlock  *block
    currentIndex  int
    
    // Resources
    reader        *blockReader
    tmpBlock      *block
    
    // Pool management
    pooled        bool
}

var blockScannerPool = pool.Register[*blockScanner]("sidx-blockScanner")

func generateBlockScanner() *blockScanner {
    v := blockScannerPool.Get()
    if v == nil {
        return &blockScanner{
            seriesFilter: make(map[common.SeriesID]struct{}),
            tagFilters:   make(map[string][]byte),
            reader:       generateBlockReader(),
            tmpBlock:     generateBlock(),
        }
    }
    return v
}

func releaseBlockScanner(bs *blockScanner) {
    bs.reset()
    blockScannerPool.Put(bs)
}

// reset clears scanner for reuse
func (bs *blockScanner) reset() {
    bs.minKey = 0
    bs.maxKey = 0
    bs.currentIndex = 0
    
    // Clear filters
    for k := range bs.seriesFilter {
        delete(bs.seriesFilter, k)
    }
    for k := range bs.tagFilters {
        delete(bs.tagFilters, k)
    }
    
    // Reset resources
    if bs.reader != nil {
        releaseBlockReader(bs.reader)
        bs.reader = generateBlockReader()
    }
    
    if bs.tmpBlock != nil {
        releaseBlock(bs.tmpBlock)
        bs.tmpBlock = generateBlock()
    }
    
    bs.currentBlock = nil
    bs.pooled = false
}

// init initializes scanner with query parameters
func (bs *blockScanner) init(minKey, maxKey int64, 
                           seriesIDs []common.SeriesID,
                           tagFilters map[string][]byte) {
    bs.reset()
    
    bs.minKey = minKey
    bs.maxKey = maxKey
    
    // Convert series slice to map for fast lookup
    for _, id := range seriesIDs {
        bs.seriesFilter[id] = struct{}{}
    }
    
    // Copy tag filters
    for k, v := range tagFilters {
        bs.tagFilters[k] = v
    }
}

// scanBlock scans a block and returns matching elements
func (bs *blockScanner) scanBlock(bm *blockMetadata, p *part) ([]*element, error) {
    // Quick check: does block overlap with query range?
    if !bm.overlapsKeyRange(bs.minKey, bs.maxKey) {
        return nil, nil
    }
    
    // Quick check: does block contain relevant series?
    if len(bs.seriesFilter) > 0 {
        if _, ok := bs.seriesFilter[bm.seriesID]; !ok {
            return nil, nil
        }
    }
    
    // Initialize reader and load block
    if err := bs.reader.init(p, bm.tagProjection); err != nil {
        return nil, fmt.Errorf("failed to init block reader: %w", err)
    }
    
    if err := bs.reader.mustReadFrom(bm, bs.tmpBlock); err != nil {
        return nil, fmt.Errorf("failed to read block: %w", err)
    }
    
    // Scan block for matching elements
    return bs.scanBlockElements(bs.tmpBlock)
}

// scanBlockElements scans elements within a loaded block
func (bs *blockScanner) scanBlockElements(block *block) ([]*element, error) {
    var results []*element
    
    for i := 0; i < len(block.userKeys); i++ {
        // Check key range
        if block.userKeys[i] < bs.minKey || block.userKeys[i] > bs.maxKey {
            continue
        }
        
        // Check tag filters
        if !bs.matchesTagFilters(block, i) {
            continue
        }
        
        // Create matching element
        elem := generateElement()
        elem.userKey = block.userKeys[i]
        elem.data = block.data[i]
        
        // Copy tag values
        for tagName, tagData := range block.tags {
            if i < len(tagData.values) && tagData.values[i] != nil {
                tag := generateTag()
                tag.name = tagName
                tag.value = tagData.values[i]
                tag.valueType = tagData.valueType
                elem.tags = append(elem.tags, tag)
            }
        }
        
        results = append(results, elem)
    }
    
    return results, nil
}

// matchesTagFilters checks if element matches tag filters
func (bs *blockScanner) matchesTagFilters(block *block, index int) bool {
    for filterTagName, filterValue := range bs.tagFilters {
        tagData, ok := block.tags[filterTagName]
        if !ok {
            return false // Tag not present
        }
        
        if index >= len(tagData.values) {
            return false // No value for this element
        }
        
        elementValue := tagData.values[index]
        if elementValue == nil {
            return false // Null value
        }
        
        if !bytes.Equal(elementValue, filterValue) {
            return false // Value doesn't match
        }
    }
    
    return true
}
```

###### E. Block Writer (`block_writer`)

The block writer handles efficient writing of blocks to disk:

refer to @banyand/stream/block_writer.go to implement the blockWriter. 

##### Component Dependency Relationships

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     Block       │    │  Block Metadata │    │  Block Writer   │
│   (Runtime)     │◄──►│   (Storage)     │◄───│   (Persist)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         ▲                        ▲                        ▲
         │                        │                        │
         │                        │                        │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Block Reader   │    │  Block Scanner  │    │      Parts      │
│   (Load)        │────│   (Query)       │────│   (Storage)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

**Key Dependencies:**

1. **Block → Block Metadata**: Blocks generate metadata during write operations
2. **Block Metadata → Block Reader**: Metadata guides what and how to read
3. **Block Reader → Block**: Reader loads data into block structures
4. **Block Scanner → Block Reader**: Scanner uses reader to load blocks for queries
5. **Block Writer → Parts**: Writer creates files within part directories
6. **Parts → Block Scanner**: Scanner operates on blocks within parts

##### Key Design Features

###### Block Size Management
```go
const (
    // maxElementsPerBlock defines the maximum number of elements per block.
	maxElementsPerBlock = 8 * 1024
)

// isFull checks if block has reached element count limit.
func (b *block) isFull() bool {
	return len(b.userKeys) >= maxElementsPerBlock
}
```

###### Compression Strategy
- **Data Payloads**: zstd compression for user data
- **User Keys**: Specialized int64 encoding for optimal space usage
- **Tag Values**: Type-specific encoding with bloom filters for indexed tags
- **Metadata**: JSON for readability, binary for performance-critical paths

###### Memory Management
- **Object Pooling**: All major structures use sync.Pool for allocation efficiency
- **Reference Counting**: Safe concurrent access with atomic operations
- **Resource Limits**: Configurable limits prevent memory exhaustion
- **Reset Methods**: Proper cleanup enables safe object reuse

###### Error Handling
- **Validation**: Comprehensive validation at all levels
- **Recovery**: Graceful handling of corruption and I/O errors
- **Logging**: Detailed error context for debugging
- **Consistency**: Atomic operations maintain data integrity

##### File Organization in Parts

```
000000001234abcd/                    # Part directory (epoch-based name)
├── manifest.json                    # Part metadata and statistics  
├── primary.bin                      # Block metadata (references to all files)
├── data.bin                        # User data payloads (compressed)
├── keys.bin                        # User-provided int64 ordering keys
├── meta.bin                        # Part-level metadata
├── tag_service_id.td               # Service ID tag data
├── tag_service_id.tm               # Service ID tag metadata
├── tag_service_id.tf               # Service ID tag filter (bloom)
├── tag_endpoint.td                 # Endpoint tag data
├── tag_endpoint.tm                 # Endpoint tag metadata
├── tag_endpoint.tf                 # Endpoint tag filter
└── ...                             # Additional tag files
```

**File Format Details:**
- **primary.bin**: Contains array of blockMetadata structures
- **data.bin**: Compressed user payloads with block boundaries
- **keys.bin**: Int64-encoded user keys with block boundaries
- ***.td**: Tag value data with type-specific encoding
- ***.tm**: Tag metadata with bloom filter parameters
- ***.tf**: Bloom filter data for fast tag filtering

This block architecture provides efficient, scalable storage for user-key-based data while maintaining consistency with the existing sidx design principles and production requirements.

#### 6. Snapshot Management
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
    for _, pw := range s.parts {
        pm := pw.p.partMetadata
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

#### 7. Enhanced API Design

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
  - Function: `partName(epoch uint64)` returns `fmt.Sprintf("%016x", epoch)`
  - Example: epoch `1` becomes directory `0000000000000001`
  - Epochs assigned sequentially by introducer loop for total ordering
- **Key ranges**: Each part covers a range of user keys
- **Directory structure**: Similar to stream module (`000000001234abcd/`)
- **File structure**: Core files plus individual tag files

#### Epoch Management and Persistence

**Epoch Assignment**: Each operation in the introducer loop increments the epoch counter:
- Memory part introduction: `epoch++`
- Flushed parts: `epoch++` 
- Merged parts: `epoch++`

**Snapshot Manifest Naming**: Snapshot manifests use epoch-based naming:
- Function: `snapshotName(snapshot uint64)` returns `fmt.Sprintf("%016x%s", snapshot, ".snapshot")`
- Example: epoch `1` becomes file `0000000000000001.snapshot`
- Contains JSON array of part directory names for recovery
- Used during startup to reconstruct snapshot state from epoch-based naming

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
func validateBlockMetadata(blocks []blockMetadata) error {
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
The introducer runs as a background goroutine that coordinates all snapshot updates through channel-based communication.

**Single-Writer Invariant**: All snapshot updates go through the introducer loop; readers only read snapshots.

The introducer loop implements the same critical single-writer architecture as the stream module:

```go
// Introduction types for different operations
type memIntroduction struct {
    memPart *partWrapper
    applied chan struct{}
}

type flusherIntroduction struct {
    flushed map[uint64]*partWrapper
    applied chan struct{}
}

type mergerIntroduction struct {
    merged  map[uint64]struct{}
    newPart *partWrapper
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
- **Centralized Coordination**: All operations that modify snapshots are serialized through the introducer
- **Sequential Processing**: Updates processed sequentially via channels for consistency
- **Atomic Replacement**: Snapshot updates are atomic with proper reference counting
- **Read-Only Access**: Readers access immutable snapshots without modification

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
    flushedParts := make(map[uint64]*partWrapper)
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
2. **Single-Writer Invariant**: All snapshot updates serialized through introducer loop for data consistency
3. **Simple Interface**: Single-function interfaces (`Flush()`, `Merge()`) without parameters
4. **Predictable Performance**: No unexpected background I/O, but reliable snapshot management
5. **Application Integration**: Operations can be coordinated with application lifecycle
6. **Efficient Coordination**: Channel-based introducer prevents race conditions
7. **Epoch-based Recovery**: Consistent epoch naming enables reliable crash recovery
8. **Flexible Policies**: Users implement custom flush/merge policies while system handles coordination

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

func (f *flusher) getMemPartsToFlush() []*partWrapper {
    // Internal logic to select memory parts for flushing
    // Could be all memory parts, or based on size/age criteria
}

// Internal merger implementation  
type merger struct {
    sidx *SIDX
}

func (m *merger) getPartsToMerge() []*partWrapper {
    // Internal logic to select parts for merging
    // Could be based on size ratios, part count, key ranges, etc.
}
```

### Architecture Summary

This hybrid architecture provides:

1. **Centralized Coordination**: Single introducer loop manages all snapshot updates
2. **Single-Writer Invariant**: All snapshot modifications serialized for consistency
3. **User Control**: Simple `Flush()` and `Merge()` interfaces give users timing control
4. **Thread Safety**: Channel-based communication prevents race conditions
5. **Epoch-based Consistency**: Sequential epoch assignment ensures total ordering
6. **Reliable Recovery**: Epoch-based part and snapshot naming enables crash recovery
7. **Simplicity**: No complex parameters or configuration - internal logic handles details
8. **Flexibility**: Users can implement any policy for when to trigger operations

The design ensures that sidx remains focused on efficient storage while providing users with predictable, controllable storage operations that integrate cleanly with application lifecycles.

#### Consistency with Stream Module

SIDX maintains architectural consistency with the stream module:

- **Same Single-Writer Pattern**: Both use introducer loops to serialize snapshot updates
- **Same Epoch Management**: Both use 16-character hex epoch naming for parts and snapshots
- **Same Recovery Mechanism**: Both use epoch-based naming for crash recovery
- **Same Reference Counting**: Both use atomic reference counting for safe concurrent access
- **Same Channel Communication**: Both use channels for thread-safe coordination

This consistency ensures that developers familiar with one module can easily understand and work with the other, while maintaining the same reliability guarantees across both systems.

### Configuration Options
```go
type Options struct {
    // Path specifies the directory where index files will be stored
    Path string
    // MergePolicy defines the strategy for merging index segments during compaction
    MergePolicy              *MergePolicy
    // Protector provides memory management and protection mechanisms
    Protector                protector.Memory
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