# SIDX Implementation TODO List

This document tracks the implementation progress of the Secondary Index File System (sidx) based on the design in [DESIGN.md](./DESIGN.md).

## Implementation Progress Overview

- [x] **Phase 1**: Core Data Structures (6 tasks) - 1/6 completed
- [ ] **Phase 2**: Memory Management (4 tasks) 
- [ ] **Phase 3**: Snapshot Management (4 tasks)
- [ ] **Phase 4**: Write Path (4 tasks)
- [ ] **Phase 5**: Flush Operations (4 tasks)
- [ ] **Phase 6**: Merge Operations (4 tasks)
- [ ] **Phase 7**: Query Path (5 tasks)
- [ ] **Phase 8**: Resource Management (3 tasks)
- [ ] **Phase 9**: Error Handling (3 tasks)
- [ ] **Phase 10**: Testing (4 tasks)

**Total Tasks**: 41

---

## Phase 1: Core Data Structures

### 1.1 Element and Elements Types (`element.go`) ✅
- [x] Create element struct with seriesID, userKey, data, tags
- [x] Implement pooling with reset methods for memory efficiency
- [x] Add size calculation methods
- [x] **Test Cases**:
  - [x] Pool allocation and deallocation correctness
  - [x] Element reset functionality preserves nothing
  - [x] Memory reuse reduces allocations
  - [x] Size calculation accuracy

### 1.2 Tag Structure (`tag.go`)
- [ ] Individual tag handling (not tag families like stream module)
- [ ] Support for tag data, metadata, filter files
- [ ] Implement tag value marshaling/unmarshaling
- [ ] **Test Cases**:
  - [ ] Tag encoding/decoding with various value types
  - [ ] Value type handling (int64, string, bytes)
  - [ ] Filter generation for indexed tags
  - [ ] Tag serialization round-trip integrity

### 1.3 PartWrapper with Reference Counting (`part_wrapper.go`)
- [ ] Atomic reference counting for safe concurrent access
- [ ] Thread-safe acquire/release methods
- [ ] State management (active, removing, removed)
- [ ] **Test Cases**:
  - [ ] Concurrent reference counting under load
  - [ ] Proper cleanup when reference count reaches zero
  - [ ] State transitions work correctly
  - [ ] No race conditions in reference management

### 1.4 Part Structure (`part.go`)
- [ ] File readers for primary.bin, data.bin, keys.bin, meta.bin
- [ ] Individual tag file readers (tag_*.td, tag_*.tm, tag_*.tf)
- [ ] Part opening/closing lifecycle
- [ ] **Test Cases**:
  - [ ] File lifecycle management
  - [ ] Reader management and cleanup
  - [ ] Memory mapping efficiency
  - [ ] Error handling for corrupted files

### 1.5 Metadata Structures (`metadata.go`)
- [ ] partMetadata with MinKey/MaxKey (replacing timestamps)
- [ ] primaryBlockMetadata with block offsets and key ranges
- [ ] Validation methods for metadata integrity
- [ ] **Test Cases**:
  - [ ] Metadata serialization/deserialization
  - [ ] Key range validation (MinKey <= MaxKey)
  - [ ] Version compatibility checks
  - [ ] Corruption detection in metadata

### 1.6 Block Structure (`block.go`) 🔥
- [ ] **Core block organization for elements within parts**
- [ ] Contains userKeys[], seriesIDs[], data[], tags[]
- [ ] Methods: reset(), mustInitFromElements(), validation
- [ ] Sorting validation for elements within blocks
- [ ] **Test Cases**:
  - [ ] Block initialization from sorted elements
  - [ ] Element organization by seriesID and userKey
  - [ ] Key ordering validation within blocks
  - [ ] Block reset and reuse functionality

---

## Phase 2: Memory Management

### 2.1 MemPart Implementation (`mempart.go`)
- [ ] In-memory buffer before flushing to disk
- [ ] Element accumulation with size tracking
- [ ] Memory usage monitoring
- [ ] **Test Cases**:
  - [ ] Memory part creation and lifecycle
  - [ ] Size limits enforcement
  - [ ] Element addition and retrieval
  - [ ] Memory usage tracking accuracy

### 2.2 Block Writer (`block_writer.go`) 🔥
- [ ] **Uses block.go to serialize block data**
- [ ] Compression support (zstd)
- [ ] Write blocks to memory/disk buffers
- [ ] **Test Cases**:
  - [ ] Block serialization with various data sizes
  - [ ] Compression ratios meet expectations
  - [ ] Data integrity after compression/decompression
  - [ ] Block writer reuse and pooling

### 2.3 Element Sorting (`elements.go`)
- [ ] Sort by seriesID first, then userKey
- [ ] Efficient in-place sorting algorithms
- [ ] Validation of sort order
- [ ] **Test Cases**:
  - [ ] Sorting correctness with various data sizes
  - [ ] Sorting stability preserves order of equal elements
  - [ ] Performance benchmarks for large datasets
  - [ ] Edge cases (empty, single element, duplicate keys)

### 2.4 Block Initialization (`block.go` methods) 🔥
- [ ] **mustInitFromElements() processes sorted elements into blocks**
- [ ] Validate key ordering within blocks
- [ ] Group elements by seriesID efficiently
- [ ] **Test Cases**:
  - [ ] Block building from various element configurations
  - [ ] Validation errors for unsorted elements
  - [ ] Edge cases (empty elements, single series)
  - [ ] Memory usage during block initialization

---

## Phase 3: Snapshot Management

### 3.1 Snapshot Structure (`snapshot.go`)
- [ ] Part collection with epoch tracking
- [ ] getParts() filters by key range
- [ ] Reference counting for snapshot safety
- [ ] **Test Cases**:
  - [ ] Snapshot creation with various part configurations
  - [ ] Part filtering accuracy by key range
  - [ ] Reference counting prevents premature cleanup
  - [ ] Snapshot immutability guarantees

### 3.2 Introducer Loop (`introducer.go`)
- [ ] Background goroutine for snapshot coordination
- [ ] Channel-based communication for thread safety
- [ ] Epoch increment management
- [ ] **Test Cases**:
  - [ ] Channel operations work under load
  - [ ] Sequential processing maintains order
  - [ ] Graceful shutdown handling
  - [ ] No deadlocks in channel communication

### 3.3 Introduction Types (`introducer.go`)
- [ ] memIntroduction, flusherIntroduction, mergerIntroduction
- [ ] Object pooling for introduction structures
- [ ] Channel synchronization with applied notifications
- [ ] **Test Cases**:
  - [ ] Introduction pooling reduces allocations
  - [ ] Channel synchronization correctness
  - [ ] Applied notifications work reliably
  - [ ] Introduction reset for reuse

### 3.4 Snapshot Replacement (`snapshot.go`)
- [ ] Atomic updates with reference counting
- [ ] Safe concurrent read access during replacement
- [ ] Old snapshot cleanup after reference release
- [ ] **Test Cases**:
  - [ ] Atomic replacement under concurrent load
  - [ ] Concurrent reads see consistent data
  - [ ] No data races during replacement
  - [ ] Memory leaks prevented through reference counting

---

## Phase 4: Write Path

### 4.1 Write Implementation (`writer.go`)
- [ ] Element accumulation and batching
- [ ] Coordinate with memory parts
- [ ] Request validation and processing
- [ ] **Test Cases**:
  - [ ] Single and batch writes work correctly
  - [ ] Throughput meets performance targets
  - [ ] Data consistency across writes
  - [ ] Error handling for invalid requests

### 4.2 Memory Part Introduction (`writer.go`)
- [ ] Automatic introduction at configured thresholds
- [ ] Send to introducer via channel
- [ ] Wait for introduction completion
- [ ] **Test Cases**:
  - [ ] Threshold detection triggers introduction
  - [ ] Introduction timing meets expectations
  - [ ] Channel communication works reliably
  - [ ] Backpressure handling when introducer is busy

### 4.3 Key Range Validation (`writer.go`)
- [ ] Validate monotonic ordering within series
- [ ] Reject invalid key sequences
- [ ] Provide meaningful error messages
- [ ] **Test Cases**:
  - [ ] Valid key sequences are accepted
  - [ ] Invalid sequences are properly rejected
  - [ ] Error messages are helpful for debugging
  - [ ] Edge cases (duplicate keys, negative keys)

### 4.4 Block Building (`writer.go` + `block.go`) 🔥
- [ ] **When memory part reaches threshold, organize elements into blocks**
- [ ] **Call block.mustInitFromElements() with sorted elements**
- [ ] Create blocks with configured size limits
- [ ] **Test Cases**:
  - [ ] Block creation triggers at correct thresholds
  - [ ] Size limits are enforced properly
  - [ ] Element distribution across blocks is optimal
  - [ ] Block building performance meets targets

---

## Phase 5: Flush Operations

### 5.1 Flusher Interface (`flusher.go`)
- [ ] Simple Flush() method for user control
- [ ] Internal part selection logic
- [ ] Error handling and retry mechanisms
- [ ] **Test Cases**:
  - [ ] Flush API works reliably
  - [ ] State management during flush operations
  - [ ] Error handling for flush failures
  - [ ] Concurrent flush operations are handled safely

### 5.2 Flush to Disk (`flusher.go`)
- [ ] Create part directories with epoch names
- [ ] Write all part files atomically
- [ ] Implement crash recovery mechanisms
- [ ] **Test Cases**:
  - [ ] File creation follows naming conventions
  - [ ] Atomic operations prevent partial writes
  - [ ] Crash recovery restores consistent state
  - [ ] Disk space management during flush

### 5.3 Tag File Writing (`flusher.go`)
- [ ] Write individual tag files (not families)
- [ ] Generate bloom filters for indexed tags
- [ ] Optimize file layout for query performance
- [ ] **Test Cases**:
  - [ ] Tag file integrity after write
  - [ ] Bloom filter accuracy for lookups
  - [ ] File format compatibility
  - [ ] Performance of tag file generation

### 5.4 Block Serialization (`flusher.go` + `block_writer.go`) 🔥
- [ ] **Serialize blocks from memory parts to disk**
- [ ] **Use block structure to write primary.bin**
- [ ] Compress block data before writing
- [ ] **Test Cases**:
  - [ ] Block persistence maintains data integrity
  - [ ] Compression reduces storage footprint
  - [ ] Read-back verification after flush
  - [ ] Performance of block serialization

---

## Phase 6: Merge Operations

### 6.1 Merger Interface (`merger.go`)
- [ ] Simple Merge() method for user control
- [ ] Internal merge strategy implementation
- [ ] Resource management during merge
- [ ] **Test Cases**:
  - [ ] Merge API responds correctly
  - [ ] Error handling for merge failures
  - [ ] Resource usage during merge operations
  - [ ] Concurrent merge safety

### 6.2 Part Selection (`merger.go`)
- [ ] Select parts by size/age criteria
- [ ] Avoid merging recent parts
- [ ] Optimize merge efficiency
- [ ] **Test Cases**:
  - [ ] Selection algorithm chooses optimal parts
  - [ ] Edge cases (no parts to merge, single part)
  - [ ] Selection criteria can be tuned
  - [ ] Selection performance is acceptable

### 6.3 Merged Part Writer (`merger.go`)
- [ ] Combine parts maintaining key order
- [ ] Deduplicate overlapping data
- [ ] Generate merged part metadata
- [ ] **Test Cases**:
  - [ ] Merge correctness preserves all data
  - [ ] Deduplication removes redundant entries
  - [ ] Key ordering is maintained across parts
  - [ ] Merged part metadata is accurate

### 6.4 Block Merging (`merger.go` + `block.go`) 🔥
- [ ] **Read blocks from multiple parts**
- [ ] **Merge blocks maintaining seriesID/key ordering**
- [ ] **Create new blocks for merged part**
- [ ] **Test Cases**:
  - [ ] Block merge correctness across parts
  - [ ] Ordering preservation during merge
  - [ ] Memory efficiency during block merge
  - [ ] Performance of block merge operations

---

## Phase 7: Query Path

### 7.1 Query Interface (`query.go`)
- [ ] Key range queries with tag filters
- [ ] Support projections and result limits
- [ ] Query validation and optimization
- [ ] **Test Cases**:
  - [ ] Query parsing handles all parameter types
  - [ ] Validation rejects invalid queries
  - [ ] Query optimization improves performance
  - [ ] Complex queries return correct results

### 7.2 Part Filtering (`query.go`)
- [ ] Filter parts by key range overlap
- [ ] Minimize I/O operations through smart filtering
- [ ] Support inclusive/exclusive bounds
- [ ] **Test Cases**:
  - [ ] Filtering accuracy eliminates non-overlapping parts
  - [ ] Performance improvement through reduced I/O
  - [ ] Boundary conditions handled correctly
  - [ ] Empty result sets handled gracefully

### 7.3 Block Scanner (`block_scanner.go`) 🔥
- [ ] **Read and scan blocks from parts**
- [ ] **Deserialize block data using block structure**
- [ ] Apply tag filters using bloom filters
- [ ] **Test Cases**:
  - [ ] Scan completeness finds all matching data
  - [ ] Filter effectiveness reduces false positives
  - [ ] Block scanning performance meets targets
  - [ ] Memory usage during scanning is controlled

### 7.4 Result Iterator (`query.go`)
- [ ] Stream results with proper ordering
- [ ] Memory-efficient iteration patterns
- [ ] Support both ASC and DESC ordering
- [ ] **Test Cases**:
  - [ ] Iterator correctness for various query types
  - [ ] Memory usage remains bounded
  - [ ] Ordering is maintained across parts
  - [ ] Iterator cleanup prevents resource leaks

### 7.5 Block Reader (`block_reader.go`) 🔥
- [ ] **Deserialize blocks from disk**
- [ ] **Reconstruct block structure with elements**
- [ ] Decompress block data efficiently
- [ ] **Test Cases**:
  - [ ] Block reading maintains data integrity
  - [ ] Decompression works correctly
  - [ ] Block structure reconstruction is accurate
  - [ ] Read performance meets requirements

---

## Phase 8: Resource Management

### 8.1 Disk Reservation (`resource_manager.go`)
- [ ] Pre-allocate space for operations
- [ ] Prevent out-of-space failures
- [ ] Monitor disk usage continuously
- [ ] **Test Cases**:
  - [ ] Reservation logic prevents space exhaustion
  - [ ] Failure handling when space unavailable
  - [ ] Disk usage monitoring accuracy
  - [ ] Reservation cleanup after operations

### 8.2 Memory Tracking (`resource_manager.go`)
- [ ] Atomic counters for usage monitoring
- [ ] Leak detection mechanisms
- [ ] Memory pressure notifications
- [ ] **Test Cases**:
  - [ ] Memory accounting tracks all allocations
  - [ ] Leak detection identifies problems
  - [ ] Pressure notifications trigger appropriately
  - [ ] Counter accuracy under concurrent load

### 8.3 Backpressure Control (`resource_manager.go`)
- [ ] Four-level system (None/Moderate/Severe/Critical)
- [ ] Adaptive throttling based on resource usage
- [ ] Recovery mechanisms when pressure decreases
- [ ] **Test Cases**:
  - [ ] Backpressure triggers at correct thresholds
  - [ ] Throttling reduces resource usage
  - [ ] Recovery restores normal operation
  - [ ] System stability under sustained pressure

---

## Phase 9: Error Handling

### 9.1 Structured Errors (`errors.go`)
- [ ] Detailed error types with context
- [ ] Error wrapping and unwrapping support
- [ ] Consistent error formatting
- [ ] **Test Cases**:
  - [ ] Error creation includes relevant context
  - [ ] Context preservation through error chains
  - [ ] Error formatting is user-friendly
  - [ ] Error classification supports proper handling

### 9.2 Corruption Recovery (`recovery.go`)
- [ ] Detect corrupted parts and blocks
- [ ] Quarantine corrupted data safely
- [ ] Implement recovery procedures
- [ ] **Test Cases**:
  - [ ] Corruption detection identifies problems
  - [ ] Quarantine prevents corruption spread
  - [ ] Recovery procedures restore functionality
  - [ ] System continues operation despite corruption

### 9.3 Health Monitoring (`health.go`)
- [ ] Continuous health checks
- [ ] Metrics collection and reporting
- [ ] Alerting hooks for external systems
- [ ] **Test Cases**:
  - [ ] Health check accuracy reflects system state
  - [ ] Metric generation provides useful data
  - [ ] Alerting triggers for significant events
  - [ ] Health monitoring has minimal overhead

---

## Phase 10: Testing

### 10.1 Unit Tests
- [ ] **Test block.go**: Block creation, initialization, validation
- [ ] Test all components individually
- [ ] Achieve >90% code coverage
- [ ] **Test Cases**:
  - [ ] Component functionality works in isolation
  - [ ] Edge cases are handled correctly
  - [ ] Error conditions produce expected results
  - [ ] Performance characteristics meet requirements

### 10.2 Integration Tests
- [ ] End-to-end workflow testing
- [ ] Write-flush-merge-query cycles
- [ ] Multi-component interaction verification
- [ ] **Test Cases**:
  - [ ] Full workflows complete successfully
  - [ ] Component interactions work correctly
  - [ ] Data consistency maintained throughout
  - [ ] Performance acceptable under realistic loads

### 10.3 Performance Benchmarks
- [ ] **Benchmark block operations**: Creation, serialization, scanning
- [ ] Throughput and latency measurements
- [ ] Memory usage profiling
- [ ] **Test Cases**:
  - [ ] Performance regression detection
  - [ ] Throughput meets design targets
  - [ ] Latency remains within bounds
  - [ ] Memory usage is reasonable

### 10.4 Concurrency Tests
- [ ] Race condition detection with race detector
- [ ] Stress testing with concurrent operations
- [ ] Deadlock prevention verification
- [ ] **Test Cases**:
  - [ ] Thread safety under concurrent load
  - [ ] No deadlocks in normal operation
  - [ ] Data races are eliminated
  - [ ] System stability under stress

---

## File Creation Checklist

### Core Implementation Files
- [ ] `sidx.go` - Main interface and SIDX struct
- [ ] `element.go` - Element structures and pooling
- [ ] `tag.go` - Tag handling and encoding
- [ ] `part.go` - Part structure and file management
- [ ] `part_wrapper.go` - Reference counting wrapper
- [ ] `mempart.go` - In-memory part implementation
- [ ] `block.go` - Block structure and operations 🔥
- [ ] `block_writer.go` - Block serialization 🔥
- [ ] `block_reader.go` - Block deserialization 🔥
- [ ] `block_scanner.go` - Block scanning for queries 🔥
- [ ] `snapshot.go` - Snapshot management
- [ ] `introducer.go` - Introducer loop coordination
- [ ] `flusher.go` - Flush operations
- [ ] `merger.go` - Merge operations
- [ ] `writer.go` - Write path implementation
- [ ] `query.go` - Query interface and execution
- [ ] `metadata.go` - Metadata structures
- [ ] `resource_manager.go` - Resource management
- [ ] `errors.go` - Error handling
- [ ] `health.go` - Health monitoring
- [ ] `options.go` - Configuration options

### Test Files
- [ ] `sidx_test.go` - Main test suite
- [ ] `element_test.go` - Element testing
- [ ] `tag_test.go` - Tag testing
- [ ] `part_test.go` - Part testing
- [ ] `block_test.go` - Block testing 🔥
- [ ] `snapshot_test.go` - Snapshot testing
- [ ] `introducer_test.go` - Introducer testing
- [ ] `flusher_test.go` - Flusher testing
- [ ] `merger_test.go` - Merger testing
- [ ] `writer_test.go` - Writer testing
- [ ] `query_test.go` - Query testing
- [ ] `benchmark_test.go` - Performance benchmarks
- [ ] `integration_test.go` - Integration tests
- [ ] `concurrency_test.go` - Concurrency tests

---

## Success Criteria for Each Phase

### Phase Completion Requirements
- [ ] All tasks in phase completed
- [ ] Unit tests pass with >90% coverage
- [ ] Integration tests pass for phase functionality
- [ ] No race conditions detected
- [ ] Performance benchmarks meet targets
- [ ] Clean linter output (`make lint`)
- [ ] Documentation updated

### Overall Success Criteria
- [ ] All 41 tasks completed
- [ ] Full test suite passes
- [ ] Performance meets design targets
- [ ] Code review approval
- [ ] Documentation complete
- [ ] Ready for production use

---

## Block.go Usage Summary 🔥

The `block.go` file is central to the SIDX implementation and is used in multiple phases:

1. **Phase 1.6**: Initial block structure creation
2. **Phase 2.2**: Block writer uses block for serialization  
3. **Phase 2.4**: Block initialization from elements
4. **Phase 4.4**: Create blocks when memory threshold reached
5. **Phase 5.4**: Serialize blocks to disk during flush
6. **Phase 6.4**: Merge blocks from multiple parts
7. **Phase 7.3**: Block scanner reads blocks during queries
8. **Phase 7.5**: Block reader deserializes blocks
9. **Phase 10.1**: Unit tests for block operations
10. **Phase 10.3**: Performance benchmarks for block operations

---

## Dependencies Between Tasks

- **Phase 1** must complete before **Phase 2** (data structures needed)
- **Phase 2** must complete before **Phase 4** (memory management needed for writes)
- **Phase 3** must complete before **Phase 4** (snapshot management needed)
- **Phase 4** must complete before **Phase 5** (write path needed for flush)
- **Phase 5** must complete before **Phase 6** (flush needed for merge)
- **Phase 1-6** must complete before **Phase 7** (all components needed for queries)
- **Phase 8-9** can be developed in parallel with other phases
- **Phase 10** requires completion of relevant phases for testing

---

*This TODO list tracks the implementation of the Secondary Index File System (sidx) as designed in [DESIGN.md](./DESIGN.md). Each checkbox represents a specific deliverable with associated test cases.*