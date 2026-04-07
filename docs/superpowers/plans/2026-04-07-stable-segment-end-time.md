# Stable Segment End Time Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist segment end time in per-segment metadata so boundaries don't shift across restarts or config changes.

**Architecture:** Extend the existing `metadata` file from a plain version string to JSON containing both version and end time. New `readSegmentMeta()` function handles both old and new formats. The write path serializes JSON; the read path reads it with fallback to current computation for old segments.

**Tech Stack:** Go standard library (`encoding/json`, `time`), existing test framework (`testify`).

---

### Task 1: Add `segmentMeta` and `readSegmentMeta()` to version.go

**Files:**
- Modify: `banyand/internal/storage/version.go`
- Test: `banyand/internal/storage/version_test.go` (new file)

- [ ] **Step 1: Write tests for `readSegmentMeta`**

Create `banyand/internal/storage/version_test.go`:

```go
package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadSegmentMeta_NewFormat(t *testing.T) {
	data := []byte(`{"version":"1.5.0","endTime":"2026-04-07T00:00:00+08:00"}`)
	meta, err := readSegmentMeta(data)
	require.NoError(t, err)
	assert.Equal(t, "1.5.0", meta.Version)
	assert.Equal(t, "2026-04-07T00:00:00+08:00", meta.EndTime)
}

func TestReadSegmentMeta_OldFormat(t *testing.T) {
	data := []byte("1.4.0")
	meta, err := readSegmentMeta(data)
	require.NoError(t, err)
	assert.Equal(t, "1.4.0", meta.Version)
	assert.Equal(t, "", meta.EndTime)
}

func TestReadSegmentMeta_OldFormatWithNewline(t *testing.T) {
	data := []byte("1.4.0\n")
	meta, err := readSegmentMeta(data)
	require.NoError(t, err)
	assert.Equal(t, "1.4.0", meta.Version)
	assert.Equal(t, "", meta.EndTime)
}

func TestReadSegmentMeta_IncompatibleVersion(t *testing.T) {
	data := []byte(`{"version":"0.1.0","endTime":"2026-04-07T00:00:00+08:00"}`)
	_, err := readSegmentMeta(data)
	assert.Error(t, err)
}

func TestReadSegmentMeta_NewFormatNoEndTime(t *testing.T) {
	data := []byte(`{"version":"1.5.0"}`)
	meta, err := readSegmentMeta(data)
	require.NoError(t, err)
	assert.Equal(t, "1.5.0", meta.Version)
	assert.Equal(t, "", meta.EndTime)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./banyand/internal/storage/ -run TestReadSegmentMeta -v`
Expected: FAIL — `readSegmentMeta` undefined.

- [ ] **Step 3: Add `segmentMeta` struct and `readSegmentMeta()` to `version.go`**

Add after the `checkVersion` function in `banyand/internal/storage/version.go`:

```go
type segmentMeta struct {
	Version string `json:"version"`
	EndTime string `json:"endTime,omitempty"`
}

func readSegmentMeta(data []byte) (segmentMeta, error) {
	var meta segmentMeta
	trimmed := strings.TrimSpace(string(data))
	if len(trimmed) > 0 && trimmed[0] == '{' {
		if unmarshalErr := json.Unmarshal(data, &meta); unmarshalErr != nil {
			return segmentMeta{}, unmarshalErr
		}
	} else {
		meta.Version = trimmed
	}
	if checkErr := checkVersion(meta.Version); checkErr != nil {
		return segmentMeta{}, checkErr
	}
	return meta, nil
}
```

Note: `encoding/json` and `strings` are already imported in `version.go`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./banyand/internal/storage/ -run TestReadSegmentMeta -v`
Expected: PASS — all 5 tests green.

- [ ] **Step 5: Commit**

```bash
git add banyand/internal/storage/version.go banyand/internal/storage/version_test.go
git commit -m "feat(storage): add segmentMeta struct and readSegmentMeta helper"
```

---

### Task 2: Bump version to 1.5.0

**Files:**
- Modify: `banyand/internal/storage/version.go:31`
- Modify: `banyand/internal/storage/versions.yml`

- [ ] **Step 1: Update `currentVersion` in `version.go`**

Change line 31 from:
```go
currentVersion             = "1.4.0"
```
to:
```go
currentVersion             = "1.5.0"
```

- [ ] **Step 2: Add `1.5.0` to `versions.yml`**

Read the current `versions.yml` and append `"1.5.0"` to the versions list.

- [ ] **Step 3: Verify existing tests still pass**

Run: `go test ./banyand/internal/storage/ -run TestReadSegmentMeta -v`
Expected: PASS — old-format test still passes because `"1.4.0"` remains in the compatible list.

- [ ] **Step 4: Commit**

```bash
git add banyand/internal/storage/version.go banyand/internal/storage/versions.yml
git commit -m "feat(storage): bump metadata version to 1.5.0"
```

---

### Task 3: Update `create()` to write JSON metadata

**Files:**
- Modify: `banyand/internal/storage/segment.go:540-544`

- [ ] **Step 1: Write a test that creates a segment and verifies metadata format**

Add to `banyand/internal/storage/segment_test.go`:

```go
func TestCreateSegmentWritesJSONMetadata(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-segment-metadata")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "test-stage",
		}
	})

	group := "test-group"
	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			return mockTSTable{ID: common.ShardID(0)}, nil
		},
		ShardNum:                       1,
		SegmentInterval:                IntervalRule{Unit: DAY, Num: 1},
		TTL:                            IntervalRule{Unit: DAY, Num: 7},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,
		nil,
		5*time.Minute,
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache,
		group,
	)

	now := time.Now().UTC()
	startTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	seg, err := sc.create(startTime)
	require.NoError(t, err)
	require.NotNil(t, seg)

	// Read metadata from disk and verify it's JSON with endTime
	suffix := startTime.Format(dayFormat)
	metadataPath := filepath.Join(tempDir, fmt.Sprintf("seg-%s", suffix), metadataFilename)
	rawMeta, readErr := os.ReadFile(metadataPath)
	require.NoError(t, readErr)

	meta, parseErr := readSegmentMeta(rawMeta)
	require.NoError(t, parseErr)
	assert.Equal(t, currentVersion, meta.Version)
	assert.NotEmpty(t, meta.EndTime, "endTime should be persisted in metadata")

	// Verify the endTime matches the segment's End
	expectedEnd := startTime.Add(24 * time.Hour)
	assert.Equal(t, expectedEnd.Format(time.RFC3339Nano), meta.EndTime)

	seg.DecRef()
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./banyand/internal/storage/ -run TestCreateSegmentWritesJSONMetadata -v`
Expected: FAIL — metadata is still plain text, `readSegmentMeta` parses it as old format so `EndTime` is empty.

- [ ] **Step 3: Update `create()` to write JSON metadata**

In `banyand/internal/storage/segment.go`, replace lines 540-544:

```go
	segPath := path.Join(sc.location, fmt.Sprintf(segTemplate, sc.format(start)))
	sc.lfs.MkdirPanicIfExist(segPath, DirPerm)
	data := []byte(currentVersion)
	metadataPath := filepath.Join(segPath, metadataFilename)
	lf, err := sc.lfs.CreateLockFile(metadataPath, FilePerm)
```

with:

```go
	segPath := path.Join(sc.location, fmt.Sprintf(segTemplate, sc.format(start)))
	sc.lfs.MkdirPanicIfExist(segPath, DirPerm)
	meta := segmentMeta{
		Version: currentVersion,
		EndTime: end.Format(time.RFC3339Nano),
	}
	data, marshalErr := json.Marshal(meta)
	if marshalErr != nil {
		logger.Panicf("cannot marshal segment metadata: %s", marshalErr)
	}
	metadataPath := filepath.Join(segPath, metadataFilename)
	lf, err := sc.lfs.CreateLockFile(metadataPath, FilePerm)
```

Add `"encoding/json"` to the imports in `segment.go` if not already present.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./banyand/internal/storage/ -run TestCreateSegmentWritesJSONMetadata -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add banyand/internal/storage/segment.go banyand/internal/storage/segment_test.go
git commit -m "feat(storage): write JSON metadata with endTime on segment creation"
```

---

### Task 4: Update `open()` to read `endTime` from metadata

**Files:**
- Modify: `banyand/internal/storage/segment.go:477-510`

- [ ] **Step 1: Write a test that loads segments with persisted endTime**

Add to `banyand/internal/storage/segment_test.go`:

```go
func TestOpenReadsPersistedEndTime(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-open-endtime")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "test-stage",
		}
	})

	group := "test-group"
	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			return mockTSTable{ID: common.ShardID(0)}, nil
		},
		ShardNum:                       1,
		SegmentInterval:                IntervalRule{Unit: DAY, Num: 1},
		TTL:                            IntervalRule{Unit: DAY, Num: 30},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,
		nil,
		5*time.Minute,
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache,
		group,
	)

	now := time.Now().UTC()
	day1 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)

	// Manually create segment directories with JSON metadata
	// day1 with a specific endTime that differs from the default NextTime
	customEnd := day1.Add(12 * time.Hour) // 12 hours instead of 24
	segPath1 := filepath.Join(tempDir, "seg-"+day1.Format(dayFormat))
	require.NoError(t, os.MkdirAll(segPath1, DirPerm))
	meta1 := segmentMeta{Version: currentVersion, EndTime: customEnd.Format(time.RFC3339Nano)}
	meta1Data, _ := json.Marshal(meta1)
	require.NoError(t, os.WriteFile(filepath.Join(segPath1, metadataFilename), meta1Data, FilePerm))

	// day2 with standard endTime
	segPath2 := filepath.Join(tempDir, "seg-"+day2.Format(dayFormat))
	require.NoError(t, os.MkdirAll(segPath2, DirPerm))
	meta2 := segmentMeta{Version: currentVersion, EndTime: day2.Add(24 * time.Hour).Format(time.RFC3339Nano)}
	meta2Data, _ := json.Marshal(meta2)
	require.NoError(t, os.WriteFile(filepath.Join(segPath2, metadataFilename), meta2Data, FilePerm))

	// Open the controller (loads segments from disk)
	openErr := sc.open()
	require.NoError(t, openErr)

	// Verify segments loaded correctly
	require.Len(t, sc.lst, 2)

	// First segment should have the custom endTime from metadata
	assert.Equal(t, customEnd, sc.lst[0].End, "segment 0 should use persisted endTime")
	// Second segment should have its endTime from metadata
	assert.Equal(t, day2.Add(24*time.Hour), sc.lst[1].End, "segment 1 should use persisted endTime")

	for _, seg := range sc.lst {
		seg.DecRef()
	}
}

func TestOpenFallbackOldFormatMetadata(t *testing.T) {
	tempDir, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	l := logger.GetLogger("test-open-fallback")
	ctx = context.WithValue(ctx, logger.ContextKey, l)
	ctx = common.SetPosition(ctx, func(_ common.Position) common.Position {
		return common.Position{
			Database: "test-db",
			Stage:    "test-stage",
		}
	})

	group := "test-group"
	opts := TSDBOpts[mockTSTable, mockTSTableOpener]{
		TSTableCreator: func(_ fs.FileSystem, _ string, _ common.Position, _ *logger.Logger,
			_ timestamp.TimeRange, _ mockTSTableOpener, _ any,
		) (mockTSTable, error) {
			return mockTSTable{ID: common.ShardID(0)}, nil
		},
		ShardNum:                       1,
		SegmentInterval:                IntervalRule{Unit: DAY, Num: 1},
		TTL:                            IntervalRule{Unit: DAY, Num: 30},
		SeriesIndexFlushTimeoutSeconds: 10,
		SeriesIndexCacheMaxBytes:       1024 * 1024,
	}

	serviceCache := NewServiceCache().(*serviceCache)
	sc := newSegmentController[mockTSTable, mockTSTableOpener](
		ctx,
		tempDir,
		l,
		opts,
		nil,
		nil,
		5*time.Minute,
		fs.NewLocalFileSystemWithLoggerAndLimit(logger.GetLogger("storage"), opts.MemoryLimit),
		serviceCache,
		group,
	)

	now := time.Now().UTC()
	day1 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	day2 := day1.Add(24 * time.Hour)

	// Create segment with OLD format metadata (plain version string)
	segPath1 := filepath.Join(tempDir, "seg-"+day1.Format(dayFormat))
	require.NoError(t, os.MkdirAll(segPath1, DirPerm))
	require.NoError(t, os.WriteFile(filepath.Join(segPath1, metadataFilename), []byte("1.4.0"), FilePerm))

	segPath2 := filepath.Join(tempDir, "seg-"+day2.Format(dayFormat))
	require.NoError(t, os.MkdirAll(segPath2, DirPerm))
	require.NoError(t, os.WriteFile(filepath.Join(segPath2, metadataFilename), []byte("1.4.0"), FilePerm))

	// Open should succeed with fallback end time computation
	openErr := sc.open()
	require.NoError(t, openErr)

	require.Len(t, sc.lst, 2)

	// First segment's end should be day2's start (fallback: end = next segment's start)
	assert.Equal(t, day2, sc.lst[0].End, "old format should use fallback end time")
	// Second segment's end should be day2 + 24h (fallback: end = NextTime(start))
	assert.Equal(t, day2.Add(24*time.Hour), sc.lst[1].End, "last segment should use NextTime fallback")

	for _, seg := range sc.lst {
		seg.DecRef()
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./banyand/internal/storage/ -run "TestOpenReadsPersistedEndTime|TestOpenFallbackOldFormatMetadata" -v`
Expected: FAIL — `open()` still computes end times from adjacent segments, ignoring metadata.

- [ ] **Step 3: Update `open()` to read `endTime` from metadata**

In `banyand/internal/storage/segment.go`, replace the callback body inside `open()` (lines 482-501):

```go
		suffix := sc.format(start)
		segmentPath := path.Join(sc.location, fmt.Sprintf(segTemplate, suffix))
		metadataPath := path.Join(segmentPath, metadataFilename)
		version, err := sc.lfs.Read(metadataPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				emptySegments = append(emptySegments, segmentPath)
				return nil
			}
			return err
		}
		if len(version) == 0 {
			emptySegments = append(emptySegments, segmentPath)
			return nil
		}
		if err = checkVersion(convert.BytesToString(version)); err != nil {
			return err
		}
		_, err = sc.load(start, end, sc.location)
		return err
```

with:

```go
		suffix := sc.format(start)
		segmentPath := path.Join(sc.location, fmt.Sprintf(segTemplate, suffix))
		metadataPath := path.Join(segmentPath, metadataFilename)
		rawMeta, readErr := sc.lfs.Read(metadataPath)
		if readErr != nil {
			if errors.Is(readErr, fs.ErrNotExist) {
				emptySegments = append(emptySegments, segmentPath)
				return nil
			}
			return readErr
		}
		if len(rawMeta) == 0 {
			emptySegments = append(emptySegments, segmentPath)
			return nil
		}
		meta, parseErr := readSegmentMeta(rawMeta)
		if parseErr != nil {
			return parseErr
		}
		segmentEnd := end
		if meta.EndTime != "" {
			parsedEnd, timeErr := time.Parse(time.RFC3339Nano, meta.EndTime)
			if timeErr != nil {
				return timeErr
			}
			segmentEnd = parsedEnd
		}
		_, err = sc.load(start, segmentEnd, sc.location)
		return err
```

- [ ] **Step 4: Run the new tests to verify they pass**

Run: `go test ./banyand/internal/storage/ -run "TestOpenReadsPersistedEndTime|TestOpenFallbackOldFormatMetadata" -v`
Expected: PASS.

- [ ] **Step 5: Run all existing segment tests to verify no regression**

Run: `go test ./banyand/internal/storage/ -run TestSegment -v`
Expected: PASS — existing tests use `openSegment()` directly, not `open()`, so they're unaffected.

- [ ] **Step 6: Commit**

```bash
git add banyand/internal/storage/segment.go banyand/internal/storage/segment_test.go
git commit -m "feat(storage): read persisted endTime from segment metadata on load"
```

---

### Task 5: Run full test suite and verify

**Files:** None changed — verification only.

- [ ] **Step 1: Run the full storage package test suite**

Run: `go test ./banyand/internal/storage/ -v -count=1`
Expected: All tests PASS.

- [ ] **Step 2: Run the linter**

Run: `make lint`
Expected: No new lint errors.

- [ ] **Step 3: Final commit (if any fixes needed)**

Only if lint or test fixes were needed.
