# Stable Segment End Time Design

## Problem

Segment end times are computed dynamically at load time. The last segment's end is always `start + interval` (a moving target), and non-last segments derive their end from the next segment's start. This causes:

1. **Query correctness risk**: end boundaries can shift across restarts or interval config changes, causing data to be missed or double-counted.
2. **External coordination difficulty**: replication, backup, and compaction systems need predictable segment boundaries.

## Solution

Persist the end time in each segment's `metadata` file alongside the existing version string. The end time is written once at segment creation and read back on load.

## Metadata File Format

**Current** (plain text):
```
1.4.0
```

**New** (JSON):
```json
{"version":"1.5.0","endTime":"2026-04-07T00:00:00+08:00"}
```

- `endTime` uses RFC3339Nano format with timezone.
- Version bumped to `1.5.0`.
- Old plain-text format remains readable via fallback.

## Write Path

In `segmentController.create()` (`segment.go:512-556`):

1. Compute `end` exactly as today (`NextTime(start)` or clamped to next segment's start).
2. Serialize `segmentMeta{Version, EndTime}` as JSON and write to `metadata` file.

No signature changes needed — `openSegment` already accepts `startTime` and `endTime` parameters.

## Read Path

In `segmentController.open()` → `loadSegments()` (`segment.go:477-510`, `segment.go:718-746`):

1. Read the metadata file and try to parse as JSON.
2. If `endTime` field is present → use it directly.
3. If absent (old format, plain version string) → fall back to current computation (next segment's start or `NextTime(start)`).

Detection heuristic: if file content starts with `{`, parse as JSON; otherwise treat as plain version string.

`checkVersion` validation is integrated into the new `readSegmentMeta` function.

## Backward Compatibility

- `currentVersion` bumped to `"1.5.0"`.
- `"1.5.0"` added to `versions.yml` compatible versions list.
- `"1.4.0"` remains in the list — old segments are fully readable.
- No migration tool required. Old segments compute end time dynamically via fallback. As segments expire via TTL, they are naturally replaced by new-format segments.

## Snapshots

`TakeFileSnapshot` (`tsdb.go:265-318`) hardlinks the `metadata` file as-is. No change needed — the JSON format is copied identically. Restored segments read their own `endTime` from their metadata.

## Files Modified

| File | Change |
|------|--------|
| `banyand/internal/storage/version.go` | Bump `currentVersion` to `"1.5.0"`, add `segmentMeta` struct, add `readSegmentMeta()` |
| `banyand/internal/storage/versions.yml` | Add `"1.5.0"` to compatible versions |
| `banyand/internal/storage/segment.go` | `create()`: write JSON metadata; `open()`: read `endTime` from metadata with fallback |
| `banyand/internal/storage/segment_test.go` | Update test helpers to write JSON metadata |

## Files Not Changed

- `segment.go` method signatures — `openSegment` already accepts `startTime, endTime`
- `tsdb.go` snapshot logic — hardlinks work as-is
- `storage.go` — no changes to interval or time range types
