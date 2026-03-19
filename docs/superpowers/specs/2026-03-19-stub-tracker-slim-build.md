# Stub Tracker in Production Builds

## Status
Approved

## Date
2026-03-19

## Context

The `pkg/pool` package provides `Tracker` for debugging resource leaks via `Acquire`/`Release` stack traces. The expensive `runtime.Stack` capture is guarded by `stackTrackingEnabled`, which is only set to `true` in integration tests. However, the Tracker struct and its atomic ref counting remain in production binaries.

Goal: Remove Tracker and stack tracking code from production (`slim`) builds entirely.

## Design

### Build Tags
- `slim` (default for production): no-op stubs, no tracking code
- `!slim` (debug/test builds): full Tracker implementation with stack capture

### Implementation

#### 1. `pkg/pool/tracker_stub.go` (new, `slim`)
```go
//go:build slim
// +build slim

package pool

// Tracker is a no-op stub in slim builds.
type Tracker struct{}

func (t *Tracker) Acquire(any) {}
func (t *Tracker) Release(any) {}
func (t *Tracker) RefsCount() int { return 0 }
func (t *Tracker) Stacks() []string { return nil }

// RegisterTracker returns a no-op tracker.
func RegisterTracker(name string) *Tracker {
    return new(Tracker)
}
```

#### 2. `pkg/pool/tracker_debug.go` (new, `!slim`)
Current `tracker.go` content with build tag `//go:build !slim`.

#### 3. `pkg/pool/pool_nop.go` (new, `slim`)
```go
//go:build slim
// +build slim

package pool

func EnableStackTracking(enabled bool) {}

func AllRefsCount() map[string]int { return nil }

func AllStacks() map[string][]string { return nil }
```

#### 4. `pkg/pool/pool_debug.go` (new, `!slim`)
Move `EnableStackTracking`, `AllRefsCount`, `AllStacks` from current `pool.go` with build tag `//go:build !slim`.

Note: `stackTrackingEnabled` variable remains in `pool.go` because `Synced[T].Get()` and `Synced[T].Put()` use it.

#### 5. `pkg/pool/pool.go`
Keep `poolMap`, `stackTrackingEnabled`, `Trackable`, `StackTracker` interfaces, `Synced[T]` struct and its methods, and `Register`. Remove debug-only functions (`EnableStackTracking`, `AllRefsCount`, `AllStacks`) to `pool_debug.go`.

### File Changes

| File | Action |
|------|--------|
| `pkg/pool/tracker.go` | Rename to `tracker_debug.go` |
| `pkg/pool/tracker_stub.go` | Create (no-op, `slim`) |
| `pkg/pool/pool.go` | Keep only pool structs/interfaces, remove debug functions |
| `pkg/pool/pool_nop.go` | Create (no-op stubs, `slim`) |
| `pkg/pool/pool_debug.go` | Create (debug fns, `!slim`) |
| `banyand/measure/query.go` | No changes |
| Test files | No changes needed (explicitly enable tracking) |

### Behavior

- **Production (`slim`)**: Tracker calls are no-ops. `AllRefsCount()`/`AllStacks()` return nil/empty. Binary is smaller.
- **Debug/Test (`!slim`)**: Full tracking with stack traces when `EnableStackTracking(true)` is called.

## Alternatives Considered

- **Build-tagged stub files (Option A)**: Similar but less clean separation. Option B (interface-based) is cleaner and matches existing `slim`/`!slim` pattern.
- **Keep as-is (Option C)**: The runtime.Stack capture is already skipped in production, but the Tracker struct and atomic ops remain. Not sufficient for code cleanliness goal.
