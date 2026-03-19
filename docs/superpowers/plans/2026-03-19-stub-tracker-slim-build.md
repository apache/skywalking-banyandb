# Stub Tracker in Slim Builds Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove Tracker and stack tracking code from production (`slim`) builds using build tags, keeping full functionality in `!slim` builds.

**Architecture:** Split `pkg/pool` into slim (no-op stubs) and !slim (full tracking) implementations via Go build tags. Tracker is stubbed out in slim builds. `stackTrackingEnabled` remains in `pool.go` since `Synced[T]` methods use it.

**Tech Stack:** Go build tags (`//go:build slim` / `//go:build !slim`)

---

## File Map

| File | Responsibility |
|------|---------------|
| `pkg/pool/tracker_stub.go` | New — no-op Tracker for slim |
| `pkg/pool/tracker_debug.go` | Rename from tracker.go — full Tracker for !slim |
| `pkg/pool/pool_nop.go` | New — no-op stubs for EnableStackTracking/AllRefsCount/AllStacks for slim |
| `pkg/pool/pool_debug.go` | New — debug functions moved from pool.go for !slim |
| `pkg/pool/pool.go` | Modified — keep Synced[T]/Register/Trackable/StackTracker/interfaces, remove debug fns |
| `pkg/pool/tracker.go` | Delete — moved to tracker_debug.go |

---

## Task 1: Create tracker_stub.go (slim no-op)

**Files:**
- Create: `pkg/pool/tracker_stub.go`

- [ ] **Step 1: Create tracker_stub.go**

```go
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

//go:build slim
// +build slim

package pool

// Tracker is a no-op stub in slim builds.
type Tracker struct{}

// Acquire is a no-op in slim builds.
func (t *Tracker) Acquire(any) {}

// Release is a no-op in slim builds.
func (t *Tracker) Release(any) {}

// RefsCount returns 0 in slim builds.
func (t *Tracker) RefsCount() int { return 0 }

// Stacks returns nil in slim builds.
func (t *Tracker) Stacks() []string { return nil }

// RegisterTracker returns a no-op tracker.
func RegisterTracker(name string) *Tracker {
	return new(Tracker)
}
```

- [ ] **Step 2: Verify build with slim tag**

Run: `go build -tags slim ./pkg/pool/...`
Expected: SUCCESS

- [ ] **Step 3: Commit**

Run:
```bash
git add pkg/pool/tracker_stub.go
git commit -m "pkg/pool: add slim tracker stub"
```

---

## Task 2: Rename tracker.go to tracker_debug.go with !slim tag

**Files:**
- Create: `pkg/pool/tracker_debug.go`
- Delete: `pkg/pool/tracker.go` (after creating tracker_debug.go)

- [ ] **Step 1: Create tracker_debug.go from current tracker.go content**

Copy the current `tracker.go` content but add `//go:build !slim` build tag at the top (after the license header, before package declaration):

```go
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

//go:build !slim
// +build !slim

package pool
// ... rest of current tracker.go content ...
```

- [ ] **Step 2: Delete old tracker.go**

Run: `rm pkg/pool/tracker.go`

- [ ] **Step 3: Verify build with !slim tag**

Run: `go build ./pkg/pool/...`
Expected: SUCCESS (default !slim build)

- [ ] **Step 4: Commit**

Run:
```bash
git add pkg/pool/tracker_debug.go
git rm pkg/pool/tracker.go
git commit -m "pkg/pool: rename tracker.go to tracker_debug.go with !slim tag"
```

---

## Task 3: Create pool_nop.go (slim no-op stubs)

**Files:**
- Create: `pkg/pool/pool_nop.go`

- [ ] **Step 1: Create pool_nop.go**

```go
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

//go:build slim
// +build slim

package pool

// EnableStackTracking is a no-op in slim builds.
func EnableStackTracking(enabled bool) {}

// AllRefsCount returns nil in slim builds.
func AllRefsCount() map[string]int { return nil }

// AllStacks returns nil in slim builds.
func AllStacks() map[string][]string { return nil }
```

- [ ] **Step 2: Verify build with slim tag**

Run: `go build -tags slim ./pkg/pool/...`
Expected: SUCCESS

- [ ] **Step 3: Commit**

Run:
```bash
git add pkg/pool/pool_nop.go
git commit -m "pkg/pool: add slim pool_nop.go with no-op debug functions"
```

---

## Task 4: Create pool_debug.go (move debug functions with !slim tag)

**Files:**
- Create: `pkg/pool/pool_debug.go`

- [ ] **Step 1: Create pool_debug.go**

From current `pool.go` lines 33-70, with `//go:build !slim` build tag:

```go
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

//go:build !slim
// +build !slim

package pool

// EnableStackTracking enables or disables stack tracking for all pools.
func EnableStackTracking(enabled bool) {
	stackTrackingEnabled.Store(enabled)
}

// AllRefsCount returns the reference count of all pools.
func AllRefsCount() map[string]int {
	result := make(map[string]int)
	poolMap.Range(func(key, value any) bool {
		result[key.(string)] = value.(Trackable).RefsCount()
		return true
	})
	return result
}

// AllStacks returns all recorded stack traces for leaked objects from all pools.
func AllStacks() map[string][]string {
	result := make(map[string][]string)
	poolMap.Range(func(key, value any) bool {
		if st, ok := value.(StackTracker); ok {
			stacks := st.Stacks()
			if len(stacks) > 0 {
				result[key.(string)] = stacks
			}
		}
		return true
	})
	return result
}
```

- [ ] **Step 2: Verify build with !slim tag**

Run: `go build ./pkg/pool/...`
Expected: SUCCESS

- [ ] **Step 3: Commit**

Run:
```bash
git add pkg/pool/pool_debug.go
git commit -m "pkg/pool: add pool_debug.go with debug functions"
```

---

## Task 5: Modify pool.go (remove debug functions)

**Files:**
- Modify: `pkg/pool/pool.go`

- [ ] **Step 1: Remove debug functions from pool.go**

Remove lines 33-70 (EnableStackTracking, AllRefsCount, AllStacks) from pool.go.

Keep the imports that are still needed: `fmt`, `runtime`, `sync`, `sync/atomic`.
- `runtime` is used by Synced[T].Get() for stack capture
- `sync` and `sync/atomic` are used by Synced[T]
- `fmt` is used by Register

- [ ] **Step 2: Verify build with !slim tag**

Run: `go build ./pkg/pool/...`
Expected: SUCCESS

- [ ] **Step 3: Verify build with slim tag**

Run: `go build -tags slim ./pkg/pool/...`
Expected: SUCCESS

- [ ] **Step 4: Verify build for entire project with slim tag**

Run: `go build -tags slim ./...`
Expected: SUCCESS

- [ ] **Step 5: Commit**

Run:
```bash
git add pkg/pool/pool.go
git commit -m "pkg/pool: remove debug functions from pool.go (moved to pool_debug.go)"
```

---

## Task 6: Run full verification

- [ ] **Step 1: Run tests with default (!slim) build**

Run: `go test ./pkg/pool/...`
Expected: SUCCESS

- [ ] **Step 2: Run tests with slim build**

Run: `go test -tags slim ./pkg/pool/...`
Expected: SUCCESS

- [ ] **Step 3: Run linter on pkg/pool**

Run: `/Users/gao/Developer/banyandb/skywalking-banyandb/bin/golangci-lint run ./pkg/pool/...`
Expected: SUCCESS

- [ ] **Step 4: Commit all remaining changes**

Run:
```bash
git status
git add -A
git commit -m "pkg/pool: complete slim build support for tracker stubbing"
```

---

## Verification Summary

| Command | Expected |
|---------|----------|
| `go build -tags slim ./pkg/pool/...` | SUCCESS |
| `go build ./pkg/pool/...` | SUCCESS |
| `go build -tags slim ./...` | SUCCESS |
| `go test -tags slim ./pkg/pool/...` | SUCCESS |
| `go test ./pkg/pool/...` | SUCCESS |
| `golangci-lint run ./pkg/pool/...` | SUCCESS |
