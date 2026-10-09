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

package export

import (
	"context"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// fakeBackend is a Backend over a temp directory: TakeExportSnapshot copies the live
// group directories into <exportSnapshotDir>/<name>. onSnapshot, when set, runs after the
// copy with that session directory so a test can tamper with it.
type fakeBackend struct {
	onSnapshot        func(sessionDir string, attempt int32)
	dataDir           string
	snapshotDir       string
	exportSnapshotDir string
	catalog           commonv1.Catalog
	snapshots         atomic.Int32
	fail              bool
}

func newFakeBackend(t *testing.T, root string, catalog commonv1.Catalog) *fakeBackend {
	t.Helper()
	name := strings.ToLower(strings.TrimPrefix(catalog.String(), "CATALOG_"))
	b := &fakeBackend{
		dataDir:           filepath.Join(root, name, storage.DataDir),
		snapshotDir:       filepath.Join(root, name, storage.SnapshotsDir),
		exportSnapshotDir: filepath.Join(root, name, storage.ExportSnapshotsDir),
		catalog:           catalog,
	}
	if err := os.MkdirAll(b.dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	return b
}

func (b *fakeBackend) GetDataPath() string { return b.dataDir }

// ReadPartMetadata implements PartReader with the catalog's real reader.
func (b *fakeBackend) ReadPartMetadata(partDir string) (queue.StreamingPartData, error) {
	return testPartReader(b.catalog)(partDir)
}
func (b *fakeBackend) GetSnapshotDir() string       { return b.snapshotDir }
func (b *fakeBackend) GetExportSnapshotDir() string { return b.exportSnapshotDir }

func (b *fakeBackend) TakeExportSnapshot(_ context.Context, name string) error {
	attempt := b.snapshots.Add(1)
	if b.fail {
		return errors.New("injected snapshot failure")
	}
	dst := filepath.Join(b.exportSnapshotDir, name)
	entries, err := os.ReadDir(b.dataDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if err := os.CopyFS(filepath.Join(dst, e.Name()), os.DirFS(filepath.Join(b.dataDir, e.Name()))); err != nil {
			return err
		}
	}
	if b.onSnapshot != nil {
		b.onSnapshot(dst, attempt)
	}
	return nil
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func allCatalogs() map[commonv1.Catalog]struct{} {
	return catalogSet(commonv1.Catalog_CATALOG_STREAM, commonv1.Catalog_CATALOG_MEASURE,
		commonv1.Catalog_CATALOG_TRACE, commonv1.Catalog_CATALOG_PROPERTY)
}

func catalogSet(cs ...commonv1.Catalog) map[commonv1.Catalog]struct{} {
	out := make(map[commonv1.Catalog]struct{}, len(cs))
	for _, c := range cs {
		out[c] = struct{}{}
	}
	return out
}

// skipIfRoot skips a test that relies on a read-only directory: root ignores the mode bits.
func skipIfRoot(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
}

// newTestBackends builds four fake backends over one temp dir and seeds the stream catalog
// with the given groups, each holding one flushed part so snapshots have something to copy.
func newTestBackends(t *testing.T, streamGroups ...string) (Backends, *fakeClock) {
	t.Helper()
	root := t.TempDir()
	backends := Backends{}
	for _, c := range []commonv1.Catalog{
		commonv1.Catalog_CATALOG_STREAM, commonv1.Catalog_CATALOG_MEASURE,
		commonv1.Catalog_CATALOG_TRACE, commonv1.Catalog_CATALOG_PROPERTY,
	} {
		backends[c] = newFakeBackend(t, root, c)
	}
	for _, g := range streamGroups {
		seg := filepath.Join(backends[commonv1.Catalog_CATALOG_STREAM].GetDataPath(), g, "seg-20260928")
		writeFile(t, filepath.Join(seg, "metadata"), []byte(`{"version":"1.5.0"}`))
		writeFile(t, filepath.Join(seg, "shard-0", "0000000000000001", "metadata.json"), partJSON(1, 2, 3, 4, 5))
	}
	return backends, &fakeClock{t: time.Unix(1_700_000_000, 0)}
}

// newTestManager builds a manager over newTestBackends with the single stream group "sw_record".
func newTestManager(t *testing.T) (*sessionManager, Backends, *fakeClock) {
	t.Helper()
	backends, clock := newTestBackends(t, "sw_record")
	m := newSessionManager(backends, logger.GetLogger("export-test"))
	m.now = clock.now
	return m, backends, clock
}

func mustCreate(t *testing.T, m *sessionManager, id string, catalogs map[commonv1.Catalog]struct{}) *createdSession {
	t.Helper()
	return mustCreateWith(t, m, id, catalogs, false)
}

func mustCreateWith(t *testing.T, m *sessionManager, id string, catalogs map[commonv1.Catalog]struct{}, preempt bool) *createdSession {
	t.Helper()
	created, err := m.create(context.Background(), id, catalogs, preempt)
	if err != nil {
		t.Fatalf("create(%s, preempt=%t): %v", id, preempt, err)
	}
	return created
}

func mustSessionDir(t *testing.T, m *sessionManager, catalog commonv1.Catalog, id string) string {
	t.Helper()
	dir, err := m.sessionDir(catalog, id)
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// sessionIDs returns the ids of every session directory set on disk, in id order.
func sessionIDs(t *testing.T, m *sessionManager) []string {
	t.Helper()
	sessions, err := m.existing()
	if err != nil {
		t.Fatal(err)
	}
	return slices.Sorted(maps.Keys(sessions))
}

// age sets the mtime of p relative to the fake clock, since the real file system stamps
// directories with wall-clock time.
func age(t *testing.T, p string, at time.Time) {
	t.Helper()
	if err := os.Chtimes(p, at, at); err != nil {
		t.Fatal(err)
	}
}

func TestSession_CreateProbeRenewRelease(t *testing.T) {
	ctx := context.Background()
	m, _, clock := newTestManager(t)
	created := mustCreate(t, m, testSessionID, catalogSet(commonv1.Catalog_CATALOG_STREAM))
	if len(created.preempted) != 0 {
		t.Fatalf("nothing to preempt on an empty node, got %v", created.preempted)
	}
	streamDir := mustSessionDir(t, m, commonv1.Catalog_CATALOG_STREAM, testSessionID)
	if got := created.roots[commonv1.Catalog_CATALOG_STREAM]; got != streamDir {
		t.Fatalf("create must hand back the stream groups root %s, got %s", streamDir, got)
	}
	lease, err := readLease(streamDir)
	if err != nil {
		t.Fatal(err)
	}
	if want := clock.now().UnixNano(); lease.SessionID != testSessionID || lease.StartedAt != want || lease.LastHeartbeatAt != want ||
		lease.ExpiresAt != clock.now().Add(defaultLease).UnixNano() {
		t.Fatalf("create must write the lease at creation time with the default length: %+v", lease)
	}
	_, err = os.Stat(filepath.Join(streamDir, "sw_record", "seg-20260928", "shard-0", "0000000000000001", "metadata.json"))
	if err != nil {
		t.Fatalf("snapshot content missing: %v", err)
	}
	measureDir := mustSessionDir(t, m, commonv1.Catalog_CATALOG_MEASURE, testSessionID)
	if _, err = os.Stat(measureDir); !os.IsNotExist(err) {
		t.Fatal("unselected catalog must not get a session dir")
	}

	probed, err := m.probe()
	if err != nil || probed.GetSessionId() != testSessionID ||
		len(probed.GetCatalogs()) != 1 || probed.GetCatalogs()[0] != commonv1.Catalog_CATALOG_STREAM {
		t.Fatalf("probe = %+v, %v", probed, err)
	}

	clock.advance(10 * time.Minute)
	roots, err := m.locateAndHeartbeat(testSessionID)
	if err != nil || roots[commonv1.Catalog_CATALOG_STREAM] != streamDir || len(roots) != 1 {
		t.Fatalf("locate = %+v, %v", roots, err)
	}
	lease, _ = readLease(streamDir)
	if lease.LastHeartbeatAt != clock.now().UnixNano() || lease.ExpiresAt != clock.now().Add(defaultLease).UnixNano() {
		t.Fatalf("renew must rewrite lastHeartbeatAt and expiresAt: %+v", lease)
	}

	if _, err = m.release(testSessionID); err != nil {
		t.Fatal(err)
	}
	if _, err = m.release(testSessionID); err != nil {
		t.Fatal("release must be idempotent:", err)
	}
	_, err = m.locateAndHeartbeat(testSessionID)
	if status.Code(err) != codes.NotFound || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("released session must be NotFound: %v", err)
	}
	if _, err = m.create(ctx, "bbbb", allCatalogs(), false); err != nil {
		t.Fatalf("a released node must accept a new session: %v", err)
	}
}

func TestSession_CreateCommitsLeaseAfterLastSnapshot(t *testing.T) {
	m, backends, clock := newTestManager(t)
	// Every snapshot takes a minute; the committed lease must be stamped after the last one.
	for _, b := range backends {
		b.(*fakeBackend).onSnapshot = func(string, int32) { clock.advance(time.Minute) }
	}
	start := clock.now()
	mustCreate(t, m, testSessionID, allCatalogs())
	for catalog := range allCatalogs() {
		l, err := readLease(mustSessionDir(t, m, catalog, testSessionID))
		if err != nil {
			t.Fatal(err)
		}
		if l.StartedAt != start.UnixNano() || l.LastHeartbeatAt != clock.now().UnixNano() ||
			l.ExpiresAt != clock.now().Add(defaultLease).UnixNano() {
			t.Fatalf("%s lease must be committed with the time of the last snapshot: %+v (start %d, end %d)",
				catalog, l, start.UnixNano(), clock.now().UnixNano())
		}
	}
}

func TestSession_CreateRefusesOccupantUnlessPreempt(t *testing.T) {
	ctx := context.Background()
	m, _, clock := newTestManager(t)
	mustCreate(t, m, testSessionID, allCatalogs())
	clock.advance(time.Hour) // how long ago the occupant last heartbeat is not create's business
	_, err := m.create(ctx, "bbbb", allCatalogs(), false)
	if status.Code(err) != codes.AlreadyExists || !strings.Contains(err.Error(), testSessionID) {
		t.Fatalf("want AlreadyExists naming aaaa, got %v", err)
	}
	if got := sessionIDs(t, m); !slices.Equal(got, []string{testSessionID}) {
		t.Fatalf("the loser must leave no trace: %v", got)
	}
	created, err := m.create(ctx, "bbbb", allCatalogs(), true)
	if err != nil || !slices.Equal(created.preempted, []string{testSessionID}) {
		t.Fatalf("preempt = %v, %v", created, err)
	}
	if got := sessionIDs(t, m); !slices.Equal(got, []string{"bbbb"}) {
		t.Fatalf("old session must be gone: %v", got)
	}
}

func TestSession_CreateRefusesExpiredOccupantWithoutPreempt(t *testing.T) {
	ctx := context.Background()
	m, _, clock := newTestManager(t)
	mustCreate(t, m, testSessionID, allCatalogs())
	clock.advance(defaultLease + time.Second)
	_, err := m.create(ctx, "bbbb", catalogSet(commonv1.Catalog_CATALOG_STREAM), false)
	if status.Code(err) != codes.AlreadyExists || !strings.Contains(err.Error(), testSessionID) {
		t.Fatalf("an expired occupant is the sweeper's business, create must still refuse: %v", err)
	}
	if got := sessionIDs(t, m); !slices.Equal(got, []string{testSessionID}) {
		t.Fatalf("the refused create must leave the occupant alone: %v", got)
	}
	created, err := m.create(ctx, "bbbb", catalogSet(commonv1.Catalog_CATALOG_STREAM), true)
	if err != nil || !slices.Equal(created.preempted, []string{testSessionID}) {
		t.Fatalf("preempt = %v, %v", created, err)
	}
	if got := sessionIDs(t, m); !slices.Equal(got, []string{"bbbb"}) {
		t.Fatalf("only the new session may remain: %v", got)
	}
}

func TestSession_PreemptRemovesEveryOccupantAndReportsAll(t *testing.T) {
	ctx := context.Background()
	m, _, clock := newTestManager(t)
	mustCreate(t, m, "aaaa", catalogSet(commonv1.Catalog_CATALOG_STREAM))
	// A second occupant can only get here through preempt; a lease-less leftover is the
	// other shape a node may hold. Both go, whatever their age or lease says.
	mustCreateWith(t, m, "bbbb", catalogSet(commonv1.Catalog_CATALOG_MEASURE), true)
	orphan := mustSessionDir(t, m, commonv1.Catalog_CATALOG_TRACE, "cccc")
	if err := os.MkdirAll(orphan, 0o755); err != nil {
		t.Fatal(err)
	}
	age(t, orphan, clock.now())
	_, err := m.create(ctx, "dddd", allCatalogs(), false)
	if status.Code(err) != codes.AlreadyExists || !strings.Contains(err.Error(), "bbbb") || !strings.Contains(err.Error(), "cccc") {
		t.Fatalf("the refusal must name every occupant, got %v", err)
	}
	created := mustCreateWith(t, m, "dddd", allCatalogs(), true)
	if !slices.Equal(created.preempted, []string{"bbbb", "cccc"}) {
		t.Fatalf("preempt must remove and report every occupant, sorted, got %v", created.preempted)
	}
	if got := sessionIDs(t, m); !slices.Equal(got, []string{"dddd"}) {
		t.Fatalf("only the new session may remain: %v", got)
	}
}

func TestSession_CreateRefusesLeaselessOccupantWithoutPreempt(t *testing.T) {
	ctx := context.Background()
	m, _, clock := newTestManager(t)
	orphan := mustSessionDir(t, m, commonv1.Catalog_CATALOG_STREAM, "dead")
	if err := os.MkdirAll(orphan, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{clock.now(), clock.now().Add(-orphanGrace - time.Minute)} {
		age(t, orphan, at)
		_, err := m.create(ctx, testSessionID, allCatalogs(), false)
		if status.Code(err) != codes.AlreadyExists || !strings.Contains(err.Error(), "dead") {
			t.Fatalf("a directory without lease aged %s is an occupant and must be refused, got %v", clock.now().Sub(at), err)
		}
	}
	created := mustCreateWith(t, m, testSessionID, allCatalogs(), true)
	if !slices.Equal(created.preempted, []string{"dead"}) {
		t.Fatalf("preempt must remove the lease-less directory, got %v", created.preempted)
	}
}

func TestSession_CreateCanceledBeforeStartRemovesNothing(t *testing.T) {
	m, _, _ := newTestManager(t)
	mustCreate(t, m, testSessionID, allCatalogs())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := m.create(ctx, "bbbb", allCatalogs(), true)
	if status.Code(err) != codes.Canceled {
		t.Fatalf("want Canceled, got %v", err)
	}
	if got := sessionIDs(t, m); !slices.Equal(got, []string{testSessionID}) {
		t.Fatalf("a canceled create must not touch the occupant: %v", got)
	}
}

func TestSession_CreateCanceledWhileWaitingForLockRemovesNothing(t *testing.T) {
	m, _, _ := newTestManager(t)
	mustCreate(t, m, testSessionID, allCatalogs())
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	errCh := make(chan error, 1)
	//panicdiag:allow-rawgo test goroutine blocked on the manager lock; its result is read from errCh
	go func() {
		_, err := m.create(ctx, "bbbb", allCatalogs(), true)
		errCh <- err
	}()
	// Give the goroutine time to pass the first ctx check and block on the lock; the result
	// is the same if it has not got there yet.
	time.Sleep(50 * time.Millisecond)
	cancel()
	m.mu.Unlock()
	if err := <-errCh; status.Code(err) != codes.Canceled {
		t.Fatalf("want Canceled, got %v", err)
	}
	if got := sessionIDs(t, m); !slices.Equal(got, []string{testSessionID}) {
		t.Fatalf("a create canceled while waiting for the lock must not touch the occupant: %v", got)
	}
	if _, err := m.locateAndHeartbeat(testSessionID); err != nil {
		t.Fatalf("the occupant must still be renewable: %v", err)
	}
}

// TestSession_CreateSingleCatalogOccupantRemovalFailureKeepsOccupant covers an occupant held
// in one catalog only: its lease removal fails first, so nothing of it is touched.
func TestSession_CreateSingleCatalogOccupantRemovalFailureKeepsOccupant(t *testing.T) {
	skipIfRoot(t)
	ctx := context.Background()
	m, _, _ := newTestManager(t)
	mustCreate(t, m, testSessionID, catalogSet(commonv1.Catalog_CATALOG_STREAM))
	dir := mustSessionDir(t, m, commonv1.Catalog_CATALOG_STREAM, testSessionID)
	// A read-only session directory refuses the removal of its lease, which comes first.
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	_, err := m.create(ctx, "bbbb", catalogSet(commonv1.Catalog_CATALOG_STREAM), true)
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "preempt export session "+testSessionID) {
		t.Fatalf("want Internal naming the occupant, got %v", err)
	}
	if err = os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if got := sessionIDs(t, m); !slices.Equal(got, []string{testSessionID}) {
		t.Fatalf("the occupant must still be intact: %v", got)
	}
	if _, err = m.locateAndHeartbeat(testSessionID); err != nil {
		t.Fatalf("the occupant must still be renewable: %v", err)
	}
}

func TestSession_ExpiredIsDistinctFromNotFound(t *testing.T) {
	m, _, clock := newTestManager(t)
	_, err := m.locateAndHeartbeat("deadbeef")
	if status.Code(err) != codes.NotFound {
		t.Fatalf("unknown session must be NotFound, got %v", err)
	}
	mustCreate(t, m, testSessionID, allCatalogs())
	clock.advance(defaultLease + time.Second)
	_, err = m.locateAndHeartbeat(testSessionID)
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("want expired FailedPrecondition, got %v", err)
	}
	m.sweep()
	if got := sessionIDs(t, m); len(got) != 0 {
		t.Fatalf("sweep must delete expired sessions: %v", got)
	}
}

func TestSession_RenewRewritesEveryCatalogCopy(t *testing.T) {
	m, _, clock := newTestManager(t)
	mustCreate(t, m, testSessionID, allCatalogs())
	clock.advance(time.Hour)
	if _, err := m.locateAndHeartbeat(testSessionID); err != nil {
		t.Fatal(err)
	}
	for catalog := range allCatalogs() {
		l, err := readLease(mustSessionDir(t, m, catalog, testSessionID))
		if err != nil {
			t.Fatal(err)
		}
		if l.LastHeartbeatAt != clock.now().UnixNano() || l.ExpiresAt != clock.now().Add(defaultLease).UnixNano() {
			t.Fatalf("%s copy was not renewed: %+v", catalog, l)
		}
	}
}

func TestSession_RenewRefusesBeforeWritingWhenOneCopyIsUnreadable(t *testing.T) {
	m, _, clock := newTestManager(t)
	mustCreate(t, m, testSessionID, catalogSet(commonv1.Catalog_CATALOG_STREAM, commonv1.Catalog_CATALOG_MEASURE))
	streamDir := mustSessionDir(t, m, commonv1.Catalog_CATALOG_STREAM, testSessionID)
	measureDir := mustSessionDir(t, m, commonv1.Catalog_CATALOG_MEASURE, testSessionID)
	before, err := readLease(streamDir)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(measureDir, LeaseFileName), []byte(`{broken`))
	clock.advance(time.Hour)
	_, err = m.locateAndHeartbeat(testSessionID)
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "no readable lease") {
		t.Fatalf("want FailedPrecondition about the lease, got %v", err)
	}
	after, err := readLease(streamDir)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("a refused renewal must not advance any copy: before %+v, after %+v", before, after)
	}
}

func TestSession_SweepKeepsLiveSessions(t *testing.T) {
	m, _, clock := newTestManager(t)
	mustCreate(t, m, testSessionID, allCatalogs())
	clock.advance(30 * time.Minute)
	m.sweep() // the restart path calls this first
	if got := sessionIDs(t, m); !slices.Equal(got, []string{testSessionID}) {
		t.Fatalf("a live session must survive the sweep: %v", got)
	}
}

func TestSession_SweepDeletesOrphansAfterGrace(t *testing.T) {
	m, _, clock := newTestManager(t)
	orphan := mustSessionDir(t, m, commonv1.Catalog_CATALOG_STREAM, "dead")
	if err := os.MkdirAll(orphan, 0o755); err != nil {
		t.Fatal(err)
	}
	age(t, orphan, clock.now().Add(-orphanGrace+time.Minute))
	m.sweep()
	if _, err := os.Stat(orphan); err != nil {
		t.Fatal("fresh orphan must survive (creation may be in flight)")
	}
	age(t, orphan, clock.now().Add(-orphanGrace-time.Minute))
	m.sweep()
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatal("stale orphan must be removed")
	}
}

func TestSession_SweepReclaimsCorruptLeaseAfterGrace(t *testing.T) {
	m, _, clock := newTestManager(t)
	mustCreate(t, m, testSessionID, catalogSet(commonv1.Catalog_CATALOG_STREAM))
	dir := mustSessionDir(t, m, commonv1.Catalog_CATALOG_STREAM, testSessionID)
	writeFile(t, filepath.Join(dir, LeaseFileName), []byte(`{broken`))
	age(t, dir, clock.now())
	m.sweep()
	if _, err := os.Stat(dir); err != nil {
		t.Fatal("a corrupt lease younger than the grace must be left alone")
	}
	age(t, dir, clock.now().Add(-orphanGrace-time.Minute))
	m.sweep()
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("a corrupt lease older than the grace must be reclaimed")
	}
}

func TestSession_SweepDecidesPerSession(t *testing.T) {
	m, _, clock := newTestManager(t)
	mustCreate(t, m, testSessionID, catalogSet(commonv1.Catalog_CATALOG_STREAM, commonv1.Catalog_CATALOG_MEASURE))
	streamDir := mustSessionDir(t, m, commonv1.Catalog_CATALOG_STREAM, testSessionID)
	measureDir := mustSessionDir(t, m, commonv1.Catalog_CATALOG_MEASURE, testSessionID)
	// One copy expired, the other is alive: every copy goes.
	l, err := readLease(streamDir)
	if err != nil {
		t.Fatal(err)
	}
	l.ExpiresAt = clock.now().Add(-time.Second).UnixNano()
	if err = writeLease(streamDir, l); err != nil {
		t.Fatal(err)
	}
	m.sweep()
	for _, dir := range []string{streamDir, measureDir} {
		if _, err = os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("%s must be removed with the expired session", dir)
		}
	}
}

func TestSession_SweepReclaimsPartiallyLeasedSessionAfterGrace(t *testing.T) {
	m, _, clock := newTestManager(t)
	mustCreate(t, m, testSessionID, catalogSet(commonv1.Catalog_CATALOG_STREAM, commonv1.Catalog_CATALOG_MEASURE))
	streamDir := mustSessionDir(t, m, commonv1.Catalog_CATALOG_STREAM, testSessionID)
	measureDir := mustSessionDir(t, m, commonv1.Catalog_CATALOG_MEASURE, testSessionID)
	// A removal that stopped after dropping one lease: renew refuses the session from now on.
	if err := os.Remove(filepath.Join(measureDir, LeaseFileName)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.locateAndHeartbeat(testSessionID); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("renew of a partially leased session: got %v, want FailedPrecondition", err)
	}
	// The remaining lease is still live; the session is kept while its newest directory is fresh.
	age(t, streamDir, clock.now())
	age(t, measureDir, clock.now().Add(-orphanGrace-time.Hour))
	m.sweep()
	for _, dir := range []string{streamDir, measureDir} {
		if _, err := os.Stat(dir); err != nil {
			t.Fatalf("%s must survive while the session is younger than the grace: %v", dir, err)
		}
	}
	// Once every directory is older than the grace, the session goes despite the live lease.
	age(t, streamDir, clock.now().Add(-orphanGrace-time.Minute))
	m.sweep()
	for _, dir := range []string{streamDir, measureDir} {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("%s must be reclaimed once the partially leased session is older than the grace", dir)
		}
	}
}

func TestSession_SweepIgnoresMalformedNames(t *testing.T) {
	m, backends, clock := newTestManager(t)
	exportDir := backends[commonv1.Catalog_CATALOG_STREAM].GetExportSnapshotDir()
	stray := filepath.Join(exportDir, "UPPER")
	writeFile(t, filepath.Join(stray, "leftover"), []byte("x"))
	age(t, stray, clock.now().Add(-orphanGrace-time.Hour))
	m.sweep()
	if _, err := os.Stat(stray); err != nil {
		t.Fatal("a directory that is not a well-formed session is none of the sweeper's business")
	}
}

func TestSession_RemovalDropsLeaseBeforeTree(t *testing.T) {
	skipIfRoot(t)
	m, _, _ := newTestManager(t)
	mustCreate(t, m, testSessionID, catalogSet(commonv1.Catalog_CATALOG_STREAM))
	dir := mustSessionDir(t, m, commonv1.Catalog_CATALOG_STREAM, testSessionID)
	// An unwritable subdirectory makes RemoveAll fail after the lease is gone.
	locked := filepath.Join(dir, "locked")
	writeFile(t, filepath.Join(locked, "x"), []byte("x"))
	if err := os.Chmod(locked, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
	if _, err := m.release(testSessionID); status.Code(err) != codes.Internal {
		t.Fatalf("want Internal from the interrupted removal, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, LeaseFileName)); !os.IsNotExist(err) {
		t.Fatal("the lease must be removed before the tree")
	}
	// A half-deleted directory is a session without lease: refused by renew, reported by
	// probe with zero times, and reclaimed by the sweeper once older than the grace.
	if _, err := m.locateAndHeartbeat(testSessionID); status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "no readable lease") {
		t.Fatalf("want FailedPrecondition about the lease, got %v", err)
	}
	lease, err := m.probe()
	if err != nil || lease == nil || lease.LastHeartbeatAt != 0 {
		t.Fatalf("probe() = %+v, %v", lease, err)
	}
	if err = os.Chmod(locked, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err = m.release(testSessionID); err != nil {
		t.Fatalf("release must finish the job once the tree is writable: %v", err)
	}
}

func TestSession_RemovalLeaseFailureDeletesNoTree(t *testing.T) {
	skipIfRoot(t)
	m, _, _ := newTestManager(t)
	mustCreate(t, m, testSessionID, allCatalogs())
	measureDir := mustSessionDir(t, m, commonv1.Catalog_CATALOG_MEASURE, testSessionID)
	// A read-only session directory refuses the removal of its lease.
	if err := os.Chmod(measureDir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(measureDir, 0o755) })
	if _, err := m.release(testSessionID); status.Code(err) != codes.Internal {
		t.Fatalf("want Internal from the refused lease removal, got %v", err)
	}
	for catalog := range allCatalogs() {
		if _, err := os.Stat(mustSessionDir(t, m, catalog, testSessionID)); err != nil {
			t.Fatalf("no tree may be deleted while a lease removal fails, %s: %v", catalog, err)
		}
	}
	if _, err := readLease(measureDir); err != nil {
		t.Fatalf("the refused lease must still be there: %v", err)
	}
	if err := os.Chmod(measureDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := m.release(testSessionID); err != nil {
		t.Fatalf("release must finish the job once the lease is removable: %v", err)
	}
	if got := sessionIDs(t, m); len(got) != 0 {
		t.Fatalf("every catalog must be gone: %v", got)
	}
}

func TestSession_RemovalTreeFailureLeavesNoLease(t *testing.T) {
	skipIfRoot(t)
	m, _, _ := newTestManager(t)
	mustCreate(t, m, testSessionID, allCatalogs())
	streamDir := mustSessionDir(t, m, commonv1.Catalog_CATALOG_STREAM, testSessionID)
	locked := filepath.Join(streamDir, "locked")
	writeFile(t, filepath.Join(locked, "x"), []byte("x"))
	if err := os.Chmod(locked, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(locked, 0o755) })
	if _, err := m.release(testSessionID); status.Code(err) != codes.Internal {
		t.Fatalf("want Internal from the interrupted tree removal, got %v", err)
	}
	for catalog := range allCatalogs() {
		dir := mustSessionDir(t, m, catalog, testSessionID)
		if _, err := os.Stat(filepath.Join(dir, LeaseFileName)); !os.IsNotExist(err) {
			t.Fatalf("every lease must be gone before any tree is removed, %s: %v", catalog, err)
		}
		if catalog == commonv1.Catalog_CATALOG_STREAM {
			continue
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("the removable trees must be gone, %s: %v", catalog, err)
		}
	}
}

func TestSession_CreateFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	m, backends, _ := newTestManager(t)
	backends[commonv1.Catalog_CATALOG_TRACE].(*fakeBackend).fail = true
	_, err := m.create(ctx, testSessionID, allCatalogs(), false)
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "snapshot CATALOG_TRACE for export session "+testSessionID) ||
		!strings.Contains(err.Error(), "injected snapshot failure") {
		t.Fatalf("want Internal naming the failed catalog, got %v", err)
	}
	if got := sessionIDs(t, m); len(got) != 0 {
		t.Fatalf("partial session must be removed on failure: %v", got)
	}
}

func TestSession_CreateFailureAfterPreemptNamesPreempted(t *testing.T) {
	ctx := context.Background()
	m, backends, _ := newTestManager(t)
	mustCreate(t, m, testSessionID, catalogSet(commonv1.Catalog_CATALOG_STREAM))
	backends[commonv1.Catalog_CATALOG_TRACE].(*fakeBackend).fail = true
	_, err := m.create(ctx, "bbbb", allCatalogs(), true)
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "preempted sessions ["+testSessionID+"] were already removed") {
		t.Fatalf("want Internal naming the preempted session, got %v", err)
	}
	if got := sessionIDs(t, m); len(got) != 0 {
		t.Fatalf("the occupant is preempted and the failed create rolled back: %v", got)
	}
}

func TestSession_LeaseCommitFailureRollsBack(t *testing.T) {
	m, backends, _ := newTestManager(t)
	// A directory squatting on the lease path makes the atomic rename of the lease fail.
	backends[commonv1.Catalog_CATALOG_MEASURE].(*fakeBackend).onSnapshot = func(root string, _ int32) {
		if err := os.MkdirAll(filepath.Join(root, LeaseFileName), 0o755); err != nil {
			t.Error(err)
		}
	}
	_, err := m.create(context.Background(), testSessionID, allCatalogs(), false)
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "commit lease of export session "+testSessionID) {
		t.Fatalf("want Internal about the lease commit, got %v", err)
	}
	if got := sessionIDs(t, m); len(got) != 0 {
		t.Fatalf("a failed lease commit must remove every catalog of the session: %v", got)
	}
}

func TestSession_RejectsUnsafeIDs(t *testing.T) {
	m, _, _ := newTestManager(t)
	for _, bad := range []string{"/../../data", "..", "ABC"} {
		if _, err := m.create(context.Background(), bad, allCatalogs(), false); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("create(%q) = %v, want InvalidArgument", bad, err)
		}
		if _, err := m.release(bad); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("release(%q) = %v, want InvalidArgument", bad, err)
		}
		if _, err := m.locateAndHeartbeat(bad); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("locate(%q) = %v, want InvalidArgument", bad, err)
		}
	}
	// The live data directory must be untouched by the rejected release above.
	if _, err := os.Stat(filepath.Join(m.backends[commonv1.Catalog_CATALOG_STREAM].GetDataPath(), "sw_record")); err != nil {
		t.Fatalf("live data was removed: %v", err)
	}
	if _, err := m.sessionDir(commonv1.Catalog_CATALOG_UNSPECIFIED, testSessionID); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a catalog without backend must be InvalidArgument, got %v", err)
	}
}

func TestSession_HeartbeatDoesNotSnapshot(t *testing.T) {
	m, backends, _ := newTestManager(t)
	mustCreate(t, m, testSessionID, allCatalogs())
	before := backends[commonv1.Catalog_CATALOG_STREAM].(*fakeBackend).snapshots.Load()
	if _, err := m.locateAndHeartbeat(testSessionID); err != nil {
		t.Fatal(err)
	}
	if after := backends[commonv1.Catalog_CATALOG_STREAM].(*fakeBackend).snapshots.Load(); after != before {
		t.Fatal("renewing must not take another snapshot")
	}
}

func TestSession_RootsAreTheSessionDirs(t *testing.T) {
	m, _, _ := newTestManager(t)
	created := mustCreate(t, m, "aabb", allCatalogs())
	roots, err := m.locateAndHeartbeat("aabb")
	if err != nil {
		t.Fatal(err)
	}
	for catalog := range allCatalogs() {
		dir := mustSessionDir(t, m, catalog, "aabb")
		if created.roots[catalog] != dir || roots[catalog] != dir {
			t.Fatalf("%s root must be the session dir %s, got create %s, renew %s", catalog, dir, created.roots[catalog], roots[catalog])
		}
	}
}

func TestSession_ProbeAllCatalogsMatchesOnDiskLease(t *testing.T) {
	m, _, _ := newTestManager(t)
	mustCreate(t, m, "aabb", allCatalogs())
	got, err := m.probe()
	if err != nil || got == nil {
		t.Fatalf("probe() = %+v, %v", got, err)
	}
	if !slices.Equal(got.Catalogs, slices.Sorted(maps.Keys(allCatalogs()))) {
		t.Fatalf("probe must list every catalog that holds the session, sorted: %v", got.Catalogs)
	}
	streamDir := mustSessionDir(t, m, commonv1.Catalog_CATALOG_STREAM, "aabb")
	diskLease, err := readLease(streamDir)
	if err != nil {
		t.Fatal(err)
	}
	if got.StartedAt != diskLease.StartedAt || got.ExpiresAt != diskLease.ExpiresAt || got.LastHeartbeatAt != diskLease.LastHeartbeatAt {
		t.Fatalf("probe times %+v must match the on-disk lease %+v", got, diskLease)
	}
	// Catalogs come from the directories, not from the lease: dropping one directory drops it.
	if rmErr := os.RemoveAll(filepath.Dir(streamDir)); rmErr != nil {
		t.Fatal(rmErr)
	}
	got, err = m.probe()
	if err != nil || got == nil || slices.Contains(got.Catalogs, commonv1.Catalog_CATALOG_STREAM) || len(got.Catalogs) != 3 {
		t.Fatalf("probe after removing the stream directory = %+v, %v", got, err)
	}
}

// One session id covers the cluster, so a node holding two ids is a half-won create or an
// interrupted release: probe reports the one started last and leaves the other to the
// sweeper, while an empty node reports nothing at all.
func TestSession_ProbeReportsNewestOfSeveralSessions(t *testing.T) {
	m, _, clock := newTestManager(t)
	if lease, err := m.probe(); err != nil || lease != nil {
		t.Fatalf("an empty node must report no session, got %+v, %v", lease, err)
	}
	mustCreate(t, m, "bbbb", allCatalogs())
	// create never leaves two sessions behind, so the second one is planted on disk the way
	// an interrupted release or a crashed preempt would leave it.
	clock.advance(time.Minute)
	planted := mustSessionDir(t, m, commonv1.Catalog_CATALOG_STREAM, "aaaa")
	if err := os.MkdirAll(planted, 0o755); err != nil {
		t.Fatal(err)
	}
	now := clock.now().UnixNano()
	if err := writeLease(planted, Lease{SessionID: "aaaa", StartedAt: now, ExpiresAt: clock.now().Add(defaultLease).UnixNano(), LastHeartbeatAt: now}); err != nil {
		t.Fatal(err)
	}
	if got := sessionIDs(t, m); !slices.Equal(got, []string{"aaaa", "bbbb"}) {
		t.Fatalf("both session directories must exist, got %v", got)
	}
	lease, err := m.probe()
	if err != nil || lease.GetSessionId() != "aaaa" || !slices.Equal(lease.GetCatalogs(), []commonv1.Catalog{commonv1.Catalog_CATALOG_STREAM}) {
		t.Fatalf("probe must report the session started last, got %+v, %v", lease, err)
	}
}

func TestSession_ProbeReportsNewestWhenItSortsLast(t *testing.T) {
	m, _, clock := newTestManager(t)
	mustCreate(t, m, "aaaa", allCatalogs())
	clock.advance(time.Minute)
	planted := mustSessionDir(t, m, commonv1.Catalog_CATALOG_MEASURE, "bbbb")
	if err := os.MkdirAll(planted, 0o755); err != nil {
		t.Fatal(err)
	}
	now := clock.now().UnixNano()
	if err := writeLease(planted, Lease{SessionID: "bbbb", StartedAt: now, ExpiresAt: clock.now().Add(defaultLease).UnixNano(), LastHeartbeatAt: now}); err != nil {
		t.Fatal(err)
	}
	lease, err := m.probe()
	if err != nil || lease.GetSessionId() != "bbbb" || lease.GetStartedAt() != now {
		t.Fatalf("probe must report the session started last even when its id sorts last, got %+v, %v", lease, err)
	}
}

func TestSession_ProbeReportsLeaselessDirectoryWithZeroTimes(t *testing.T) {
	m, _, _ := newTestManager(t)
	orphan := mustSessionDir(t, m, commonv1.Catalog_CATALOG_MEASURE, "dead")
	if err := os.MkdirAll(orphan, 0o755); err != nil {
		t.Fatal(err)
	}
	got, err := m.probe()
	if err != nil || got == nil {
		t.Fatalf("probe() = %+v, %v", got, err)
	}
	if got.SessionId != "dead" || got.StartedAt != 0 || got.ExpiresAt != 0 || got.LastHeartbeatAt != 0 ||
		!slices.Equal(got.Catalogs, []commonv1.Catalog{commonv1.Catalog_CATALOG_MEASURE}) {
		t.Fatalf("a directory without lease must be reported with zero times and its catalog: %+v", got)
	}
}

func TestSession_ExistingIgnoresMalformedExportDirs(t *testing.T) {
	m, backends, _ := newTestManager(t)
	exportDir := backends[commonv1.Catalog_CATALOG_STREAM].GetExportSnapshotDir()
	for _, name := range []string{"UPPER", "has-dash", "aaaa.removing", "export-aaaa", "20260928000000-00000001"} {
		if err := os.MkdirAll(filepath.Join(exportDir, name), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	writeFile(t, filepath.Join(exportDir, "bbbb"), []byte("a file, not a session"))
	sessions, err := m.existing()
	if err != nil || len(sessions) != 0 {
		t.Fatalf("existing() must skip malformed dirs, got %d sessions, err=%v", len(sessions), err)
	}
	if lease, probeErr := m.probe(); probeErr != nil || lease != nil {
		t.Fatalf("probe() must return no session when only malformed dirs exist, got %+v, %v", lease, probeErr)
	}
	mustCreate(t, m, testSessionID, catalogSet(commonv1.Catalog_CATALOG_STREAM))
}

func TestSession_ConcurrentCreateAndRelease(t *testing.T) {
	ctx := context.Background()
	m, _, _ := newTestManager(t)
	ids := []string{"aa01", "aa02", "aa03", "aa04", "aa05", "aa06", "aa07", "aa08"}
	var wg sync.WaitGroup
	errs := make([]error, len(ids))
	for i, id := range ids {
		wg.Add(1)
		//panicdiag:allow-rawgo test goroutine racing create/release; failures are collected in errs
		go func(idx int, sessionID string) {
			defer wg.Done()
			if _, err := m.create(ctx, sessionID, allCatalogs(), true); err != nil {
				errs[idx] = err
				return
			}
			if idx%2 == 0 {
				_, errs[idx] = m.release(sessionID)
			}
		}(i, id)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("%s: serialized create/release must not fail: %v", ids[i], err)
		}
	}
	if got := sessionIDs(t, m); len(got) > 1 {
		t.Fatalf("at most one session may exist after concurrent create/release, got %v", got)
	}
}
