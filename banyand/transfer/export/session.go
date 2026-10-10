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
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// sessionManager owns the export snapshot directories of this node, one <id> per session
// under every catalog's export snapshot directory. All state is on disk (directory +
// .lease); the struct only serializes create/release/sweep and validates every id before
// it touches the file system.
type sessionManager struct {
	backends Backends
	now      func() time.Time
	l        *logger.Logger
	mu       sync.Mutex
}

func newSessionManager(backends Backends, l *logger.Logger) *sessionManager {
	return &sessionManager{backends: backends, now: time.Now, l: l}
}

// sessionDir returns <exportSnapshotDir>/<id> for a catalog after checking that the id is
// well formed.
func (m *sessionManager) sessionDir(catalog commonv1.Catalog, id string) (string, error) {
	if err := validateSessionID(id); err != nil {
		return "", err
	}
	b, ok := m.backends[catalog]
	if !ok {
		return "", status.Errorf(codes.InvalidArgument, "catalog %s has no export backend on this node", catalog)
	}
	return sessionPath(b, id), nil
}

// sessionPath joins an id that validateSessionID accepted under the export snapshot
// directory of b; such an id is plain hex, so the result stays directly under it.
func sessionPath(b Backend, id string) string {
	return filepath.Join(b.GetExportSnapshotDir(), id)
}

// existing lists the well-formed <id> directories across the four catalogs' export
// snapshot directories as id -> catalog -> dir. Anything else in there is not a session:
// it is left alone and logged at Debug.
func (m *sessionManager) existing() (map[string]map[commonv1.Catalog]string, error) {
	sessions := map[string]map[commonv1.Catalog]string{}
	for catalog, b := range m.backends {
		root := b.GetExportSnapshotDir()
		entries, err := readDirOrEmpty(root)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			id := e.Name()
			if !sessionIDPattern.MatchString(id) {
				m.l.Debug().Str("dir", filepath.Join(root, id)).Msg("export snapshot directory is not a well-formed export session; ignored")
				continue
			}
			if sessions[id] == nil {
				sessions[id] = map[commonv1.Catalog]string{}
			}
			sessions[id][catalog] = filepath.Join(root, id)
		}
	}
	return sessions, nil
}

// probe is the read-only ACTION_LIST body: the one session this node holds, or nil when it
// holds none. The catalogs come from the directories that hold the id and the times from
// the first readable .lease; a session whose .lease is unreadable everywhere reports zero
// times. One session id covers the whole cluster, so several ids mean a half-won create or
// an interrupted release: the one with the newest started_at is reported and the others are
// logged, since the sweeper or a preempting create reclaims them. It never renews and never
// removes anything. It holds m.mu, so it never observes a create or release halfway.
func (m *sessionManager) probe() (*transferv1.SessionLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sessions, err := m.existing()
	if err != nil {
		return nil, err
	}
	var newest *transferv1.SessionLease
	var others []string
	for _, id := range slices.Sorted(maps.Keys(sessions)) {
		catalogs := slices.Sorted(maps.Keys(sessions[id]))
		lease := &transferv1.SessionLease{SessionId: id, Catalogs: catalogs}
		if l, ok := firstLease(sessions[id], catalogs); ok {
			lease.StartedAt, lease.ExpiresAt, lease.LastHeartbeatAt = l.StartedAt, l.ExpiresAt, l.LastHeartbeatAt
		}
		switch {
		case newest == nil:
			newest = lease
		case lease.StartedAt > newest.StartedAt:
			others = append(others, newest.SessionId)
			newest = lease
		default:
			others = append(others, id)
		}
	}
	if len(others) > 0 {
		m.l.Warn().Str("reported", newest.SessionId).Strs("others", others).
			Msg("node holds more than one export session; reporting the newest, the others await reclamation")
	}
	return newest, nil
}

// firstLease returns the first readable .lease of a session in catalog order; catalogs is
// the sorted key set of dirs. Every catalog of a committed session carries the same
// document, so one copy is enough.
func firstLease(dirs map[commonv1.Catalog]string, catalogs []commonv1.Catalog) (Lease, bool) {
	for _, catalog := range catalogs {
		if l, err := readLease(dirs[catalog]); err == nil {
			return l, true
		}
	}
	return Lease{}, false
}

// stale reports whether the newest directory of a session is older than orphanGrace. A
// session without any readable lease is the leftover of a crash or of a failed rollback
// (the sweeper never sees an in-flight create, which holds m.mu). Directories that
// vanished meanwhile count as stale: there is nothing left to protect.
func stale(dirs map[commonv1.Catalog]string, now time.Time) bool {
	var newest time.Time
	for _, dir := range dirs {
		if info, err := os.Stat(dir); err == nil && info.ModTime().After(newest) {
			newest = info.ModTime()
		}
	}
	return now.Sub(newest) > orphanGrace
}

// reclaimable is the sweeper's rule for a whole session and mirrors locateAndHeartbeat: when
// any copy lacks a readable lease the session can no longer be renewed, so it goes once
// its newest directory is older than orphanGrace; otherwise it goes when any lease expired.
func reclaimable(dirs map[commonv1.Catalog]string, now time.Time) bool {
	expired := false
	for _, dir := range dirs {
		l, err := readLease(dir)
		if err != nil {
			return stale(dirs, now)
		}
		expired = expired || l.expired(now)
	}
	return expired
}

// createdSession is what create hands back: the session directory (the groups root) of
// every snapshotted catalog and the sessions it removed because preempt was set.
type createdSession struct {
	roots     map[commonv1.Catalog]string
	preempted []string
}

// create snapshots the selected catalogs under <id> and commits their leases. Any other
// session directory on this node, whatever its lease says, answers ALREADY_EXISTS naming
// the occupants unless preempt is set, in which case every one of them is removed and
// reported back; create takes no liveness decision, the sweeper alone reclaims expired
// and abandoned sessions. A failure in any catalog removes whatever this id created, so a
// session is all-or-nothing per node.
func (m *sessionManager) create(
	ctx context.Context, id string, catalogs map[commonv1.Catalog]struct{}, preempt bool,
) (*createdSession, error) {
	if err := validateSessionID(id); err != nil {
		return nil, err
	}
	// A sibling node may already have failed and the liaison's rollback may be on its way:
	// a create canceled before it starts must not destroy the occupants it would otherwise
	// have replaced. Once preemption began, a failure reports the occupants already removed.
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	// The lock may have been held by a long create or release: check again before touching
	// any occupant.
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	sessions, err := m.existing()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "scan export sessions: %v", err)
	}
	now := m.now()
	occupants := slices.Sorted(maps.Keys(sessions))
	if len(occupants) > 0 && !preempt {
		return nil, status.Errorf(codes.AlreadyExists, "export session %s already exists on this node", strings.Join(occupants, ", "))
	}
	for i, occupant := range occupants {
		if rmErr := removeDirs(sessions[occupant]); rmErr != nil {
			return nil, withPreempted(status.Errorf(codes.Internal, "preempt export session %s: %v", occupant, rmErr), occupants[:i])
		}
		m.l.Warn().Str("preempted", occupant).Str("session", id).Msg("export session preempted")
	}
	dirs := make(map[commonv1.Catalog]string, len(catalogs))
	// Sorted so the snapshot order, and therefore what a rollback has to undo, is deterministic.
	for _, catalog := range slices.Sorted(maps.Keys(catalogs)) {
		dir, snapErr := m.snapshotCatalog(ctx, catalog, id)
		if snapErr != nil {
			m.rollback(id)
			if errors.Is(snapErr, context.Canceled) || errors.Is(snapErr, context.DeadlineExceeded) {
				return nil, withPreempted(status.FromContextError(snapErr).Err(), occupants)
			}
			return nil, withPreempted(status.Errorf(codes.Internal, "snapshot %s for export session %s: %v", catalog, id, snapErr), occupants)
		}
		dirs[catalog] = dir
	}
	// Single commit point: the lease is written only after the last snapshot, so a crash
	// before this leaves lease-less directories that the sweeper reclaims after orphanGrace.
	heartbeat := m.now()
	l := Lease{SessionID: id, StartedAt: now.UnixNano(), ExpiresAt: heartbeat.Add(defaultLease).UnixNano(), LastHeartbeatAt: heartbeat.UnixNano()}
	for _, dir := range dirs {
		if writeErr := writeLease(dir, l); writeErr != nil {
			m.rollback(id)
			return nil, withPreempted(status.Errorf(codes.Internal, "commit lease of export session %s: %v", id, writeErr), occupants)
		}
	}
	return &createdSession{roots: dirs, preempted: occupants}, nil
}

// withPreempted appends the sessions a failed create already removed to its status error,
// keeping the status code.
func withPreempted(err error, preempted []string) error {
	if len(preempted) == 0 {
		return err
	}
	st := status.Convert(err)
	return status.Errorf(st.Code(), "%s; preempted sessions [%s] were already removed", st.Message(), strings.Join(preempted, ", "))
}

// rollback removes whatever a failed create left behind under id, logging instead of
// failing: the caller already has the error that matters.
func (m *sessionManager) rollback(id string) {
	if err := m.removeAll(id); err != nil {
		m.l.Warn().Err(err).Str("session", id).Msg("rollback of a failed export session left directories behind")
	}
}

// snapshotCatalog creates the session directory of one catalog and takes the export
// snapshot into it, returning the directory. The lease is committed by the caller.
func (m *sessionManager) snapshotCatalog(ctx context.Context, catalog commonv1.Catalog, id string) (string, error) {
	b, ok := m.backends[catalog]
	if !ok {
		return "", fmt.Errorf("no backend for catalog %s", catalog)
	}
	dir := sessionPath(b, id)
	if err := os.MkdirAll(dir, storage.DirPerm); err != nil {
		return "", err
	}
	if err := b.TakeExportSnapshot(ctx, id); err != nil {
		return "", err
	}
	return dir, nil
}

// locateAndHeartbeat resolves the per-catalog groups roots of a live session and extends its
// lease on every copy. A session this node does not hold is NotFound; an expired or
// unreadable lease is FailedPrecondition (design §3.5). Every copy is validated before
// any is written, so a refused renewal leaves the copies in step.
func (m *sessionManager) locateAndHeartbeat(id string) (map[commonv1.Catalog]string, error) {
	if err := validateSessionID(id); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	sessions, err := m.existing()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "scan export sessions: %v", err)
	}
	dirs, ok := sessions[id]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "export session %s not found on this node", id)
	}
	now := m.now()
	leases := make(map[commonv1.Catalog]Lease, len(dirs))
	for catalog, dir := range dirs {
		l, readErr := readLease(dir)
		if readErr != nil {
			return nil, status.Errorf(codes.FailedPrecondition, "export session %s has no readable lease: %v", id, readErr)
		}
		if l.expired(now) {
			return nil, status.Errorf(codes.FailedPrecondition, "export session %s expired at %s",
				id, time.Unix(0, l.ExpiresAt).UTC().Format(time.RFC3339))
		}
		leases[catalog] = l
	}
	for catalog, dir := range dirs {
		l := leases[catalog]
		l.LastHeartbeatAt = now.UnixNano()
		l.ExpiresAt = now.Add(defaultLease).UnixNano()
		if writeErr := writeLease(dir, l); writeErr != nil {
			return nil, status.Errorf(codes.Internal, "renew lease of export session %s: %v", id, writeErr)
		}
	}
	return dirs, nil
}

// verify reports NotFound unless session id still has a directory and a readable lease in
// every catalog; a plan calls it after its walk to detect a session removed meanwhile.
func (m *sessionManager) verify(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	sessions, err := m.existing()
	if err != nil {
		return status.Errorf(codes.Internal, "scan export sessions: %v", err)
	}
	dirs, ok := sessions[id]
	if !ok {
		return status.Errorf(codes.NotFound, "export session %s was removed during the plan", id)
	}
	for _, dir := range dirs {
		if _, err = readLease(dir); err != nil {
			return status.Errorf(codes.NotFound, "export session %s was removed during the plan: %v", id, err)
		}
	}
	return nil
}

// release deletes <id> in every catalog's export snapshot directory and reports whether
// this node held it in any catalog. Releasing a session the node does not hold is not an
// error, so a rollback or a retry can target nodes that never had it or already dropped it.
func (m *sessionManager) release(id string) (bool, error) {
	if err := validateSessionID(id); err != nil {
		return false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	sessions, err := m.existing()
	if err != nil {
		return false, status.Errorf(codes.Internal, "release export session %s: %v", id, err)
	}
	if err = m.removeAll(id); err != nil {
		return false, status.Errorf(codes.Internal, "release export session %s: %v", id, err)
	}
	return len(sessions[id]) > 0, nil
}

// removeAll removes id, which the caller already validated, from every catalog.
func (m *sessionManager) removeAll(id string) error {
	dirs := make(map[commonv1.Catalog]string, len(m.backends))
	for catalog, b := range m.backends {
		dirs[catalog] = sessionPath(b, id)
	}
	return removeDirs(dirs)
}

// removeDirs deletes every directory of a session in two phases. First the .lease of
// every directory goes; a failure there stops before any tree is touched, but leases
// already removed stay removed, so a session can be left with leases in only some
// catalogs. Then every tree goes. Either way a session missing any lease is refused by
// renew and reclaimed by the sweeper after orphanGrace. Missing leases and directories
// are fine; every other error is reported.
func removeDirs(dirs map[commonv1.Catalog]string) error {
	for _, dir := range dirs {
		if err := os.Remove(filepath.Join(dir, LeaseFileName)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	var err error
	for _, dir := range dirs {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			err = errors.Join(err, rmErr)
		}
	}
	return err
}

// sweep reclaims sessions that expired or were abandoned mid-creation. It runs at start-up,
// which is how a restarted node recovers its sessions (live ones are kept, the rest
// reclaimed), and every SweepInterval after that. The lock covers only the scan; the
// deletions run outside it so a large tree never stalls create or renew. A create racing
// those deletions may therefore still see the doomed session and answer ALREADY_EXISTS,
// and a preempting create may remove it too and list it among its preempted sessions.
func (m *sessionManager) sweep() {
	for id, dirs := range m.doomed() {
		if err := removeDirs(dirs); err != nil {
			m.l.Warn().Err(err).Str("session", id).Msg("export session sweep: remove failed, retrying next sweep")
			continue
		}
		m.l.Info().Str("session", id).Msg("export session sweep: removed")
	}
}

// doomed is the locked half of sweep: the directories of every reclaimable session.
func (m *sessionManager) doomed() map[string]map[commonv1.Catalog]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	sessions, err := m.existing()
	if err != nil {
		m.l.Warn().Err(err).Msg("export session sweep: cannot list snapshot directories")
		return nil
	}
	now := m.now()
	maps.DeleteFunc(sessions, func(_ string, dirs map[commonv1.Catalog]string) bool { return !reclaimable(dirs, now) })
	return sessions
}
