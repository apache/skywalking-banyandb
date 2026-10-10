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
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pkgfs "github.com/apache/skywalking-banyandb/pkg/fs"
)

const (
	// LeaseFileName is the lease document inside every <exportSnapshotDir>/<id> session directory.
	LeaseFileName = ".lease"
	// defaultLease is how long a session survives without a heartbeat before the sweeper
	// reclaims it: every renewal resets expiresAt to now + defaultLease, so the task length
	// is irrelevant and nothing on the wire carries it.
	defaultLease = 5 * 24 * time.Hour
	// orphanGrace is how long a session directory without a readable .lease may exist
	// before the sweeper treats it as the leftover of a crashed creation and removes it.
	orphanGrace = 10 * time.Minute
)

// SweepInterval is how often a data node scans for expired sessions. It is a variable only
// so integration suites can raise it; production never changes it.
var SweepInterval = time.Minute

var (
	errNoLease       = errors.New("export session has no lease file")
	sessionIDPattern = regexp.MustCompile(`^[0-9a-f]{1,64}$`)
)

// Lease is the on-disk <session dir>/.lease document (design §3.5). Times are UnixNano;
// lastHeartbeatAt records the last heartbeat (create counts as the first) and a
// document without expiresAt reads as expired.
type Lease struct {
	SessionID       string `json:"sessionId"`
	StartedAt       int64  `json:"startedAt"`
	ExpiresAt       int64  `json:"expiresAt"`
	LastHeartbeatAt int64  `json:"lastHeartbeatAt"`
}

func (l Lease) expired(now time.Time) bool { return now.UnixNano() >= l.ExpiresAt }

// ReadLease parses <sessionDir>/.lease; it exists for test helpers outside this package.
func ReadLease(sessionDir string) (Lease, error) { return readLease(sessionDir) }

// readLease parses <sessionDir>/.lease. Unknown keys are ignored, so a document written by
// an earlier build (which also recorded client and catalogs) still loads.
func readLease(sessionDir string) (Lease, error) {
	raw, err := os.ReadFile(filepath.Join(sessionDir, LeaseFileName))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return Lease{}, errNoLease
		}
		return Lease{}, err
	}
	var l Lease
	if err := json.Unmarshal(raw, &l); err != nil {
		return Lease{}, err
	}
	return l, nil
}

// WriteLease atomically replaces <sessionDir>/.lease; it exists for test helpers outside this package.
func WriteLease(sessionDir string, l Lease) error { return writeLease(sessionDir, l) }

// writeLease atomically replaces <sessionDir>/.lease (tmp + fsync + rename).
func writeLease(sessionDir string, l Lease) error {
	raw, err := json.Marshal(l)
	if err != nil {
		return err
	}
	_, err = pkgfs.NewLocalFileSystem().WriteAtomic(raw, filepath.Join(sessionDir, LeaseFileName), 0o600)
	return err
}

// validateSessionID rejects anything that could escape the export snapshot directory. The proto
// carries the same rule; this is the defense for the data-node port, which has no auth.
func validateSessionID(id string) error {
	if !sessionIDPattern.MatchString(id) {
		return status.Errorf(codes.InvalidArgument, "invalid export session id %q: want 1-64 lowercase hex characters", id)
	}
	return nil
}
