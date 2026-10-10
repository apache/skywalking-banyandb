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
	"fmt"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// Backend is what the export unit needs from one catalog's storage service: where the
// live groups are, where export snapshots go, and how to take a snapshot under a chosen
// name. stream/measure/trace/property implement it; the wiring in pkg/cmdsetup asserts it.
type Backend interface {
	GetDataPath() string
	// GetSnapshotDir is the backup snapshot directory of this catalog; no export snapshot
	// directory may be it or lie inside it.
	GetSnapshotDir() string
	// GetExportSnapshotDir is the directory that holds this catalog's export session
	// snapshots (--<catalog>-export-snapshot-path). It is separate from the backup snapshot
	// directory, so the snapshot reclaimers never see a session.
	GetExportSnapshotDir() string
	// TakeExportSnapshot snapshots every group of the catalog as <GetExportSnapshotDir()>/<name>/<group>.
	// It never runs the generic reclaimers and never renames. A catalog or group without
	// data returns nil; it may still create empty directories (stream and trace do), so
	// an inventory of the snapshot must tolerate groups without any segment.
	TakeExportSnapshot(ctx context.Context, name string) error
}

// PartReader is implemented by the backends whose groups hold parts (stream, measure,
// trace): each reads a part's metadata.json with its own catalog's reader. A missing file
// is an error matching fs.ErrNotExist.
type PartReader interface {
	ReadPartMetadata(partDir string) (queue.StreamingPartData, error)
}

// Backends maps every catalog to its Backend. All four catalogs must be present.
type Backends map[commonv1.Catalog]Backend

// validatePartReaders rejects a stream, measure or trace backend that cannot read its parts.
func (bs Backends) validatePartReaders() error {
	for catalog, b := range bs {
		if catalog == commonv1.Catalog_CATALOG_PROPERTY {
			continue
		}
		if _, ok := b.(PartReader); !ok {
			return fmt.Errorf("%s: the storage service cannot read part metadata", catalog)
		}
	}
	return nil
}

// validateDirs rejects export snapshot directories that would collide: two catalogs
// sharing one (or one nested in another) would see each other's sessions, one inside
// any catalog's backup snapshot directory would be reclaimed as a stale backup, and one
// equal to, inside or containing any catalog's data directory would mix sessions with
// live groups (the orphan sweeper could remove a live group).
func (bs Backends) validateDirs() error {
	for catalog, b := range bs {
		exportDir := b.GetExportSnapshotDir()
		for other, ob := range bs {
			if nested(exportDir, ob.GetDataPath()) {
				return fmt.Errorf("%s: export snapshot path %q and the data path %q of %s must not be equal or nested",
					catalog, exportDir, ob.GetDataPath(), other)
			}
			if storage.PathWithin(exportDir, ob.GetSnapshotDir()) {
				return fmt.Errorf("%s: export snapshot path %q must not be the snapshot directory %q or inside it",
					catalog, exportDir, ob.GetSnapshotDir())
			}
			if other != catalog && nested(exportDir, ob.GetExportSnapshotDir()) {
				return fmt.Errorf("export snapshot paths of %s (%q) and %s (%q) must not be equal or nested",
					catalog, exportDir, other, ob.GetExportSnapshotDir())
			}
		}
	}
	return nil
}

// nested reports whether a and b are the same directory or one lies inside the other.
func nested(a, b string) bool {
	return storage.PathWithin(a, b) || storage.PathWithin(b, a)
}

// MustBackend asserts that a storage service implements Backend, panicking at wiring time
// otherwise. Only the data-bearing service flavors (data node, standalone) do.
func MustBackend(svc any, name string) Backend {
	b, ok := svc.(Backend)
	if !ok {
		logger.Panicf("%s service %T does not implement export.Backend", name, svc)
	}
	return b
}
