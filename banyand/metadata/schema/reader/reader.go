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

// Package reader provides offline read access to the property-based schema
// catalog index written by the property schema server. It is used by tools that
// must load schemas without a running schema server, e.g. the data-migration
// CLI reading a backup snapshot or a live PVC mount.
package reader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/property"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

// Property-document field and directory names mirror the unexported layout
// written by banyand/property/db.
const (
	propShardDirPrefix = "shard-"
	propSourceField    = "_source"
	propGroupField     = "_group"
	propEntityIDField  = "_entity_id"
	propDeleteField    = "_deleted"
)

// Doc is one decoded doc emitted by WalkShard; the caller decides whether
// the kind / group matches what it wants.
type Doc struct {
	PropID     string
	KindName   string // schema.Kind String() value: "group" / "stream" / "measure" / ...
	Group      string // the "group" tag; empty for docs that carry none
	SourceJSON string // embedded protobuf JSON ready for kind-specific Unmarshal
	ModRev     int64
	Deleted    bool
}

// WalkShard opens one shard of the schema-property index and invokes visit
// for every document the requested kinds select. No kinds walks every live
// document.
func WalkShard(shardPath string, visit func(Doc) error, kinds ...schema.Kind) error {
	documentVisit := func(document inverted.StoredDocument) error {
		return decodeSchemaDocument(shardPath, document, visit)
	}
	if len(kinds) == 0 {
		if walkErr := inverted.ReadOnlyWalkDocuments(context.Background(), shardPath, documentVisit); walkErr != nil {
			return fmt.Errorf("walk schema docs in %s: %w", shardPath, walkErr)
		}
		return nil
	}
	terms := make([][]byte, len(kinds))
	for kindIndex, kind := range kinds {
		terms[kindIndex] = []byte(kind.String())
	}
	selection := inverted.TermSelection{Field: index.IndexModeName, Terms: terms}
	if walkErr := inverted.ReadOnlySelectDocuments(context.Background(), shardPath, selection, documentVisit); walkErr != nil {
		return fmt.Errorf("walk schema docs in %s: %w", shardPath, walkErr)
	}
	return nil
}

func decodeSchemaDocument(shardPath string, document inverted.StoredDocument, visit func(Doc) error) error {
	var sourceBytes []byte
	var deleted bool
	if visitErr := document.VisitStoredFields(func(field string, value []byte) bool {
		switch field {
		case propSourceField:
			sourceBytes = append([]byte(nil), value...)
		case propDeleteField:
			if len(value) > 0 {
				deleted = true
			}
		}
		return true
	}); visitErr != nil {
		return fmt.Errorf("visit schema doc: %w", visitErr)
	}
	if len(sourceBytes) == 0 {
		return nil
	}
	var prop propertyv1.Property
	if unmarshalErr := protojson.Unmarshal(sourceBytes, &prop); unmarshalErr != nil {
		return fmt.Errorf("unmarshal property doc in %s (%d source bytes): %w", shardPath, len(sourceBytes), unmarshalErr)
	}
	parsed := property.ParseTags(prop.GetTags())
	return visit(Doc{
		PropID:     prop.GetId(),
		KindName:   prop.GetMetadata().GetName(),
		Group:      parsed.Group,
		SourceJSON: parsed.Source,
		ModRev:     prop.GetMetadata().GetModRevision(),
		Deleted:    deleted,
	})
}

// WalkShards reads the shard-* subdirectories of a `_schema` bluge root and
// invokes fn(shardPath) for each. The caller owns all scanning and
// candidate-merging logic; this helper owns only the readDir-filter
// boilerplate that would otherwise appear in every loader.
func WalkShards(schemaRoot string, fn func(shardPath string) error) error {
	shards, err := os.ReadDir(schemaRoot)
	if err != nil {
		return fmt.Errorf("read schema-property root %q: %w", schemaRoot, err)
	}
	for _, sh := range shards {
		if !sh.IsDir() || !strings.HasPrefix(sh.Name(), propShardDirPrefix) {
			continue
		}
		if fnErr := fn(filepath.Join(schemaRoot, sh.Name())); fnErr != nil {
			return fnErr
		}
	}
	return nil
}

// WalkDocs walks every shard under a `_schema` bluge root, optionally
// narrowed to the given schema kinds, and invokes visit once per property ID
// with its latest revision (highest mod_revision). IDs whose latest revision
// is a tombstone are skipped. Revisions are resolved across ALL shards before
// visiting — a property's revisions normally share one shard (entity-hash
// sharding), but resharding can spread them — so callers never see stale
// duplicates and need no per-ID dedup of their own.
func WalkDocs(schemaRoot string, visit func(Doc) error, kinds ...schema.Kind) error {
	winners := map[string]Doc{}
	if err := WalkShards(schemaRoot, func(shardPath string) error {
		return WalkShard(shardPath, func(d Doc) error {
			if cur, ok := winners[d.PropID]; ok && cur.ModRev >= d.ModRev {
				return nil
			}
			winners[d.PropID] = d
			return nil
		}, kinds...)
	}); err != nil {
		return err
	}
	for _, d := range winners {
		if d.Deleted {
			continue
		}
		if err := visit(d); err != nil {
			return err
		}
	}
	return nil
}
