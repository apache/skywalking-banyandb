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
// catalog (the `_schema` bluge index written by the property schema server).
// It is used by tools that must load schemas without a running schema server,
// e.g. the data-migration CLI reading a backup snapshot or a live PVC mount.
package reader

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/blugelabs/bluge"
	"google.golang.org/protobuf/encoding/protojson"

	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/property"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
)

const schemaSearchSize = 200000

// Bluge field / directory names of property documents, mirroring the
// (unexported) layout written by banyand/property/db so the offline reader
// can re-open the index via raw bluge.
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

// kindsQuery narrows a shard scan to the requested schema kinds by reusing
// the exact per-kind query the schema server issues against the property db
// (property.BuildSchemaQuery → inverted.BuildPropertyQuery), so a term match
// skips loading the stored _source of every other doc (nodes, other kinds)
// instead of match-all'ing the whole shard. No kinds means every doc.
func kindsQuery(kinds []schema.Kind) (bluge.Query, error) {
	if len(kinds) == 0 {
		return bluge.NewMatchAllQuery(), nil
	}
	q := bluge.NewBooleanQuery().SetMinShould(1)
	for _, k := range kinds {
		iq, err := inverted.BuildPropertyQuery(property.BuildSchemaQuery(k, "", "", 0),
			propGroupField, propEntityIDField)
		if err != nil {
			return nil, fmt.Errorf("build %s schema query: %w", k.String(), err)
		}
		bq, ok := inverted.BlugeQuery(iq)
		if !ok {
			return nil, fmt.Errorf("unexpected %s schema query type %T", k.String(), iq)
		}
		q.AddShould(bq)
	}
	return q, nil
}

// WalkShard opens one shard of the schema-property bluge index and invokes
// visit() for each doc. Passing kinds narrows the scan to those schema kinds
// via the indexed kind field; no kinds means every doc.
func WalkShard(shardPath string, visit func(Doc) error, kinds ...schema.Kind) error {
	blugeReader, err := bluge.OpenReader(bluge.DefaultConfig(shardPath))
	if err != nil {
		return fmt.Errorf("open bluge reader: %w", err)
	}
	defer func() { _ = blugeReader.Close() }()
	query, err := kindsQuery(kinds)
	if err != nil {
		return err
	}
	dmi, err := blugeReader.Search(context.Background(),
		bluge.NewTopNSearch(schemaSearchSize, query))
	if err != nil {
		return fmt.Errorf("search schema docs: %w", err)
	}
	matched := 0
	for {
		next, nextErr := dmi.Next()
		if nextErr != nil {
			return fmt.Errorf("iterate schema docs: %w", nextErr)
		}
		if next == nil {
			if matched >= schemaSearchSize {
				return fmt.Errorf("shard %s: schema docs hit the search limit %d; results may be truncated", shardPath, schemaSearchSize)
			}
			return nil
		}
		matched++
		var sourceBytes []byte
		var deleted bool
		if visitErr := next.VisitStoredFields(func(field string, value []byte) bool {
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
			continue
		}
		var prop propertyv1.Property
		if err := protojson.Unmarshal(sourceBytes, &prop); err != nil {
			// Surface the failure with the shard path + the source size so an
			// operator hitting a corrupt catalog can see WHERE the parse broke
			// instead of just "missing schema" later in the run.
			return fmt.Errorf("unmarshal property doc in %s (%d source bytes): %w",
				shardPath, len(sourceBytes), err)
		}
		parsed := property.ParseTags(prop.GetTags())
		if vErr := visit(Doc{
			PropID:     prop.GetId(),
			KindName:   prop.GetMetadata().GetName(),
			Group:      parsed.Group,
			SourceJSON: parsed.Source,
			ModRev:     prop.GetMetadata().GetModRevision(),
			Deleted:    deleted,
		}); vErr != nil {
			return vErr
		}
	}
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
