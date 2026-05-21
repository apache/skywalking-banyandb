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

package measure

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/blugelabs/bluge"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	backupsnapshot "github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
)

// errSchemaPropertyMissing signals "no schema-property/_schema under
// backupDir". Returned by findSchemaPropertyRoot when a caller asks for
// a backup-tree discovery but the snapshot omitted the catalog.
var errSchemaPropertyMissing = errors.New("schema-property catalog not found in backup")

// schemaPropDoc is one decoded doc emitted by walkSchemaPropertyShard;
// the caller decides whether the kind / group matches what it wants.
type schemaPropDoc struct {
	propID     string
	kindName   string // "measure" / "group" / "stream" / ...
	group      string // empty for non-measure docs
	sourceJSON string // embedded protobuf JSON ready for kind-specific Unmarshal
	modRev     int64
	deleted    bool
}

// walkSchemaPropertyShard opens one shard of the backup's
// schema-property bluge index and invokes visit() for each doc.
// Visitors filter on kindName and fold into their own dedup tables.
func walkSchemaPropertyShard(shardPath string, visit func(schemaPropDoc) error) error {
	reader, err := bluge.OpenReader(bluge.DefaultConfig(shardPath))
	if err != nil {
		return fmt.Errorf("open bluge reader: %w", err)
	}
	defer func() { _ = reader.Close() }()
	dmi, err := reader.Search(context.Background(),
		bluge.NewTopNSearch(schemaSearchSize, bluge.NewMatchAllQuery()))
	if err != nil {
		return fmt.Errorf("search schema docs: %w", err)
	}
	for {
		next, err := dmi.Next()
		if err != nil {
			return fmt.Errorf("iterate schema docs: %w", err)
		}
		if next == nil {
			return nil
		}
		var sourceBytes []byte
		var deleted bool
		if err := next.VisitStoredFields(func(field string, value []byte) bool {
			switch field {
			case schemaSourceField:
				sourceBytes = append([]byte(nil), value...)
			case schemaDeletedTag:
				if len(value) > 0 {
					deleted = true
				}
			}
			return true
		}); err != nil {
			return fmt.Errorf("visit schema doc: %w", err)
		}
		if len(sourceBytes) == 0 {
			continue
		}
		var prop propertyv1.Property
		if err := protojson.Unmarshal(sourceBytes, &prop); err != nil {
			// Surface the failure with the shard path + a snippet of the
			// offending JSON so an operator hitting a corrupt catalog can
			// see WHERE the parse broke instead of just "missing schema"
			// later in the run.
			return fmt.Errorf("unmarshal property doc in %s (%d source bytes): %w",
				shardPath, len(sourceBytes), err)
		}
		var group, srcJSON string
		for _, t := range prop.GetTags() {
			sv := t.GetValue().GetStr()
			if sv == nil {
				continue
			}
			switch t.GetKey() {
			case "group":
				group = sv.GetValue()
			case "source":
				srcJSON = sv.GetValue()
			}
		}
		if vErr := visit(schemaPropDoc{
			propID:     prop.GetId(),
			kindName:   prop.GetMetadata().GetName(),
			group:      group,
			sourceJSON: srcJSON,
			modRev:     prop.GetMetadata().GetModRevision(),
			deleted:    deleted,
		}); vErr != nil {
			return vErr
		}
	}
}

const (
	schemaSourceField = "_source"
	schemaDeletedTag  = "_deleted"
	schemaShardPrefix = "shard-"
	// schemaSearchSize sets the per-page document budget for the bluge
	// scan; the schema-property index in production tops out at ~10k docs so
	// a single page is plenty.
	schemaSearchSize = 200000
)

// resolveSchemaRoot picks the `_schema` bluge directory to read schemas
// from. If schemaPropertyPath is non-empty it is honored directly (used
// by callers that mount a live cluster's schema-property PVC and want
// to skip the backup-style `<node>/<date>/` discovery). Otherwise the
// caller falls back to walking the backup snapshot via
// findSchemaPropertyRoot, restricted to date (when non-empty) so the
// schemas come from the same snapshot as the migrated measure data.
func resolveSchemaRoot(backupDir, date, schemaPropertyPath string) (string, error) {
	if schemaPropertyPath != "" {
		return schemaPropertyPath, nil
	}
	return findSchemaPropertyRoot(backupDir, date)
}

// loadMeasureSchemasFromSchemaCatalog reads every measure schema under the
// given groups directly from the schema-property bluge index. Returns
// (group -> measure-name -> schema). When schemaPropertyPath is set, the
// catalog is read straight from that bluge dir (the live PVC mount path);
// otherwise the function walks the backup-style `<node>/<date>/` layout
// rooted at backupDir.
//
// The bluge index retains historical revisions across segments (a schema
// updated N times leaves N docs sharing the same propID with distinct
// mod_revisions). This loader mirrors the live cluster's
// SchemaRegistry.listSchemas dedup: propID is the dedup key, the entry
// with the highest mod_revision wins, and the winning entry is dropped
// entirely if it carries a non-zero _deleted marker (the schema was
// tombstoned at that revision).
func loadMeasureSchemasFromSchemaCatalog(backupDir, date, schemaPropertyPath string, groups []string) (map[string]map[string]*measureSchemaInfo, error) {
	byGroup, err := fetchMeasureSchemasFromSchema(backupDir, date, schemaPropertyPath, groups)
	if err != nil {
		return nil, err
	}
	out := make(map[string]map[string]*measureSchemaInfo, len(groups))
	for _, group := range groups {
		list := byGroup[group]
		byName := make(map[string]*measureSchemaInfo, len(list))
		for _, s := range list {
			byName[s.Name] = s
		}
		out[group] = byName
	}
	return out, nil
}

func fetchMeasureSchemasFromSchema(backupDir, date, schemaPropertyPath string, groups []string) (map[string][]*measureSchemaInfo, error) {
	if backupDir == "" && schemaPropertyPath == "" {
		return nil, errors.New("either backup-dir or schema-property-path is required")
	}
	wanted := make(map[string]bool, len(groups))
	for _, g := range groups {
		wanted[g] = true
	}
	schemaRoot, err := resolveSchemaRoot(backupDir, date, schemaPropertyPath)
	if err != nil {
		return nil, err
	}
	shards, err := os.ReadDir(schemaRoot)
	if err != nil {
		return nil, fmt.Errorf("read schema-property root %q: %w", schemaRoot, err)
	}
	candidates := make(map[string]*schemaCandidate)
	for _, sh := range shards {
		if !sh.IsDir() || !strings.HasPrefix(sh.Name(), schemaShardPrefix) {
			continue
		}
		shardPath := filepath.Join(schemaRoot, sh.Name())
		if loadErr := loadMeasureSchemasFromShard(shardPath, wanted, candidates); loadErr != nil {
			return nil, fmt.Errorf("load schemas from %s: %w", shardPath, loadErr)
		}
	}
	result := make(map[string][]*measureSchemaInfo)
	for _, c := range candidates {
		if c.deleted || c.info == nil {
			continue
		}
		result[c.info.Group] = append(result[c.info.Group], c.info)
	}
	return result, nil
}

// schemaCandidate captures the best (highest mod_revision) doc seen for
// one propID while iterating the schema-property bluge index. deleted is
// true when the winning revision is a tombstone, in which case the entry
// must be skipped entirely (an older live revision must not resurrect a
// deleted schema).
type schemaCandidate struct {
	info    *measureSchemaInfo
	modRev  int64
	deleted bool
}

// findSchemaPropertyRoot walks <backup>/<node>/<date>/ and returns the
// first schema-property/_schema directory it finds. Hot/schema-server
// pods are the only ones whose backup carries this catalog.
//
// When date is non-empty, only `<node>/<date>/...` subtrees are
// considered — this is the typical case where the caller migrates a
// specific snapshot and must not silently pick up schemas from a
// different (older/newer) date that happens to live under the same
// backup root.
func findSchemaPropertyRoot(backupDir, date string) (string, error) {
	nodes, err := os.ReadDir(backupDir)
	if err != nil {
		return "", fmt.Errorf("read backup dir %q: %w", backupDir, err)
	}
	tryDate := func(nodeRoot, d string) (string, bool) {
		p := filepath.Join(nodeRoot, d, backupsnapshot.SchemaPropertyCatalogName, schema.SchemaGroup)
		info, statErr := os.Stat(p)
		return p, statErr == nil && info.IsDir()
	}
	for _, node := range nodes {
		if !node.IsDir() {
			continue
		}
		nodeRoot := filepath.Join(backupDir, node.Name())
		candidates := []string{date}
		if date == "" {
			dates, readErr := os.ReadDir(nodeRoot)
			if readErr != nil {
				continue
			}
			candidates = candidates[:0]
			for _, d := range dates {
				if d.IsDir() {
					candidates = append(candidates, d.Name())
				}
			}
		}
		for _, c := range candidates {
			if p, ok := tryDate(nodeRoot, c); ok {
				return p, nil
			}
		}
	}
	dateClause := ""
	if date != "" {
		dateClause = fmt.Sprintf(" for date %q", date)
	}
	return "", errors.WithMessagef(errSchemaPropertyMissing,
		"no %s/%s directory found under %q%s (only hot/schema-server node backups carry schemas)",
		backupsnapshot.SchemaPropertyCatalogName, schema.SchemaGroup, backupDir, dateClause)
}

// loadGroupResourceOptsFromSchema scans the schema-property bluge index
// for commonv1.Group docs and returns each requested group's
// ResourceOpts (carrying SegmentInterval + Stages). Missing groups are
// simply absent from the result map.
func loadGroupResourceOptsFromSchema(backupDir, date, schemaPropertyPath string, groups []string) (map[string]*commonv1.ResourceOpts, error) {
	if backupDir == "" && schemaPropertyPath == "" {
		return nil, errors.New("either backup-dir or schema-property-path is required")
	}
	wanted := make(map[string]bool, len(groups))
	for _, g := range groups {
		wanted[g] = true
	}
	schemaRoot, err := resolveSchemaRoot(backupDir, date, schemaPropertyPath)
	if err != nil {
		return nil, err
	}
	shards, err := os.ReadDir(schemaRoot)
	if err != nil {
		return nil, fmt.Errorf("read schema-property root %q: %w", schemaRoot, err)
	}
	candidates := map[string]*groupOptsCandidate{}
	for _, sh := range shards {
		if !sh.IsDir() || !strings.HasPrefix(sh.Name(), schemaShardPrefix) {
			continue
		}
		if scanErr := loadGroupResourceOptsFromShard(filepath.Join(schemaRoot, sh.Name()), wanted, candidates); scanErr != nil {
			return nil, scanErr
		}
	}
	out := map[string]*commonv1.ResourceOpts{}
	for _, c := range candidates {
		if c.deleted || c.opts == nil {
			continue
		}
		out[c.name] = c.opts
	}
	return out, nil
}

// groupOptsCandidate is the analog of schemaCandidate for group
// ResourceOpts: tracks the best mod_revision per propID and whether
// the winning revision is a tombstone.
type groupOptsCandidate struct {
	opts    *commonv1.ResourceOpts
	name    string
	modRev  int64
	deleted bool
}

func loadGroupResourceOptsFromShard(
	shardPath string,
	wanted map[string]bool,
	candidates map[string]*groupOptsCandidate,
) error {
	return walkSchemaPropertyShard(shardPath, func(d schemaPropDoc) error {
		if d.kindName != schema.KindGroup.String() || d.sourceJSON == "" {
			return nil
		}
		var g commonv1.Group
		if uErr := protojson.Unmarshal([]byte(d.sourceJSON), &g); uErr != nil {
			return nil
		}
		name := g.GetMetadata().GetName()
		if !wanted[name] {
			return nil
		}
		if cur, ok := candidates[d.propID]; ok && cur.modRev >= d.modRev {
			return nil
		}
		candidates[d.propID] = &groupOptsCandidate{
			opts:    g.GetResourceOpts(),
			name:    name,
			modRev:  d.modRev,
			deleted: d.deleted,
		}
		return nil
	})
}

// loadMeasureSchemasFromShard folds every MEASURE doc in one shard
// into the candidates map. Updates win by mod_revision; tombstones
// drop the propID later.
func loadMeasureSchemasFromShard(shardPath string, wantedGroups map[string]bool,
	candidates map[string]*schemaCandidate,
) error {
	return walkSchemaPropertyShard(shardPath, func(d schemaPropDoc) error {
		if d.kindName != schema.KindMeasure.String() {
			return nil
		}
		if len(wantedGroups) > 0 && !wantedGroups[d.group] {
			return nil
		}
		if d.sourceJSON == "" {
			return fmt.Errorf("schema property %q missing source tag", d.propID)
		}
		var measure databasev1.Measure
		if uErr := protojson.Unmarshal([]byte(d.sourceJSON), &measure); uErr != nil {
			return fmt.Errorf("unmarshal measure %q source: %w", d.propID, uErr)
		}
		if existing, ok := candidates[d.propID]; ok && existing.modRev >= d.modRev {
			return nil
		}
		candidates[d.propID] = &schemaCandidate{
			info:    measureSchemaInfoFromProto(d.group, &measure),
			modRev:  d.modRev,
			deleted: d.deleted,
		}
		return nil
	})
}

// measureSchemaInfoFromProto builds the lookup-table view of a measure
// schema directly from its proto definition.
func measureSchemaInfoFromProto(group string, m *databasev1.Measure) *measureSchemaInfo {
	si := &measureSchemaInfo{
		Group:          group,
		Name:           m.GetMetadata().GetName(),
		EntityTagNames: append([]string(nil), m.GetEntity().GetTagNames()...),
		IndexMode:      m.GetIndexMode(),
	}
	for _, tf := range m.GetTagFamilies() {
		tags := make([]string, 0, len(tf.GetTags()))
		for _, t := range tf.GetTags() {
			tags = append(tags, t.GetName())
			for _, en := range si.EntityTagNames {
				if t.GetName() == en {
					si.EntityFamily = tf.GetName()
				}
			}
		}
		si.TagFamilies = append(si.TagFamilies, tagFamilyInfo{Name: tf.GetName(), Tags: tags})
	}
	for _, f := range m.GetFields() {
		si.Fields = append(si.Fields, fieldInfo{
			Name:      f.GetName(),
			FieldType: f.GetFieldType(),
		})
	}
	return si
}

// measureSchemaInfo is the subset of a measure's schema MigrationCopy
// cares about: enough to drive tag-family projection and reject IndexMode
// groups (their field values live inside sidx, not in part data, and
// broadcasting a union sidx would break query correctness).
type measureSchemaInfo struct {
	Group          string
	Name           string
	EntityTagNames []string
	EntityFamily   string
	TagFamilies    []tagFamilyInfo
	Fields         []fieldInfo
	IndexMode      bool
}

type tagFamilyInfo struct {
	Name string
	Tags []string
}

type fieldInfo struct {
	Name      string
	FieldType databasev1.FieldType
}
