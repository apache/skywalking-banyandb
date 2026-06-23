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
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/blugelabs/bluge"
	blugesearch "github.com/blugelabs/bluge/search"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index"
	"github.com/apache/skywalking-banyandb/pkg/index/inverted"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// Reserved bluge stored-field names written by the inverted store's toDoc
// (pkg/index/inverted/inverted.go:62-65). They are not tags and must be
// handled explicitly rather than treated as tag fields.
const (
	imDocIDField     = "_id"
	imTimestampField = "_timestamp"
	imVersionField   = "_version"
)

// classifyGroup decides how to route a measure group. The internal
// _top_n_result (TopNSchemaName) measure is auto-created by BanyanDB for every
// measure group, so it never counts as a "real" normal measure. A group with
// at least one index-mode measure and no real normal measure is an index-mode
// group (isIndexMode=true); a group with no index-mode measure stays on the
// existing normal path (false, nil); a group that mixes index-mode measures
// with real normal measures is unsupported and returns an error listing the
// offending normal measure names.
func classifyGroup(group string, schemas map[string]*measureSchemaInfo) (isIndexMode bool, err error) {
	var indexModes, realNormals []string
	for name, m := range schemas {
		if name == TopNSchemaName {
			continue
		}
		if m.IndexMode {
			indexModes = append(indexModes, name)
		} else {
			realNormals = append(realNormals, name)
		}
	}
	if len(indexModes) == 0 {
		return false, nil
	}
	if len(realNormals) > 0 {
		sort.Strings(realNormals)
		return false, fmt.Errorf("group %q mixes index-mode measures with non-_top_n_result normal measure(s) %v; "+
			"mixed groups are unsupported", group, realNormals)
	}
	return true, nil
}

// collectTagNames gathers every tag name across the group's measures. A stored
// field whose name is a known tag name is a non-indexed tag; this is what lets
// classifyStoredField tell a 4-CHARACTER tag name apart from a 4-byte IndexRuleID
// (both are 4 bytes wide, so name length alone is ambiguous).
func collectTagNames(schemasBySubject map[string]*measureSchemaInfo) map[string]struct{} {
	out := map[string]struct{}{}
	for _, sc := range schemasBySubject {
		if sc == nil {
			continue
		}
		for _, tf := range sc.TagFamilies {
			for _, tag := range tf.Tags {
				out[tag] = struct{}{}
			}
		}
	}
	return out
}

// classifyStoredField reverses index.FieldKey.Marshal (pkg/index/index.go:73) for
// one stored field name. A name that matches a schema tag name is a non-indexed
// tag (TagName). Otherwise the name is a marshaled 4-byte IndexRuleID — an indexed
// field — so it is kept INDEXED with that rule id; when the rule is missing from
// ruleByID the field stays indexed (Analyzer/NoSort fall back to defaults) and the
// id is returned as missingRuleID so the caller can warn rather than silently
// dropping its searchability. A name that is neither a known tag nor 4 bytes wide
// is unexpected; it is kept as a TagName best-effort.
func classifyStoredField(name string, tagNames map[string]struct{}, ruleByID map[uint32]indexRuleInfo) (
	key index.FieldKey, indexed bool, missingRuleID uint32,
) {
	if _, isTag := tagNames[name]; isTag {
		return index.FieldKey{TagName: name}, false, 0
	}
	if len(name) == 4 {
		id := convert.BytesToUint32([]byte(name))
		if _, ok := ruleByID[id]; ok {
			return index.FieldKey{IndexRuleID: id}, true, 0
		}
		return index.FieldKey{IndexRuleID: id}, true, id
	}
	return index.FieldKey{TagName: name}, false, 0
}

// readIndexModeDocs scans every committed bluge doc under sidxDir and rebuilds
// each as an index.Document from TWO sources:
//
//	A) stored fields -> regular tag fields (+ timestamp/version), and
//	B) the doc _id unmarshaled to a pbv1.Series -> regenerate the index-only
//	   _im_name / _im_entity_tag_* fields, which the write path never stores.
//
// ruleByID restores Analyzer/NoSort on indexed fields; schemasBySubject maps
// each series' Subject to its measure schema so the entity-derived fields are
// regenerated with the right tag names/types and skip conditions.
func readIndexModeDocs(ctx context.Context, sidxDir string, ruleByID map[uint32]indexRuleInfo,
	schemasBySubject map[string]*measureSchemaInfo,
) ([]index.Document, error) {
	r, err := bluge.OpenReader(bluge.DefaultConfig(sidxDir))
	if err != nil {
		// A sidx directory can exist on disk with no committed bluge snapshot
		// (a fresh segment whose writer never received a doc). Treat that as an
		// empty sidx instead of failing the whole copy.
		if strings.Contains(err.Error(), "unable to find a usable snapshot") {
			return nil, nil
		}
		return nil, fmt.Errorf("open sidx reader %s: %w", sidxDir, err)
	}
	defer func() { _ = r.Close() }()
	dmi, err := r.Search(ctx, bluge.NewAllMatches(bluge.NewMatchAllQuery()))
	if err != nil {
		return nil, fmt.Errorf("search sidx %s: %w", sidxDir, err)
	}
	var out []index.Document
	var timeless []uint64
	tagNames := collectTagNames(schemasBySubject)
	missingRules := map[uint32]int{}
	for {
		match, nextErr := dmi.Next()
		if nextErr != nil {
			return nil, fmt.Errorf("iterate sidx %s: %w", sidxDir, nextErr)
		}
		if match == nil {
			break
		}
		doc, buildErr := rebuildOneDoc(match, ruleByID, schemasBySubject, tagNames, missingRules)
		if buildErr != nil {
			return nil, fmt.Errorf("rebuild doc in %s: %w", sidxDir, buildErr)
		}
		// The write path always stamps a checked, non-zero _timestamp on every
		// index-mode doc (write_standalone.go), so Timestamp==0 here means the
		// stored _timestamp is missing or undecodable — corrupt or unexpected
		// source data. Routing such a doc by a guessed segment boundary would
		// silently misplace it, so collect the offenders (without retaining the
		// corrupt docs) and surface them once the whole sidx has been scanned.
		if doc.Timestamp == 0 {
			timeless = append(timeless, doc.DocID)
			continue
		}
		out = append(out, doc)
	}
	// Surface corrupt data before anything else: a missing-rule warning implies
	// "continuing", which would be misleading when the read is about to abort.
	if len(timeless) > 0 {
		sample := timeless
		if len(sample) > 10 {
			sample = sample[:10]
		}
		return nil, fmt.Errorf("sidx %s: %d index-mode doc(s) have a missing or undecodable _timestamp (ts==0), "+
			"which never occurs for valid data; the source is corrupt or unexpected — sample series IDs (first %d) %v",
			sidxDir, len(timeless), len(sample), sample)
	}
	warnMissingRules(sidxDir, missingRules)
	return out, nil
}

// warnMissingRules logs a single warning enumerating every IndexRuleID that an
// indexed stored field decoded to but that is NOT registered in the schema-
// property catalog, with its occurrence count. The fields are still kept indexed
// (their Analyzer/NoSort fall back to defaults), but a non-empty list means the
// catalog is missing index rules the data was written with, so the operator should
// know rather than have a possibly-searchable field silently degraded.
func warnMissingRules(sidxDir string, missingRules map[uint32]int) {
	if len(missingRules) == 0 {
		return
	}
	ids := make([]string, 0, len(missingRules))
	total := 0
	for id, count := range missingRules {
		ids = append(ids, fmt.Sprintf("%d×%d", id, count))
		total += count
	}
	sort.Strings(ids)
	logger.GetLogger("measure-migration").Warn().
		Str("sidx", sidxDir).
		Int("distinctRules", len(missingRules)).
		Int("totalFields", total).
		Strs("ruleIDxCount", ids).
		Msg("index-mode rebuild: indexed field(s) decode to an IndexRuleID missing from the schema-property " +
			"catalog; kept indexed with default Analyzer/NoSort — restore the missing index rule(s) for exact fidelity")
}

// rebuildOneDoc reconstructs a single index.Document from a bluge match,
// combining the two sources documented on readIndexModeDocs. tagNames is the
// group's known tag-name set (to tell a tag from a rule id). missingRules, when
// non-nil, accumulates (rule-id -> count) for fields that decode to an
// IndexRuleID NOT present in ruleByID, so the caller can warn.
func rebuildOneDoc(match *blugesearch.DocumentMatch, ruleByID map[uint32]indexRuleInfo,
	schemasBySubject map[string]*measureSchemaInfo, tagNames map[string]struct{}, missingRules map[uint32]int,
) (index.Document, error) {
	var entityValues []byte
	var ts, version int64
	var fields []index.Field
	visitErr := match.VisitStoredFields(func(name string, value []byte) bool {
		switch name {
		case imDocIDField:
			entityValues = append([]byte(nil), value...)
		case imTimestampField:
			if dt, decErr := bluge.DecodeDateTime(value); decErr == nil {
				ts = dt.UnixNano()
			}
		case imVersionField:
			version = convert.BytesToInt64(value)
		default:
			key, indexed, missingRuleID := classifyStoredField(name, tagNames, ruleByID)
			// NewBytesField clones value internally, so no extra copy of the
			// reusable bluge visit buffer is needed here.
			f := index.NewBytesField(key, value)
			f.Store = true
			f.Index = indexed
			if ri, ok := ruleByID[key.IndexRuleID]; indexed && ok {
				f.NoSort = ri.NoSort
				f.Key.Analyzer = ri.Analyzer
			}
			// A field that decodes to an IndexRuleID the catalog does not know: it
			// stays indexed (defaults), but the operator must be told rather than
			// have a possibly-searchable field silently degraded.
			if missingRuleID != 0 && missingRules != nil {
				missingRules[missingRuleID]++
			}
			fields = append(fields, f)
		}
		return true
	})
	if visitErr != nil {
		return index.Document{}, fmt.Errorf("visit stored fields: %w", visitErr)
	}
	// Source B: rebuild the index-only entity-derived fields from _id.
	var series pbv1.Series
	if err := series.Unmarshal(entityValues); err != nil {
		return index.Document{}, fmt.Errorf("unmarshal series from _id: %w", err)
	}
	fields = appendRegeneratedEntityFields(fields, &series, schemasBySubject[series.Subject])
	return index.Document{
		Fields:       fields,
		EntityValues: entityValues,
		Timestamp:    ts,
		DocID:        uint64(series.ID),
		Version:      version,
	}, nil
}

// appendRegeneratedEntityFields rebuilds the index-only fields the production
// write path adds in appendEntityTagsToIndexFields (write_standalone.go:410):
// _im_name (subject) and _im_entity_tag_<tag> (entity values). These are never
// stored, so they must be regenerated from the series, or entity-tag / measure-
// name search breaks after migration.
//
// The entity-tag fields replicate the production skip condition: an entity tag
// already indexed by an index rule is not re-emitted as _im_entity_tag_*.
func appendRegeneratedEntityFields(fields []index.Field, series *pbv1.Series, schema *measureSchemaInfo) []index.Field {
	subj := index.NewStringField(index.FieldKey{TagName: index.IndexModeName}, series.Subject)
	subj.Index = true
	subj.NoSort = true
	fields = append(fields, subj)
	if schema == nil {
		return fields
	}
	for i, tagName := range schema.EntityTagNames {
		if i >= len(series.EntityValues) {
			break
		}
		if _, indexed := schema.IndexedEntityTags[tagName]; indexed {
			continue
		}
		nv := encodeTagValue(tagName, schema.TagType[tagName], series.EntityValues[i])
		value := nv.value
		releaseNameValue(nv)
		if value == nil {
			continue
		}
		f := index.NewBytesField(index.FieldKey{TagName: index.IndexModeEntityTagPrefix + tagName}, value)
		f.Index = true
		f.NoSort = true
		fields = append(fields, f)
	}
	return fields
}

// ── Target idx store pool ────────────────────────────────────────────────────.

// targetIdxStore lazily opens one inverted store per target sidx path
// (BatchWaitSec:0) and closes them all at the end, so multiple source segs
// feeding the same target sidx share a single writer. Copied (cross-package,
// unexported) from stream/migration_element_index.go:126-158.
type targetIdxStore struct {
	stores map[string]index.SeriesStore
}

func newTargetIdxStore() *targetIdxStore {
	return &targetIdxStore{stores: map[string]index.SeriesStore{}}
}

func (t *targetIdxStore) get(path string) (index.SeriesStore, error) {
	if s, ok := t.stores[path]; ok {
		return s, nil
	}
	if err := os.MkdirAll(path, storage.DirPerm); err != nil {
		return nil, fmt.Errorf("mkdir idx %s: %w", path, err)
	}
	s, err := inverted.NewStore(inverted.StoreOpts{Path: path, BatchWaitSec: 0})
	if err != nil {
		return nil, fmt.Errorf("open idx store %s: %w", path, err)
	}
	t.stores[path] = s
	return s, nil
}

func (t *targetIdxStore) closeAll() error {
	var firstErr error
	for path, s := range t.stores {
		if err := s.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close idx store %s: %w", path, err)
		}
	}
	t.stores = map[string]index.SeriesStore{}
	return firstErr
}

// ── Segment routing ──────────────────────────────────────────────────────────.

// ── Slow path: rebuild + route + merge keeping max version ───────────────────.

// copyIndexModeSlowDocs routes already-read docs (docsByDir, keyed by source
// sidx dir) by their _timestamp to the aligned target seg's sidx, deduplicates
// per series (DocID) keeping the highest Version within each target seg, and
// upserts the survivors via UpdateSeriesBatch. The target seg's <segDir>/metadata
// is written too, since a segment without it is treated as invalid and removed
// at startup (storage/segment.go open loop). Routing all slow-path sources
// through a single call is what makes the max-version dedup work ACROSS sources
// that merge into the same target seg. Returns the number
// of rows written.
func copyIndexModeSlowDocs(ctx context.Context, srcSidxDirs []string, docsByDir map[string][]index.Document,
	dstGroupRoot string, ir storage.IntervalRule, stores *targetIdxStore,
) (rows int64, err error) {
	// per target sidx path -> seriesID(DocID) -> survivor doc (max version).
	perTarget := map[string]map[uint64]index.Document{}
	// targetSegName per sidx path, to write the seg metadata once flushed.
	segNameByPath := map[string]string{}
	var segCache alignedSegCache
	for _, dir := range srcSidxDirs {
		docs := docsByDir[dir]
		for di := range docs {
			d := docs[di]
			seg := segCache.segNameFor(ir, d.Timestamp)
			dstSidx := filepath.Join(dstGroupRoot, seg, directCopySidxDirName)
			segNameByPath[dstSidx] = seg
			m := perTarget[dstSidx]
			if m == nil {
				m = map[uint64]index.Document{}
				perTarget[dstSidx] = m
			}
			if prev, ok := m[d.DocID]; !ok || d.Version >= prev.Version {
				m[d.DocID] = d
			}
		}
	}
	for dstSidx, m := range perTarget {
		if ctx.Err() != nil {
			return rows, ctx.Err()
		}
		store, getErr := stores.get(dstSidx)
		if getErr != nil {
			return rows, getErr
		}
		batch := index.Batch{Documents: make(index.Documents, 0, len(m))}
		for _, d := range m {
			batch.Documents = append(batch.Documents, d)
			rows++
		}
		if updateErr := store.UpdateSeriesBatch(batch); updateErr != nil {
			return rows, fmt.Errorf("write target sidx %s: %w", dstSidx, updateErr)
		}
		segDir := filepath.Join(dstGroupRoot, segNameByPath[dstSidx])
		if _, metaErr := writeIndexModeSegMetadata(segDir, ir); metaErr != nil {
			return rows, metaErr
		}
	}
	return rows, nil
}

// sortedSegList renders a target-segment set as a sorted comma-separated string
// for deterministic per-source rebuild log lines.
func sortedSegList(segs map[string]struct{}) string {
	names := make([]string, 0, len(segs))
	for s := range segs {
		names = append(names, s)
	}
	sort.Strings(names)
	return strings.Join(names, ",")
}

// ── Top-level orchestration ──────────────────────────────────────────────────.

// copyIndexModeGroup copies one (entry, group) for an index-mode measure group.
// It rejects groups carrying shard parts (a non-_top_n_result normal measure
// has data), then for each source seg sidx either byte-copies the whole sidx
// directory (fast path: all docs align to one not-yet-written target seg) or
// rebuilds and routes docs through the slow path. Each touched target segment
// gets its <segDir>/metadata so the runtime loads it instead of pruning it.
func copyIndexModeGroup(ctx context.Context, in migration.EntryGroupInput,
	ruleByID map[uint32]indexRuleInfo, schemasBySubject map[string]*measureSchemaInfo,
) (res migration.EntryGroupResult, err error) {
	hasPart, err := hasShardPart(ctx, in.SrcRoots)
	if err != nil {
		return res, fmt.Errorf("scan for shard parts: %w", err)
	}
	if hasPart {
		return res, fmt.Errorf("index-mode group %q has shard part(s) on disk "+
			"(a non-_top_n_result normal measure has data?); refusing", in.Group)
	}

	srcSidxDirs, err := collectSourceSidxDirs(in.SrcRoots)
	if err != nil {
		return res, err
	}
	if len(srcSidxDirs) == 0 {
		logStep("%s stage=%s group %s: skipped (no sidx in selected nodes)",
			in.EntryTag, in.Entry.Stage, in.Group)
		return res, nil
	}

	stores := newTargetIdxStore()
	// Close() flushes buffered batches and lands bluge's own snapshot; a close
	// failure means an incomplete sidx on disk, so it must surface even on the
	// happy path.
	defer func() {
		if cerr := stores.closeAll(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	// Pass 1: read every source's docs once and compute the FULL set of target
	// segs each source routes docs into (not just "single target or not").
	// docsByDir caches the docs so the slow path never re-reads them.
	// targetSegsBySrc holds each source's target-seg set; segSrcCount counts how
	// many sources route ANY doc into each target seg — split sources included —
	// so byte-copy exclusivity is judged against every writer, not just other
	// single-target sources.
	docsByDir := make(map[string][]index.Document, len(srcSidxDirs))
	targetSegsBySrc := make(map[string]map[string]struct{}, len(srcSidxDirs))
	segSrcCount := map[string]int{}
	for _, srcSidx := range srcSidxDirs {
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		docs, readErr := readIndexModeDocs(ctx, srcSidx, ruleByID, schemasBySubject)
		if readErr != nil {
			return res, readErr
		}
		if len(docs) == 0 {
			continue
		}
		docsByDir[srcSidx] = docs
		segs := make(map[string]struct{})
		var segCache alignedSegCache
		for di := range docs {
			segs[segCache.segNameFor(in.Interval, docs[di].Timestamp)] = struct{}{}
		}
		targetSegsBySrc[srcSidx] = segs
		for seg := range segs {
			segSrcCount[seg]++
		}
	}

	// Pass 2: a source is byte-copy eligible iff all its docs fall into ONE
	// target seg AND that target seg has no OTHER source routing any doc into it
	// (exclusive — split sources count) AND the target sidx is not already on
	// disk. Every other source is routed through a SINGLE copyIndexModeSlowDocs
	// call so max-version dedup spans all sources merging into the same target
	// seg.
	var slowDirs []string
	for _, srcSidx := range srcSidxDirs {
		docs := docsByDir[srcSidx]
		if len(docs) == 0 {
			continue
		}
		// A source byte-copies only when its docs all fall in ONE target seg that
		// no other source touches. The single element is read inside the len==1
		// guard so a multi-target (split) source never reads a stale seg value.
		if segs := targetSegsBySrc[srcSidx]; len(segs) == 1 {
			var seg string
			for s := range segs {
				seg = s
			}
			if segSrcCount[seg] == 1 {
				dstSidx := filepath.Join(in.TargetGroupRoot, seg, directCopySidxDirName)
				// The dirty-target pre-flight (orchestrator.RunCopy) guarantees the
				// target group dir is empty at run start, so a missing dstSidx means
				// no other source wrote it either — a true exclusive byte-copy.
				if _, statErr := os.Stat(dstSidx); os.IsNotExist(statErr) {
					n, copyErr := migration.CopyDir(srcSidx, dstSidx)
					if copyErr != nil {
						return res, fmt.Errorf("byte-copy sidx %s -> %s: %w", srcSidx, dstSidx, copyErr)
					}
					segDir := filepath.Join(in.TargetGroupRoot, seg)
					metaBytes, metaErr := writeIndexModeSegMetadata(segDir, in.Interval)
					if metaErr != nil {
						return res, metaErr
					}
					res.Bytes += n + metaBytes
					res.Rows += int64(len(docs))
					logStep("%s stage=%s group %s: sidx %s -> byte-copy into seg %s (%d docs)",
						in.EntryTag, in.Entry.Stage, in.Group, srcSidx, seg, len(docs))
					continue
				}
			}
		}
		// Per-source visibility: log that this source segment takes the rebuild
		// (read-docs-then-write) path and which target segment(s) its docs route
		// into, the counterpart of the byte-copy line above.
		logStep("%s stage=%s group %s: sidx %s -> rebuild into seg(s) %s (%d docs)",
			in.EntryTag, in.Entry.Stage, in.Group, srcSidx,
			sortedSegList(targetSegsBySrc[srcSidx]), len(docs))
		slowDirs = append(slowDirs, srcSidx)
	}

	if len(slowDirs) > 0 {
		rows, slowErr := copyIndexModeSlowDocs(ctx, slowDirs, docsByDir, in.TargetGroupRoot, in.Interval, stores)
		if slowErr != nil {
			return res, slowErr
		}
		res.Rows += rows
		logStep("%s stage=%s group %s: %d sidx -> rebuild total (%d rows after max-version merge)",
			in.EntryTag, in.Entry.Stage, in.Group, len(slowDirs), rows)
	}

	// Flush the slow-path stores before counting segments so every target sidx
	// (and its bluge snapshot) is on disk. closeAll is idempotent; the deferred
	// call becomes a no-op.
	if closeErr := stores.closeAll(); closeErr != nil {
		return res, closeErr
	}
	res.Segments = countTargetSegs(in.TargetGroupRoot)
	return res, nil
}

// hasShardPart reports whether any source root holds a measure shard PART
// (seg-*/shard-*/<partID>). Index-mode groups keep data only in the segment-level
// sidx, so an actual part means a non-_top_n_result normal measure has on-disk
// data, which the index-mode copy path refuses. An EMPTY shard-* directory is
// benign: the segment controller can create shard-0 eagerly (or a dataless
// _top_n_result leaves one behind) without any part landing in it, so the guard
// keys off real parts — matching directCopyPartDirPattern, exactly what
// discoverPartTasks consumes — not bare directory existence. It short-circuits on
// the first part instead of building the full task list.
func hasShardPart(ctx context.Context, srcRoots []string) (bool, error) {
	for _, root := range srcRoots {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		segEntries, err := os.ReadDir(root)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return false, fmt.Errorf("read %s: %w", root, err)
		}
		for _, segE := range segEntries {
			if !segE.IsDir() || !strings.HasPrefix(segE.Name(), directCopySegPrefix) {
				continue
			}
			shardEntries, err := os.ReadDir(filepath.Join(root, segE.Name()))
			if err != nil {
				return false, fmt.Errorf("read %s: %w", filepath.Join(root, segE.Name()), err)
			}
			for _, shardE := range shardEntries {
				if !shardE.IsDir() || !strings.HasPrefix(shardE.Name(), directCopyShardPrefix) {
					continue
				}
				shardDir := filepath.Join(root, segE.Name(), shardE.Name())
				partEntries, err := os.ReadDir(shardDir)
				if err != nil {
					return false, fmt.Errorf("read %s: %w", shardDir, err)
				}
				for _, partE := range partEntries {
					if partE.IsDir() && directCopyPartDirPattern.MatchString(partE.Name()) {
						return true, nil
					}
				}
			}
		}
	}
	return false, nil
}

// collectSourceSidxDirs lists every "<root>/seg-*/sidx" directory across the
// entry's source roots. A seg directory without a sidx subdir is skipped.
func collectSourceSidxDirs(srcRoots []string) ([]string, error) {
	var out []string
	for _, root := range srcRoots {
		segEntries, err := os.ReadDir(root)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("read %s: %w", root, err)
		}
		for _, segE := range segEntries {
			if !segE.IsDir() || !strings.HasPrefix(segE.Name(), directCopySegPrefix) {
				continue
			}
			sidxDir := filepath.Join(root, segE.Name(), directCopySidxDirName)
			if info, statErr := os.Stat(sidxDir); statErr == nil && info.IsDir() {
				out = append(out, sidxDir)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

// countTargetSegs counts "seg-*" directories under the target group root.
func countTargetSegs(dstGroupRoot string) int {
	entries, err := os.ReadDir(dstGroupRoot)
	if err != nil {
		return 0
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), directCopySegPrefix) {
			n++
		}
	}
	return n
}

// writeIndexModeSegMetadata writes <segDir>/metadata for an index-mode target
// segment. The segment's end boundary is the target grid's NextTime after the
// segment start parsed from its directory name. Without this file the runtime
// prunes the segment as invalid at startup.
func writeIndexModeSegMetadata(segDir string, ir storage.IntervalRule) (int64, error) {
	segName := filepath.Base(segDir)
	start, err := parseDirectCopySegStart(segName, ir.Unit)
	if err != nil {
		return 0, fmt.Errorf("parse target seg start %s: %w", segName, err)
	}
	return writeDirectCopySegmentMetadata(segDir, ir.NextTime(start))
}

// ── Phase 3 / 4: value digest, verify, analyze ───────────────────────────────.

// docValueDigest hashes ALL data values of one rebuilt index-mode document so
// that any changed value or any dropped field flips the digest. It covers:
//
//   - Version, and
//   - every field's (Key.Marshal(), value bytes).
//
// The regenerated index-only fields (_im_name / _im_entity_tag_*) are part of
// Fields by the time docValueDigest runs, so a doc that lost an entity-derived
// value produces a different digest than the intact source doc.
// The (name, value) pairs are sorted before hashing, so the digest is
// independent of field order — two docs equal in content hash identically even
// if their Fields slices were assembled in a different sequence.
func docValueDigest(d index.Document) uint64 {
	return docDigest(d, true)
}

// docDataDigest hashes only the document's data values (every field's
// (Key.Marshal(), bytes)) and EXCLUDES Version. analyze's value-conflict check
// uses it to tell apart a normal version duplicate (same data, version merely
// bumped — identical data digest) from a genuine value conflict (same (series,
// timestamp) but diverging data — different data digest).
func docDataDigest(d index.Document) uint64 {
	return docDigest(d, false)
}

// docDigest is the shared field-hashing core. The (name, value) pairs are sorted
// before hashing, so the digest is independent of field order. When
// includeVersion is true the document Version is folded in first; the
// regenerated index-only fields (_im_name / _im_entity_tag_*) are part of Fields
// by the time this runs, so a doc that lost an entity-derived value digests
// differently from the intact one.
func docDigest(d index.Document, includeVersion bool) uint64 {
	type nv struct {
		name  string
		value []byte
	}
	pairs := make([]nv, 0, len(d.Fields))
	for i := range d.Fields {
		pairs = append(pairs, nv{name: d.Fields[i].Key.Marshal(), value: d.Fields[i].GetBytes()})
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].name != pairs[j].name {
			return pairs[i].name < pairs[j].name
		}
		return string(pairs[i].value) < string(pairs[j].value)
	})
	h := fnv.New64a()
	var scratch [8]byte
	if includeVersion {
		binary.LittleEndian.PutUint64(scratch[:], uint64(d.Version))
		_, _ = h.Write(scratch[:])
	}
	for i := range pairs {
		binary.LittleEndian.PutUint64(scratch[:], uint64(len(pairs[i].name)))
		_, _ = h.Write(scratch[:])
		_, _ = io.WriteString(h, pairs[i].name)
		binary.LittleEndian.PutUint64(scratch[:], uint64(len(pairs[i].value)))
		_, _ = h.Write(scratch[:])
		_, _ = h.Write(pairs[i].value)
	}
	return h.Sum64()
}

// imValueKey identifies one logical data point in an index-mode group: the
// series (DocID) plus its timestamp. Only analyze keys on this; verify keys on
// imSegKey (segment, series), matching the runtime's per-segment upsert.
type imValueKey struct {
	sid uint64
	ts  int64
}

// imSegKey identifies one logical index-mode data point for verify
// reconciliation: the target segment plus the series (DocID). The runtime stores
// index-mode data as an upsert-by-series-PER-SEGMENT, so the correct unit is
// (segment, series) — NOT (series, timestamp). When several source segments
// merge into one coarser target segment, each series collapses to a single
// max-version doc, so two source docs of one series at different timestamps are
// EXPECTED to become one; a (series, timestamp) key would mis-report the dropped
// one as "missing".
type imSegKey struct {
	seg string
	sid uint64
}

// segSurvivor is the max-version doc kept for an imSegKey: its version drives the
// max-version reduction, its digest the value comparison.
type segSurvivor struct {
	version int64
	digest  uint64
}

// collectTargetSidxDirs lists every "<groupRoot>/seg-*/sidx" directory under one
// target group root. It is the target-side analog of collectSourceSidxDirs and
// is what the index-mode verify reads to count migrated documents.
func collectTargetSidxDirs(groupRoot string) ([]string, error) {
	return collectSourceSidxDirs([]string{groupRoot})
}

// readAllIndexModeDocs reads and concatenates the docs of every sidx dir in dirs,
// reusing the double-source rebuild so callers see the regenerated index-only
// fields too. analyze uses it to group docs by (series, timestamp) across every
// source segment.
func readAllIndexModeDocs(ctx context.Context, dirs []string, ruleByID map[uint32]indexRuleInfo,
	schemasBySubject map[string]*measureSchemaInfo,
) ([]index.Document, error) {
	var out []index.Document
	for _, dir := range dirs {
		docs, err := readIndexModeDocs(ctx, dir, ruleByID, schemasBySubject)
		if err != nil {
			return nil, err
		}
		out = append(out, docs...)
	}
	return out, nil
}

// segDigestResult is segKeyedDigests' output: max-version digests keyed by
// (segment, series), the distinct-series set, the total doc count, the per-target
// -segment doc count, and (source side only) the set of source segments that
// routed docs into each target segment — used to report per-segment alignment.
type segDigestResult struct {
	byKey            map[imSegKey]segSurvivor
	distinctIDs      map[uint64]struct{}
	docsPerSeg       map[string]uint64
	srcSegsPerTarget map[string]map[string]struct{}
	total            uint64
}

// segKeyedDigests reads every dir's docs and reduces them to one max-version
// value digest per (target segment, series). When routed is true the segment is
// where each doc ROUTES under ir — the source side, mirroring the copy router —
// so it matches the target's already-laid-out segments and records which source
// segment fed each target; otherwise the segment is the dir's own seg-* name (the
// target side).
func segKeyedDigests(ctx context.Context, dirs []string, ruleByID map[uint32]indexRuleInfo,
	schemasBySubject map[string]*measureSchemaInfo, ir storage.IntervalRule, routed bool,
) (*segDigestResult, error) {
	res := &segDigestResult{
		byKey:            map[imSegKey]segSurvivor{},
		distinctIDs:      map[uint64]struct{}{},
		docsPerSeg:       map[string]uint64{},
		srcSegsPerTarget: map[string]map[string]struct{}{},
	}
	var segCache alignedSegCache
	for _, dir := range dirs {
		docs, readErr := readIndexModeDocs(ctx, dir, ruleByID, schemasBySubject)
		if readErr != nil {
			return nil, readErr
		}
		dirSeg := filepath.Base(filepath.Dir(dir)) // seg-* name is the sidx dir's parent
		for i := range docs {
			d := docs[i]
			res.total++
			res.distinctIDs[d.DocID] = struct{}{}
			seg := dirSeg
			if routed {
				seg = segCache.segNameFor(ir, d.Timestamp)
				srcSet := res.srcSegsPerTarget[seg]
				if srcSet == nil {
					srcSet = map[string]struct{}{}
					res.srcSegsPerTarget[seg] = srcSet
				}
				srcSet[dirSeg] = struct{}{}
			}
			res.docsPerSeg[seg]++
			k := imSegKey{seg: seg, sid: d.DocID}
			if prev, ok := res.byKey[k]; !ok || d.Version >= prev.version {
				res.byKey[k] = segSurvivor{version: d.Version, digest: docValueDigest(d)}
			}
		}
	}
	return res, nil
}

// buildSegAligns reports, per target segment, how many source segments fed it,
// how many source docs routed in, and how many target docs survived. A segment
// is "aligned" — a true 1:1 byte-copy candidate, matching the copy's exclusivity
// rule — iff exactly ONE source segment fed it AND that source segment fed NO
// OTHER target (a clean 1:1 pairing) AND no docs collapsed. A source segment that
// splits across several targets disqualifies every target it touches, even one it
// is the sole contributor to.
func buildSegAligns(src, tgt *segDigestResult) []IndexModeSegAlign {
	// Count how many distinct target segments each source segment feeds.
	srcSegTargets := map[string]int{}
	for targetSeg := range src.srcSegsPerTarget {
		for srcSeg := range src.srcSegsPerTarget[targetSeg] {
			srcSegTargets[srcSeg]++
		}
	}
	segs := make(map[string]struct{}, len(src.docsPerSeg))
	for s := range src.docsPerSeg {
		segs[s] = struct{}{}
	}
	for s := range tgt.docsPerSeg {
		segs[s] = struct{}{}
	}
	out := make([]IndexModeSegAlign, 0, len(segs))
	for s := range segs {
		srcDocs := src.docsPerSeg[s]
		tgtDocs := tgt.docsPerSeg[s]
		sources := src.srcSegsPerTarget[s]
		aligned := false
		if len(sources) == 1 && srcDocs == tgtDocs {
			for only := range sources {
				aligned = srcSegTargets[only] == 1
			}
		}
		out = append(out, IndexModeSegAlign{
			Segment: s, SrcSegs: len(sources), SrcDocs: srcDocs, TgtDocs: tgtDocs, Aligned: aligned,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Segment < out[j].Segment })
	return out
}

// verifyIndexModeGroup reconciles one index-mode (entry, group) at the document
// level:
//
//	① counts: source/target total docs + distinct doc-id. With aligned, non-
//	   merging copy the distinct doc-id count matches exactly; when several
//	   source segments merge into one target segment the target collapses each
//	   series to one upserted doc, so the expected target distinct doc-id count
//	   is the UNION of the source doc-ids (computed here, not a sum).
//	② values: per (series, timestamp) digest (source side reduced to max
//	   version first). A key in source but not target is "missing", in target
//	   but not source is "extra", present on both with differing digest is
//	   "tampered" (a changed value or a dropped index-only field).
//
// The report reuses EntryGroupReport with its IndexMode-prefixed fields filled.
func verifyIndexModeGroup(ctx context.Context, in migration.EntryGroupInput,
	ruleByID map[uint32]indexRuleInfo, schemasBySubject map[string]*measureSchemaInfo,
) (EntryGroupReport, error) {
	report := EntryGroupReport{
		Group:       in.Group,
		EntryStage:  in.Entry.Stage,
		EntryTarget: in.Entry.Target,
		EntryNodes:  in.Entry.Nodes,
		SrcRoots:    in.SrcRoots,
		TargetGroup: in.TargetGroupRoot,
		IndexMode:   true,
	}

	srcSidxDirs, err := collectSourceSidxDirs(in.SrcRoots)
	if err != nil {
		return report, fmt.Errorf("collect source sidx dirs: %w", err)
	}
	tgtSidxDirs, err := collectTargetSidxDirs(in.TargetGroupRoot)
	if err != nil {
		return report, fmt.Errorf("collect target sidx dirs: %w", err)
	}

	// Source docs are keyed by the segment they ROUTE to (mirroring the copy
	// router); target docs by the segment they already live in. Both reduce to
	// one max-version doc per (segment, series), so the two sides are directly
	// comparable even when the copy merged several source segments into one.
	src, err := segKeyedDigests(ctx, srcSidxDirs, ruleByID, schemasBySubject, in.Interval, true)
	if err != nil {
		return report, fmt.Errorf("read source docs: %w", err)
	}
	tgt, err := segKeyedDigests(ctx, tgtSidxDirs, ruleByID, schemasBySubject, in.Interval, false)
	if err != nil {
		return report, fmt.Errorf("read target docs: %w", err)
	}

	report.SrcDocs = src.total
	report.TgtDocs = tgt.total
	report.SrcDistinctIDs = uint64(len(src.distinctIDs))
	report.TgtDistinctIDs = uint64(len(tgt.distinctIDs))
	// Expected target distinct doc-id is the union of source doc-ids: under
	// upsert merge it equals the source distinct count, while a no-merge copy
	// trivially keeps the same set — one formula covers both.
	report.ExpectDistinctIDs = uint64(len(src.distinctIDs))

	report.ValueMismatches = diffValueDigests(src.byKey, tgt.byKey)
	report.SegAligns = buildSegAligns(src, tgt)
	return report, nil
}

// diffValueDigests reconciles two (segment, series) → max-version digest maps and
// returns every key that did not match, tagged missing / extra / tampered.
// Results are sorted (segment, sid) for deterministic reporting.
func diffValueDigests(src, tgt map[imSegKey]segSurvivor) []IndexModeValueMismatch {
	var out []IndexModeValueMismatch
	for k, sv := range src {
		tv, ok := tgt[k]
		switch {
		case !ok:
			out = append(out, IndexModeValueMismatch{Kind: "missing", Segment: k.seg, SeriesID: k.sid})
		case sv.digest != tv.digest:
			out = append(out, IndexModeValueMismatch{Kind: "tampered", Segment: k.seg, SeriesID: k.sid})
		}
	}
	for k := range tgt {
		if _, ok := src[k]; !ok {
			out = append(out, IndexModeValueMismatch{Kind: "extra", Segment: k.seg, SeriesID: k.sid})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Segment != out[j].Segment {
			return out[i].Segment < out[j].Segment
		}
		if out[i].SeriesID != out[j].SeriesID {
			return out[i].SeriesID < out[j].SeriesID
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

// IsIndexModeGroup loads the group's measure schemas from schemaRoot and reports
// whether it is an index-mode group, so the analyze CLI can pick the sidx-based
// path. It returns an error for an unsupported mixed group (the same rule
// classifyGroup enforces during copy/verify).
func IsIndexModeGroup(schemaRoot, group string) (bool, error) {
	schemas, err := loadMeasureSchemas(schemaRoot, []string{group})
	if err != nil {
		return false, fmt.Errorf("load measure schemas: %w", err)
	}
	if len(schemas[group]) == 0 {
		return false, fmt.Errorf("group %q has no measures in backup schema-property catalog", group)
	}
	return classifyGroup(group, schemas[group])
}

// AnalyzeIndexModeGroup is the exported entry the analyze CLI calls for an
// index-mode group: it loads the group's schemas + index rules, collects every
// source sidx dir under srcRoots, and runs the sidx-document analysis.
func AnalyzeIndexModeGroup(ctx context.Context, schemaRoot, group string, srcRoots []string, sampleCap int) (AnalyzeGroupResult, error) {
	var res AnalyzeGroupResult
	schemas, err := loadMeasureSchemas(schemaRoot, []string{group}) //nolint:contextcheck // offline bluge schema read, no cancellation
	if err != nil {
		return res, fmt.Errorf("load measure schemas: %w", err)
	}
	ruleByID, err := loadIndexRuleInfoByID(schemaRoot, []string{group}) //nolint:contextcheck // offline bluge schema read, no cancellation
	if err != nil {
		return res, fmt.Errorf("load index rules: %w", err)
	}
	srcSidxDirs, err := collectSourceSidxDirs(srcRoots)
	if err != nil {
		return res, fmt.Errorf("collect source sidx dirs: %w", err)
	}
	return analyzeIndexModeGroup(ctx, srcSidxDirs, ruleByID, schemas[group], sampleCap)
}

// analyzeIndexModeGroup is the index-mode flavor of AnalyzeGroupRows: it reads
// the group's sidx documents (not part block metadata) and
// reports the same shape — total docs, distinct (series, timestamp) keys,
// version-duplicate rows, sample keys — PLUS value conflicts. A value conflict
// is a (series, timestamp) key that appears in more than one document whose
// value digests differ; that distinguishes a normal version-evolved duplicate
// from an abnormal value inconsistency or a dropped field.
func analyzeIndexModeGroup(ctx context.Context, srcSidxDirs []string, ruleByID map[uint32]indexRuleInfo,
	schemasBySubject map[string]*measureSchemaInfo, sampleCap int,
) (AnalyzeGroupResult, error) {
	var res AnalyzeGroupResult
	docs, err := readAllIndexModeDocs(ctx, srcSidxDirs, ruleByID, schemasBySubject)
	if err != nil {
		return res, fmt.Errorf("read index-mode docs: %w", err)
	}

	type rowInfo struct {
		version int64
		digest  uint64
	}
	byKey := make(map[imValueKey][]rowInfo, len(docs))
	for i := range docs {
		d := docs[i]
		res.TotalRows++
		k := imValueKey{sid: d.DocID, ts: d.Timestamp}
		// Conflict detection compares data values WITHOUT version: two docs that
		// differ only by version (normal evolution) must NOT count as a conflict.
		byKey[k] = append(byKey[k], rowInfo{version: d.Version, digest: docDataDigest(d)})
	}
	res.UniqueKeys = uint64(len(byKey))
	res.DuplicateRows = res.TotalRows - res.UniqueKeys
	res.PartsScanned = len(srcSidxDirs)

	keys := make([]imValueKey, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].sid != keys[j].sid {
			return keys[i].sid < keys[j].sid
		}
		return keys[i].ts < keys[j].ts
	})

	for _, k := range keys {
		rows := byKey[k]
		if len(rows) <= 1 {
			continue
		}
		res.KeysWithDuplicates++
		if sampleCap <= 0 || len(res.SamplesByVersion) < sampleCap {
			versions := make([]VersionWithPath, len(rows))
			for i := range rows {
				versions[i] = VersionWithPath{Version: rows[i].version}
			}
			res.SamplesByVersion = append(res.SamplesByVersion, AnalyzeKeyMulti{
				SeriesID:  common.SeriesID(k.sid),
				Timestamp: k.ts,
				Versions:  versions,
			})
		}
		distinctDigest := rows[0].digest
		conflict := false
		for i := 1; i < len(rows); i++ {
			if rows[i].digest != distinctDigest {
				conflict = true
				break
			}
		}
		if !conflict {
			continue
		}
		res.ValueConflictKeys++
		if sampleCap <= 0 || len(res.ValueConflicts) < sampleCap {
			vc := AnalyzeValueConflict{SeriesID: k.sid, Timestamp: k.ts}
			for i := range rows {
				vc.Versions = append(vc.Versions, rows[i].version)
				vc.Digests = append(vc.Digests, rows[i].digest)
			}
			res.ValueConflicts = append(res.ValueConflicts, vc)
		}
	}
	return res, nil
}
