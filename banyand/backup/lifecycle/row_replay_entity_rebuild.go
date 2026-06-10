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

package lifecycle

import (
	"errors"
	"fmt"
	"strings"

	"github.com/apache/skywalking-banyandb/api/common"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumpmeasure "github.com/apache/skywalking-banyandb/banyand/internal/dump/measure"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
)

// skipReason classifies why a series was dropped during row-replay.
type skipReason string

const (
	// skipReasonRebuildFailed: the sidx returned no EntityValues for the seriesID
	// (a series-index gap) and, though the part's entity columns were present, no
	// candidate's recomputed hash matched — so the entity could not be rebuilt.
	skipReasonRebuildFailed skipReason = "rebuild-failed"
	// skipReasonIncompletePart: the part carries no entity tag columns at all, so
	// neither sidx nor column rebuild can recover the entity (S5:残缺 part).
	skipReasonIncompletePart skipReason = "incomplete-part"
)

// skipError locates a skipped series so the migration report can point an
// operator at the exact source part rather than only a count. It wraps
// errSkipSeries so callers keep using errors.Is(err, errSkipSeries) to detect a
// skip, while errors.As recovers the location for the report.
type skipError struct {
	partPath string
	reason   skipReason
	seriesID uint64
}

func (c *skipError) Error() string {
	return fmt.Sprintf("%s: part=%s seriesID=%d reason=%s", errSkipSeries.Error(), c.partPath, c.seriesID, c.reason)
}

// Unwrap lets errors.Is(err, errSkipSeries) match a skipError.
func (c *skipError) Unwrap() error { return errSkipSeries }

// asSkipError extracts the locating skipError from a skip error, or nil when
// the error carries no location (a bare errSkipSeries).
func asSkipError(err error) *skipError {
	var c *skipError
	if errors.As(err, &c) {
		return c
	}
	return nil
}

// rebuildCandidate describes one measure whose entity can be rebuilt from part
// columns: its subject (measure name) and the ordered "family.tag" column keys
// of its entity tags, with each entity tag's schema value type as the decode
// fallback when the block omits a per-column type.
type rebuildCandidate struct {
	subject    string
	entityCols []string
	colTypes   []pbv1.ValueType
}

// entityRebuildIndex groups rebuild candidates by the signature of their entity
// column keys. Measures that share an entity layout (the common case for
// SkyWalking metrics) land in one bucket, so a rebuild usually probes a single
// bucket.
type entityRebuildIndex struct {
	bySignature map[string][]rebuildCandidate
}

// buildEntityRebuildIndex precomputes, once per replayer, the entity-column
// layout of every measure whose entity tags all live in column-backed tag
// families. A measure whose entity tag is index-only (not column-backed) cannot
// be rebuilt from columns and is omitted (it falls back to the skip path).
func buildEntityRebuildIndex(schemas map[string]*databasev1.Measure) *entityRebuildIndex {
	idx := &entityRebuildIndex{bySignature: make(map[string][]rebuildCandidate)}
	for name, m := range schemas {
		tagNames := m.GetEntity().GetTagNames()
		if len(tagNames) == 0 {
			continue
		}
		families := m.GetTagFamilies()
		cols := make([]string, 0, len(tagNames))
		types := make([]pbv1.ValueType, 0, len(tagNames))
		ok := true
		for _, tn := range tagNames {
			fi, _, tag := pbv1.FindTagByName(families, tn)
			if tag == nil {
				ok = false
				break
			}
			cols = append(cols, families[fi].GetName()+"."+tn)
			types = append(types, pbv1.TagValueSpecToValueType(tag.GetType()))
		}
		if !ok {
			continue
		}
		sig := strings.Join(cols, "\x00")
		idx.bySignature[sig] = append(idx.bySignature[sig], rebuildCandidate{subject: name, entityCols: cols, colTypes: types})
	}
	return idx
}

// columnLookup reads the entity tag value (raw bytes + value type) for a column
// key from a block/row view. ok is false when the column is absent. The columnar
// and row replay paths each supply one of these so rebuildEntity serves both.
type columnLookup func(col string) (raw []byte, vt pbv1.ValueType, ok bool)

// columnarBlockLookup adapts a ColumnarBlock to columnLookup. Every row of a
// block shares one series, so the entity value is read from row 0.
func columnarBlockLookup(cb *dumpmeasure.ColumnarBlock) columnLookup {
	return func(col string) ([]byte, pbv1.ValueType, bool) {
		vals, ok := cb.TagCols[col]
		if !ok || len(vals) == 0 {
			return nil, pbv1.ValueTypeUnknown, false
		}
		return vals[0], cb.TagTypes[col], true
	}
}

// rowLookup adapts a decoded measure Row to columnLookup for the row-based
// replay path.
func rowLookup(row dumpmeasure.Row) columnLookup {
	return func(col string) ([]byte, pbv1.ValueType, bool) {
		raw, ok := row.Tags[col]
		if !ok {
			return nil, pbv1.ValueTypeUnknown, false
		}
		return raw, row.TagTypes[col], true
	}
}

// decodeOrRebuildEntity resolves (subject, entityValues) for a block/row: first
// from the sidx-provided EntityValues bytes, then — on a sidx gap — by rebuilding
// from the part's own entity tag columns (S4). When neither works it returns
// ok=false plus the reason classifying the failure (incomplete-part when the part
// lacked entity columns, else rebuild-failed), leaving the caller to attach its
// own source location. Shared by the columnar (ensureBlock) and row
// (buildWriteRequest) paths so the fallback logic lives in one place.
func (r *measureRowReplayer) decodeOrRebuildEntity(
	seriesID common.SeriesID, rawEntity []byte, lookup columnLookup,
) (subject string, ev pbv1.EntityValues, reason skipReason, ok bool) {
	if subject, ev, err := decodeSeriesEntityValues(rawEntity); err == nil {
		return subject, ev, "", true
	}
	sub, rebuilt, hadEntityCols, rebuiltOK := r.rebuildEntity(seriesID, lookup)
	if rebuiltOK {
		return sub, rebuilt, "", true
	}
	if hadEntityCols {
		return "", nil, skipReasonRebuildFailed, false
	}
	return "", nil, skipReasonIncompletePart, false
}

// rebuildEntity recovers (subject, entityValues) for a seriesID whose entity the
// sidx could not resolve, by reading the entity tag values straight from the
// part's own columns and rehashing them with the exact write-side function
// (pbv1.Series.Marshal). A candidate is accepted only when its recomputed
// SeriesID equals the part's seriesID, so a false match is impossible: the hash
// equality is the proof the reconstruction is byte-exact. It is called once per
// block (one series), reusing the replayer's Series/EntityValues buffers so the
// hot path allocates only the returned copy. hadEntityCols reports whether any
// candidate's entity columns were present, distinguishing a rebuild miss
// (columns present, no hash match) from an incomplete part (no entity columns).
func (r *measureRowReplayer) rebuildEntity(seriesID common.SeriesID, lookup columnLookup) (subject string, ev pbv1.EntityValues, hadEntityCols, ok bool) {
	if r.rebuildIdx == nil {
		return "", nil, false, false
	}
	s := &r.rebuildSeries
	for _, cands := range r.rebuildIdx.bySignature {
		for ci := range cands {
			c := &cands[ci]
			values := r.rebuildEV[:0]
			complete := true
			for k, col := range c.entityCols {
				raw, vt, has := lookup(col)
				if !has {
					complete = false
					break
				}
				hadEntityCols = true
				if vt == pbv1.ValueTypeUnknown {
					vt = c.colTypes[k]
				}
				values = append(values, dump.DecodeTagValue(vt, raw, nil))
			}
			r.rebuildEV = values
			if !complete {
				continue
			}
			s.Subject = c.subject
			s.EntityValues = values
			s.Buffer = s.Buffer[:0]
			if s.Marshal() == nil && s.ID == seriesID {
				return c.subject, append(pbv1.EntityValues(nil), values...), true, true
			}
		}
	}
	return "", nil, hadEntityCols, false
}
