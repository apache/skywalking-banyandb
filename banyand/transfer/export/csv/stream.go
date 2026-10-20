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

package csv

import (
	"bytes"
	"fmt"
	"path/filepath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/dump"
	dumpstream "github.com/apache/skywalking-banyandb/banyand/internal/dump/stream"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// entityColumn binds one entity tag of a resource to its layout column.
type entityColumn struct {
	index int
	ok    bool
}

// entityColumns maps each Entity.TagNames position to the layout column of that tag.
func entityColumns(layout *csvfmt.Layout, resource string, entity *databasev1.Entity, families []*databasev1.TagFamilySpec) []entityColumn {
	out := make([]entityColumn, len(entity.GetTagNames()))
	for i, name := range entity.GetTagNames() {
		for _, fam := range families {
			for _, t := range fam.GetTags() {
				if t.GetName() != name || out[i].ok {
					continue
				}
				out[i].index, out[i].ok = layout.TagIndex(resource, fam.GetName(), name)
			}
		}
	}
	return out
}

// tagColumns maps "family.tag" of every tag of a resource to its layout column.
func tagColumns(layout *csvfmt.Layout, resource string, families []*databasev1.TagFamilySpec) map[string]int {
	out := map[string]int{}
	for _, fam := range families {
		for _, t := range fam.GetTags() {
			if idx, ok := layout.TagIndex(resource, fam.GetName(), t.GetName()); ok {
				out[fam.GetName()+"."+t.GetName()] = idx
			}
		}
	}
	return out
}

// seriesDecoder decodes the EntityValues of a row (pbv1.Series: subject, then the entity
// tag values) and remembers the last one, since rows of a block share it.
type seriesDecoder struct {
	err     error
	subject string
	last    []byte
	values  []*modelv1.TagValue
}

func (d *seriesDecoder) decode(entityValues []byte) (string, []*modelv1.TagValue, error) {
	if d.last != nil && bytes.Equal(d.last, entityValues) {
		return d.subject, d.values, d.err
	}
	d.last = append(d.last[:0], entityValues...)
	var s pbv1.Series
	d.err = s.Unmarshal(entityValues)
	d.subject, d.values = s.Subject, s.EntityValues
	return d.subject, d.values, d.err
}

// subjectOf returns the resource of a row from its EntityValues, counting the rows whose
// resource cannot be told.
func subjectOf(run *unitRun, d *seriesDecoder, catalog string, entityValues []byte) (string, []*modelv1.TagValue, bool) {
	if len(entityValues) == 0 {
		run.dropRow(catalog + " rows without series metadata")
		return "", nil, false
	}
	subject, values, err := d.decode(entityValues)
	if err != nil || subject == "" {
		run.dropRow(catalog + " rows with undecodable series metadata")
		return "", nil, false
	}
	return subject, values, true
}

// setIfNull fills a cell that no earlier source filled.
func setIfNull(cells []string, idx int, value string) {
	if cells[idx] == csvfmt.NullCell {
		cells[idx] = value
	}
}

type streamResource struct {
	tags   map[string]int
	entity []entityColumn
}

func produceStreamCSV(run *unitRun, unit *transferv1.SegmentUnit, segDir string) error {
	group := unit.GetGroup()
	streams, err := run.meta.StreamRegistry().ListStream(run.ctx, schema.ListOpt{Group: group})
	if err != nil {
		return status.Errorf(codes.Internal, "list streams of group %s: %v", group, err)
	}
	layout, err := csvfmt.StreamLayout(streams)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "stream group %s cannot be exported as CSV: %v", group, err)
	}
	resources := make(map[string]*streamResource, len(streams))
	revisions := make(map[string]int64, len(streams))
	for _, s := range streams {
		name := s.GetMetadata().GetName()
		resources[name] = &streamResource{
			tags:   tagColumns(layout, name, s.GetTagFamilies()),
			entity: entityColumns(layout, name, s.GetEntity(), s.GetTagFamilies()),
		}
		revisions[name] = s.GetMetadata().GetModRevision()
	}
	// The series index only backs EntityValues for parts without smeta.bin.
	resolver, err := dump.NewIndexResolver(segDir, dump.DefaultIndexCacheSize, nil)
	if err != nil {
		return readError("open the series index of "+segDir, err)
	}
	defer resolver.Close()
	fixed := fixedColumns(layout)
	for i, shard := range unit.GetShardIds() {
		fw := run.newWriter(layout, shardPrefix(unit, shard), shard, uint32(i), false, revisions)
		shardDir := filepath.Join(segDir, fmt.Sprintf("%s%d", shardDirPrefix, shard))
		if err = writeStreamShard(run, fw, shardDir, resolver, layout, fixed, resources); err != nil {
			return err
		}
		if err = fw.close(); err != nil {
			return readError("finish "+shardDir, err)
		}
	}
	return nil
}

func writeStreamShard(run *unitRun, fw *fileWriter, shardDir string, resolver *dump.IndexResolver, layout *csvfmt.Layout,
	fixed map[csvfmt.ColumnKind]int, resources map[string]*streamResource,
) error {
	partIDs, err := LivePartIDs(shardDir)
	if err != nil {
		return readError("list parts of "+shardDir, err)
	}
	cells := layout.NewRow()
	var series seriesDecoder
	for _, id := range partIDs {
		rofs := newReadOnlyFS()
		part, openErr := dumpstream.OpenPart(id, shardDir, rofs)
		if openErr == nil && rofs.err != nil {
			_ = part.Close()
			openErr = rofs.err
		}
		if openErr != nil {
			return readError(fmt.Sprintf("open part %016x of %s", id, shardDir), openErr)
		}
		part.SetIndexResolver(resolver)
		part.SetReuseBuffers(true)
		it := part.Iterator()
		for it.Next() {
			row := it.Row()
			subject, entityValues, ok := subjectOf(run, &series, "stream", row.EntityValues)
			if !ok {
				continue
			}
			res, known := resources[subject]
			if !known {
				run.dropRow("stream " + subject + " is not in the schema registry")
				continue
			}
			resetRow(cells)
			cells[fixed[csvfmt.KindElementID]] = csvfmt.EncodeElementID(row.ElementID)
			cells[fixed[csvfmt.KindTimestamp]] = csvfmt.EncodeTimestamp(row.Timestamp)
			setStoredTags(cells, layout, row.Tags, row.TagTypes, res.tags, run, subject)
			fillEntity(cells, res.entity, entityValues)
			if err = fw.writeRow(subject, cells); err != nil {
				_ = it.Close()
				_ = part.Close()
				return readError("write "+subject+" row", err)
			}
		}
		err = it.Err()
		_ = it.Close()
		_ = part.Close()
		if err != nil {
			return readError(fmt.Sprintf("read part %016x of %s", id, shardDir), err)
		}
	}
	return nil
}

// fillEntity places the entity tag values decoded from the series into their columns,
// unless a stored value already filled them.
func fillEntity(cells []string, cols []entityColumn, values []*modelv1.TagValue) {
	for i, col := range cols {
		if !col.ok || i >= len(values) {
			continue
		}
		setIfNull(cells, col.index, csvfmt.EncodeTagValue(values[i]))
	}
}
