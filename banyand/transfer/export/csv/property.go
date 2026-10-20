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
	"errors"
	"fmt"
	"path/filepath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/index/native"
	"github.com/apache/skywalking-banyandb/pkg/transfer"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/csvfmt"
)

// Stored fields of a property document (banyand/property/db/shard.go buildUpdateDocument).
const (
	propertySourceField = "_source"
	propertyDeleteField = "_deleted"
)

func producePropertyCSV(run *unitRun, unit *transferv1.PropertyUnit, groupDir string) error {
	group := unit.GetGroup()
	props, err := run.meta.PropertyRegistry().ListProperty(run.ctx, schema.ListOpt{Group: group})
	if err != nil {
		return status.Errorf(codes.Internal, "list properties of group %s: %v", group, err)
	}
	layout, err := csvfmt.PropertyLayout(props)
	if err != nil {
		return status.Errorf(codes.FailedPrecondition, "property group %s cannot be exported as CSV: %v", group, err)
	}
	resources := make(map[string]map[string]int, len(props))
	revisions := make(map[string]int64, len(props))
	for _, p := range props {
		name := p.GetMetadata().GetName()
		tags := map[string]int{}
		for _, t := range p.GetTags() {
			if idx, ok := layout.TagIndex(name, "", t.GetName()); ok {
				tags[t.GetName()] = idx
			}
		}
		resources[name] = tags
		revisions[name] = p.GetMetadata().GetModRevision()
	}
	fixed := fixedColumns(layout)
	for i, shard := range unit.GetShardIds() {
		shardDir := filepath.Join(groupDir, fmt.Sprintf("%s%d", shardDirPrefix, shard))
		prefix := fmt.Sprintf("%s/%s/%s%d", transfer.CatalogName(commonv1.Catalog_CATALOG_PROPERTY), csvfmt.EscapePathComponent(group), shardDirPrefix, shard)
		fw := run.newWriter(layout, prefix, shard, uint32(i), false, revisions)
		if err = writePropertyShard(run, fw, shardDir, layout, fixed, resources); err != nil {
			return err
		}
		if err = fw.close(); err != nil {
			return readError("finish "+shardDir, err)
		}
	}
	return nil
}

// writePropertyShard streams every live document of the shard's index, tombstones
// included: a tombstone carries _deleted and must reach the target, or the deleted
// property would come back to life there.
func writePropertyShard(run *unitRun, fw *fileWriter, shardDir string, layout *csvfmt.Layout, fixed map[csvfmt.ColumnKind]int,
	resources map[string]map[string]int,
) error {
	generation, err := native.OpenReadOnlyGeneration(shardDir)
	if err != nil {
		if errors.Is(err, native.ErrNoSnapshot) {
			return nil // a shard that never committed holds no property
		}
		return readError("open the property index of "+shardDir, err)
	}
	defer func() { _ = generation.Close() }()
	pw := &propertyShardWriter{run: run, fw: fw, layout: layout, shardDir: shardDir, fixed: fixed, resources: resources, cells: layout.NewRow()}
	err = generation.VisitLiveDocuments(run.ctx, pw.write)
	return readError("read properties of "+shardDir, err)
}

// propertyShardWriter turns the stored documents of one shard into CSV rows.
type propertyShardWriter struct {
	run       *unitRun
	fw        *fileWriter
	layout    *csvfmt.Layout
	fixed     map[csvfmt.ColumnKind]int
	resources map[string]map[string]int
	shardDir  string
	cells     []string
	source    []byte
}

// write appends the row of one stored document. A _deleted value that is not an 8-byte
// timestamp is index corruption: the unit fails instead of guessing whether it is a tombstone.
func (pw *propertyShardWriter) write(doc native.StoredDocument) error {
	pw.source = pw.source[:0]
	var deleteTime int64
	corruptLen := -1
	if err := doc.VisitStoredFields(func(name string, value []byte) bool {
		switch name {
		case propertySourceField:
			pw.source = append(pw.source, value...)
		case propertyDeleteField:
			if len(value) != 8 {
				corruptLen = len(value)
				return true
			}
			deleteTime = convert.BytesToInt64(value)
		}
		return true
	}); err != nil {
		return err
	}
	var p propertyv1.Property
	malformed := len(pw.source) > 0 && protojson.Unmarshal(pw.source, &p) != nil
	if corruptLen >= 0 {
		document := "a document without a readable _source"
		if len(pw.source) > 0 && !malformed {
			document = fmt.Sprintf("property %s/%s", p.GetMetadata().GetName(), p.GetId())
		}
		return status.Errorf(codes.Internal, "shard %s: %s has a corrupt _deleted value of %d bytes (want 8): the index is corrupt",
			pw.shardDir, document, corruptLen)
	}
	if len(pw.source) == 0 {
		return nil // not a property document
	}
	if malformed {
		pw.run.dropRow("property documents with a malformed _source")
		return nil
	}
	name := p.GetMetadata().GetName()
	tags, known := pw.resources[name]
	if !known {
		pw.run.dropRow("property " + name + " is not in the schema registry")
		return nil
	}
	cells := pw.cells
	resetRow(cells)
	cells[pw.fixed[csvfmt.KindPropertyID]] = csvfmt.EncodeString(p.GetId())
	cells[pw.fixed[csvfmt.KindModRevision]] = csvfmt.EncodeInt(p.GetMetadata().GetModRevision())
	cells[pw.fixed[csvfmt.KindCreateRevision]] = csvfmt.EncodeInt(p.GetMetadata().GetCreateRevision())
	if p.GetUpdatedAt() != nil {
		cells[pw.fixed[csvfmt.KindUpdatedAt]] = csvfmt.EncodeTimestamp(p.GetUpdatedAt().AsTime().UnixNano())
	}
	cells[pw.fixed[csvfmt.KindDeleteTime]] = csvfmt.EncodeTimestamp(deleteTime)
	for _, t := range p.GetTags() {
		idx, declared := tags[t.GetKey()]
		if !declared || !csvfmt.FitsColumn(t.GetValue(), pw.layout.Columns[idx].Type) {
			pw.run.dropTag(name, t.GetKey())
			continue
		}
		cells[idx] = csvfmt.EncodeTagValue(t.GetValue())
	}
	return pw.fw.writeRow(name, cells)
}
