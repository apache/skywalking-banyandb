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

package schemasync

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"path/filepath"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
)

// newMessage returns an empty message of a schema kind.
func newMessage(kind string) (proto.Message, error) {
	switch kind {
	case KindGroup:
		return &commonv1.Group{}, nil
	case KindStream:
		return &databasev1.Stream{}, nil
	case KindMeasure:
		return &databasev1.Measure{}, nil
	case KindTrace:
		return &databasev1.Trace{}, nil
	case KindProperty:
		return &databasev1.Property{}, nil
	case KindIndexRule:
		return &databasev1.IndexRule{}, nil
	case KindIndexRuleBinding:
		return &databasev1.IndexRuleBinding{}, nil
	case KindTopNAggregation:
		return &databasev1.TopNAggregation{}, nil
	default:
		return nil, fmt.Errorf("unknown schema kind %q", kind)
	}
}

var (
	strictJSON  = protojson.UnmarshalOptions{}
	lenientJSON = protojson.UnmarshalOptions{DiscardUnknown: true}
)

// decodeLines decodes the JSON Lines of one schema file of kind, named rel in errors, and
// calls visit with every resource. Each line is decoded strictly first; when that fails it is
// decoded leniently and visit gets the strict error as unknownFields (nil otherwise). Blank
// lines are skipped.
func decodeLines(rel, kind string, data []byte, visit func(lineNo int, msg proto.Message, unknownFields error) error) error {
	if _, err := newMessage(kind); err != nil {
		return fmt.Errorf("schema file %s: %w", rel, err)
	}
	lineNo := 0
	for raw := range bytes.Lines(data) {
		lineNo++
		line := bytes.TrimSpace(raw)
		if len(line) == 0 {
			continue
		}
		msg, _ := newMessage(kind)
		strictErr := strictJSON.Unmarshal(line, msg)
		if strictErr != nil {
			if err := lenientJSON.Unmarshal(line, msg); err != nil {
				return fmt.Errorf("schema file %s line %d: %w", rel, lineNo, err)
			}
		}
		if err := visit(lineNo, msg, strictErr); err != nil {
			return err
		}
	}
	return nil
}

// Load rebuilds the Result of an Export from the schema files it wrote below dir/schema: the
// files with their sizes and CRC-32, the mod revisions, the fingerprints and the measures. A resumed export uses
// it instead of listing the registries again, so the schema it checks the new files against is
// the one the artifact holds.
func Load(dir string) (*Result, error) {
	res := &Result{revisions: map[resourceKey]int64{}, fingerprints: map[string]string{}}
	for _, kind := range Kinds {
		rel := path.Join(Dir, kind+".jsonl")
		data, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(rel)))
		if err != nil {
			return nil, fmt.Errorf("read schema file %s: %w", rel, err)
		}
		res.Files = append(res.Files, File{Path: rel, Bytes: uint64(len(data)), CRC32: crc32.ChecksumIEEE(data)})
		catalog := kindCatalog(kind)
		if err = decodeLines(rel, kind, data, func(_ int, msg proto.Message, _ error) error {
			res.record(kind, catalog, msg)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return res, nil
}

// kindCatalog is the catalog whose resources a kind holds, for the kinds Export records mod
// revisions of; UNSPECIFIED for the others.
func kindCatalog(kind string) commonv1.Catalog {
	switch kind {
	case KindStream:
		return commonv1.Catalog_CATALOG_STREAM
	case KindMeasure:
		return commonv1.Catalog_CATALOG_MEASURE
	case KindTrace:
		return commonv1.Catalog_CATALOG_TRACE
	case KindProperty:
		return commonv1.Catalog_CATALOG_PROPERTY
	default:
		return commonv1.Catalog_CATALOG_UNSPECIFIED
	}
}
