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

// Package nativefmt is the contract between the files of a native artifact and the chunked
// sync protocol the data nodes receive them over: which files of a payload are sent, under
// which name a data node expects each of them, and how a part's metadata file becomes the
// part description the receiver places the part with. The names mirror the storage engines
// (banyand/stream, banyand/measure, banyand/trace and banyand/internal/sidx); this package
// must not import banyand/ so that bydbctl can use it too.
package nativefmt

import (
	"encoding/json"
	"fmt"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/bus"
)

// Kind is what one ImportParts payload carries.
type Kind = transferv1.ImportPartsRequest_Kind

// The payload kinds.
const (
	KindPart         = transferv1.ImportPartsRequest_KIND_PART
	KindSeriesIndex  = transferv1.ImportPartsRequest_KIND_SERIES_INDEX
	KindElementIndex = transferv1.ImportPartsRequest_KIND_ELEMENT_INDEX
	KindProperty     = transferv1.ImportPartsRequest_KIND_PROPERTY
)

// File names shared by the engines.
const (
	// CoreMetaFile is the metadata file of a core part.
	CoreMetaFile = "metadata.json"
	// SecondaryMetaFile is the manifest of a trace secondary index part.
	SecondaryMetaFile = "manifest.json"
	// SegmentMetaFile is the version file of a segment.
	SegmentMetaFile = "metadata"
	// SnapshotExt is the extension of a native index manifest and of a part list.
	SnapshotExt = ".snp"
	// SegDirPrefix prefixes the directory name of a segment.
	SegDirPrefix = "seg-"
	// ShardDirPrefix prefixes the directory name of a shard.
	ShardDirPrefix = "shard-"
	// SeriesIndexDir is the directory of a segment's series index.
	SeriesIndexDir = "sidx"
	// ElementIndexDir is the directory of a stream shard's element index.
	ElementIndexDir = "idx"
	// SecondaryIndexDir is the directory of a trace shard's secondary index parts.
	SecondaryIndexDir = "sidx"

	indexSegmentExt    = ".seg"
	seriesMetadataFile = "smeta.bin"
	tagTypeFile        = "tag.type"
	traceIDFilterFile  = "traceID.filter"
)

// MaxChunkSize is the most bytes one ImportParts chunk may carry. The liaison relays a chunk
// as one SyncPart frame, so it stays well below a data node's default 10 MiB receive limit
// once the file slices and the part descriptions are added.
const MaxChunkSize = 4 << 20

// catalogSpec is what a catalog accepts over ImportParts: its payload kinds, the wire names of
// its core part files, whether a part payload may carry secondary index parts, and whether its
// data lives in segments.
type catalogSpec struct {
	names          map[string]string
	prefixes       map[string]string
	kinds          []kindSpec
	secondaryParts bool
	segmented      bool
}

// kindSpec is one payload kind a catalog accepts: the chunked sync topic it is received on and
// whether it is duplicable (stored twice, it stores its rows or documents twice).
type kindSpec struct {
	topic      bus.Topic
	kind       Kind
	duplicable bool
}

// kind returns the spec of kind k, false when the catalog does not take it.
func (c catalogSpec) kind(k Kind) (kindSpec, bool) {
	for _, ks := range c.kinds {
		if ks.kind == k {
			return ks, true
		}
	}
	return kindSpec{}, false
}

// catalogs is the one table of the (catalog, kind) pairs a native import carries.
var catalogs = map[commonv1.Catalog]catalogSpec{
	commonv1.Catalog_CATALOG_STREAM: {
		kinds: []kindSpec{
			{kind: KindPart, duplicable: true, topic: data.TopicStreamPartSync},
			{kind: KindSeriesIndex, topic: data.TopicStreamSeriesSync},
			{kind: KindElementIndex, duplicable: true, topic: data.TopicStreamElementIndexSync},
		},
		names: streamNames, prefixes: streamPrefixes, segmented: true,
	},
	commonv1.Catalog_CATALOG_MEASURE: {
		kinds: []kindSpec{
			{kind: KindPart, topic: data.TopicMeasurePartSync},
			{kind: KindSeriesIndex, topic: data.TopicMeasureSeriesSync},
		},
		names: measureNames, prefixes: measurePrefixes, segmented: true,
	},
	commonv1.Catalog_CATALOG_TRACE: {
		kinds: []kindSpec{
			{kind: KindPart, duplicable: true, topic: data.TopicTracePartSync},
			{kind: KindSeriesIndex, topic: data.TopicTraceSeriesSync},
		},
		names: traceNames, prefixes: tracePrefixes, secondaryParts: true, segmented: true,
	},
	commonv1.Catalog_CATALOG_PROPERTY: {kinds: []kindSpec{{kind: KindProperty, topic: data.TopicPropertyPartSync}}},
}

// Supports returns an error unless catalog takes payloads of kind.
func Supports(catalog commonv1.Catalog, kind Kind) error {
	_, err := lookup(catalog, kind)
	return err
}

// Topic returns the chunked sync topic a payload of kind of catalog is received on.
func Topic(catalog commonv1.Catalog, kind Kind) (string, error) {
	ks, err := lookup(catalog, kind)
	if err != nil {
		return "", err
	}
	return ks.topic.String(), nil
}

// lookup returns the spec of kind in catalog, an error when the catalog does not take it.
func lookup(catalog commonv1.Catalog, kind Kind) (kindSpec, error) {
	spec, ok := catalogs[catalog]
	if !ok {
		return kindSpec{}, fmt.Errorf("catalog %s has no native payloads", catalog)
	}
	ks, ok := spec.kind(kind)
	if !ok {
		return kindSpec{}, fmt.Errorf("catalog %s has no %s payload", catalog, kind)
	}
	return ks, nil
}

// Duplicable tells whether a payload of kind stored twice in catalog stores its rows or
// documents twice, so it must not be pushed again once it may have been committed.
func Duplicable(catalog commonv1.Catalog, kind Kind) bool {
	ks, _ := catalogs[catalog].kind(kind)
	return ks.duplicable
}

// Segmented tells whether the data of catalog lives in segments, so its payloads carry the
// file format version of their source segment.
func Segmented(catalog commonv1.Catalog) bool {
	return catalogs[catalog].segmented
}

// ShardDir returns the directory name of shard id.
func ShardDir(id uint32) string {
	return ShardDirPrefix + strconv.FormatUint(uint64(id), 10)
}

// HasSecondaryParts tells whether a part payload of catalog may carry secondary index parts.
func HasSecondaryParts(catalog commonv1.Catalog) bool {
	return catalogs[catalog].secondaryParts
}

// ValidName tells whether name is one path component a receiver may create in its own
// directory: not empty, not "." or "..", and without a separator.
func ValidName(name string) bool {
	return name != "." && filepath.IsLocal(name) && filepath.Base(name) == name
}

// SyncFileName maps the on-disk name of one file of a payload to the name the receiving data
// node expects. skip reports a file that is not sent: the metadata of a part, which travels as
// the part description instead, the series metadata a receiver does not take, and the
// manifest of an index, whose segments are introduced one by one. An unknown file is an error:
// sending it would fail on the data node.
func SyncFileName(catalog commonv1.Catalog, kind Kind, partType, disk string) (name string, skip bool, err error) {
	if err = Supports(catalog, kind); err != nil {
		return "", false, err
	}
	if !ValidName(disk) {
		return "", false, fmt.Errorf("invalid file name %q", disk)
	}
	switch kind {
	case KindProperty:
		// The property receiver stages the index directory as it is, manifest included.
		return disk, false, nil
	case KindSeriesIndex, KindElementIndex:
		switch path.Ext(disk) {
		case indexSegmentExt:
			return disk, false, nil
		case SnapshotExt:
			return "", true, nil
		}
		return "", false, fmt.Errorf("unknown index file %q", disk)
	case KindPart:
		if disk == CoreMetaFile || disk == SecondaryMetaFile || disk == seriesMetadataFile {
			return "", true, nil
		}
		spec := catalogs[catalog]
		if partType == "" {
			return mapName(disk, spec.names, spec.prefixes)
		}
		if !spec.secondaryParts {
			return "", false, fmt.Errorf("catalog %s has no secondary index part %q", catalog, partType)
		}
		return mapName(disk, sidxNames, sidxPrefixes)
	}
	return "", false, fmt.Errorf("unknown payload kind %s", kind)
}

var (
	streamNames = map[string]string{
		"primary.bin": "primary", "meta.bin": "meta", "timestamps.bin": "timestamps", tagTypeFile: tagTypeFile,
	}
	streamPrefixes = map[string]string{".tf": "tf:", ".tfm": "tfm:", ".tff": "tff:"}
	measureNames   = map[string]string{
		"primary.bin": "primary", "meta.bin": "meta", "timestamps.bin": "timestamps", "fv.bin": "fv", tagTypeFile: tagTypeFile,
	}
	measurePrefixes = map[string]string{".tf": "tf:", ".tfm": "tfm:"}
	traceNames      = map[string]string{
		"primary.bin": "primary", "meta.bin": "meta", "spans.bin": "spans", tagTypeFile: tagTypeFile, traceIDFilterFile: traceIDFilterFile,
	}
	tracePrefixes = map[string]string{".t": "t:", ".tm": "tm:"}
	sidxNames     = map[string]string{"primary.bin": "primary", "data.bin": "data", "keys.bin": "keys", "meta.bin": "meta"}
	sidxPrefixes  = map[string]string{".td": "td:", ".tm": "tm:", ".tf": "tf:"}
)

// mapName maps a fixed file by its name and a column file "<column><ext>" to "<prefix><column>".
func mapName(disk string, names, prefixes map[string]string) (string, bool, error) {
	if n, ok := names[disk]; ok {
		return n, false, nil
	}
	ext := path.Ext(disk)
	if prefix, ok := prefixes[ext]; ok {
		if column := strings.TrimSuffix(disk, ext); column != "" {
			return prefix + column, false, nil
		}
	}
	return "", false, fmt.Errorf("unknown part file %q", disk)
}

// partMeta holds the fields of every engine's part metadata file a receiver needs.
type partMeta struct {
	MinTimestamp              *int64 `json:"minTimestamp"`
	MaxTimestamp              *int64 `json:"maxTimestamp"`
	CompressedSizeBytes       uint64 `json:"compressedSizeBytes"`
	UncompressedSizeBytes     uint64 `json:"uncompressedSizeBytes"`
	UncompressedSpanSizeBytes uint64 `json:"uncompressedSpanSizeBytes"`
	TotalCount                uint64 `json:"totalCount"`
	BlocksCount               uint64 `json:"blocksCount"`
	MinKey                    int64  `json:"minKey"`
	MaxKey                    int64  `json:"maxKey"`
}

// PartTypeAt returns the part type of part idx of a payload, "" for the core part or an index
// out of range.
func PartTypeAt(init *transferv1.ImportPartsRequest_Init, idx uint32) string {
	if parts := init.GetParts(); int(idx) < len(parts) {
		return parts[idx].GetPartType()
	}
	return ""
}

// ParsePartInfo builds the description of one part from its metadata file: the metadata.json
// of a core part, or the manifest.json of a trace secondary index part, whose time range
// falls back to that of its core part (core may be nil for a core part).
func ParsePartInfo(id uint64, partType string, meta []byte, core *transferv1.ImportPartsRequest_PartInfo) (*transferv1.ImportPartsRequest_PartInfo, error) {
	var pm partMeta
	if err := json.Unmarshal(meta, &pm); err != nil {
		return nil, fmt.Errorf("parse the metadata of part %016x %q: %w", id, partType, err)
	}
	info := &transferv1.ImportPartsRequest_PartInfo{
		Id:                    id,
		PartType:              partType,
		CompressedSizeBytes:   pm.CompressedSizeBytes,
		UncompressedSizeBytes: max(pm.UncompressedSizeBytes, pm.UncompressedSpanSizeBytes),
		TotalCount:            pm.TotalCount,
		BlocksCount:           pm.BlocksCount,
		MinKey:                pm.MinKey,
		MaxKey:                pm.MaxKey,
	}
	if pm.MinTimestamp != nil {
		info.MinTimestamp = *pm.MinTimestamp
	} else if core != nil {
		info.MinTimestamp = core.GetMinTimestamp()
	}
	if pm.MaxTimestamp != nil {
		info.MaxTimestamp = *pm.MaxTimestamp
	} else if core != nil {
		info.MaxTimestamp = core.GetMaxTimestamp()
	}
	if partType == "" && info.MinTimestamp <= 0 {
		return nil, fmt.Errorf("part %016x has no minimum timestamp", id)
	}
	return info, nil
}
