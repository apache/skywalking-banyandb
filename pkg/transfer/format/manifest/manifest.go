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

package manifest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ArtifactVersion is the artifact layout version written by this package.
const ArtifactVersion = 1

// FileName is the manifest file name at the artifact root.
const FileName = "manifest.json"

// Manifest is the content of manifest.json.
type Manifest struct {
	Format          string   `json:"format"`
	Compress        string   `json:"compress"`
	ExportedAt      string   `json:"exportedAt"`
	Source          Source   `json:"source"`
	Groups          []Group  `json:"groups"`
	Units           []Unit   `json:"units"`
	SchemaFiles     []File   `json:"schemaFiles"`
	Warnings        []string `json:"warnings"`
	ArtifactVersion int      `json:"artifactVersion"`
	SchemaOnly      bool     `json:"schemaOnly,omitempty"`
}

// Source describes the exported cluster.
type Source struct {
	Nodes []Node `json:"nodes"`
}

// Node pairs a source node identity with its artifact directory name.
type Node struct {
	// ID is the raw node id, e.g. "10.0.0.5:17912".
	ID string `json:"id"`
	// Dir is the directory under nodes/, derived by NodeDir.
	Dir string `json:"dir"`
	// Timezone is the IANA time zone of the node at export time.
	Timezone string `json:"timezone"`
}

// Group describes an exported group.
type Group struct {
	Name      string  `json:"name"`
	Catalog   string  `json:"catalog"`
	Stages    []Stage `json:"stages"`
	IndexMode bool    `json:"indexMode"`
}

// Stage describes one lifecycle stage of a group.
type Stage struct {
	Name            string `json:"name"`
	SegmentInterval string `json:"segmentInterval"`
	TTL             string `json:"ttl"`
	ShardNum        uint32 `json:"shardNum"`
	Replicas        uint32 `json:"replicas"`
}

// Unit is one exported unit: (catalog, group, stage, segment) with its sources.
type Unit struct {
	Catalog        string       `json:"catalog"`
	Group          string       `json:"group"`
	Stage          string       `json:"stage"`
	Segment        string       `json:"segment,omitempty"`
	SegmentVersion string       `json:"segmentVersion,omitempty"`
	Sources        []UnitSource `json:"sources"`
	MinTimestamp   int64        `json:"minTimestamp"`
	MaxTimestamp   int64        `json:"maxTimestamp"`
}

// UnitSource is the contribution of one source node to a unit. RowCounts and TotalCount count
// the rows of SegmentFiles; the rows of a shard's files are counted on the shard.
type UnitSource struct {
	RowCounts    map[string]uint64 `json:"rowCounts,omitempty"`
	Node         string            `json:"node"`
	SegmentFiles []File            `json:"segmentFiles,omitempty"`
	Shards       []Shard           `json:"shards"`
	TotalCount   uint64            `json:"totalCount,omitempty"`
}

// Shard is the contribution of one source node to one shard of a unit.
type Shard struct {
	RowCounts                  map[string]uint64 `json:"rowCounts"`
	Parts                      []Part            `json:"parts"`
	Files                      []File            `json:"files"`
	MinTimestamp               int64             `json:"minTimestamp"`
	MaxTimestamp               int64             `json:"maxTimestamp"`
	TotalCount                 uint64            `json:"totalCount"`
	DocCount                   uint64            `json:"docCount,omitempty"`
	EstimatedCompressedBytes   uint64            `json:"estimatedCompressedBytes"`
	EstimatedUncompressedBytes uint64            `json:"estimatedUncompressedBytes"`
	Shard                      uint32            `json:"shard"`
}

// Part describes one source part of a shard.
type Part struct {
	ID           uint64 `json:"id"`
	MinTimestamp int64  `json:"minTimestamp"`
	MaxTimestamp int64  `json:"maxTimestamp"`
	TotalCount   uint64 `json:"totalCount"`
}

// File registers one artifact file. Path is relative to the artifact root.
type File struct {
	Path       string `json:"path"`
	CRC32      string `json:"crc32"`
	Bytes      uint64 `json:"bytes"`
	EntryCount uint32 `json:"entryCount,omitempty"`
}

// NodeDir derives the artifact directory name of a node id: lower-case it, collapse runs
// of bytes outside [a-z0-9] into '-', trim '-', and fall back to "node" when empty. The
// mapping is lossy, so the exporter refuses two node ids that derive the same name.
func NodeDir(nodeID string) string {
	lower := strings.ToLower(nodeID)
	b := make([]byte, 0, len(lower))
	for i := 0; i < len(lower); i++ {
		c := lower[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' {
			b = append(b, c)
			continue
		}
		if len(b) > 0 && b[len(b)-1] != '-' {
			b = append(b, '-')
		}
	}
	dir := strings.Trim(string(b), "-")
	if dir == "" {
		return "node"
	}
	return dir
}

// FormatCRC32 renders a CRC-32 as in the manifest, e.g. "0x1f4a03bb".
func FormatCRC32(v uint32) string {
	return fmt.Sprintf("0x%08x", v)
}

// Write atomically writes m as indented JSON to path with mode 0600: it writes a temp file
// in the same directory, fsyncs it, renames it over path and fsyncs the directory.
// Nil top-level slices are written as empty arrays.
func Write(path string, m *Manifest) (err error) {
	out := *m
	if out.Groups == nil {
		out.Groups = []Group{}
	}
	if out.Units == nil {
		out.Units = []Unit{}
	}
	if out.SchemaFiles == nil {
		out.SchemaFiles = []File{}
	}
	if out.Warnings == nil {
		out.Warnings = []string{}
	}
	if out.Source.Nodes == nil {
		out.Source.Nodes = []Node{}
	}
	data, err := json.MarshalIndent(&out, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return fmt.Errorf("create temp manifest: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		if err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmpName)
		}
	}()
	if err = tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("chmod temp manifest: %w", err)
	}
	if _, err = tmp.Write(data); err != nil {
		return fmt.Errorf("write temp manifest: %w", err)
	}
	if err = tmp.Sync(); err != nil {
		return fmt.Errorf("sync temp manifest: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("close temp manifest: %w", err)
	}
	if err = os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename manifest: %w", err)
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open manifest dir: %w", err)
	}
	defer d.Close()
	if err = d.Sync(); err != nil {
		return fmt.Errorf("sync manifest dir: %w", err)
	}
	return nil
}

// Read loads a manifest written by Write.
func Read(path string) (*Manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}
	m := &Manifest{}
	if err = json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("parse manifest %s: %w", path, err)
	}
	return m, nil
}
