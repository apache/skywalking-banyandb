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

package main

import (
	"context"
	"fmt"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/transfer/format/bnc"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/nativefmt"
)

// runNative validates a native export artifact:
//
//	checker native <artifact dir>
//
// It prints the format, the compression, the catalogs the units cover and one line per check: every
// listed file on disk with its size and CRC32 and nothing else, every unit's container volumes
// opening as one volume set whose members verify, every shard container splitting into whole
// parts (no orphan) and holding at least one, every segment unit carrying its segment metadata
// file, and no manifest warning. The exit code is 1 when a check failed.
func runNative(args []string) int {
	if len(args) != 1 {
		usage()
	}
	dir := args[0]
	m, err := manifest.Read(filepath.Join(dir, manifest.FileName))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		fmt.Println("manifest: missing")
		return 1
	}
	fmt.Printf("format: %s\n", m.Format)
	fmt.Printf("compress: %s\n", m.Compress)
	var catalogs []string
	var files []manifest.File
	files = append(files, m.SchemaFiles...)
	var containers, parts, metadata []string
	for _, u := range m.Units {
		catalogs = append(catalogs, strings.ToLower(u.Catalog))
		for _, src := range u.Sources {
			files = append(files, src.SegmentFiles...)
			if u.Segment != "" && !slices.ContainsFunc(src.SegmentFiles, func(f manifest.File) bool { return path.Base(f.Path) == nativefmt.SegmentMetaFile }) {
				metadata = append(metadata, fmt.Sprintf("%s/%s/%s on %s has no segment metadata", u.Catalog, u.Group, u.Segment, src.Node))
			}
			verify, _ := containerProblems(dir, src.SegmentFiles, false)
			containers = append(containers, verify...)
			for _, sh := range src.Shards {
				files = append(files, sh.Files...)
				shardVerify, shardParts := containerProblems(dir, sh.Files, u.Segment != "")
				containers, parts = append(containers, shardVerify...), append(parts, shardParts...)
			}
		}
	}
	slices.Sort(catalogs)
	fmt.Printf("catalogs: [%s]\n", strings.Join(slices.Compact(catalogs), ", "))
	var r report
	var formatProblems []string
	if m.Format != manifest.FormatNative || m.Compress != manifest.CompressNone {
		formatProblems = append(formatProblems, fmt.Sprintf("format %q compress %q, want native and none", m.Format, m.Compress))
	}
	r.check("formatNative", formatProblems)
	r.check("filesMatchManifest", fileProblems(dir, files))
	r.check("containersVerify", containers)
	r.check("shardsHoldWholeParts", parts)
	r.check("segmentMetadata", metadata)
	r.check("noWarnings", m.Warnings)
	fmt.Println("checks:")
	r.print("  ")
	if failed {
		return 1
	}
	return 0
}

// containerProblems opens every container among files (its volumes "<prefix>-NNNNN.bnc") as a
// volume set and verifies it; for a shard container of a segment unit it also checks that the
// container splits into whole parts and holds at least one. It returns the open, verify and
// close problems apart from the part problems.
func containerProblems(dir string, files []manifest.File, shard bool) (verify, parts []string) {
	rels := make([]string, len(files))
	for i, f := range files {
		rels[i] = f.Path
	}
	byPrefix := bnc.GroupVolumes(rels)
	for _, prefix := range slices.Sorted(maps.Keys(byPrefix)) {
		vols := make([]string, len(byPrefix[prefix]))
		for i, rel := range byPrefix[prefix] {
			vols[i] = filepath.Join(dir, filepath.FromSlash(rel))
		}
		vs, err := bnc.OpenVolumes(vols)
		if err != nil {
			verify = append(verify, fmt.Sprintf("%s: open: %v", prefix, err))
			continue
		}
		if _, err = vs.Verify(context.Background()); err != nil {
			verify = append(verify, fmt.Sprintf("%s: verify: %v", prefix, err))
		}
		isShard := shard && strings.HasPrefix(path.Base(prefix), nativefmt.ShardDirPrefix)
		if isShard {
			layout := vs.Shard()
			if len(layout.Orphans) > 0 {
				parts = append(parts, fmt.Sprintf("%s: %d orphan file(s), first %s", prefix, len(layout.Orphans), layout.Orphans[0].Path))
			}
			if len(layout.Parts) == 0 {
				parts = append(parts, fmt.Sprintf("%s: no part", prefix))
			}
		}
		if err = vs.Close(); err != nil {
			verify = append(verify, fmt.Sprintf("%s: close: %v", prefix, err))
		}
	}
	return verify, parts
}
