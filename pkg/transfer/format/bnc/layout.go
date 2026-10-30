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

package bnc

import (
	"cmp"
	"maps"
	"slices"
	"strconv"
	"strings"
)

// Kind classifies a member of a shard container (a stream, measure or trace shard directory).
type Kind uint8

const (
	// KindOther is any member the other kinds do not cover.
	KindOther Kind = iota
	// KindPart is a file of a part: "<016x>/<file>" for a core part, or
	// "sidx/<index>/<016x>/<file>" for a trace secondary index part.
	KindPart
	// KindSnapshot is a part visibility list "<016x>.snp" at the shard root.
	KindSnapshot
	// KindElementIndex is a file of the stream element index "idx/".
	KindElementIndex
)

const (
	partIDLen          = 16
	snapshotExt        = ".snp"
	elementIndexDir    = "idx"
	secondaryIndexDir  = "sidx"
	coreMetaFile       = "metadata.json"
	secondaryIndexMeta = "manifest.json"
)

// Class is the classification of a shard container member.
type Class struct {
	// PartType is "" for a core part and the index name for a trace secondary index part.
	PartType string
	PartID   uint64
	Kind     Kind
}

// ClassifyShardPath classifies a member path of a shard container. Part IDs must be written as
// the engine writes them: sixteen lower-case hexadecimal digits.
func ClassifyShardPath(p string) Class {
	parts := strings.Split(p, "/")
	switch {
	case len(parts) >= 4 && parts[0] == secondaryIndexDir:
		if id, ok := parsePartID(parts[2]); ok && parts[1] != "" {
			return Class{Kind: KindPart, PartID: id, PartType: parts[1]}
		}
	case len(parts) >= 2 && parts[0] == elementIndexDir:
		return Class{Kind: KindElementIndex}
	case len(parts) >= 2:
		if id, ok := parsePartID(parts[0]); ok {
			return Class{Kind: KindPart, PartID: id}
		}
	case len(parts) == 1 && strings.HasSuffix(p, snapshotExt):
		if _, ok := parsePartID(strings.TrimSuffix(p, snapshotExt)); ok {
			return Class{Kind: KindSnapshot}
		}
	}
	return Class{Kind: KindOther}
}

func parsePartID(s string) (uint64, bool) {
	if len(s) != partIDLen {
		return 0, false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return 0, false
		}
	}
	id, err := strconv.ParseUint(s, 16, 64)
	return id, err == nil
}

// OrderShard returns the members of a stream, measure or trace shard directory in write order:
// part by part in ascending ID (core files, then each secondary index part by index name,
// files by path), the .snp files, the idx/ tree (segment files before .snp files), then every
// other file. Members are kept as given: the engine snapshot already leaves out runtime files.
func OrderShard(files []Member) []Member {
	b := bucketShard(files, memberPath)
	out := make([]Member, 0, len(files))
	for _, p := range b.parts {
		for _, t := range p.types {
			sortByPath(t.items, memberPath)
			out = append(out, t.items...)
		}
	}
	sortByPath(b.snapshots, memberPath)
	out = append(out, b.snapshots...)
	sortNative(b.elementIndex, memberPath)
	out = append(out, b.elementIndex...)
	sortByPath(b.others, memberPath)
	return append(out, b.others...)
}

// OrderIndexDir returns every file of a native index directory (a segment's sidx/ or a property
// shard) in write order, segment files before .snp manifests, so a manifest never precedes the
// segments it names.
func OrderIndexDir(files []Member) []Member {
	out := slices.Clone(files)
	sortNative(out, memberPath)
	return out
}

// shardBuckets holds the members of a shard container by kind, parts by ascending ID.
type shardBuckets[T any] struct {
	parts        []partBucket[T]
	snapshots    []T
	elementIndex []T
	others       []T
}

// partBucket holds the files of one part ID by part type, the core part ("") first.
type partBucket[T any] struct {
	types []typeBucket[T]
	id    uint64
}

type typeBucket[T any] struct {
	partType string
	items    []T
}

// bucketShard classifies items by their paths: the writer orders members with it and the
// reader groups entries with it, so both sides read the same grammar. Items keep their input
// order within a bucket.
func bucketShard[T any](items []T, pathOf func(T) string) shardBuckets[T] {
	var b shardBuckets[T]
	byPart := map[uint64]map[string][]T{}
	for _, item := range items {
		c := ClassifyShardPath(pathOf(item))
		switch c.Kind {
		case KindPart:
			if byPart[c.PartID] == nil {
				byPart[c.PartID] = map[string][]T{}
			}
			byPart[c.PartID][c.PartType] = append(byPart[c.PartID][c.PartType], item)
		case KindSnapshot:
			b.snapshots = append(b.snapshots, item)
		case KindElementIndex:
			b.elementIndex = append(b.elementIndex, item)
		default:
			b.others = append(b.others, item)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(byPart)) {
		p := partBucket[T]{id: id}
		for _, t := range slices.Sorted(maps.Keys(byPart[id])) {
			p.types = append(p.types, typeBucket[T]{partType: t, items: byPart[id][t]})
		}
		b.parts = append(b.parts, p)
	}
	return b
}

func memberPath(m Member) string { return m.Path }

func sortByPath[T any](items []T, pathOf func(T) string) {
	slices.SortFunc(items, func(a, b T) int { return strings.Compare(pathOf(a), pathOf(b)) })
}

// sortNative orders the files of a native index directory: everything but .snp manifests by
// path, then the manifests by path, so a manifest never precedes the segments it names.
func sortNative[T any](items []T, pathOf func(T) string) {
	slices.SortFunc(items, func(a, b T) int {
		aPath, bPath := pathOf(a), pathOf(b)
		aSnp, bSnp := strings.HasSuffix(aPath, snapshotExt), strings.HasSuffix(bPath, snapshotExt)
		if aSnp != bSnp {
			if aSnp {
				return 1
			}
			return -1
		}
		return cmp.Compare(aPath, bPath)
	})
}
