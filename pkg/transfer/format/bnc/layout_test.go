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
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	id1 = "0000000000000001"
	id2 = "0000000000000002"
)

func TestClassifyShardPath(t *testing.T) {
	cases := map[string]Class{
		id1 + "/primary.bin":                     {Kind: KindPart, PartID: 1},
		"ffffffffffffffff/meta.bin":              {Kind: KindPart, PartID: ^uint64(0)},
		id1 + "/a.b.tf":                          {Kind: KindPart, PartID: 1},
		"sidx/latency/" + id2 + "/manifest.json": {Kind: KindPart, PartID: 2, PartType: "latency"},
		"sidx/Duration/" + id1 + "/data.bin":     {Kind: KindPart, PartID: 1, PartType: "Duration"},
		"sidx/a.b-c/" + id1 + "/f":               {Kind: KindPart, PartID: 1, PartType: "a.b-c"},
		"sidx/idx/" + id1 + "/f":                 {Kind: KindPart, PartID: 1, PartType: "idx"},
		"sidx/lock/" + id1 + "/f":                {Kind: KindPart, PartID: 1, PartType: "lock"},
		"sidx/" + id2 + "/" + id1 + "/f":         {Kind: KindPart, PartID: 1, PartType: id2},
		"18e29603bfcd2125.snp":                   {Kind: KindSnapshot},
		"idx/000000000001.snp":                   {Kind: KindElementIndex},
		"idx/000000000000.seg":                   {Kind: KindElementIndex},
		"idx/sub/x":                              {Kind: KindElementIndex},
		"000000000000001/f":                      {Kind: KindOther},
		"00000000000000001/f":                    {Kind: KindOther},
		"000000000000000A/f":                     {Kind: KindOther},
		"000000000000000g/f":                     {Kind: KindOther},
		"sidx/latency/000000000000000A/f":        {Kind: KindOther},
		"sidx/latency/f":                         {Kind: KindOther},
		"idx":                                    {Kind: KindOther},
		"finalize.json":                          {Kind: KindOther},
		"000000000096.snp":                       {Kind: KindOther},
		id1:                                      {Kind: KindOther},
	}
	for p, want := range cases {
		assert.Equal(t, want, ClassifyShardPath(p), p)
	}
}

func members(paths ...string) []Member {
	out := make([]Member, len(paths))
	for i, p := range paths {
		out[i] = Member{Path: p, Size: int64(i)}
	}
	return out
}

func memberPaths(ms []Member) []string { return paths(ms, memberPath) }

func shuffled(m []Member) []Member {
	rng := rand.New(rand.NewPCG(7, 8)) //nolint:gosec // a fixed seed keeps failures reproducible
	rng.Shuffle(len(m), func(i, j int) { m[i], m[j] = m[j], m[i] })
	return m
}

func TestOrderShard(t *testing.T) {
	in := shuffled(members(
		"finalize.json",
		"idx/000000000004.snp",
		"idx/000000000001.seg",
		"idx/000000000000.seg",
		"18e29603bfcd2125.snp",
		"18e29603bfcd2122.snp",
		id2+"/primary.bin",
		id2+"/metadata.json",
		"sidx/Abc/"+id1+"/primary.bin",
		"sidx/abc/"+id1+"/manifest.json",
		id1+"/primary.bin",
		id1+"/metadata.json",
		"sidx/Abc/"+id1+"/manifest.json",
		id1+"/smeta.bin",
	))
	want := []string{
		id1 + "/metadata.json", id1 + "/primary.bin", id1 + "/smeta.bin",
		"sidx/Abc/" + id1 + "/manifest.json", "sidx/Abc/" + id1 + "/primary.bin", "sidx/abc/" + id1 + "/manifest.json",
		id2 + "/metadata.json", id2 + "/primary.bin",
		"18e29603bfcd2122.snp",
		"18e29603bfcd2125.snp",
		"idx/000000000000.seg", "idx/000000000001.seg", "idx/000000000004.snp",
		"finalize.json",
	}
	assert.Equal(t, want, memberPaths(OrderShard(in)))
}

func TestOrderShard_KeepsEveryMember(t *testing.T) {
	// Directory names are not rules: a trace index may be named "lock" or "a.tmp".
	paths := []string{
		"lock",
		id1 + "/metadata.json.tmp",
		"idx/.native-external-1-3",
		"sidx/lock/" + id1 + "/manifest.json",
		"sidx/a.tmp/" + id1 + "/manifest.json",
		id1 + "/metadata.json",
		"finalize.json",
	}
	assert.ElementsMatch(t, paths, memberPaths(OrderShard(members(paths...))))
	assert.Empty(t, OrderShard(nil))
}

func TestOrderShard_ElementIndexOnly(t *testing.T) {
	got := memberPaths(OrderShard(members("idx/000000000001.snp", "idx/000000000000.seg")))
	assert.Equal(t, []string{"idx/000000000000.seg", "idx/000000000001.snp"}, got)
}

func TestOrderIndexDir(t *testing.T) {
	in := shuffled(members(
		"000000000096.snp",
		"000000000075.seg",
		"0000000000a0.seg",
		"000000000001.snp",
	))
	want := []string{"000000000075.seg", "0000000000a0.seg", "000000000001.snp", "000000000096.snp"}
	assert.Equal(t, want, memberPaths(OrderIndexDir(in)))
	assert.Equal(t, []string{"000000000004.snp"}, memberPaths(OrderIndexDir(members("000000000004.snp"))))
}

func TestShard_Grouping(t *testing.T) {
	// Footer order deliberately interleaves parts and sub parts.
	b, _ := writeContainer(t,
		testMember{id1 + "/core-b", []byte("b")},
		testMember{"sidx/x/" + id1 + "/manifest.json", []byte("{}")},
		testMember{id2 + "/metadata.json", []byte("{}")},
		testMember{id1 + "/metadata.json", []byte("{}")},
		testMember{"sidx/x/" + id1 + "/data.bin", nil},
		testMember{id1 + "/core-a", []byte("a")},
		testMember{"idx/000000000001.snp", []byte("s")},
		testMember{"idx/000000000000.seg", []byte("s")},
		testMember{"18e29603bfcd2125.snp", []byte("[]")},
		testMember{"finalize.json", []byte("{}")},
	)
	layout := setOf(t, b).Shard()
	require.Len(t, layout.Parts, 2)
	p1 := layout.Parts[0]
	assert.Equal(t, uint64(1), p1.PartID)
	require.Len(t, p1.Subs, 2)
	assert.Equal(t, "", p1.Subs[0].PartType)
	assert.Equal(t, id1+"/metadata.json", p1.Subs[0].Meta.Path)
	assert.Equal(t, []string{id1 + "/core-a", id1 + "/core-b"}, entryPaths(p1.Subs[0].Files))
	assert.Equal(t, "x", p1.Subs[1].PartType)
	assert.Equal(t, "sidx/x/"+id1+"/manifest.json", p1.Subs[1].Meta.Path)
	assert.Equal(t, []string{"sidx/x/" + id1 + "/data.bin"}, entryPaths(p1.Subs[1].Files))
	assert.Equal(t, uint64(2), layout.Parts[1].PartID)
	assert.Empty(t, layout.Parts[1].Subs[0].Files)
	assert.Equal(t, []string{"18e29603bfcd2125.snp"}, entryPaths(layout.Snapshots))
	assert.Equal(t, []string{"idx/000000000000.seg", "idx/000000000001.snp"}, entryPaths(layout.ElementIndex))
	assert.Equal(t, []string{"finalize.json"}, entryPaths(layout.Others))
}

// Incomplete parts, which a closed segment can hold, are set aside rather than failing the
// whole shard.
func TestShard_Orphans(t *testing.T) {
	b, _ := writeContainer(t,
		// part 1: its secondary index outlived the core part
		testMember{"sidx/x/" + id1 + "/manifest.json", nil},
		testMember{"sidx/x/" + id1 + "/data.bin", nil},
		// part 2: complete core, secondary index without its manifest
		testMember{id2 + "/metadata.json", nil},
		testMember{id2 + "/primary.bin", nil},
		testMember{"sidx/x/" + id2 + "/data.bin", nil},
		// part 3: core part without metadata.json, with a complete secondary index
		testMember{"0000000000000003/primary.bin", nil},
		testMember{"sidx/x/0000000000000003/manifest.json", nil},
	)
	layout := setOf(t, b).Shard()
	require.Len(t, layout.Parts, 1)
	assert.Equal(t, uint64(2), layout.Parts[0].PartID)
	require.Len(t, layout.Parts[0].Subs, 1)
	assert.Equal(t, []string{
		"0000000000000003/primary.bin",
		"sidx/x/0000000000000001/data.bin",
		"sidx/x/0000000000000001/manifest.json",
		"sidx/x/0000000000000002/data.bin",
		"sidx/x/0000000000000003/manifest.json",
	}, entryPaths(layout.Orphans))
}

func entryPaths(files []File) []string { return paths(files, filePath) }

func paths[T any](items []T, pathOf func(T) string) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, pathOf(item))
	}
	return out
}
