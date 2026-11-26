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
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
	"github.com/apache/skywalking-banyandb/pkg/transfer/resume"
)

func TestPlanUnitsMatchTheManifestUnits(t *testing.T) {
	dir := t.TempDir()
	seg := func(c commonv1.Catalog, group, suffix string) *transferv1.UnitInventory {
		return &transferv1.UnitInventory{Kind: &transferv1.UnitInventory_Segment{Segment: &transferv1.SegmentInventory{
			Unit: &transferv1.SegmentUnit{Catalog: c, Group: group, SegmentSuffix: suffix},
		}}}
	}
	prop := &transferv1.UnitInventory{Kind: &transferv1.UnitInventory_Property{Property: &transferv1.PropertyInventory{Group: "p"}}}
	var plan struct {
		Frames []json.RawMessage `json:"frames"`
	}
	for _, f := range []*transferv1.UnitFrame{
		{NodeId: "hot", Units: []*transferv1.UnitInventory{seg(commonv1.Catalog_CATALOG_STREAM, "s", "20260101"), prop}},
		{NodeId: "warm", Stage: "warm", Units: []*transferv1.UnitInventory{seg(commonv1.Catalog_CATALOG_STREAM, "s", "20251201"), prop}},
	} {
		b, err := protojson.Marshal(f)
		require.NoError(t, err)
		plan.Frames = append(plan.Frames, b)
	}
	// The progress file an export writes: the plan in its side file, the digest of it inside.
	progressPath := filepath.Join(dir, exporter.ProgressFileName)
	digest, err := resume.WriteSide(resume.SidePath(progressPath, "plan"), plan)
	require.NoError(t, err)
	require.NoError(t, resume.WriteFile(progressPath, map[string]any{"kind": "export-progress", "version": 2, "planDigest": digest}))

	got, err := planUnitLines(dir)
	require.NoError(t, err)
	assert.Equal(t, []string{"hot PROPERTY p  ", "hot STREAM s hot 20260101", "warm PROPERTY p  ", "warm STREAM s warm 20251201"}, got)

	// The manifest the export writes from that plan: the default tier is the hot stage, and one
	// property unit spans both nodes.
	m := &manifest.Manifest{Units: []manifest.Unit{
		{Catalog: "PROPERTY", Group: "p", Sources: []manifest.UnitSource{{Node: "hot"}, {Node: "warm"}}},
		{Catalog: "STREAM", Group: "s", Stage: "hot", Segment: "20260101", Sources: []manifest.UnitSource{{Node: "hot"}}},
		{Catalog: "STREAM", Group: "s", Stage: "warm", Segment: "20251201", Sources: []manifest.UnitSource{{Node: "warm"}}},
	}}
	b, err := json.Marshal(m)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, manifest.FileName), b, 0o600))
	fromManifest, err := manifestUnitLines(dir)
	require.NoError(t, err)
	assert.Equal(t, got, fromManifest)
}
