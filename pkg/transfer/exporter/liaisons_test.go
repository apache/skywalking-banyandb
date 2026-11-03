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

package exporter

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/manifest"
)

// targets lists the target node of every Export stream a fake liaison opened.
func (f *fakeLiaison) targets() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, in := range f.exportInits {
		out = append(out, in.GetTargetNode())
	}
	return out
}

func TestPreflight_KeepsEveryUsableLiaisonAndWarnsAboutTheOthers(t *testing.T) {
	first, second := newFakeLiaison(), newFakeLiaison()
	firstAddr, secondAddr := serveLiaison(t, first), serveLiaison(t, second)
	var warn bytes.Buffer
	ls, info, err := Preflight(context.Background(), []string{"127.0.0.1:1", firstAddr, secondAddr},
		ConnectOptions{Timeout: 2 * time.Second, Log: &warn})
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Close()
	if len(ls.All) != 2 || ls.All[0].Addr != firstAddr || ls.All[1].Addr != secondAddr || info.Liaison != firstAddr {
		t.Fatalf("liaisons = %+v, info = %+v", ls.All, info)
	}
	if !strings.HasPrefix(warn.String(), "WARN liaison 127.0.0.1:1 is not usable, skipped: ") || strings.Count(warn.String(), "\n") != 1 {
		t.Fatalf("one WARN line per skipped candidate, got %q", warn.String())
	}
}

// Nodes, in id order, are dealt round robin over the liaisons; Plan and Sessions use the
// primary only.
func TestRunExport_DealsNodesOverTheLiaisons(t *testing.T) {
	primary, other := exportFake(), exportFake()
	opts, log := exportOptions(t, primary)
	if err := runExportVia(t, opts, primary, other); err != nil {
		t.Fatalf("%v\n%s", err, log)
	}
	if got := primary.targets(); len(got) != 1 || got[0] != nodeA {
		t.Fatalf("primary streams = %v", got)
	}
	if got := other.targets(); len(got) != 1 || got[0] != nodeB {
		t.Fatalf("second liaison streams = %v", got)
	}
	if other.lastPlan() != nil || other.countSessions(transferv1.SessionsRequest_ACTION_RELEASE) != 0 || len(other.schema.calls()) != 0 {
		t.Fatal("Plan, Sessions and the schema go through the primary only")
	}
	if !strings.Contains(log.String(), "via 2 liaison(s)") || !strings.Contains(log.String(), "NODE") {
		t.Fatalf("log:\n%s", log)
	}
}

// Opening a stream refused as unavailable or by a liaison that cannot see the node moves on
// to the next liaison.
func TestRunExport_MovesToTheNextLiaisonWhenRefused(t *testing.T) {
	saved := exportRetryBase
	exportRetryBase = 10 * time.Millisecond
	t.Cleanup(func() { exportRetryBase = saved })
	for _, code := range []codes.Code{codes.Unavailable, codes.NotFound} {
		primary, refusing := exportFake(), exportFake()
		refusing.exportScript = func(*fakeLiaison, *transferv1.ExportRequest_Init, transferv1.ExportService_ExportServer) error {
			return status.Error(code, "cannot reach the node (injected)")
		}
		opts, log := exportOptions(t, primary)
		opts.SendRetryTimeout = time.Minute
		env := envFor(t, opts, primary, refusing)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := RunExport(ctx, env, opts.Export)
		cancel()
		if err != nil {
			t.Fatalf("%s: %v\n%s", code, err, log)
		}
		if got := primary.targets(); len(got) != 2 || got[0] != nodeA && got[1] != nodeA {
			t.Fatalf("%s: the primary must take over data-b, streams = %v", code, got)
		}
		want := "node " + nodeB + " is busy or unreachable via " + env.Liaisons.All[1].Addr + " (" + code.String() + "); retrying via " + env.Liaisons.All[0].Addr
		if !strings.Contains(log.String(), want) {
			t.Fatalf("%s: log lacks %q:\n%s", code, want, log)
		}
	}
	// Any other refusal is final.
	primary, refusing := exportFake(), exportFake()
	refusing.exportScript = func(*fakeLiaison, *transferv1.ExportRequest_Init, transferv1.ExportService_ExportServer) error {
		return status.Error(codes.InvalidArgument, "bad init (injected)")
	}
	opts, _ := exportOptions(t, primary)
	opts.SendRetryTimeout = time.Minute
	requireExit(t, runExportVia(t, opts, primary, refusing), ExitRuntime, "bad init")
}

func TestRenderSummary(t *testing.T) {
	shard := func(n, rows uint64) manifest.Shard {
		return manifest.Shard{Files: []manifest.File{{Bytes: n}}, TotalCount: rows}
	}
	m := &manifest.Manifest{
		Warnings: []string{"w"},
		Units: []manifest.Unit{
			{Catalog: "MEASURE", Group: "g", Stage: "hot", Sources: []manifest.UnitSource{
				{Node: "b", Shards: []manifest.Shard{shard(1024, 5)}},
				{
					Node:         "a",
					SegmentFiles: []manifest.File{{Bytes: 10}},
					TotalCount:   5,
					Shards:       []manifest.Shard{shard(14, 3)},
				},
			}},
			{Catalog: "MEASURE", Group: "g", Stage: "hot", Sources: []manifest.UnitSource{
				{Node: "a", Shards: []manifest.Shard{shard(1, 2)}},
			}},
		},
	}
	var out bytes.Buffer
	elapsed := map[string]time.Duration{
		elapsedKey("a", "MEASURE", "g"): 1500 * time.Millisecond,
		elapsedKey("b", "MEASURE", "g"): 90 * time.Second,
	}
	if err := renderSummary(&out, "/x", m, elapsed, 95*time.Second); err != nil {
		t.Fatal(err)
	}
	want := "NODE  CATALOG  GROUP  STAGE  UNITS  FILES  ROWS  SIZE     TIME\n" +
		"a     measure  g      hot    2      3      10    25 B     1.5s\n" +
		"b     measure  g      hot    1      1      5     1.0 KiB  1m30s\n" +
		"\nexport written to /x: 2 unit(s), 4 file(s), 1.0 KiB, 1 warning(s) in 1m35s\n"
	if out.String() != want {
		t.Fatalf("got\n%s\nwant\n%s", out.String(), want)
	}
}
