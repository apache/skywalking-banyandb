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
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
)

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestNormalizeStage(t *testing.T) {
	if NormalizeStage("") != "hot" || NormalizeStage("warm") != "warm" {
		t.Fatal("empty stage must normalize to hot")
	}
}

func TestRunPlan_CollectsFramesAndDropsUnreachableNodes(t *testing.T) {
	f := newFakeLiaison()
	f.planFrames = []*transferv1.PlanResponse{
		unitFrame("data-a:17912", "", "m", 100, 200, 5),
		unitFrame("data-b:17912", "warm", "m", 150, 250, 6), // data-b failed after its frame was relayed
	}
	f.summary = &transferv1.PlanSummary{AnsweredNodes: []string{"data-a:17912"}, UnreachableNodes: []string{"data-b:17912"}}
	conn := dial(t, serveLiaison(t, f))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	res, err := RunPlan(ctx, conn, []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_MEASURE}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Frames) != 1 || res.Frames[0].GetNodeId() != "data-a:17912" {
		t.Fatalf("frames of an unreachable node must be dropped, got %+v", res.Frames)
	}
	if res.Frames[0].GetStage() != DefaultStageName {
		t.Fatalf("stage must be normalized, got %q", res.Frames[0].GetStage())
	}
	if !slices.Equal(res.UnreachableNodes, []string{"data-b:17912"}) || !slices.Equal(res.AnsweredNodes, []string{"data-a:17912"}) {
		t.Fatalf("summary = answered %v, unreachable %v", res.AnsweredNodes, res.UnreachableNodes)
	}
}

func TestRunPlan_RejectsProtocolViolations(t *testing.T) {
	created := &transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Created{Created: &transferv1.SessionCreated{SessionId: "feed"}}}
	summary := summaryFrame(&transferv1.PlanSummary{AnsweredNodes: []string{"data-a:17912"}})
	for name, tc := range map[string]struct {
		want   string
		frames []*transferv1.PlanResponse
	}{
		"created frame in a dry run": {frames: []*transferv1.PlanResponse{created, summary}, want: "in a dry run"},
		"second summary":             {frames: []*transferv1.PlanResponse{summary, summary}, want: "after the summary"},
		"units after the summary":    {frames: []*transferv1.PlanResponse{summary, unitFrame("data-a:17912", "", "m", 1, 2, 1)}, want: "after the summary"},
		"units from an unlisted node": {
			frames: []*transferv1.PlanResponse{unitFrame("data-z:17912", "", "m", 1, 2, 1), summary},
			want:   "neither answered nor unreachable",
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeLiaison()
			f.skipSummary = true
			f.planFrames = tc.frames
			conn := dial(t, serveLiaison(t, f))
			_, err := RunPlan(context.Background(), conn, nil)
			var exitErr *ExitError
			if !errors.As(err, &exitErr) || exitErr.Code != ExitRuntime || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want exit 3 mentioning %q, got %v", tc.want, err)
			}
		})
	}
}

func TestRunPlan_ServerWithoutExportService(t *testing.T) {
	f := newFakeLiaison()
	f.noExport = true
	conn := dial(t, serveLiaison(t, f))
	_, err := RunPlan(context.Background(), conn, nil)
	if err == nil || !strings.Contains(err.Error(), "does not support data export (needs 0.12+)") {
		t.Fatalf("an older server must get the upgrade hint, got %v", err)
	}
	if code := ExitCodeFor(err); code != ExitPreflight {
		t.Fatalf("UNIMPLEMENTED is a preflight rejection, got exit %d", code)
	}
}

func TestRunPlan_RejectsUnstampedFrames(t *testing.T) {
	f := newFakeLiaison()
	f.planFrames = []*transferv1.PlanResponse{unitFrame("", "", "m", 1, 2, 1)}
	conn := dial(t, serveLiaison(t, f))
	if _, err := RunPlan(context.Background(), conn, nil); err == nil || !strings.Contains(err.Error(), "node_id") {
		t.Fatalf("a units frame without node_id must be rejected, got %v", err)
	}
}

func TestValidateSelectors(t *testing.T) {
	groups := []*commonv1.Group{
		{Metadata: &commonv1.Metadata{Name: "sw_metric"}, Catalog: commonv1.Catalog_CATALOG_MEASURE},
		{Metadata: &commonv1.Metadata{Name: "sw_record"}, Catalog: commonv1.Catalog_CATALOG_STREAM},
	}
	ok := []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_MEASURE, Groups: []string{"sw_metric"}}, {Catalog: commonv1.Catalog_CATALOG_STREAM}}
	if err := ValidateSelectors(groups, ok); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSelectors(groups, []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_STREAM, Groups: []string{"sw_metric"}}}); err == nil {
		t.Fatal("a group in the wrong catalog must be rejected")
	}
	if err := ValidateSelectors(groups, []*transferv1.Selector{{Catalog: commonv1.Catalog_CATALOG_STREAM, Groups: []string{"nope"}}}); err == nil {
		t.Fatal("an unknown group must be rejected")
	}
}

func TestCollectPlanFrames_StreamEndsWithoutSummaryFrame(t *testing.T) {
	f := newFakeLiaison()
	f.skipSummary = true
	f.planFrames = []*transferv1.PlanResponse{
		unitFrame("data-a:17912", "hot", "g1", 100, 200, 5),
	}
	conn := dial(t, serveLiaison(t, f))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := RunPlan(ctx, conn, nil)
	var exitErr *ExitError
	if !errors.As(err, &exitErr) || exitErr.Code != ExitRuntime || !strings.Contains(err.Error(), "without a summary frame") {
		t.Fatalf("a stream without the summary frame leaves the coverage unknown and must fail with exit 3, got %v", err)
	}
}
