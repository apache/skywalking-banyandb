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

package export

import (
	"context"
	"net"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
)

const roundTripNode = "data-a:17912"

// framesLiaison relays the frames splitUnitFrames produced for one node, stamped with its
// node id, then the summary, as the liaison relays a data node's Plan stream.
type framesLiaison struct {
	transferv1.UnimplementedExportServiceServer
	frames []*transferv1.PlanResponse
}

func (l *framesLiaison) Plan(_ *transferv1.PlanRequest, stream transferv1.ExportService_PlanServer) error {
	for _, f := range l.frames {
		f.GetUnits().NodeId = roundTripNode
		if err := stream.Send(f); err != nil {
			return err
		}
	}
	return stream.Send(&transferv1.PlanResponse{Frame: &transferv1.PlanResponse_Summary{
		Summary: &transferv1.PlanSummary{AnsweredNodes: []string{roundTripNode}},
	}})
}

// roundTrip serves the split frames of units over gRPC with the receive limit the export
// client uses and returns what the client collected.
func roundTrip(t *testing.T, units []*transferv1.UnitInventory, budget int) *cluster.PlanResult {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	transferv1.RegisterExportServiceServer(srv, &framesLiaison{frames: splitUnitFrames("warm", units, budget)})
	//panicdiag:allow-rawgo test server
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(receiveLimit)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	res, err := cluster.RunPlan(ctx, conn, nil)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// assertRoundTrip checks that the client reassembles exactly units, in order, and reports them
// exactly as it reports the unsplit inventory.
func assertRoundTrip(t *testing.T, units []*transferv1.UnitInventory, res *cluster.PlanResult) {
	t.Helper()
	var got []*transferv1.UnitInventory
	for _, f := range res.Frames {
		got = append(got, f.GetUnits()...)
	}
	if len(got) != len(units) {
		t.Fatalf("want %d units, got %d", len(units), len(got))
	}
	for i := range units {
		if !proto.Equal(got[i], units[i]) {
			t.Fatalf("unit %d (%s) did not survive the round trip", i, units[i].GetSegment().GetUnit().GetSegmentSuffix())
		}
	}
	want := exporter.BuildReport(&cluster.Info{}, &cluster.PlanResult{
		Frames:        []*transferv1.UnitFrame{{NodeId: roundTripNode, Stage: "warm", Units: units}},
		AnsweredNodes: []string{roundTripNode},
	})
	if rep := exporter.BuildReport(&cluster.Info{}, res); !reflect.DeepEqual(rep, want) {
		t.Fatalf("the reassembled report differs from the unsplit one:\n got %+v\nwant %+v", rep, want)
	}
}

// The 1.2 million part shard of the review: the data node splits it within the frame budget
// and the export client merges it back into the same inventory and report.
func TestPlanRoundTrip_OneShardOverTheReceiveLimit(t *testing.T) {
	units := syntheticSegments(1, 1, 1_200_000)
	varyShards(units)
	assertRoundTrip(t, units, roundTrip(t, units, planFrameBudget))
}

// Many segments mixing whole shards, split shards (shard 0 among them) and shards without
// parts, under a budget that splits segments and shards alike.
func TestPlanRoundTrip_MixedShardsAndSegments(t *testing.T) {
	units := syntheticSegments(5, 6, 4)
	for s, u := range units {
		shards := u.GetSegment().GetShards()
		for _, id := range []int{0, s % 6} {
			for p := len(shards[id].Parts); p < 300+50*s; p++ {
				shards[id].Parts = append(shards[id].Parts, &transferv1.PartStat{Id: uint64(p + 1)})
			}
		}
		shards[(s+3)%6].Parts = nil
	}
	varyShards(units)
	assertRoundTrip(t, units, roundTrip(t, units, 2048))
}
