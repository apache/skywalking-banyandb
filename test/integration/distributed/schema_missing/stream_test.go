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

package schema_missing_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"slices"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

const streamName = "sw"

// A stream query fans out to every data node, so the node whose schema watch is paused is asked
// for a stream it does not know. The groups are laid out so that the expected rows do not depend
// on which node owns which shard:
//
//   - missingGroup has one shard with two replicas, so all three nodes hold every row of it, and
//     it is created while the paused node misses the event: the other two nodes answer for it.
//   - knownGroup has three shards without replica, so each node owns one of them, and it is
//     created before the pause: its rows are complete only if the paused node still answers for it.
var _ = Describe("Stream query with a data node missing the schema", Ordered, func() {
	const (
		missingRows = 20
		knownRows   = 30
	)
	var (
		ctx          context.Context
		suffix       string
		missingGroup string
		knownGroup   string
		missingNode  string
		paused       map[string]bool
		baseTime     time.Time
		groupClient  databasev1.GroupRegistryServiceClient
		streamClient databasev1.StreamRegistryServiceClient
		queryClient  streamv1.StreamServiceClient
	)

	pause := func(addr string) {
		Expect(setup.PauseDataNodeWatch(addr)).To(Succeed())
		paused[addr] = true
	}
	resume := func(addr string) {
		Expect(setup.ResumeDataNodeWatch(addr)).To(Succeed())
		delete(paused, addr)
	}

	createStream := func(group string, shards, replicas uint32) {
		_, err := groupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: group},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum:        shards,
				Replicas:        replicas,
				SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
				Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
			},
		}})
		Expect(err).NotTo(HaveOccurred())
		_, err = streamClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{Stream: &databasev1.Stream{
			Metadata: &commonv1.Metadata{Group: group, Name: streamName},
			Entity:   &databasev1.Entity{TagNames: []string{"svc"}},
			TagFamilies: []*databasev1.TagFamilySpec{{
				Name: "default",
				// seq carries the writer's row ID: a query returns the element's internal ID, and
				// a projection of the entity tag alone returns no element.
				Tags: []*databasev1.TagSpec{
					{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "seq", Type: databasev1.TagType_TAG_TYPE_STRING},
				},
			}},
		}})
		Expect(err).NotTo(HaveOccurred())
	}

	// rowID names row i of group; the rows of a group are a millisecond apart, in that order.
	rowID := func(group string, i int) string { return fmt.Sprintf("%s-%03d", group, i) }

	// write writes rows rows to group, each under its own service so they spread over the
	// group's shards. The liaison can refuse every row while it still catches up with a group it
	// was just given; a refused row stores nothing, so the whole batch is written again then.
	write := func(group string, rows int) {
		Eventually(func(g Gomega) {
			wc, err := queryClient.Write(ctx)
			g.Expect(err).NotTo(HaveOccurred())
			for i := 0; i < rows; i++ {
				g.Expect(wc.Send(&streamv1.WriteRequest{
					Metadata: &commonv1.Metadata{Group: group, Name: streamName},
					Element: &streamv1.ElementValue{
						ElementId: rowID(group, i),
						Timestamp: timestamppb.New(baseTime.Add(time.Duration(i) * time.Millisecond)),
						TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("svc-%d", i)}}},
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: rowID(group, i)}}},
						}}},
					},
					MessageId: uint64(i + 1),
				})).To(Succeed())
			}
			g.Expect(wc.CloseSend()).To(Succeed())
			var statuses []string
			for {
				resp, recvErr := wc.Recv()
				if errors.Is(recvErr, io.EOF) {
					break
				}
				g.Expect(recvErr).NotTo(HaveOccurred())
				statuses = append(statuses, resp.GetStatus())
			}
			succeeded := 0
			for _, st := range statuses {
				if st == modelv1.Status_STATUS_SUCCEED.String() {
					succeeded++
				}
			}
			if succeeded > 0 && succeeded < rows {
				StopTrying(fmt.Sprintf("the write to %s was partly refused: %v", group, statuses)).Now()
			}
			g.Expect(succeeded).To(Equal(rows), "the write to %s was refused: %v", group, statuses)
		}, flags.EventuallyTimeout).Should(Succeed())
	}

	request := func(groups []string, limit uint32, order modelv1.Sort) *streamv1.QueryRequest {
		return &streamv1.QueryRequest{
			Groups: groups,
			Name:   streamName,
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(baseTime.Add(-time.Hour)),
				End:   timestamppb.New(baseTime.Add(time.Hour)),
			},
			Limit:      limit,
			OrderBy:    &modelv1.QueryOrder{Sort: order},
			Projection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: "default", Tags: []string{"svc", "seq"}}}},
		}
	}
	query := func(req *streamv1.QueryRequest) ([]string, error) {
		resp, err := queryClient.Query(ctx, req)
		if err != nil {
			return nil, err
		}
		// A response carries the element's internal ID, not the writer's, so a row is named by
		// the seq tag it was written with.
		ids := make([]string, 0, len(resp.GetElements()))
		for _, e := range resp.GetElements() {
			for _, family := range e.GetTagFamilies() {
				for _, tag := range family.GetTags() {
					if tag.GetKey() == "seq" {
						ids = append(ids, tag.GetValue().GetStr().GetValue())
					}
				}
			}
		}
		return ids, nil
	}
	// port is the port of a node address. setup returns a data node's address under its own host
	// name while the node registers itself under 127.0.0.1, so nodes are matched by port.
	port := func(addr string) string {
		_, p, err := net.SplitHostPort(addr)
		Expect(err).NotTo(HaveOccurred())
		return p
	}
	// rowsByNode returns how many rows of group each data node stores, by the node's port.
	rowsByNode := func(group string) map[string]int64 {
		resp, err := groupClient.Inspect(ctx, &databasev1.GroupRegistryServiceInspectRequest{Group: group})
		Expect(err).NotTo(HaveOccurred())
		rows := map[string]int64{}
		for _, info := range resp.GetDataInfo() {
			for _, segment := range info.GetSegmentInfo() {
				for _, shard := range segment.GetShardInfo() {
					rows[port(info.GetNode().GetGrpcAddress())] += shard.GetDataCount()
				}
			}
		}
		return rows
	}
	rowIDs := func(group string, from, to int) []string {
		ids := make([]string, 0, to-from)
		for i := from; i < to; i++ {
			ids = append(ids, rowID(group, i))
		}
		return ids
	}

	BeforeAll(func() {
		ctx = context.Background()
		paused = map[string]bool{}
		groupClient = databasev1.NewGroupRegistryServiceClient(connection)
		streamClient = databasev1.NewStreamRegistryServiceClient(connection)
		queryClient = streamv1.NewStreamServiceClient(connection)
		baseTime = time.Now().Truncate(time.Minute)
		suffix = fmt.Sprint(time.Now().UnixNano())
		missingGroup = "missing-" + suffix
		knownGroup = "known-" + suffix
		missingNode = dataNodeAddrs[dataNodeCount-1]
		DeferCleanup(func() {
			for addr := range paused {
				_ = setup.ResumeDataNodeWatch(addr)
			}
			for _, group := range []string{missingGroup, knownGroup} {
				_, _ = groupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: group})
			}
		})

		By("Creating a group every node knows, one shard on each node, and writing to it")
		createStream(knownGroup, dataNodeCount, 0)
		write(knownGroup, knownRows)
		Eventually(func(g Gomega) {
			ids, err := query(request([]string{knownGroup}, knownRows+1, modelv1.Sort_SORT_ASC))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ids).To(Equal(rowIDs(knownGroup, 0, knownRows)))
		}, flags.EventuallyTimeout).Should(Succeed())
		rows := rowsByNode(knownGroup)
		Expect(rows).To(HaveLen(dataNodeCount), "every node owns a shard of the known group: %v", rows)
		Expect(rows[port(missingNode)]).To(BeNumerically(">", 0),
			"the node to pause must hold rows of the known group, or the multi-group spec proves nothing: %v", rows)

		By("Pausing one data node's schema watch, then creating a group that node never hears of")
		pause(missingNode)
		createStream(missingGroup, 1, dataNodeCount-1)
		write(missingGroup, missingRows)
	})

	It("returns every row the other nodes hold, in order and within the limit", func() {
		Eventually(func(g Gomega) {
			ids, err := query(request([]string{missingGroup}, missingRows+1, modelv1.Sort_SORT_ASC))
			g.Expect(err).NotTo(HaveOccurred(), "a node missing the schema must not fail the query")
			g.Expect(ids).To(Equal(rowIDs(missingGroup, 0, missingRows)))
		}, flags.EventuallyTimeout).Should(Succeed())
		_, inspectErr := groupClient.Inspect(ctx, &databasev1.GroupRegistryServiceInspectRequest{Group: missingGroup})
		Expect(inspectErr).To(MatchError(ContainSubstring("group %s not found", missingGroup)),
			"the paused node must still lack the group, or the query above did not meet a missing schema")

		ids, err := query(request([]string{missingGroup}, 5, modelv1.Sort_SORT_ASC))
		Expect(err).NotTo(HaveOccurred())
		Expect(ids).To(Equal(rowIDs(missingGroup, 0, 5)), "the first rows by time")
		ids, err = query(request([]string{missingGroup}, 5, modelv1.Sort_SORT_DESC))
		Expect(err).NotTo(HaveOccurred())
		last := rowIDs(missingGroup, missingRows-5, missingRows)
		slices.Reverse(last)
		Expect(ids).To(Equal(last), "the last rows by time")
	})

	It("still returns the other group's rows from the node missing this group's schema", func() {
		ids, err := query(request([]string{missingGroup, knownGroup}, missingRows+knownRows+1, modelv1.Sort_SORT_ASC))
		Expect(err).NotTo(HaveOccurred())
		want := append(rowIDs(missingGroup, 0, missingRows), rowIDs(knownGroup, 0, knownRows)...)
		Expect(ids).To(ConsistOf(want), "the paused node owns a shard of the known group and must still answer for it")
	})

	It("reports a stream the liaison does not know as an error", func() {
		req := request([]string{missingGroup}, 10, modelv1.Sort_SORT_ASC)
		req.Name = "not_registered"
		_, err := query(req)
		Expect(err).To(HaveOccurred(), "only a data node skips a schema it lacks; the liaison still reports it")
	})

	It("returns an empty result when every data node misses the schema", func() {
		allMissing := "all-missing-" + suffix
		DeferCleanup(func() {
			_, _ = groupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: allMissing})
		})
		for _, addr := range dataNodeAddrs {
			if !paused[addr] {
				pause(addr)
			}
		}
		createStream(allMissing, 1, 0)
		Eventually(func(g Gomega) {
			ids, err := query(request([]string{allMissing}, 10, modelv1.Sort_SORT_ASC))
			g.Expect(err).NotTo(HaveOccurred(), "the liaison knows the stream, so the query succeeds")
			g.Expect(ids).To(BeEmpty())
		}, flags.EventuallyTimeout).Should(Succeed())
		for _, addr := range dataNodeAddrs {
			if addr != missingNode {
				resume(addr)
			}
		}
	})

	It("answers with the same rows once the node catches up", func() {
		resume(missingNode)
		Eventually(func(g Gomega) {
			ids, err := query(request([]string{missingGroup}, missingRows+1, modelv1.Sort_SORT_ASC))
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ids).To(Equal(rowIDs(missingGroup, 0, missingRows)), "replicas are merged into one row each")
		}, flags.EventuallyTimeout).Should(Succeed())
	})
})
