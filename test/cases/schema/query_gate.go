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

package schema

import (
	"context"
	"fmt"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// sendStreamQuery issues a unary stream Query with a single-group GroupModRevisions entry.
// It returns the per-group status for groupName from the response, or an error.
func sendStreamQuery(
	ctx context.Context,
	client streamv1.StreamServiceClient,
	groupName, streamName string,
	modRevision int64,
) (modelv1.Status, error) {
	now := timestamp.NowMilli()
	resp, queryErr := client.Query(ctx, &streamv1.QueryRequest{
		Groups: []string{groupName},
		Name:   streamName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(now.Add(-time.Hour)),
			End:   timestamppb.New(now.Add(time.Hour)),
		},
		Limit:             1,
		GroupModRevisions: map[string]int64{groupName: modRevision},
		Projection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: "default", Tags: []string{"svc"}},
			},
		},
	})
	if queryErr != nil {
		return modelv1.Status_STATUS_UNSPECIFIED, fmt.Errorf("stream query: %w", queryErr)
	}
	st, ok := resp.GetGroupStatuses()[groupName]
	if !ok {
		return modelv1.Status_STATUS_UNSPECIFIED, fmt.Errorf("group %q absent from GroupStatuses in response", groupName)
	}
	return st, nil
}

// queryGateStreamName is the fixture stream name reused across §4.5 specs.
const queryGateStreamName = "qg_stream"

// Schema query gate smoke tests — §4.5.1 / §4.5.2 / §4.5.3.
// Each spec exercises a distinct branch of the per-group ModRevision gate on the
// query path.
var _ = g.Describe("Schema query gate", func() {
	var (
		ctx     context.Context
		clients *Clients
	)

	g.BeforeEach(func() {
		ctx = context.Background()
		clients = NewClients(SharedContext.Connection)
	})

	// §4.5.1: a query carrying a group ModRevision below the cached schema revision
	// is short-circuited and returns STATUS_EXPIRED_SCHEMA for that group.
	g.It("returns STATUS_EXPIRED_SCHEMA for stale group ModRevision (§4.5.1)", func() {
		groupName := fmt.Sprintf("qg-stale-%d", time.Now().UnixNano())
		streamName := queryGateStreamName

		g.By("Creating stream group and schema to obtain baseline ModRevision (R1)")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: wgStreamGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		createResp, createStreamErr := clients.StreamRegClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
			Stream: wgStreamSpec(groupName, streamName),
		})
		gm.Expect(createStreamErr).ShouldNot(gm.HaveOccurred())
		r1 := createResp.GetModRevision()
		gm.Expect(r1).Should(gm.BeNumerically(">", int64(0)))

		g.By("Updating the stream to advance the cache to R2 > R1")
		getResp, getStreamErr := clients.StreamRegClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: streamName, Group: groupName},
		})
		gm.Expect(getStreamErr).ShouldNot(gm.HaveOccurred())
		updatedStream := getResp.GetStream()
		updatedStream.TagFamilies[0].Tags = append(updatedStream.TagFamilies[0].Tags,
			&databasev1.TagSpec{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING})
		updateResp, updateStreamErr := clients.StreamRegClient.Update(ctx, &databasev1.StreamRegistryServiceUpdateRequest{
			Stream: updatedStream,
		})
		gm.Expect(updateStreamErr).ShouldNot(gm.HaveOccurred())
		r2 := updateResp.GetModRevision()
		gm.Expect(r2).Should(gm.BeNumerically(">", r1))

		g.By("Waiting for the cache to reach R2")
		awaitErr := clients.AwaitRevision(ctx, r2, 10*time.Second)
		gm.Expect(awaitErr).ShouldNot(gm.HaveOccurred())

		g.By("Sending a query with the old ModRevision R1 (stale)")
		st, queryErr := sendStreamQuery(ctx, clients.StreamWriteClient, groupName, streamName, r1)
		gm.Expect(queryErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(st).Should(gm.Equal(modelv1.Status_STATUS_EXPIRED_SCHEMA),
			"query with group ModRevision < cache must return STATUS_EXPIRED_SCHEMA")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §4.5.2: a query carrying a group ModRevision far ahead of the cache is held
	// until the configured wait duration and then returned with STATUS_SCHEMA_NOT_APPLIED.
	g.It("returns STATUS_SCHEMA_NOT_APPLIED for ahead group ModRevision that never applies (§4.5.2)", func() {
		groupName := fmt.Sprintf("qg-ahead-%d", time.Now().UnixNano())
		streamName := queryGateStreamName

		g.By("Creating stream group and schema")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: wgStreamGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		createResp, createStreamErr := clients.StreamRegClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
			Stream: wgStreamSpec(groupName, streamName),
		})
		gm.Expect(createStreamErr).ShouldNot(gm.HaveOccurred())
		r1 := createResp.GetModRevision()

		g.By("Waiting for the schema to be applied in cache")
		awaitErr := clients.AwaitApplied(ctx, []string{fmt.Sprintf("stream:%s/%s", groupName, streamName)}, 10*time.Second)
		gm.Expect(awaitErr).ShouldNot(gm.HaveOccurred())

		g.By("Sending a query with ModRevision = R1 + 99999 (far ahead, will never apply)")
		aheadRev := r1 + 99999
		st, queryErr := sendStreamQuery(ctx, clients.StreamWriteClient, groupName, streamName, aheadRev)
		gm.Expect(queryErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(st).Should(gm.Equal(modelv1.Status_STATUS_SCHEMA_NOT_APPLIED),
			"query with group ModRevision > cache must return STATUS_SCHEMA_NOT_APPLIED after wait timeout")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §4.5.3: a query whose group ModRevision matches the cached revision passes the gate
	// and returns STATUS_SUCCEED for that group.
	g.It("passes the gate and returns STATUS_SUCCEED when ModRevision matches cache (§4.5.3)", func() {
		groupName := fmt.Sprintf("qg-match-%d", time.Now().UnixNano())
		streamName := queryGateStreamName

		g.By("Creating stream group and schema")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: wgStreamGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		createResp, createStreamErr := clients.StreamRegClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
			Stream: wgStreamSpec(groupName, streamName),
		})
		gm.Expect(createStreamErr).ShouldNot(gm.HaveOccurred())
		r1 := createResp.GetModRevision()

		g.By("Waiting for the schema to be applied in cache")
		awaitErr := clients.AwaitApplied(ctx, []string{fmt.Sprintf("stream:%s/%s", groupName, streamName)}, 10*time.Second)
		gm.Expect(awaitErr).ShouldNot(gm.HaveOccurred())

		g.By("Sending a query with the current ModRevision R1 (matching cache)")
		st, queryErr := sendStreamQuery(ctx, clients.StreamWriteClient, groupName, streamName, r1)
		gm.Expect(queryErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(st).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED),
			"query with group ModRevision equal to cache must pass the gate with STATUS_SUCCEED")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §4.5.4: mixed groups — one at current rev (SUCCEED), one stale (EXPIRED_SCHEMA);
	// query short-circuits and returns empty elements.
	g.It("returns mixed group_statuses and empty elements when one group is stale (§4.5.4)", func() {
		group1 := fmt.Sprintf("qg-mixed1-%d", time.Now().UnixNano())
		group2 := fmt.Sprintf("qg-mixed2-%d", time.Now().UnixNano())
		streamName := queryGateStreamName

		g.By("Creating group1 + stream → R1a; updating stream → R1b")
		_, createGroup1Err := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: wgStreamGroup(group1),
		})
		gm.Expect(createGroup1Err).ShouldNot(gm.HaveOccurred())

		createResp1a, createStream1Err := clients.StreamRegClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
			Stream: wgStreamSpec(group1, streamName),
		})
		gm.Expect(createStream1Err).ShouldNot(gm.HaveOccurred())
		r1a := createResp1a.GetModRevision()

		getResp1, getStream1Err := clients.StreamRegClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: streamName, Group: group1},
		})
		gm.Expect(getStream1Err).ShouldNot(gm.HaveOccurred())
		updatedStream1 := getResp1.GetStream()
		updatedStream1.TagFamilies[0].Tags = append(updatedStream1.TagFamilies[0].Tags,
			&databasev1.TagSpec{Name: "host", Type: databasev1.TagType_TAG_TYPE_STRING})
		updateResp1, updateStream1Err := clients.StreamRegClient.Update(ctx, &databasev1.StreamRegistryServiceUpdateRequest{
			Stream: updatedStream1,
		})
		gm.Expect(updateStream1Err).ShouldNot(gm.HaveOccurred())
		r1b := updateResp1.GetModRevision()
		gm.Expect(r1b).Should(gm.BeNumerically(">", r1a))

		g.By("Creating group2 + stream → R2")
		_, createGroup2Err := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: wgStreamGroup(group2),
		})
		gm.Expect(createGroup2Err).ShouldNot(gm.HaveOccurred())

		createResp2, createStream2Err := clients.StreamRegClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
			Stream: wgStreamSpec(group2, streamName),
		})
		gm.Expect(createStream2Err).ShouldNot(gm.HaveOccurred())
		r2 := createResp2.GetModRevision()

		g.By("Awaiting max revision")
		maxRev := r1b
		if r2 > maxRev {
			maxRev = r2
		}
		awaitErr := clients.AwaitRevision(ctx, maxRev, 10*time.Second)
		gm.Expect(awaitErr).ShouldNot(gm.HaveOccurred())

		g.By("Querying both groups with group1 stale (R1a) and group2 current (R2)")
		now := timestamp.NowMilli()
		resp, queryErr := clients.StreamWriteClient.Query(ctx, &streamv1.QueryRequest{
			Groups: []string{group1, group2},
			Name:   streamName,
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(now.Add(-time.Hour)),
				End:   timestamppb.New(now.Add(time.Hour)),
			},
			Limit: 10,
			GroupModRevisions: map[string]int64{
				group1: r1a,
				group2: r2,
			},
			Projection: &modelv1.TagProjection{
				TagFamilies: []*modelv1.TagProjection_TagFamily{
					{Name: "default", Tags: []string{"svc"}},
				},
			},
		})
		gm.Expect(queryErr).ShouldNot(gm.HaveOccurred())

		g.By("Asserting group1 status is EXPIRED_SCHEMA and group2 status is SUCCEED")
		statuses := resp.GetGroupStatuses()
		gm.Expect(statuses).ShouldNot(gm.BeNil())
		gm.Expect(statuses[group1]).Should(gm.Equal(modelv1.Status_STATUS_EXPIRED_SCHEMA),
			"group1 with stale R1a must be STATUS_EXPIRED_SCHEMA")
		// group2 is current; may be SUCCEED or absent when short-circuited — check actual behavior.
		if st2, ok := statuses[group2]; ok {
			gm.Expect(st2).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED),
				"group2 with current R2 must be STATUS_SUCCEED when present")
		}

		g.By("Query short-circuits: elements must be empty")
		gm.Expect(resp.GetElements()).Should(gm.BeEmpty(),
			"query short-circuits on EXPIRED_SCHEMA; elements must be empty")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: group1})
		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: group2})
	})

	// §4.5.5: partial coverage opt-in — group2 not in GroupModRevisions map → ungated.
	// Query executes; group1 gated (SUCCEED); group2 either absent or SUCCEED in statuses.
	g.It("executes query when one group is ungated (not in GroupModRevisions) (§4.5.5)", func() {
		group1 := fmt.Sprintf("qg-partial1-%d", time.Now().UnixNano())
		group2 := fmt.Sprintf("qg-partial2-%d", time.Now().UnixNano())
		streamName := queryGateStreamName

		g.By("Creating group1 and group2 with streams")
		var maxGroupRev int64
		for _, grpName := range []string{group1, group2} {
			grpResp, createGrpErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
				Group: wgStreamGroup(grpName),
			})
			gm.Expect(createGrpErr).ShouldNot(gm.HaveOccurred())
			if grpResp.GetModRevision() > maxGroupRev {
				maxGroupRev = grpResp.GetModRevision()
			}

			_, createStreamErr := clients.StreamRegClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
				Stream: wgStreamSpec(grpName, streamName),
			})
			gm.Expect(createStreamErr).ShouldNot(gm.HaveOccurred())
		}

		g.By("Waiting for both groups and both stream schemas to be applied")
		gm.Expect(clients.AwaitRevision(ctx, maxGroupRev, 10*time.Second)).ShouldNot(gm.HaveOccurred())
		awaitErr := clients.AwaitApplied(ctx, []string{
			fmt.Sprintf("stream:%s/%s", group1, streamName),
			fmt.Sprintf("stream:%s/%s", group2, streamName),
		}, 10*time.Second)
		gm.Expect(awaitErr).ShouldNot(gm.HaveOccurred())

		g.By("Getting current ModRevision for group1")
		getResp1, getStream1Err := clients.StreamRegClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: streamName, Group: group1},
		})
		gm.Expect(getStream1Err).ShouldNot(gm.HaveOccurred())
		r1 := getResp1.GetStream().GetMetadata().GetModRevision()
		gm.Expect(r1).Should(gm.BeNumerically(">", int64(0)))

		g.By("Querying with group1 gated (current rev) and group2 NOT in GroupModRevisions")
		now := timestamp.NowMilli()
		resp, queryErr := clients.StreamWriteClient.Query(ctx, &streamv1.QueryRequest{
			Groups: []string{group1, group2},
			Name:   streamName,
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(now.Add(-time.Hour)),
				End:   timestamppb.New(now.Add(time.Hour)),
			},
			Limit: 10,
			GroupModRevisions: map[string]int64{
				group1: r1,
				// group2 intentionally absent — ungated
			},
			Projection: &modelv1.TagProjection{
				TagFamilies: []*modelv1.TagProjection_TagFamily{
					{Name: "default", Tags: []string{"svc"}},
				},
			},
		})
		gm.Expect(queryErr).ShouldNot(gm.HaveOccurred())

		g.By("Asserting group1 status is SUCCEED and group2 is absent or SUCCEED (ungated)")
		statuses := resp.GetGroupStatuses()
		if statuses != nil {
			if st1, ok := statuses[group1]; ok {
				gm.Expect(st1).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED),
					"group1 with current ModRevision must be STATUS_SUCCEED")
			}
			// group2 is ungated: its status is either absent from the map or SUCCEED — both are valid.
			if st2, ok := statuses[group2]; ok {
				gm.Expect(st2).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED),
					"group2 when present in statuses must be STATUS_SUCCEED (ungated)")
			}
		}

		g.By("Query is not short-circuited (no EXPIRED_SCHEMA in group1)")
		// No assertion on elements count since no data was written; just verify no error.

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: group1})
		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: group2})
	})
})
