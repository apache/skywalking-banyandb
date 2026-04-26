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
	"errors"
	"fmt"
	"io"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
)

// wgStreamGroup returns a CATALOG_STREAM Group proto for the write-gate tests.
func wgStreamGroup(name string) *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: name},
		Catalog:  commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        2,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}
}

// wgStreamSpec returns a minimal Stream proto for the write-gate tests.
// Entity is [svc] and the default tag family contains only "svc".
func wgStreamSpec(group, name string) *databasev1.Stream {
	return &databasev1.Stream{
		Metadata: &commonv1.Metadata{Name: name, Group: group},
		Entity:   &databasev1.Entity{TagNames: []string{"svc"}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{
				{Name: "svc", Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		}},
	}
}

// sendSingleStreamWrite opens a bidi write stream, sends one WriteRequest with the
// given ModRevision (and a minimal valid timestamp), and returns the status string
// from the server's first response. The function is intentionally free of entity
// tags so that the revision gate — not the routing layer — controls the outcome.
func sendSingleStreamWrite(
	ctx context.Context,
	client streamv1.StreamServiceClient,
	groupName, streamName string,
	modRevision int64,
) (string, error) {
	writeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	writeClient, dialErr := client.Write(writeCtx)
	if dialErr != nil {
		return "", fmt.Errorf("open write stream: %w", dialErr)
	}
	sendErr := writeClient.Send(&streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{Name: streamName, Group: groupName, ModRevision: modRevision},
		Element: &streamv1.ElementValue{
			Timestamp: timestamppb.New(time.Now().Truncate(time.Millisecond)),
		},
		MessageId: uint64(time.Now().UnixNano()),
	})
	if sendErr != nil {
		return "", fmt.Errorf("send write request: %w", sendErr)
	}
	if closeErr := writeClient.CloseSend(); closeErr != nil {
		return "", fmt.Errorf("close send: %w", closeErr)
	}
	// Collect all responses and return the status from the first one.
	var firstStatus string
	for {
		resp, recvErr := writeClient.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return "", fmt.Errorf("recv response: %w", recvErr)
		}
		if resp != nil && firstStatus == "" {
			firstStatus = resp.Status
		}
	}
	return firstStatus, nil
}

// sendValidStreamWrite opens a bidi write stream, sends one fully-formed WriteRequest
// (including the entity svc tag) with ModRevision == 0, and returns the status string
// from the server's first response. Used to verify the "skip check" path succeeds
// end-to-end.
func sendValidStreamWrite(
	ctx context.Context,
	client streamv1.StreamServiceClient,
	groupName, streamName string,
) (string, error) {
	writeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	writeClient, dialErr := client.Write(writeCtx)
	if dialErr != nil {
		return "", fmt.Errorf("open write stream: %w", dialErr)
	}
	sendErr := writeClient.Send(&streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{Name: streamName, Group: groupName},
		Element: &streamv1.ElementValue{
			ElementId: fmt.Sprintf("wg-%d", time.Now().UnixNano()),
			Timestamp: timestamppb.New(time.Now().Truncate(time.Millisecond)),
			TagFamilies: []*modelv1.TagFamilyForWrite{{
				Tags: []*modelv1.TagValue{{
					Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc-a"}},
				}},
			}},
		},
		MessageId: uint64(time.Now().UnixNano()),
	})
	if sendErr != nil {
		return "", fmt.Errorf("send write request: %w", sendErr)
	}
	if closeErr := writeClient.CloseSend(); closeErr != nil {
		return "", fmt.Errorf("close send: %w", closeErr)
	}
	var firstStatus string
	for {
		resp, recvErr := writeClient.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return "", fmt.Errorf("recv response: %w", recvErr)
		}
		if resp != nil && firstStatus == "" {
			firstStatus = resp.Status
		}
	}
	return firstStatus, nil
}

// sendSingleMeasureWrite opens a bidi measure write stream, sends one WriteRequest with
// the given ModRevision and a minimal "host" entity tag, and returns the status string
// from the server's first response.
func sendSingleMeasureWrite(
	ctx context.Context,
	client measurev1.MeasureServiceClient,
	groupName, measureName string,
	modRevision int64,
) (string, error) {
	writeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	writeClient, dialErr := client.Write(writeCtx)
	if dialErr != nil {
		return "", fmt.Errorf("open measure write stream: %w", dialErr)
	}
	sendErr := writeClient.Send(&measurev1.WriteRequest{
		Metadata: &commonv1.Metadata{Name: measureName, Group: groupName, ModRevision: modRevision},
		DataPoint: &measurev1.DataPointValue{
			Timestamp: timestamppb.New(time.Now().Truncate(time.Millisecond)),
			TagFamilies: []*modelv1.TagFamilyForWrite{{
				Tags: []*modelv1.TagValue{{
					Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "h1"}},
				}},
			}},
			Fields: []*modelv1.FieldValue{{
				Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}},
			}},
		},
		MessageId: uint64(time.Now().UnixNano()),
	})
	if sendErr != nil {
		return "", fmt.Errorf("send measure write request: %w", sendErr)
	}
	if closeErr := writeClient.CloseSend(); closeErr != nil {
		return "", fmt.Errorf("close measure send: %w", closeErr)
	}
	var firstStatus string
	for {
		resp, recvErr := writeClient.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return "", fmt.Errorf("recv measure response: %w", recvErr)
		}
		if resp != nil && firstStatus == "" {
			firstStatus = resp.Status
		}
	}
	return firstStatus, nil
}

// sendMeasureWriteAtTime opens a bidi measure write stream, sends one WriteRequest with the
// caller-specified timestamp and ModRevision, and returns the status string from the first response.
func sendMeasureWriteAtTime(
	ctx context.Context,
	client measurev1.MeasureServiceClient,
	groupName, measureName string,
	ts time.Time,
	modRevision int64,
	tagValues []*modelv1.TagFamilyForWrite,
) (string, error) {
	writeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	writeClient, dialErr := client.Write(writeCtx)
	if dialErr != nil {
		return "", fmt.Errorf("open measure write stream: %w", dialErr)
	}
	sendErr := writeClient.Send(&measurev1.WriteRequest{
		Metadata: &commonv1.Metadata{Name: measureName, Group: groupName, ModRevision: modRevision},
		DataPoint: &measurev1.DataPointValue{
			Timestamp:   timestamppb.New(ts.Truncate(time.Millisecond)),
			TagFamilies: tagValues,
			Fields: []*modelv1.FieldValue{{
				Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}},
			}},
		},
		MessageId: uint64(time.Now().UnixNano()),
	})
	if sendErr != nil {
		return "", fmt.Errorf("send measure write request: %w", sendErr)
	}
	if closeErr := writeClient.CloseSend(); closeErr != nil {
		return "", fmt.Errorf("close measure send: %w", closeErr)
	}
	var firstStatus string
	for {
		resp, recvErr := writeClient.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return "", fmt.Errorf("recv measure response: %w", recvErr)
		}
		if resp != nil && firstStatus == "" {
			firstStatus = resp.Status
		}
	}
	return firstStatus, nil
}

// Schema write gate smoke tests — §4.4.1 / §4.4.2 / §4.4.3.
// Each spec exercises a distinct branch of the three-way ModRevision split.
var _ = g.Describe("Schema write gate", func() {
	var (
		ctx     context.Context
		clients *Clients
	)

	g.BeforeEach(func() {
		ctx = context.Background()
		clients = NewClients(SharedContext.Connection)
	})

	// §4.4.1: a write carrying a client ModRevision below the cached schema revision
	// is rejected immediately with STATUS_EXPIRED_SCHEMA.
	g.It("rejects write with stale ModRevision (§4.4.1)", func() {
		groupName := fmt.Sprintf("wg-stale-%d", time.Now().UnixNano())
		streamName := "wg_stream"

		g.By("Creating stream group")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: wgStreamGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		g.By("Creating stream schema to obtain baseline ModRevision (R1)")
		createResp, createStreamErr := clients.StreamRegClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
			Stream: wgStreamSpec(groupName, streamName),
		})
		gm.Expect(createStreamErr).ShouldNot(gm.HaveOccurred())
		r1 := createResp.GetModRevision()
		gm.Expect(r1).Should(gm.BeNumerically(">", int64(0)), "create response must carry a non-zero mod_revision")

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
		gm.Expect(r2).Should(gm.BeNumerically(">", r1), "update response must carry a mod_revision > R1")

		g.By("Waiting for the cache to reach R2")
		awaitErr := clients.AwaitRevision(ctx, r2, 10*time.Second)
		gm.Expect(awaitErr).ShouldNot(gm.HaveOccurred())

		g.By("Sending a stream write with the old ModRevision R1 (stale)")
		writeStatus, writeErr := sendSingleStreamWrite(ctx, clients.StreamWriteClient, groupName, streamName, r1)
		gm.Expect(writeErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(writeStatus).Should(gm.Equal(modelv1.Status_STATUS_EXPIRED_SCHEMA.String()),
			"write with client ModRevision < cache must return STATUS_EXPIRED_SCHEMA")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §4.4.2: a write carrying a client ModRevision ahead of the cache is held until the
	// configured wait duration and then returned with STATUS_SCHEMA_NOT_APPLIED.
	g.It("returns STATUS_SCHEMA_NOT_APPLIED for ahead ModRevision that never applies (§4.4.2)", func() {
		groupName := fmt.Sprintf("wg-ahead-%d", time.Now().UnixNano())
		streamName := "wg_stream"

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

		g.By("Sending a write with ModRevision = R1 + 99999 (far ahead, will never apply)")
		aheadRev := r1 + 99999
		writeStatus, writeErr := sendSingleStreamWrite(ctx, clients.StreamWriteClient, groupName, streamName, aheadRev)
		gm.Expect(writeErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(writeStatus).Should(gm.Equal(modelv1.Status_STATUS_SCHEMA_NOT_APPLIED.String()),
			"write with client ModRevision > cache must return STATUS_SCHEMA_NOT_APPLIED after wait timeout")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})

	// §4.4.3: a write with ModRevision == 0 skips the revision gate entirely and is accepted.
	g.It("skips the revision check and succeeds when ModRevision is zero (§4.4.3)", func() {
		groupName := fmt.Sprintf("wg-zero-%d", time.Now().UnixNano())
		streamName := "wg_stream"

		g.By("Creating stream group and schema")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: wgStreamGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())

		_, createStreamErr := clients.StreamRegClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
			Stream: wgStreamSpec(groupName, streamName),
		})
		gm.Expect(createStreamErr).ShouldNot(gm.HaveOccurred())

		g.By("Waiting for schema to be applied in cache")
		awaitErr := clients.AwaitApplied(ctx, []string{fmt.Sprintf("stream:%s/%s", groupName, streamName)}, 10*time.Second)
		gm.Expect(awaitErr).ShouldNot(gm.HaveOccurred())

		g.By("Sending a fully valid stream write with ModRevision == 0")
		writeStatus, writeErr := sendValidStreamWrite(ctx, clients.StreamWriteClient, groupName, streamName)
		gm.Expect(writeErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(writeStatus).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()),
			"write with ModRevision == 0 must skip the revision gate and return STATUS_SUCCEED")

		_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
	})
})
