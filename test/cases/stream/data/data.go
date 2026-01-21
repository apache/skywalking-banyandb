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

// Package data contains integration test cases of the stream.
package data

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/yaml"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

//go:embed input/*.yaml
var inputFS embed.FS

//go:embed want/*.yaml
var wantFS embed.FS

//go:embed testdata/*.json
var dataFS embed.FS

//go:embed input/*.ql
var qlFS embed.FS

// VerifyFn verify whether the query response matches the wanted result.
// It also validates that the corresponding QL file can produce the same QueryRequest.
var VerifyFn = func(innerGm gm.Gomega, sharedContext helpers.SharedContext, args helpers.Args) {
	i, err := inputFS.ReadFile("input/" + args.Input + ".yaml")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	query := &streamv1.QueryRequest{}
	helpers.UnmarshalYAML(i, query)
	verifyQLWithRequest(innerGm, args, query, sharedContext.Connection)
	query.TimeRange = helpers.TimeRange(args, sharedContext)
	query.Stages = args.Stages
	c := streamv1.NewStreamServiceClient(sharedContext.Connection)
	ctx := context.Background()
	resp, err := c.Query(ctx, query)
	if args.WantErr {
		if err == nil {
			g.Fail("expect error")
		}
		return
	}
	innerGm.Expect(err).NotTo(gm.HaveOccurred(), query.String())
	if args.WantEmpty {
		innerGm.Expect(resp.Elements).To(gm.BeEmpty())
		return
	}
	if args.Want == "" {
		args.Want = args.Input
	}
	ww, err := wantFS.ReadFile("want/" + args.Want + ".yaml")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	want := &streamv1.QueryResponse{}
	helpers.UnmarshalYAML(ww, want)
	if args.DisOrder {
		slices.SortFunc(want.Elements, func(a, b *streamv1.Element) int {
			return strings.Compare(a.ElementId, b.ElementId)
		})
		slices.SortFunc(resp.Elements, func(a, b *streamv1.Element) int {
			return strings.Compare(a.ElementId, b.ElementId)
		})
	}
	var extra []cmp.Option
	extra = append(extra, protocmp.IgnoreUnknown(),
		protocmp.IgnoreFields(&streamv1.Element{}, "timestamp"),
		protocmp.Transform())
	if args.IgnoreElementID {
		extra = append(extra, protocmp.IgnoreFields(&streamv1.Element{}, "element_id"))
	}
	success := innerGm.Expect(cmp.Equal(resp, want,
		extra...)).
		To(gm.BeTrue(), func() string {
			var j []byte
			j, err = protojson.Marshal(resp)
			if err != nil {
				return err.Error()
			}
			var y []byte
			y, err = yaml.JSONToYAML(j)
			if err != nil {
				return err.Error()
			}
			return string(y)
		})
	if !success {
		return
	}
	query.Trace = true
	resp, err = c.Query(ctx, query)
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	innerGm.Expect(resp.Trace).NotTo(gm.BeNil())
	innerGm.Expect(resp.Trace.GetSpans()).NotTo(gm.BeEmpty())
}

func loadData(stream streamv1.StreamService_WriteClient, metadata *commonv1.Metadata, dataFile string, baseTime time.Time, interval time.Duration, elementCounter *int) {
	var templates []interface{}
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	gm.Expect(err).ShouldNot(gm.HaveOccurred())
	gm.Expect(json.Unmarshal(content, &templates)).ShouldNot(gm.HaveOccurred())
	gm.Expect(elementCounter).NotTo(gm.BeNil(), "elementCounter must be provided")
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")

	for _, template := range templates {
		rawSearchTagFamily, errMarshal := json.Marshal(template)
		gm.Expect(errMarshal).ShouldNot(gm.HaveOccurred())
		searchTagFamily := &modelv1.TagFamilyForWrite{}
		gm.Expect(protojson.Unmarshal(rawSearchTagFamily, searchTagFamily)).ShouldNot(gm.HaveOccurred())
		elementID := *elementCounter
		*elementCounter++
		e := &streamv1.ElementValue{
			ElementId: strconv.Itoa(elementID),
			Timestamp: timestamppb.New(baseTime.Add(interval * time.Duration(elementID))),
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: []*modelv1.TagValue{
						{
							Value: &modelv1.TagValue_BinaryData{
								BinaryData: bb,
							},
						},
					},
				},
			},
		}
		e.TagFamilies = append(e.TagFamilies, searchTagFamily)
		errInner := stream.Send(&streamv1.WriteRequest{
			Metadata:  metadata,
			Element:   e,
			MessageId: uint64(time.Now().UnixNano()),
		})
		gm.Expect(errInner).ShouldNot(gm.HaveOccurred())
	}
}

func loadDataWithSpec(stream streamv1.StreamService_WriteClient, md *commonv1.Metadata, dataFile string,
	baseTime time.Time, interval time.Duration, spec []*streamv1.TagFamilySpec, elementCounter *int,
) {
	var templates []interface{}
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	gm.Expect(err).ShouldNot(gm.HaveOccurred())
	gm.Expect(json.Unmarshal(content, &templates)).ShouldNot(gm.HaveOccurred())
	gm.Expect(elementCounter).NotTo(gm.BeNil(), "elementCounter must be provided")

	isFirst := true
	for _, template := range templates {
		rawElementValue, errMarshal := json.Marshal(template)
		gm.Expect(errMarshal).ShouldNot(gm.HaveOccurred())
		elementValue := &streamv1.ElementValue{}
		gm.Expect(protojson.Unmarshal(rawElementValue, elementValue)).ShouldNot(gm.HaveOccurred())
		elementID := *elementCounter
		*elementCounter++
		elementValue.ElementId = strconv.Itoa(elementID)
		elementValue.Timestamp = timestamppb.New(baseTime.Add(time.Duration(elementID) * interval))
		req := &streamv1.WriteRequest{Element: elementValue, MessageId: uint64(time.Now().UnixNano())}
		if isFirst {
			req.Metadata = md
			req.TagFamilySpec = spec
			isFirst = false
		}
		gm.Expect(stream.Send(req)).Should(gm.Succeed())
	}
}

// verifyQLWithRequest verifies that the QL file produces an equivalent QueryRequest to the YAML.
func verifyQLWithRequest(innerGm gm.Gomega, args helpers.Args, yamlQuery *streamv1.QueryRequest, conn *grpclib.ClientConn) {
	qlContent, err := qlFS.ReadFile("input/" + args.Input + ".ql")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	var qlQueryStr string
	for _, line := range strings.Split(string(qlContent), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
			if qlQueryStr != "" {
				qlQueryStr += " "
			}
			qlQueryStr += trimmed
		}
	}

	// Auto-inject stages clause if args.Stages is not empty
	if len(args.Stages) > 0 {
		stageClause := " ON " + strings.Join(args.Stages, ", ") + " STAGES"
		// Use regex to find and replace IN clause with groups
		// Pattern: IN followed by groups (with or without parentheses)
		re := regexp.MustCompile(`(?i)\s+IN\s+(\([^)]+\)|[a-zA-Z0-9_-]+(?:\s*,\s*[a-zA-Z0-9_-]+)*)`)
		qlQueryStr = re.ReplaceAllString(qlQueryStr, " IN $1"+stageClause)
	}

	// generate mock metadata repo
	ctrl := gomock.NewController(g.GinkgoT())
	defer ctrl.Finish()
	mockRepo := metadata.NewMockRepo(ctrl)
	stream := schema.NewMockStream(ctrl)
	stream.EXPECT().GetStream(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Stream, error) {
		client := databasev1.NewStreamRegistryServiceClient(conn)
		resp, getErr := client.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{Metadata: metadata})
		if getErr != nil {
			return nil, getErr
		}
		return resp.GetStream(), nil
	}).AnyTimes()
	mockRepo.EXPECT().StreamRegistry().AnyTimes().Return(stream)

	// parse QL to QueryRequest
	query, errStrs := bydbql.ParseQuery(qlQueryStr)
	innerGm.Expect(errStrs).To(gm.BeNil())
	transformer := bydbql.NewTransformer(mockRepo)
	transform, err := transformer.Transform(context.Background(), query)
	if args.WantErr && err != nil {
		return
	}
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	qlQuery, ok := transform.QueryRequest.(*streamv1.QueryRequest)
	innerGm.Expect(ok).To(gm.BeTrue())
	// ignore timestamp, element_id, and stages fields in comparison
	equal := cmp.Equal(qlQuery, yamlQuery,
		protocmp.IgnoreUnknown(),
		protocmp.IgnoreFields(&streamv1.QueryRequest{}, "time_range", "stages"),
		protocmp.Transform())
	if !equal {
		// empty the time range for better output
		qlQuery.TimeRange = nil
	}
	innerGm.Expect(equal).To(gm.BeTrue(), "QL:\n%s\nYAML:\n%s", qlQuery.String(), yamlQuery.String())

	// simple check the QL can be executed
	client := bydbqlv1.NewBydbQLServiceClient(conn)
	ctx := context.Background()
	bydbqlResp, err := client.Query(ctx, &bydbqlv1.QueryRequest{
		Query: qlQueryStr,
	})
	if args.WantErr {
		innerGm.Expect(err).To(gm.HaveOccurred())
		return
	}
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	innerGm.Expect(bydbqlResp).NotTo(gm.BeNil())
	_, ok = bydbqlResp.Result.(*bydbqlv1.QueryResponse_StreamResult)
	innerGm.Expect(ok).To(gm.BeTrue())
}

// Write data into the server.
func Write(conn *grpclib.ClientConn, name string, baseTime time.Time, interval time.Duration) {
	WriteToGroup(conn, name, "default", name, baseTime, interval)
}

// WriteToGroup data into the server with a specific group.
func WriteToGroup(conn *grpclib.ClientConn, name, group, fileName string, baseTime time.Time, interval time.Duration) {
	metadata := &commonv1.Metadata{
		Name:  name,
		Group: group,
	}
	schema := databasev1.NewStreamRegistryServiceClient(conn)
	resp, err := schema.Get(context.Background(), &databasev1.StreamRegistryServiceGetRequest{Metadata: metadata})
	gm.Expect(err).NotTo(gm.HaveOccurred())
	metadata = resp.GetStream().GetMetadata()

	c := streamv1.NewStreamServiceClient(conn)
	ctx := context.Background()
	writeClient, err := c.Write(ctx)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	elementCounter := 0
	loadData(writeClient, metadata, fmt.Sprintf("%s.json", fileName), baseTime, interval, &elementCounter)
	gm.Expect(writeClient.CloseSend()).To(gm.Succeed())
	gm.Eventually(func() error {
		_, err := writeClient.Recv()
		return err
	}, flags.EventuallyTimeout).Should(gm.Equal(io.EOF))
}

// WriteSpec defines the specification for writing stream data.
type WriteSpec struct {
	Metadata *commonv1.Metadata
	DataFile string
	Spec     []*streamv1.TagFamilySpec
}

// WriteMixed writes stream data in schema order first, and then in spec order.
func WriteMixed(conn *grpclib.ClientConn, baseTime time.Time, interval time.Duration, writeSpecs ...WriteSpec) {
	ctx := context.Background()
	schemaClient := databasev1.NewStreamRegistryServiceClient(conn)
	c := streamv1.NewStreamServiceClient(conn)
	writeClient, err := c.Write(ctx)
	gm.Expect(err).NotTo(gm.HaveOccurred())

	var currentMd *commonv1.Metadata
	elementCounter := 0
	currentTime := baseTime
	for idx, ws := range writeSpecs {
		if ws.Metadata != nil {
			resp, getErr := schemaClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{Metadata: ws.Metadata})
			gm.Expect(getErr).NotTo(gm.HaveOccurred())
			currentMd = resp.GetStream().GetMetadata()
		}
		gm.Expect(currentMd).NotTo(gm.BeNil(), "first WriteSpec must have Metadata")

		var templates []interface{}
		content, readErr := dataFS.ReadFile("testdata/" + ws.DataFile)
		gm.Expect(readErr).ShouldNot(gm.HaveOccurred())
		gm.Expect(json.Unmarshal(content, &templates)).ShouldNot(gm.HaveOccurred())

		if idx == 0 {
			loadData(writeClient, currentMd, ws.DataFile, currentTime, interval, &elementCounter)
		} else {
			var mdToSend *commonv1.Metadata
			if ws.Metadata != nil {
				mdToSend = currentMd
			}
			loadDataWithSpec(writeClient, mdToSend, ws.DataFile, currentTime, interval, ws.Spec, &elementCounter)
		}
		currentTime = currentTime.Add(time.Duration(len(templates)) * interval)
	}

	gm.Expect(writeClient.CloseSend()).To(gm.Succeed())
	gm.Eventually(func() error {
		_, recvErr := writeClient.Recv()
		return recvErr
	}, flags.EventuallyTimeout).Should(gm.Equal(io.EOF))
}

// loadDataWithElementIDMap loads data with element IDs specified in the data file for deduplication tests.
func loadDataWithElementIDMap(stream streamv1.StreamService_WriteClient, metadata *commonv1.Metadata, dataFile string, baseTime time.Time, interval time.Duration) {
	var templates []map[string]interface{}
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	gm.Expect(err).ShouldNot(gm.HaveOccurred())
	gm.Expect(json.Unmarshal(content, &templates)).ShouldNot(gm.HaveOccurred())
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")

	for idx, template := range templates {
		// Extract element_id from the template (required for deduplication tests)
		elementIDStr, ok := template["element_id"].(string)
		gm.Expect(ok).To(gm.BeTrue(), "element_id is required in data file for deduplication tests")
		elementID, parseErr := strconv.Atoi(elementIDStr)
		gm.Expect(parseErr).ShouldNot(gm.HaveOccurred())
		// Create a copy without element_id for tag family parsing
		templateCopy := make(map[string]interface{})
		for k, v := range template {
			if k != "element_id" {
				templateCopy[k] = v
			}
		}
		rawSearchTagFamily, errMarshal := json.Marshal(templateCopy)
		gm.Expect(errMarshal).ShouldNot(gm.HaveOccurred())
		searchTagFamily := &modelv1.TagFamilyForWrite{}
		gm.Expect(protojson.Unmarshal(rawSearchTagFamily, searchTagFamily)).ShouldNot(gm.HaveOccurred())
		e := &streamv1.ElementValue{
			ElementId: strconv.Itoa(elementID),
			Timestamp: timestamppb.New(baseTime.Add(interval * time.Duration(idx))),
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: []*modelv1.TagValue{
						{
							Value: &modelv1.TagValue_BinaryData{
								BinaryData: bb,
							},
						},
					},
				},
			},
		}
		e.TagFamilies = append(e.TagFamilies, searchTagFamily)
		errInner := stream.Send(&streamv1.WriteRequest{
			Metadata:  metadata,
			Element:   e,
			MessageId: uint64(time.Now().UnixNano()),
		})
		gm.Expect(errInner).ShouldNot(gm.HaveOccurred())
	}
}

// WriteDeduplicationTest writes data with element IDs specified in the data file for deduplication tests.
func WriteDeduplicationTest(conn *grpclib.ClientConn, name string, baseTime time.Time, interval time.Duration) {
	metadata := &commonv1.Metadata{
		Name:  name,
		Group: "default",
	}
	schema := databasev1.NewStreamRegistryServiceClient(conn)
	resp, err := schema.Get(context.Background(), &databasev1.StreamRegistryServiceGetRequest{Metadata: metadata})
	gm.Expect(err).NotTo(gm.HaveOccurred())
	metadata = resp.GetStream().GetMetadata()

	c := streamv1.NewStreamServiceClient(conn)
	ctx := context.Background()
	writeClient, err := c.Write(ctx)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	loadDataWithElementIDMap(writeClient, metadata, fmt.Sprintf("%s.json", name), baseTime, interval)
	gm.Expect(writeClient.CloseSend()).To(gm.Succeed())
	gm.Eventually(func() error {
		_, err := writeClient.Recv()
		return err
	}, flags.EventuallyTimeout).Should(gm.Equal(io.EOF))
}
