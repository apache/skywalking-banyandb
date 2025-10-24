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

// Package data contains integration test cases of the topN.
package data

import (
	"context"
	"embed"
	"regexp"
	"strings"

	"github.com/google/go-cmp/cmp"
	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
	"sigs.k8s.io/yaml"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

//go:embed input/*.yaml
var inputFS embed.FS

//go:embed want/*.yaml
var wantFS embed.FS

//go:embed input/*.ql
var qlFS embed.FS

// VerifyFn verify whether the query response matches the wanted result.
var VerifyFn = func(innerGm gm.Gomega, sharedContext helpers.SharedContext, args helpers.Args) {
	i, err := inputFS.ReadFile("input/" + args.Input + ".yaml")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	query := &measurev1.TopNRequest{}
	helpers.UnmarshalYAML(i, query)
	query.TimeRange = helpers.TimeRange(args, sharedContext)
	query.Stages = args.Stages
	c := measurev1.NewMeasureServiceClient(sharedContext.Connection)
	ctx := context.Background()
	verifyQLWithRequest(ctx, innerGm, args, query, sharedContext.Connection)
	resp, err := c.TopN(ctx, query)
	if args.WantErr {
		if err == nil {
			g.Fail("expect error")
		}
		return
	}
	innerGm.Expect(err).NotTo(gm.HaveOccurred(), query.String())
	if args.WantEmpty {
		innerGm.Expect(resp.Lists).To(gm.BeEmpty())
		return
	}
	if args.Want == "" {
		args.Want = args.Input
	}
	ww, err := wantFS.ReadFile("want/" + args.Want + ".yaml")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	want := &measurev1.TopNResponse{}
	helpers.UnmarshalYAML(ww, want)
	success := innerGm.Expect(cmp.Equal(resp, want,
		protocmp.IgnoreUnknown(),
		protocmp.IgnoreFields(&measurev1.TopNList{}, "timestamp"),
		protocmp.Transform())).
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
	resp, err = c.TopN(ctx, query)
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	innerGm.Expect(resp.Trace).NotTo(gm.BeNil())
	innerGm.Expect(resp.Trace.GetSpans()).NotTo(gm.BeEmpty())
}

// verifyQLWithRequest verifies that the QL file produces an equivalent QueryRequest to the YAML.
func verifyQLWithRequest(ctx context.Context, innerGm gm.Gomega, args helpers.Args, yamlQuery *measurev1.TopNRequest, conn *grpclib.ClientConn) {
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
	measure := schema.NewMockMeasure(ctrl)
	measure.EXPECT().GetMeasure(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.Measure, error) {
		client := databasev1.NewMeasureRegistryServiceClient(conn)
		resp, getErr := client.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: metadata})
		if getErr != nil {
			return nil, getErr
		}
		return resp.Measure, nil
	}).AnyTimes()
	topn := schema.NewMockTopNAggregation(ctrl)
	topn.EXPECT().GetTopNAggregation(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, metadata *commonv1.Metadata) (*databasev1.TopNAggregation, error) {
		client := databasev1.NewTopNAggregationRegistryServiceClient(conn)
		resp, getErr := client.Get(ctx, &databasev1.TopNAggregationRegistryServiceGetRequest{Metadata: metadata})
		if getErr != nil {
			return nil, getErr
		}
		return resp.TopNAggregation, nil
	}).AnyTimes()
	mockRepo.EXPECT().MeasureRegistry().AnyTimes().Return(measure)
	mockRepo.EXPECT().TopNAggregationRegistry().AnyTimes().Return(topn)

	// parse QL to QueryRequest
	query, errStrs := bydbql.ParseQuery(qlQueryStr)
	innerGm.Expect(errStrs).To(gm.BeNil())
	transformer := bydbql.NewTransformer(mockRepo)
	transform, err := transformer.Transform(ctx, query)
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	qlQuery, ok := transform.QueryRequest.(*measurev1.TopNRequest)
	innerGm.Expect(ok).To(gm.BeTrue())
	// ignore timestamp, element_id, and stages fields in comparison
	equal := cmp.Equal(qlQuery, yamlQuery,
		protocmp.IgnoreUnknown(),
		protocmp.IgnoreFields(&measurev1.TopNRequest{}, "time_range", "stages"),
		protocmp.Transform())
	if !equal {
		// empty the time range for better output
		qlQuery.TimeRange = nil
	}
	innerGm.Expect(equal).To(gm.BeTrue(), "QL:\n%s\nYAML:\n%s", qlQuery.String(), yamlQuery.String())

	// simple check the QL can be executed
	client := bydbqlv1.NewBydbQLServiceClient(conn)
	bydbqlResp, err := client.Query(ctx, &bydbqlv1.QueryRequest{
		Query: qlQueryStr,
	})
	if args.WantErr {
		innerGm.Expect(err).To(gm.HaveOccurred())
		return
	}
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	innerGm.Expect(bydbqlResp).NotTo(gm.BeNil())
	_, ok = bydbqlResp.Result.(*bydbqlv1.QueryResponse_TopnResult)
	innerGm.Expect(ok).To(gm.BeTrue())
}
