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

// Package data is used to test the trace pipeline service.
package data

import (
	"bytes"
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"io"
	"regexp"
	"slices"
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
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	metadata "github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

//go:embed input/*.yml
var inputFS embed.FS

//go:embed want/*.yml
var wantFS embed.FS

//go:embed input/*.ql
var qlFS embed.FS

//go:embed testdata/*.json
var dataFS embed.FS

// timestampTagIndex is the position of the timestamp tag in the filter trace schema.
// filter.json defines: trace_id(0), span_id(1), timestamp(2), service_id(3), duration(4), status(5).
// The JSON testdata omits timestamp; WriteBatch inserts it at this position.
const timestampTagIndex = 2

// VerifyFn verifies whether the query response matches the wanted result.
var VerifyFn = func(innerGm gm.Gomega, sharedContext helpers.SharedContext, args helpers.Args) {
	i, err := inputFS.ReadFile("input/" + args.Input + ".yml")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	query := &tracev1.QueryRequest{}
	helpers.UnmarshalYAML(i, query)
	if !args.WantErr {
		verifyQLWithRequest(innerGm, args, query, sharedContext.Connection)
	}
	query.TimeRange = helpers.TimeRange(args, sharedContext)
	query.Stages = args.Stages
	c := tracev1.NewTraceServiceClient(sharedContext.Connection)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, queryErr := c.Query(ctx, query)
	if args.WantErr {
		if queryErr == nil {
			g.Fail("expect error")
		}
		return
	}
	innerGm.Expect(queryErr).NotTo(gm.HaveOccurred(), query.String())
	if args.WantEmpty {
		innerGm.Expect(resp.Traces).To(gm.BeEmpty(), func() string {
			var j []byte
			j, err = marshalToJSONWithStringBytes(resp)
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
		return
	}
	if args.Want == "" {
		args.Want = args.Input
	}
	ww, wantErr := wantFS.ReadFile("want/" + args.Want + ".yml")
	innerGm.Expect(wantErr).NotTo(gm.HaveOccurred())
	want := &tracev1.QueryResponse{}
	unmarshalYAMLWithSpanEncoding(ww, want)
	for idx := range want.Traces {
		slices.SortFunc(want.Traces[idx].Spans, func(a, b *tracev1.Span) int {
			return bytes.Compare(a.Span, b.Span)
		})
	}
	for idx := range resp.Traces {
		slices.SortFunc(resp.Traces[idx].Spans, func(a, b *tracev1.Span) int {
			return bytes.Compare(a.Span, b.Span)
		})
	}

	var extra []cmp.Option
	extra = append(extra, protocmp.IgnoreUnknown(), protocmp.Transform())
	success := innerGm.Expect(cmp.Equal(resp, want, extra...)).
		To(gm.BeTrue(), func() string {
			var j []byte
			j, err = marshalToJSONWithStringBytes(resp)
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
	innerGm.Expect(resp.TraceQueryResult).NotTo(gm.BeNil())
	innerGm.Expect(resp.TraceQueryResult.GetSpans()).NotTo(gm.BeEmpty())
}

func verifyQLWithRequest(innerGm gm.Gomega, args helpers.Args, yamlQuery *tracev1.QueryRequest, conn *grpclib.ClientConn) {
	qlContent, err := qlFS.ReadFile("input/" + args.Input + ".ql")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	qlQueryStr, qlParams, err := helpers.ExtractQL(string(qlContent))
	innerGm.Expect(err).NotTo(gm.HaveOccurred())

	// Auto-inject stages clause if args.Stages is not empty.
	if len(args.Stages) > 0 {
		stageClause := " ON " + strings.Join(args.Stages, ", ") + " STAGES"
		re := regexp.MustCompile(`(?i)\s+IN\s+(\([^)]+\)|[a-zA-Z0-9_-]+(?:\s*,\s*[a-zA-Z0-9_-]+)*)`)
		qlQueryStr = re.ReplaceAllString(qlQueryStr, " IN $1"+stageClause)
	}

	ctrl := gomock.NewController(g.GinkgoT())
	defer ctrl.Finish()

	mockRepo := metadata.NewMockRepo(ctrl)
	traceSchema := schema.NewMockTrace(ctrl)
	traceSchema.EXPECT().GetTrace(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, md *commonv1.Metadata) (*databasev1.Trace, error) {
		client := databasev1.NewTraceRegistryServiceClient(conn)
		resp, getErr := client.Get(ctx, &databasev1.TraceRegistryServiceGetRequest{Metadata: md})
		if getErr != nil {
			return nil, getErr
		}
		return resp.GetTrace(), nil
	}).AnyTimes()
	mockRepo.EXPECT().TraceRegistry().AnyTimes().Return(traceSchema)

	parsed, errStrs := bydbql.ParseQuery(qlQueryStr)
	innerGm.Expect(errStrs).To(gm.BeNil())
	innerGm.Expect(bydbql.BindParams(parsed, qlParams)).To(gm.Succeed())

	transformer := bydbql.NewTransformer(mockRepo)
	transformCtx, transformCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer transformCancel()
	result, transformErr := transformer.Transform(transformCtx, parsed)
	innerGm.Expect(transformErr).NotTo(gm.HaveOccurred())

	qlQuery, ok := result.QueryRequest.(*tracev1.QueryRequest)
	innerGm.Expect(ok).To(gm.BeTrue())

	equal := cmp.Equal(qlQuery, yamlQuery,
		protocmp.IgnoreUnknown(),
		protocmp.IgnoreFields(&tracev1.QueryRequest{}, "time_range", "stages"),
		protocmp.Transform())
	if !equal {
		qlQuery.TimeRange = nil
	}
	innerGm.Expect(equal).To(gm.BeTrue(), "QL:\n%s\nYAML:\n%s", qlQuery.String(), yamlQuery.String())

	bydbqlClient := bydbqlv1.NewBydbQLServiceClient(conn)
	qlCtx, qlCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer qlCancel()
	bydbqlResp, qlErr := bydbqlClient.Query(qlCtx, &bydbqlv1.QueryRequest{
		Query:  qlQueryStr,
		Params: qlParams,
	})
	innerGm.Expect(qlErr).NotTo(gm.HaveOccurred())
	innerGm.Expect(bydbqlResp).NotTo(gm.BeNil())
	_, ok = bydbqlResp.Result.(*bydbqlv1.QueryResponse_TraceResult)
	innerGm.Expect(ok).To(gm.BeTrue())
}

// TraceRow is a single trace row for WriteBatchEntries.
// Tags must be in schema order (5 entries): trace_id, span_id, service_id, duration, status.
// WriteBatchEntries inserts the timestamp tag at position 2 (schema index for timestamp).
type TraceRow struct {
	Span string
	Tags []*modelv1.TagValue
}

// WriteBatch writes a single batch file as one write stream (one mem-part).
// The filter trace schema places timestamp at position 2; this function inserts
// the computed timestamp tag at that position so the server can locate it by schema index.
func WriteBatch(conn *grpclib.ClientConn, fileName, group string, baseTime time.Time, interval time.Duration) {
	var templates []interface{}
	content, readErr := dataFS.ReadFile("testdata/" + fileName)
	gm.Expect(readErr).ShouldNot(gm.HaveOccurred())
	gm.Expect(json.Unmarshal(content, &templates)).ShouldNot(gm.HaveOccurred())

	rows := make([]TraceRow, 0, len(templates))
	for _, template := range templates {
		templateMap, ok := template.(map[string]interface{})
		gm.Expect(ok).To(gm.BeTrue())
		spanData, ok := templateMap["span"].(string)
		gm.Expect(ok).To(gm.BeTrue())
		tagsData, ok := templateMap["tags"].([]interface{})
		gm.Expect(ok).To(gm.BeTrue())

		// Decode the JSON tags (5 tags: trace_id, span_id, service_id, duration, status).
		var jsonTags []*modelv1.TagValue
		for _, tag := range tagsData {
			tagBytes, marshalErr := json.Marshal(tag)
			gm.Expect(marshalErr).ShouldNot(gm.HaveOccurred())
			tagValue := &modelv1.TagValue{}
			gm.Expect(protojson.Unmarshal(tagBytes, tagValue)).ShouldNot(gm.HaveOccurred())
			jsonTags = append(jsonTags, tagValue)
		}
		rows = append(rows, TraceRow{Span: spanData, Tags: jsonTags})
	}

	WriteBatchEntries(conn, group, baseTime, interval, rows)
}

// WriteBatchEntries writes in-memory rows as a single write stream (one mem-part).
// Each TraceRow.Tags must contain 5 tags in order [trace_id, span_id, service_id, duration, status];
// WriteBatchEntries inserts the computed timestamp tag at schema position 2.
func WriteBatchEntries(conn *grpclib.ClientConn, group string, baseTime time.Time, interval time.Duration, rows []TraceRow) {
	md := &commonv1.Metadata{
		Name:  "filter",
		Group: group,
	}
	schemaClient := databasev1.NewTraceRegistryServiceClient(conn)
	resp, schemaErr := schemaClient.Get(context.Background(), &databasev1.TraceRegistryServiceGetRequest{Metadata: md})
	// Fail fast: the schema is preloaded and synced before seeding, so a Get error
	// is a setup failure. Returning silently would seed nothing and surface only as
	// a confusing empty-query timeout later.
	gm.Expect(schemaErr).NotTo(gm.HaveOccurred())
	md = resp.GetTrace().GetMetadata()

	c := tracev1.NewTraceServiceClient(conn)
	writeClient, writeErr := c.Write(context.Background())
	gm.Expect(writeErr).NotTo(gm.HaveOccurred())

	for version, row := range rows {
		// Compute the timestamp for this row and build the full tag slice with
		// timestamp inserted at schema position 2: [trace_id, span_id, timestamp, service_id, duration, status].
		ts := baseTime.Add(interval * time.Duration(version)).Truncate(time.Millisecond)
		timestampTag := &modelv1.TagValue{
			Value: &modelv1.TagValue_Timestamp{
				Timestamp: timestamppb.New(ts),
			},
		}
		tagValues := make([]*modelv1.TagValue, 0, len(row.Tags)+1)
		tagValues = append(tagValues, row.Tags[:timestampTagIndex]...)
		tagValues = append(tagValues, timestampTag)
		tagValues = append(tagValues, row.Tags[timestampTagIndex:]...)

		sendErr := writeClient.Send(&tracev1.WriteRequest{
			Metadata: md,
			Tags:     tagValues,
			Span:     []byte(row.Span),
			Version:  uint64(version + 1),
		})
		gm.Expect(sendErr).ShouldNot(gm.HaveOccurred())
	}

	gm.Expect(writeClient.CloseSend()).To(gm.Succeed())
	gm.Eventually(func() error {
		_, recvErr := writeClient.Recv()
		return recvErr
	}, flags.EventuallyTimeout).Should(gm.Equal(io.EOF))
}

// unmarshalYAMLWithSpanEncoding decodes YAML with special handling for span data.
func unmarshalYAMLWithSpanEncoding(yamlData []byte, response *tracev1.QueryResponse) {
	j, err := yaml.YAMLToJSON(yamlData)
	gm.Expect(err).NotTo(gm.HaveOccurred())

	var jsonData map[string]interface{}
	err = json.Unmarshal(j, &jsonData)
	gm.Expect(err).NotTo(gm.HaveOccurred())

	if traces, ok := jsonData["traces"].([]interface{}); ok {
		for _, traceInterface := range traces {
			if traceMap, ok := traceInterface.(map[string]interface{}); ok {
				if spans, ok := traceMap["spans"].([]interface{}); ok {
					for _, spanInterface := range spans {
						if spanMap, ok := spanInterface.(map[string]interface{}); ok {
							if spanValue, ok := spanMap["span"].(string); ok {
								spanMap["span"] = base64.StdEncoding.EncodeToString([]byte(spanValue))
							}
						}
					}
				}
			}
		}
	}

	modifiedJSON, err := json.Marshal(jsonData)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	gm.Expect(protojson.Unmarshal(modifiedJSON, response)).To(gm.Succeed())
}

// marshalToJSONWithStringBytes marshals a QueryResponse to JSON with []byte span fields as strings.
func marshalToJSONWithStringBytes(resp *tracev1.QueryResponse) ([]byte, error) {
	j, err := protojson.Marshal(resp)
	if err != nil {
		return nil, err
	}

	var jsonData map[string]interface{}
	if err = json.Unmarshal(j, &jsonData); err != nil {
		return nil, err
	}

	if traces, ok := jsonData["traces"].([]interface{}); ok {
		for _, traceInterface := range traces {
			if traceMap, ok := traceInterface.(map[string]interface{}); ok {
				if spans, ok := traceMap["spans"].([]interface{}); ok {
					for _, spanInterface := range spans {
						if spanMap, ok := spanInterface.(map[string]interface{}); ok {
							if spanB64, ok := spanMap["span"].(string); ok {
								spanBytes, decodeErr := base64.StdEncoding.DecodeString(spanB64)
								if decodeErr != nil {
									continue
								}
								spanMap["span"] = string(spanBytes)
							}
						}
					}
				}
			}
		}
	}

	return json.Marshal(jsonData)
}
