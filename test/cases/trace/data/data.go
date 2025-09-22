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

// Package data is used to test the trace service.
package data

import (
	"bytes"
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"
	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/yaml"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

//go:embed input/*.yml
var inputFS embed.FS

//go:embed want/*.yml
var wantFS embed.FS

//go:embed testdata/*.json
var dataFS embed.FS

// VerifyFn verify whether the query response matches the wanted result.
var VerifyFn = func(innerGm gm.Gomega, sharedContext helpers.SharedContext, args helpers.Args) {
	i, err := inputFS.ReadFile("input/" + args.Input + ".yml")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	query := &tracev1.QueryRequest{}
	helpers.UnmarshalYAML(i, query)
	query.TimeRange = helpers.TimeRange(args, sharedContext)
	query.Stages = args.Stages
	c := tracev1.NewTraceServiceClient(sharedContext.Connection)
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
	ww, err := wantFS.ReadFile("want/" + args.Want + ".yml")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	want := &tracev1.QueryResponse{}
	unmarshalYAMLWithSpanEncoding(ww, want)
	for i := range want.Traces {
		slices.SortFunc(want.Traces[i].Spans, func(a, b *tracev1.Span) int {
			return bytes.Compare(a.Span, b.Span)
		})
	}
	for i := range resp.Traces {
		slices.SortFunc(resp.Traces[i].Spans, func(a, b *tracev1.Span) int {
			return bytes.Compare(a.Span, b.Span)
		})
	}

	if args.DisOrder {
		// Sort traces by first span's tag for consistency
		slices.SortFunc(want.Traces, func(a, b *tracev1.Trace) int {
			if len(a.Spans) > 0 && len(b.Spans) > 0 && len(a.Spans[0].Tags) > 0 && len(b.Spans[0].Tags) > 0 {
				return strings.Compare(a.Spans[0].Tags[0].Value.GetStr().GetValue(), b.Spans[0].Tags[0].Value.GetStr().GetValue())
			}
			return 0
		})
		slices.SortFunc(resp.Traces, func(a, b *tracev1.Trace) int {
			if len(a.Spans) > 0 && len(b.Spans) > 0 && len(a.Spans[0].Tags) > 0 && len(b.Spans[0].Tags) > 0 {
				return strings.Compare(a.Spans[0].Tags[0].Value.GetStr().GetValue(), b.Spans[0].Tags[0].Value.GetStr().GetValue())
			}
			return 0
		})
		// Sort spans within each trace for consistent ordering
		for _, trace := range want.Traces {
			slices.SortFunc(trace.Spans, func(a, b *tracev1.Span) int {
				if len(a.Tags) > 0 && len(b.Tags) > 0 {
					return strings.Compare(a.Tags[0].Value.GetStr().GetValue(), b.Tags[0].Value.GetStr().GetValue())
				}
				return 0
			})
		}
		for _, trace := range resp.Traces {
			slices.SortFunc(trace.Spans, func(a, b *tracev1.Span) int {
				if len(a.Tags) > 0 && len(b.Tags) > 0 {
					return strings.Compare(a.Tags[0].Value.GetStr().GetValue(), b.Tags[0].Value.GetStr().GetValue())
				}
				return 0
			})
		}
	}
	var extra []cmp.Option
	extra = append(extra, protocmp.IgnoreUnknown(),
		protocmp.Transform())
	success := innerGm.Expect(cmp.Equal(resp, want,
		extra...)).
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

func loadData(stream tracev1.TraceService_WriteClient, metadata *commonv1.Metadata, dataFile string, baseTime time.Time, interval time.Duration) {
	var templates []interface{}
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	gm.Expect(err).ShouldNot(gm.HaveOccurred())
	gm.Expect(json.Unmarshal(content, &templates)).ShouldNot(gm.HaveOccurred())

	for i, template := range templates {
		// Extract span data from template
		templateMap, ok := template.(map[string]interface{})
		gm.Expect(ok).To(gm.BeTrue())

		// Get span data
		spanData, ok := templateMap["span"].(string)
		gm.Expect(ok).To(gm.BeTrue())

		// Get tags data
		tagsData, ok := templateMap["tags"].([]interface{})
		gm.Expect(ok).To(gm.BeTrue())

		// Convert tags to TagValue format
		var tagValues []*modelv1.TagValue
		for _, tag := range tagsData {
			tagBytes, err := json.Marshal(tag)
			gm.Expect(err).ShouldNot(gm.HaveOccurred())
			tagValue := &modelv1.TagValue{}
			gm.Expect(protojson.Unmarshal(tagBytes, tagValue)).ShouldNot(gm.HaveOccurred())
			tagValues = append(tagValues, tagValue)
		}

		// Add timestamp tag as the last tag
		timestamp := baseTime.Add(interval * time.Duration(i))
		timestampTag := &modelv1.TagValue{
			Value: &modelv1.TagValue_Timestamp{
				Timestamp: timestamppb.New(timestamp),
			},
		}
		tagValues = append(tagValues, timestampTag)

		errInner := stream.Send(&tracev1.WriteRequest{
			Metadata: metadata,
			Tags:     tagValues,
			Span:     []byte(spanData),
			Version:  uint64(i + 1),
		})
		gm.Expect(errInner).ShouldNot(gm.HaveOccurred())
	}
}

// Write writes trace data to the database.
func Write(conn *grpclib.ClientConn, name string, baseTime time.Time, interval time.Duration) {
	WriteToGroup(conn, name, "test-trace-group", name, baseTime, interval)
}

// WriteToGroup writes trace data to a specific group.
func WriteToGroup(conn *grpclib.ClientConn, name, group, fileName string, baseTime time.Time, interval time.Duration) {
	metadata := &commonv1.Metadata{
		Name:  name,
		Group: group,
	}
	schema := databasev1.NewTraceRegistryServiceClient(conn)
	resp, err := schema.Get(context.Background(), &databasev1.TraceRegistryServiceGetRequest{Metadata: metadata})
	if err != nil {
		return
	}
	metadata = resp.GetTrace().GetMetadata()

	c := tracev1.NewTraceServiceClient(conn)
	ctx := context.Background()
	writeClient, err := c.Write(ctx)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	loadData(writeClient, metadata, fmt.Sprintf("%s.json", fileName), baseTime, interval)
	gm.Expect(writeClient.CloseSend()).To(gm.Succeed())
	gm.Eventually(func() error {
		_, err := writeClient.Recv()
		return err
	}, flags.EventuallyTimeout).Should(gm.Equal(io.EOF))
}

// unmarshalYAMLWithSpanEncoding decodes YAML with special handling for span data.
// It converts plain strings in the YAML to base64 encoded strings before protobuf unmarshaling.
func unmarshalYAMLWithSpanEncoding(yamlData []byte, response *tracev1.QueryResponse) {
	// First convert YAML to JSON
	j, err := yaml.YAMLToJSON(yamlData)
	gm.Expect(err).NotTo(gm.HaveOccurred())

	// Parse JSON to modify span fields
	var jsonData map[string]interface{}
	err = json.Unmarshal(j, &jsonData)
	gm.Expect(err).NotTo(gm.HaveOccurred())

	// Convert span strings to base64 in traces structure
	if traces, ok := jsonData["traces"].([]interface{}); ok {
		for _, traceInterface := range traces {
			if trace, ok := traceInterface.(map[string]interface{}); ok {
				if spans, ok := trace["spans"].([]interface{}); ok {
					for _, spanInterface := range spans {
						if span, ok := spanInterface.(map[string]interface{}); ok {
							if spanValue, ok := span["span"].(string); ok {
								// Encode the plain string as base64
								span["span"] = base64.StdEncoding.EncodeToString([]byte(spanValue))
							}
						}
					}
				}
			}
		}
	}

	// Convert back to JSON
	modifiedJSON, err := json.Marshal(jsonData)
	gm.Expect(err).NotTo(gm.HaveOccurred())

	// Finally unmarshal to protobuf
	gm.Expect(protojson.Unmarshal(modifiedJSON, response)).To(gm.Succeed())
}

// marshalToJSONWithStringBytes marshals the QueryResponse to JSON with []byte fields as strings instead of base64.
func marshalToJSONWithStringBytes(resp *tracev1.QueryResponse) ([]byte, error) {
	// First marshal to JSON using protojson
	j, err := protojson.Marshal(resp)
	if err != nil {
		return nil, err
	}

	// Parse the JSON to modify byte fields
	var jsonData map[string]interface{}
	err = json.Unmarshal(j, &jsonData)
	if err != nil {
		return nil, err
	}

	// Convert base64 encoded span fields back to strings in traces structure
	if traces, ok := jsonData["traces"].([]interface{}); ok {
		for _, traceInterface := range traces {
			if trace, ok := traceInterface.(map[string]interface{}); ok {
				if spans, ok := trace["spans"].([]interface{}); ok {
					for _, spanInterface := range spans {
						if span, ok := spanInterface.(map[string]interface{}); ok {
							if spanB64, ok := span["span"].(string); ok {
								// Decode base64 back to original string
								spanBytes, err := base64.StdEncoding.DecodeString(spanB64)
								if err != nil {
									// If it's not valid base64, keep the original value
									continue
								}
								span["span"] = string(spanBytes)
							}
						}
					}
				}
			}
		}
	}

	// Convert back to JSON
	return json.Marshal(jsonData)
}

// GetSpanDataAsString extracts the span data as a string from a Span.
// This converts the raw bytes back to the original string value like "trace_001_span_1".
func GetSpanDataAsString(span *tracev1.Span) string {
	if span == nil || len(span.Span) == 0 {
		return ""
	}
	return string(span.Span)
}
