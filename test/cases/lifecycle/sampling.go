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

package lifecycle_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/pipeline/sdk"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const (
	sampledTraceGroup  = "sw_lifecycle_sampled_trace"
	sampledTraceSchema = "sampled_trace"
	sampledTraceIndex  = "sampled_timestamp"
)

var _ = ginkgo.Describe("Trace lifecycle native sampling", ginkgo.Ordered, func() {
	ginkgo.It("retains whole traces and rebuilds destination indexes before deleting the source", func() {
		directory, err := os.MkdirTemp("", "lifecycle-native-sampling")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer os.RemoveAll(directory)
		pluginDirectory := filepath.Join(directory, "plugins")
		gomega.Expect(os.Mkdir(pluginDirectory, 0o700)).To(gomega.Succeed())
		buildLifecycleSampler(pluginDirectory)
		connection, dialErr := grpchelper.Conn(SharedContext.LiaisonAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(dialErr).NotTo(gomega.HaveOccurred())
		defer connection.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		createSampledTraceSchema(ctx, connection)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, cleanupErr := databasev1.NewGroupRegistryServiceClient(connection).Delete(cleanupCtx,
				&databasev1.GroupRegistryServiceDeleteRequest{Group: sampledTraceGroup, Force: true})
			gomega.Expect(cleanupErr).NotTo(gomega.HaveOccurred())
		}()
		_, left, right := crossSegmentTimestamps()
		client := tracev1.NewTraceServiceClient(connection)
		for round := 0; round < 3; round++ {
			timestamp := left.Add(time.Duration(round) * time.Second)
			if round == 2 {
				timestamp = right
			}
			gomega.Eventually(func() error { return writeSampledSpans(client, timestamp, round) }, flags.EventuallyTimeout).Should(gomega.Succeed())
			// Wait for persistence between writes so one trace is present in several source parts.
			gomega.Eventually(func() int { return countSampledCoreParts(SharedContext.SrcDir) }, flags.EventuallyTimeout).
				Should(gomega.BeNumerically(">=", round+1))
		}
		queryRange := &modelv1.TimeRange{Begin: timestamppb.New(left.Add(-time.Hour)), End: timestamppb.New(right.Add(time.Hour))}
		verifySampledTraceQuery(client, queryRange, "hot", []string{"sample-drop", "sample-keep"}, false)
		verifySampledTraceQuery(client, queryRange, "hot", []string{"sample-drop", "sample-keep"}, true)
		reports := filepath.Join(directory, "reports")
		ginkgo.By("executing the real lifecycle command with native plugin hosting")
		runLifecycleMigration(filepath.Join(directory, "progress.json"), reports,
			"--trace-sampling-enabled", "--trace-sampling-trusted-dir", pluginDirectory, "--node-labels", "type=hot")
		verifySampledTraceQuery(client, queryRange, "warm", []string{"sample-keep"}, false)
		verifySampledTraceQuery(client, queryRange, "warm", []string{"sample-keep"}, true)
		verifySampledTraceQuery(client, queryRange, "hot", nil, false)
		gomega.Expect(countSampledCoreParts(SharedContext.SrcDir)).To(gomega.BeZero(), "successful sampled migration removes source parts")
		verifySampledTraceReport(reports)
		entries, readErr := os.ReadDir(filepath.Join(directory, "progress.json.sampling"))
		gomega.Expect(readErr).NotTo(gomega.HaveOccurred())
		for _, entry := range entries {
			gomega.Expect(entry.Name()).NotTo(gomega.Equal(sampledTraceGroup), "artifacts remain until source deletion, then are reclaimed")
		}
	})
})

func buildLifecycleSampler(directory string) {
	root, buildFlags := lifecycleNativeBuildContext()
	args := append([]string{"build", "-buildmode=plugin", "-o", filepath.Join(directory, "lifecycle-sampler.so")}, buildFlags...)
	args = append(args, "./test/cases/lifecycle/testdata/sampling-plugin")
	command := exec.Command("go", args...)
	command.Dir = root
	command.Env = append(os.Environ(), "CGO_ENABLED=1")
	output, err := command.CombinedOutput()
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "native sampler must build with the lifecycle test toolchain: %s", output)
}

func createSampledTraceSchema(ctx context.Context, connection *grpc.ClientConn) {
	createSampledTraceSchemaForGroup(ctx, connection, sampledTraceGroup)
}

func createSampledTraceSchemaForGroup(ctx context.Context, connection *grpc.ClientConn, group string) {
	_, err := databasev1.NewGroupRegistryServiceClient(connection).Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: group}, Catalog: commonv1.Catalog_CATALOG_TRACE, ResourceOpts: orphanStages(),
		Pipeline: &commonv1.TracePipelineConfig{Enabled: true, SchemaNames: []string{sampledTraceSchema}, Stages: []*commonv1.StageRule{{
			Stage: "hot",
			Plugins: []*commonv1.Plugin{{Name: "whole-trace-e2e", Kind: &commonv1.Plugin_Sampler{Sampler: &commonv1.SamplerPlugin{
				Path: "lifecycle-sampler.so", AbiVersion: sdk.ABIVersion,
			}}}},
		}}},
	}})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = databasev1.NewTraceRegistryServiceClient(connection).Create(ctx, &databasev1.TraceRegistryServiceCreateRequest{Trace: &databasev1.Trace{
		Metadata:       &commonv1.Metadata{Name: sampledTraceSchema, Group: group},
		TraceIdTagName: "trace_id", SpanIdTagName: "span_id", TimestampTagName: "timestamp",
		Tags: []*databasev1.TraceTagSpec{
			{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "span_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			{Name: "timestamp", Type: databasev1.TagType_TAG_TYPE_TIMESTAMP},
		},
	}})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = databasev1.NewIndexRuleRegistryServiceClient(connection).Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{IndexRule: &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: sampledTraceIndex, Group: group}, Tags: []string{"timestamp"}, Type: databasev1.IndexRule_TYPE_TREE,
	}})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = databasev1.NewIndexRuleBindingRegistryServiceClient(connection).Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
		IndexRuleBinding: &databasev1.IndexRuleBinding{
			Metadata: &commonv1.Metadata{Name: "sampled-index-binding", Group: group},
			Rules:    []string{sampledTraceIndex}, Subject: &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_TRACE, Name: sampledTraceSchema},
			BeginAt: timestamppb.New(time.Unix(0, 0)), ExpireAt: timestamppb.New(time.Now().AddDate(100, 0, 0)),
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func writeSampledSpans(client tracev1.TraceServiceClient, timestamp time.Time, round int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := client.Write(ctx)
	if err != nil {
		return err
	}
	for _, id := range []string{"sample-drop", "sample-keep"} {
		request := &tracev1.WriteRequest{
			Metadata: &commonv1.Metadata{Name: sampledTraceSchema, Group: sampledTraceGroup}, Version: uint64(round + 1),
			Tags: []*modelv1.TagValue{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: id}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: fmt.Sprintf("span-%d", round)}}},
				{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(timestamp)}},
			}, Span: []byte(fmt.Sprintf("payload-%s-%d", id, round)),
		}
		if err = stream.Send(request); err != nil {
			return err
		}
	}
	return drainWriteResult(stream.Recv, stream.CloseSend)
}

func countSampledCoreParts(root string) int {
	return countSampledCorePartsForGroup(root, sampledTraceGroup)
}

func countSampledCorePartsForGroup(root, group string) int {
	count := 0
	_ = filepath.WalkDir(filepath.Join(root, "trace", "data", group), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if entry.IsDir() && entry.Name() == "sidx" {
			return filepath.SkipDir
		}
		if entry.IsDir() && len(entry.Name()) == 16 {
			if _, err = os.Stat(filepath.Join(path, "primary.bin")); err == nil {
				count++
			}
		}
		return nil
	})
	return count
}

func verifySampledTraceQuery(client tracev1.TraceServiceClient, queryRange *modelv1.TimeRange, stage string, wantIDs []string, indexed bool) {
	gomega.Eventually(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		request := &tracev1.QueryRequest{
			Groups: []string{sampledTraceGroup}, Name: sampledTraceSchema, TimeRange: queryRange,
			Stages: []string{stage}, TagProjection: []string{"trace_id", "span_id"}, Limit: 100,
		}
		if indexed {
			request.OrderBy = &modelv1.QueryOrder{IndexRuleName: sampledTraceIndex, Sort: modelv1.Sort_SORT_ASC}
		} else {
			request.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
				Name: "trace_id", Op: modelv1.Condition_BINARY_OP_IN,
				Value: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: []string{"sample-drop", "sample-keep"}}}},
			}}}
		}
		response, err := client.Query(ctx, request)
		if err != nil {
			return err
		}
		ids := make([]string, 0, len(response.Traces))
		for _, trace := range response.Traces {
			ids = append(ids, trace.TraceId)
			spanIDs := make([]string, 0, len(trace.Spans))
			for _, span := range trace.Spans {
				spanIDs = append(spanIDs, span.SpanId)
				if string(span.Span) != fmt.Sprintf("payload-%s-%s", trace.TraceId, strings.TrimPrefix(span.SpanId, "span-")) {
					return fmt.Errorf("retained span body changed: %s", span.SpanId)
				}
			}
			sort.Strings(spanIDs)
			if fmt.Sprint(spanIDs) != "[span-0 span-1 span-2]" {
				return fmt.Errorf("trace %s lost spans: %v", trace.TraceId, spanIDs)
			}
		}
		sort.Strings(ids)
		if fmt.Sprint(ids) != fmt.Sprint(wantIDs) {
			return fmt.Errorf("stage %s indexed=%v: want %v got %v", stage, indexed, wantIDs, ids)
		}
		return nil
	}, flags.EventuallyTimeout).Should(gomega.Succeed())
}

func verifySampledTraceReport(directory string) {
	paths, err := filepath.Glob(filepath.Join(directory, "*.json"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(paths).NotTo(gomega.BeEmpty())
	payload, err := os.ReadFile(paths[len(paths)-1])
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var report struct {
		Sampling map[string]struct {
			Manifest struct {
				Group     string `json:"group"`
				Input     uint64 `json:"traces"`
				Kept      uint64 `json:"kept_traces"`
				Spans     uint64 `json:"spans"`
				KeptSpans uint64 `json:"kept_spans"`
			} `json:"manifest"`
		} `json:"trace_sampling"`
	}
	gomega.Expect(json.Unmarshal(payload, &report)).To(gomega.Succeed())
	found := false
	for _, entry := range report.Sampling {
		if entry.Manifest.Group != sampledTraceGroup {
			continue
		}
		found = true
		gomega.Expect(entry.Manifest.Input).To(gomega.Equal(uint64(2)))
		gomega.Expect(entry.Manifest.Kept).To(gomega.Equal(uint64(1)))
		gomega.Expect(entry.Manifest.Spans).To(gomega.Equal(uint64(6)))
		gomega.Expect(entry.Manifest.KeptSpans).To(gomega.Equal(uint64(3)))
	}
	gomega.Expect(found).To(gomega.BeTrue(), "real lifecycle report must contain committed sampling decisions")
}
