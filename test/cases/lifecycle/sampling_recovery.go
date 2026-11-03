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
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
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
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const (
	recoveredTraceGroup = "sw_lifecycle_sampled_recovery"
	recoveryTracePairs  = 512
)

var _ = ginkgo.Describe("Trace lifecycle native sampling recovery", func() {
	ginkgo.It("resumes committed sampling after killing the lifecycle process without resampling", func() {
		if runtime.GOOS != "linux" {
			ginkgo.Skip("checkpoint interruption uses Linux SIGSTOP and /proc")
		}
		directory, err := os.MkdirTemp("", "lifecycle-sampling-recovery")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer os.RemoveAll(directory)
		plugins := filepath.Join(directory, "plugins")
		gomega.Expect(os.Mkdir(plugins, 0o700)).To(gomega.Succeed())
		buildLifecycleSampler(plugins)
		binary := buildSampledLifecycleBinary(directory)
		connection, dialErr := grpchelper.Conn(SharedContext.LiaisonAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(dialErr).NotTo(gomega.HaveOccurred())
		defer connection.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		createSampledTraceSchemaForGroup(ctx, connection, recoveredTraceGroup)
		defer func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, cleanupErr := databasev1.NewGroupRegistryServiceClient(connection).Delete(cleanupCtx,
				&databasev1.GroupRegistryServiceDeleteRequest{Group: recoveredTraceGroup, Force: true})
			gomega.Expect(cleanupErr).NotTo(gomega.HaveOccurred())
		}()
		_, timestamp, _ := crossSegmentTimestamps()
		client := tracev1.NewTraceServiceClient(connection)
		for round := 0; round < 3; round++ {
			gomega.Eventually(func() error {
				return writeRecoveryTraceRound(client, timestamp.Add(time.Duration(round)*time.Second), round)
			}, flags.EventuallyTimeout).
				Should(gomega.Succeed())
			gomega.Eventually(func() int { return countSampledCorePartsForGroup(SharedContext.SrcDir, recoveredTraceGroup) }, flags.EventuallyTimeout).
				Should(gomega.BeNumerically(">=", round+1))
		}
		queryRange := &modelv1.TimeRange{Begin: timestamppb.New(timestamp.Add(-time.Hour)), End: timestamppb.New(timestamp.Add(time.Hour))}
		verifyRecoveryTraceInventory(client, queryRange, "hot", false, true)
		ginkgo.By("killing a separate lifecycle process after an acknowledged incomplete cursor")
		initial := startSampledLifecycle(directory, binary, plugins)
		defer initial.cleanup()
		artifact, before := stopAtSampledCheckpoint(initial, 0)
		identities := preparedFileIdentity(artifact)
		verifyRecoveryManifest(artifact)
		gomega.Expect(countSampledCorePartsForGroup(SharedContext.SrcDir, recoveredTraceGroup)).To(gomega.BeNumerically(">", 0))
		killSampledLifecycle(initial)
		gomega.Expect(countSampledCorePartsForGroup(SharedContext.SrcDir, recoveredTraceGroup)).To(gomega.BeNumerically(">", 0), "SIGKILL cannot delete the source")
		gomega.Expect(preparedFileIdentity(artifact)).To(gomega.Equal(identities))
		gomega.Expect(os.Rename(filepath.Join(plugins, "lifecycle-sampler.so"), filepath.Join(plugins, "lifecycle-sampler.so.unavailable"))).To(gomega.Succeed())
		ginkgo.By("restarting with the same progress and an unavailable native sampler")
		resumed := startSampledLifecycle(directory, binary, plugins)
		defer resumed.cleanup()
		resumedArtifact, after := stopAtSampledCheckpoint(resumed, before.Sequence)
		gomega.Expect(resumedArtifact).To(gomega.Equal(artifact))
		gomega.Expect(preparedFileIdentity(artifact)).To(gomega.Equal(identities), "committed decisions must never be regenerated")
		gomega.Expect(after.ManifestSHA256).To(gomega.Equal(before.ManifestSHA256))
		gomega.Expect(after.Sequence).To(gomega.BeNumerically(">", before.Sequence))
		gomega.Expect(after.Cursor).NotTo(gomega.Equal(before.Cursor), "acknowledged source cursor must advance")
		gomega.Expect(resumed.command.Process.Signal(syscall.SIGCONT)).To(gomega.Succeed())
		gomega.Eventually(resumed.done, 2*time.Minute).Should(gomega.Receive(gomega.Succeed()))
		duplicateCopies := verifyRecoveryTraceInventory(client, queryRange, "warm", false, true)
		verifyRecoveryTraceInventory(client, queryRange, "warm", true, true)
		verifyRecoveryTraceInventory(client, queryRange, "hot", false, false)
		verifyRecoveryResumeReport(filepath.Join(directory, "reports"))
		gomega.Expect(countSampledCorePartsForGroup(SharedContext.SrcDir, recoveredTraceGroup)).To(gomega.BeZero())
		_, artifactErr := os.Stat(filepath.Join(initial.progressFile+".sampling", recoveredTraceGroup))
		gomega.Expect(os.IsNotExist(artifactErr)).To(gomega.BeTrue(), "artifacts are reclaimed only after successful source deletion")
		ginkgo.GinkgoWriter.Printf("sampling recovery: sequence %d -> %d, unique kept spans %d, exact retry copies %d\n",
			before.Sequence, after.Sequence, recoveryTracePairs*3, duplicateCopies)
	})
})

func writeRecoveryTraceRound(client tracev1.TraceServiceClient, timestamp time.Time, round int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stream, err := client.Write(ctx)
	if err != nil {
		return err
	}
	for index := 0; index < recoveryTracePairs; index++ {
		for _, kind := range []string{"keep", "drop"} {
			id := fmt.Sprintf("sample-%s-%04d", kind, index)
			request := &tracev1.WriteRequest{
				Metadata: &commonv1.Metadata{Name: sampledTraceSchema, Group: recoveredTraceGroup}, Version: uint64(round + 1),
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
	}
	return drainWriteResult(stream.Recv, stream.CloseSend)
}

func verifyRecoveryManifest(directory string) {
	payload, err := os.ReadFile(filepath.Join(directory, "manifest.json"))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var manifest struct {
		Snapshot   string `json:"snapshot"`
		Traces     uint64 `json:"traces"`
		KeptTraces uint64 `json:"kept_traces"`
		Spans      uint64 `json:"spans"`
		KeptSpans  uint64 `json:"kept_spans"`
	}
	gomega.Expect(json.Unmarshal(payload, &manifest)).To(gomega.Succeed())
	gomega.Expect(manifest.Traces).To(gomega.Equal(uint64(recoveryTracePairs * 2)))
	gomega.Expect(manifest.KeptTraces).To(gomega.Equal(uint64(recoveryTracePairs)))
	gomega.Expect(manifest.Spans).To(gomega.Equal(uint64(recoveryTracePairs * 6)))
	gomega.Expect(manifest.KeptSpans).To(gomega.Equal(uint64(recoveryTracePairs * 3)))
	info, statErr := os.Stat(manifest.Snapshot)
	gomega.Expect(statErr).NotTo(gomega.HaveOccurred())
	gomega.Expect(info.IsDir()).To(gomega.BeTrue())
}

func verifyRecoveryTraceInventory(client tracev1.TraceServiceClient, queryRange *modelv1.TimeRange, stage string, indexed, wantRows bool) uint64 {
	var duplicates uint64
	for begin := 0; begin < recoveryTracePairs; begin += 64 {
		end := begin + 64
		ids := make([]string, 0, 128)
		for index := begin; index < end; index++ {
			ids = append(ids, fmt.Sprintf("sample-keep-%04d", index), fmt.Sprintf("sample-drop-%04d", index))
		}
		if indexed {
			ids = nil
			end = recoveryTracePairs
		}
		var batchDuplicates uint64
		gomega.Eventually(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			request := &tracev1.QueryRequest{
				Groups: []string{recoveredTraceGroup}, Name: sampledTraceSchema, TimeRange: queryRange,
				Stages: []string{stage}, TagProjection: []string{"trace_id", "span_id"}, Limit: uint32(len(ids)),
			}
			if indexed {
				request.Limit = recoveryTracePairs
				request.OrderBy = &modelv1.QueryOrder{IndexRuleName: sampledTraceIndex, Sort: modelv1.Sort_SORT_ASC}
			} else {
				request.Criteria = &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
					Name: "trace_id", Op: modelv1.Condition_BINARY_OP_IN,
					Value: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: ids}}},
				}}}
			}
			response, queryErr := client.Query(ctx, request)
			if queryErr != nil {
				return queryErr
			}
			expected := map[string]map[string]int{}
			for index := begin; index < end; index++ {
				for _, kind := range []string{"keep", "drop"} {
					if !wantRows || stage == "warm" && kind == "drop" {
						continue
					}
					expected[fmt.Sprintf("sample-%s-%04d", kind, index)] = map[string]int{"span-0": 0, "span-1": 0, "span-2": 0}
				}
			}
			for _, result := range response.Traces {
				spans, exists := expected[result.TraceId]
				if !exists {
					return fmt.Errorf("unexpected or dropped trace %s", result.TraceId)
				}
				for _, span := range result.Spans {
					if _, exists = spans[span.SpanId]; !exists {
						return fmt.Errorf("unexpected span identity %s", span.SpanId)
					}
					if string(span.Span) != fmt.Sprintf("payload-%s-%s", result.TraceId, strings.TrimPrefix(span.SpanId, "span-")) {
						return fmt.Errorf("span body changed")
					}
					spans[span.SpanId]++
				}
			}
			var copies uint64
			for _, spans := range expected {
				for _, count := range spans {
					if count == 0 {
						return fmt.Errorf("retained span missing")
					}
					copies += uint64(count - 1)
				}
			}
			batchDuplicates = copies
			return nil
		}, flags.EventuallyTimeout).Should(gomega.Succeed())
		duplicates += batchDuplicates
		if indexed {
			break
		}
	}
	return duplicates
}

func verifyRecoveryResumeReport(directory string) {
	paths, globErr := filepath.Glob(filepath.Join(directory, "*.json"))
	gomega.Expect(globErr).NotTo(gomega.HaveOccurred())
	gomega.Expect(paths).NotTo(gomega.BeEmpty())
	payload, readErr := os.ReadFile(paths[len(paths)-1])
	gomega.Expect(readErr).NotTo(gomega.HaveOccurred())
	var report struct {
		Summary struct {
			TraceMigration struct {
				SyncBreakdown map[string]struct {
					Rows uint64 `json:"row_replay_rows"`
				} `json:"sync_breakdown"`
			} `json:"trace_migration"`
		} `json:"summary"`
		Sampling map[string]struct {
			Manifest struct {
				Group string `json:"group"`
			} `json:"manifest"`
			ResumeCount uint64 `json:"resume_count"`
		} `json:"trace_sampling"`
	}
	gomega.Expect(json.Unmarshal(payload, &report)).To(gomega.Succeed())
	replayedRows := report.Summary.TraceMigration.SyncBreakdown[recoveredTraceGroup].Rows
	gomega.Expect(replayedRows).To(gomega.BeNumerically(">", 0))
	gomega.Expect(replayedRows).To(gomega.BeNumerically("<", recoveryTracePairs*3), "resumed delivery skips acknowledged source rows")
	var resumes uint64
	for _, attempt := range report.Sampling {
		if attempt.Manifest.Group == recoveredTraceGroup {
			resumes += attempt.ResumeCount
		}
	}
	gomega.Expect(resumes).To(gomega.BeNumerically(">", 0))
}
