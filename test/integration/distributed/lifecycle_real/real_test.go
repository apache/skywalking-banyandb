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

package lifecyclereal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

// TestRealLifecycleAgent runs actual networking nodes and a separate lifecycle process.
// Downloaded fixtures and persistent evidence remain outside the repository.
func TestRealLifecycleAgent(t *testing.T) {
	root := os.Getenv("REAL_TRACE_DIR")
	if root == "" {
		t.Skip("set REAL_TRACE_DIR to the downloaded showcase snapshot")
	}
	executable := os.Getenv("REAL_LIFECYCLE_BIN")
	if executable == "" {
		executable = filepath.Join(root, "bin", "lifecycle")
	}
	require.FileExists(t, executable)
	gomega.RegisterTestingT(t)
	require.NoError(t, logger.Init(logger.Logging{Env: "dev", Level: "warn"}))
	output, err := os.MkdirTemp(root, "agent-e2e-")
	require.NoError(t, err)
	t.Logf("persistent real lifecycle evidence: %s", output)
	var baseline *verificationReport
	for _, interrupted := range []bool{false, true} {
		name := "normal"
		if interrupted {
			name = "recovery"
		}
		t.Run(name, func(t *testing.T) {
			report := runRealScenario(t, root, filepath.Join(output, name), executable, interrupted)
			if interrupted && baseline != nil {
				baselineUnique, recoveredUnique := *baseline, report
				baselineUnique.DuplicateSpans = 0
				recoveredUnique.DuplicateSpans = 0
				require.Equal(t, baselineUnique, recoveredUnique, "recovery must match uninterrupted unique retained payloads")
			}
			if !interrupted {
				baseline = &report
			}
		})
	}
}

func runRealScenario(t *testing.T, root, output, executable string, interrupted bool) verificationReport {
	scenarioStarted := time.Now()
	require.NoError(t, os.MkdirAll(output, 0o750))
	group := &commonv1.Group{}
	traceSchema := &databasev1.Trace{}
	readMetadata(t, filepath.Join(root, "metadata", "group.json"), "group", group)
	readMetadata(t, filepath.Join(root, "metadata", "trace.json"), "trace", traceSchema)
	group.Metadata.ModRevision = 0
	traceSchema.Metadata.ModRevision = 0
	group.ResourceOpts.Ttl = &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_HOUR, Num: 1}
	warm := proto.Clone(group.ResourceOpts.Stages[0]).(*commonv1.LifecycleStage)
	warm.ShardNum = group.ResourceOpts.ShardNum
	warm.Ttl = &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_HOUR, Num: 3650}
	group.ResourceOpts.Stages = []*commonv1.LifecycleStage{warm}
	config, err := structpb.NewStruct(map[string]any{"durationThresholdMs": 500, "keepErrors": true, "healthySampleRate": 0.01})
	require.NoError(t, err)
	group.Pipeline.Plugins = nil
	group.Pipeline.Stages = []*commonv1.StageRule{{Stage: "hot", Plugins: []*commonv1.Plugin{{Name: "real-sampler", Kind: &commonv1.Plugin_Sampler{
		Sampler: &commonv1.SamplerPlugin{Path: "sw-trace-sampler.so", AbiVersion: 1, Config: config},
	}}}}}
	plugins := filepath.Join(output, "plugins")
	copyTree(t, filepath.Join(root, "plugins"), plugins)
	hotRoot := filepath.Join(output, "hot")
	source := filepath.Join(hotRoot, "trace", "data", group.Metadata.Name, "seg-20261012")
	copyTree(t, filepath.Join(root, "source", group.Metadata.Name, "seg-20261012"), source)
	writer := setup.NewDiscoveryFileWriter(output)
	cluster := setup.PropertyClusterConfig(writer)
	hot, _, _, stopHot := setup.DataNodeFromDataDir(cluster, hotRoot, "--node-labels=type=hot", "--trace-flush-timeout=200ms")
	defer stopHot()
	warmAddresses := []string{}
	for i := 0; i < 2; i++ {
		address, _, _, stopWarm := setup.DataNodeFromDataDir(cluster, filepath.Join(output, fmt.Sprintf("warm-%d", i)),
			"--node-labels=type=warm", "--has-meta-role=false", "--trace-flush-timeout=200ms")
		defer stopWarm()
		warmAddresses = append(warmAddresses, address)
	}
	setup.PreloadSchemaViaProperty(cluster, func(ctx context.Context, registry schema.Registry) error {
		if _, createErr := registry.CreateGroup(ctx, group); createErr != nil {
			return createErr
		}
		if _, createErr := registry.CreateTrace(ctx, traceSchema); createErr != nil {
			return createErr
		}
		for _, rule := range readRules(t, root) {
			rule.Metadata.ModRevision = 0
			if _, createErr := registry.CreateIndexRule(ctx, rule); createErr != nil {
				return createErr
			}
		}
		for _, binding := range readBindings(t, root) {
			binding.Metadata.ModRevision = 0
			if _, createErr := registry.CreateIndexRuleBinding(ctx, binding); createErr != nil {
				return createErr
			}
		}
		return nil
	})
	cluster.AddLoadedKinds(schema.KindTrace)
	liaison, stopLiaison := setup.LiaisonNode(cluster, "--data-node-selector=type=hot")
	defer stopLiaison()
	connection, connectionErr := grpchelper.Conn(liaison, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20)))
	require.NoError(t, connectionErr)
	defer connection.Close()
	client := tracev1.NewTraceServiceClient(connection)
	verifyRealHotSource(t, client, traceSchema, filepath.Join(root, "source", group.Metadata.Name, "seg-20261012", "shard-1"))
	progress := filepath.Join(output, "progress.json")
	args := []string{
		"--node-labels=type=hot",
		"--grpc-addr=" + hot, "--node-discovery-mode=file", "--node-discovery-file-path=" + writer.Path(),
		"--stream-root-path=" + hotRoot, "--measure-root-path=" + hotRoot, "--trace-root-path=" + hotRoot,
		"--progress-file=" + progress, "--report-dir=" + filepath.Join(output, "report"), "--trace-sampling-enabled",
		"--trace-sampling-trusted-dir=" + plugins, "--row-replay-max-batch-rows=64",
	}
	evidence := map[string]any{"hot": hot, "warm": warmAddresses, "liaison": liaison, "command": append([]string{executable}, args...), "interrupted": interrupted}
	process, done := startAgent(t, output, "initial", executable, args)
	evidence["initial_pid"] = process.Pid
	artifact := waitCheckpoint(t, progress+".sampling", process, done, 0)
	stopAgent(t, process)
	copyTree(t, artifact, filepath.Join(output, "verified-artifacts"))
	manifestPayload, manifestErr := os.ReadFile(filepath.Join(artifact, "manifest.json"))
	require.NoError(t, manifestErr)
	var pinned struct {
		Parts []string `json:"parts"`
	}
	require.NoError(t, json.Unmarshal(manifestPayload, &pinned))
	require.NotEmpty(t, pinned.Parts)
	verificationSource := filepath.Join(output, "verification-source")
	copyTree(t, filepath.Dir(pinned.Parts[0]), verificationSource)
	if interrupted {
		before := readCheckpoint(t, artifact)
		prepared := artifactIdentity(t, artifact)
		require.DirExists(t, source, "source must remain before final acknowledgment")
		require.Positive(t, nativeSpanCount(t, source), "source core parts remain before final acknowledgment")
		copyTree(t, artifact, filepath.Join(output, "interrupted-artifacts"))
		require.NoError(t, process.Kill())
		require.Error(t, <-done)
		require.Positive(t, nativeSpanCount(t, source), "SIGKILL must preserve source data")
		evidence["interrupted_at"] = time.Now().UTC()
		evidence["interrupted_checkpoint"] = before
		evidence["prepared_identity"] = prepared
		require.NoError(t, os.Rename(filepath.Join(plugins, "sw-trace-sampler.so"), filepath.Join(plugins, "sw-trace-sampler.so-unavailable")))
		process, done = startAgent(t, output, "resumed", executable, args)
		evidence["resumed_pid"] = process.Pid
		waitCheckpoint(t, progress+".sampling", process, done, before.Sequence)
		stopAgent(t, process)
		after := readCheckpoint(t, artifact)
		require.Equal(t, prepared, artifactIdentity(t, artifact), "restart must reuse pinned decisions without loading the now unavailable plugin")
		require.Greater(t, after.Sequence, before.Sequence)
		evidence["resumed_cursor_observed_at"] = time.Now().UTC()
		evidence["resumed_checkpoint"] = after
		require.NoError(t, process.Signal(syscall.SIGCONT))
	} else {
		require.NoError(t, process.Signal(syscall.SIGCONT))
	}
	select {
	case waitErr := <-done:
		require.NoError(t, waitErr)
	case <-time.After(3 * time.Minute):
		_ = process.Kill()
		t.Fatal("lifecycle child exceeded deadline")
	}
	require.NoDirExists(t, source, "source cleanup follows successful delivery")
	evidence["delivery_completed"] = true
	evidence["delivery_elapsed_seconds"] = time.Since(scenarioStarted).Seconds()
	writeEvidence(t, output, evidence)
	verification := verifyRealTraceMigration(t, client, verificationSource, filepath.Join(output, "verified-artifacts"), traceSchema)
	evidence["verification"] = verification
	t.Logf("verified unique traces=%d spans=%d dropped_traces=%d duplicate_delivery_spans=%d output=%s",
		verification.KeptTraces, verification.KeptSpans, verification.DroppedTraces, verification.DuplicateSpans, output)
	evidence["completed"] = true
	evidence["elapsed_seconds"] = time.Since(scenarioStarted).Seconds()
	warmCounts := make([]uint64, 2)
	for i := range warmCounts {
		warmCounts[i] = nativeSpanCount(t, filepath.Join(output, fmt.Sprintf("warm-%d", i), "trace", "data", group.Metadata.Name))
	}
	require.Positive(t, warmCounts[0]+warmCounts[1], "real warm nodes persisted received spans")
	evidence["warm_node_physical_spans"] = warmCounts
	writeEvidence(t, output, evidence)
	return verification
}

func readMetadata(t *testing.T, path, key string, message proto.Message) {
	t.Helper()
	payload, err := os.ReadFile(path)
	require.NoError(t, err)
	var wrapper map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &wrapper))
	require.NoError(t, protojson.Unmarshal(wrapper[key], message))
}

func readRules(t *testing.T, root string) []*databasev1.IndexRule {
	payload, err := os.ReadFile(filepath.Join(root, "metadata", "index-rules.json"))
	require.NoError(t, err)
	var wrapper map[string][]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &wrapper))
	var result []*databasev1.IndexRule
	for _, raw := range wrapper["indexRule"] {
		rule := &databasev1.IndexRule{}
		require.NoError(t, protojson.Unmarshal(raw, rule))
		result = append(result, rule)
	}
	return result
}

func readBindings(t *testing.T, root string) []*databasev1.IndexRuleBinding {
	payload, err := os.ReadFile(filepath.Join(root, "metadata", "index-bindings.json"))
	require.NoError(t, err)
	var wrapper map[string][]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &wrapper))
	var result []*databasev1.IndexRuleBinding
	for _, raw := range wrapper["indexRuleBinding"] {
		binding := &databasev1.IndexRuleBinding{}
		require.NoError(t, protojson.Unmarshal(raw, binding))
		result = append(result, binding)
	}
	return result
}

func copyTree(t *testing.T, source, destination string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o750)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unexpected nonregular fixture file %s", path)
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		_, err = io.Copy(output, input)
		closeErr := output.Close()
		if err != nil {
			return err
		}
		return closeErr
	}))
}

func startAgent(t *testing.T, output, name, executable string, args []string) (*os.Process, <-chan error) {
	t.Helper()
	log, err := os.Create(filepath.Join(output, name+".log"))
	require.NoError(t, err)
	command := exec.Command(executable, args...)
	command.Dir = output
	command.Env = append(os.Environ(), "POD_NAME=localhost")
	command.Stdout = log
	command.Stderr = log
	require.NoError(t, command.Start())
	done := make(chan error, 1)
	go func() { done <- command.Wait(); _ = log.Close() }()
	t.Cleanup(func() { _ = command.Process.Kill() })
	return command.Process, done
}

type checkpoint struct {
	ManifestSHA256 string          `json:"manifest_sha256"`
	Cursor         json.RawMessage `json:"cursor"`
	Sequence       uint64          `json:"sequence"`
	Delivered      bool            `json:"delivered"`
}

func readCheckpoint(t *testing.T, directory string) checkpoint {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join(directory, "checkpoint.json"))
	require.NoError(t, err)
	var envelope struct {
		Checkpoint checkpoint `json:"checkpoint"`
	}
	require.NoError(t, json.Unmarshal(payload, &envelope))
	return envelope.Checkpoint
}

func waitCheckpoint(t *testing.T, root string, process *os.Process, done <-chan error, minimum uint64) string {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("agent ended before interruption: %v", err)
		default:
		}
		found := ""
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil || entry.IsDir() || entry.Name() != "checkpoint.json" || strings.Contains(path, ".preparing-") {
				return nil
			}
			payload, readErr := os.ReadFile(path)
			if readErr != nil {
				return nil
			}
			var envelope struct {
				Checkpoint checkpoint `json:"checkpoint"`
			}
			if json.Unmarshal(payload, &envelope) == nil && envelope.Checkpoint.Sequence > minimum && !envelope.Checkpoint.Delivered {
				found = filepath.Dir(path)
			}
			return nil
		})
		if found != "" {
			return found
		}
		time.Sleep(time.Millisecond)
	}
	_ = process.Kill()
	t.Fatal("no incomplete acknowledged checkpoint appeared")
	return ""
}

func artifactIdentity(t *testing.T, directory string) map[string]any {
	t.Helper()
	result := map[string]any{}
	for _, name := range []string{"manifest.json", "decisions", "offsets"} {
		path := filepath.Join(directory, name)
		payload, err := os.ReadFile(path)
		require.NoError(t, err)
		info, err := os.Stat(path)
		require.NoError(t, err)
		sum := sha256.Sum256(payload)
		stat := info.Sys().(*syscall.Stat_t)
		result[name] = map[string]any{"sha256": hex.EncodeToString(sum[:]), "inode": stat.Ino, "mtime": info.ModTime().UnixNano()}
	}
	return result
}

func nativeSpanCount(t *testing.T, root string) uint64 {
	t.Helper()
	var count uint64
	walkErr := filepath.WalkDir(root, func(path string, entry os.DirEntry, entryErr error) error {
		if os.IsNotExist(entryErr) {
			return nil
		}
		if entryErr != nil {
			return entryErr
		}
		if entry.IsDir() || entry.Name() != "metadata.json" || strings.Contains(path, string(filepath.Separator)+"sidx"+string(filepath.Separator)) {
			return nil
		}
		payload, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		var metadata struct {
			TotalCount uint64 `json:"totalCount"`
		}
		if decodeErr := json.Unmarshal(payload, &metadata); decodeErr != nil {
			return decodeErr
		}
		count += metadata.TotalCount
		return nil
	})
	require.NoError(t, walkErr)
	return count
}

func stopAgent(t *testing.T, process *os.Process) {
	t.Helper()
	require.NoError(t, process.Signal(syscall.SIGSTOP))
	require.Eventually(t, func() bool {
		status, readErr := os.ReadFile(fmt.Sprintf("/proc/%d/status", process.Pid))
		return readErr == nil && strings.Contains(string(status), "State:\tT")
	}, time.Second, time.Millisecond, "child must be stopped before capturing durable state")
}

func writeEvidence(t *testing.T, output string, evidence map[string]any) {
	t.Helper()
	payload, err := json.MarshalIndent(evidence, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(output, "result.json"), payload, 0o600))
}
