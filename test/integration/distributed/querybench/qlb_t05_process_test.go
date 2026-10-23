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

package querybench

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const qlbT05ChildEnv = "DQB_T05_CACHE_CHILD"

const qlbT05ProcessProbeEnv = "DQB_T05_PROCESS_PROBE"

func runQLBT05CacheProcess(t *testing.T, cfg Config, setting string) ([]Result, error) {
	t.Helper()
	binary, executableErr := os.Executable()
	if executableErr != nil {
		return nil, fmt.Errorf("locate T05 test binary: %w", executableErr)
	}
	reportDir := filepath.Join(cfg.ReportDir, "children", "cache_"+setting)
	if createErr := os.MkdirAll(reportDir, 0o755); createErr != nil {
		return nil, fmt.Errorf("create T05 child directory: %w", createErr)
	}
	logPath := filepath.Join(reportDir, "process.log")
	logFile, createErr := os.Create(logPath)
	if createErr != nil {
		return nil, fmt.Errorf("create T05 child log: %w", createErr)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	command := qlbT05ChildCommand(ctx, binary, reportDir, setting)
	command.Stdout, command.Stderr = logFile, logFile
	runErr := command.Run()
	closeErr := logFile.Close()
	if runErr != nil {
		return nil, fmt.Errorf("run T05 cache=%s child; see %s: %w", setting, logPath, runErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close T05 child log: %w", closeErr)
	}
	shards, readErr := readShards(reportDir)
	if readErr != nil {
		return nil, fmt.Errorf("read T05 child result: %w", readErr)
	}
	if len(shards) != 1 || len(shards[0].Variants) != 2 {
		return nil, fmt.Errorf("T05 child must report exactly two query shapes")
	}
	for _, variant := range shards[0].Variants {
		if variant.BatchMetrics["benchmark_process_id"] != uint64(command.Process.Pid) {
			return nil, fmt.Errorf("T05 child result has the wrong process identity")
		}
	}
	t.Logf("T05 cache=%s child process=%d completed; log=%s", setting, command.Process.Pid, logPath)
	return shards[0].Variants, nil
}

func qlbT05ChildCommand(ctx context.Context, binary, reportDir, setting string) *exec.Cmd {
	command := exec.CommandContext(ctx, binary, "-test.run=^TestDistributedQueryBench$", "-test.v", "-test.timeout=5m")
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, envReportDir+"=") && !strings.HasPrefix(value, qlbT05ChildEnv+"=") {
			command.Env = append(command.Env, value)
		}
	}
	command.Env = append(command.Env, envReportDir+"="+reportDir, qlbT05ChildEnv+"="+setting)
	return command
}

type qlbT05ProcessProbe struct {
	Setting   string
	ReportDir string
	PID       int
}

func TestQLBT05CacheSettingsUseIndependentProcesses(t *testing.T) {
	t.Setenv(envReportDir, "parent-report")
	t.Setenv(qlbT05ChildEnv, "parent-setting")
	binary, executableErr := os.Executable()
	if executableErr != nil {
		t.Fatal(executableErr)
	}
	processes := make(map[int]bool)
	for _, setting := range []string{"off", "on"} {
		reportDir := filepath.Join(t.TempDir(), setting)
		command := qlbT05ChildCommand(t.Context(), binary, reportDir, setting)
		command.Args[1] = "-test.run=^TestQLBT05ChildProcessProbe$"
		command.Env = append(command.Env, qlbT05ProcessProbeEnv+"=1")
		for _, key := range []string{envReportDir, qlbT05ChildEnv} {
			count := 0
			for _, value := range command.Env {
				if len(value) > len(key) && value[:len(key)+1] == key+"=" {
					count++
				}
			}
			if count != 1 {
				t.Fatalf("%s must be replaced once, found %d copies", key, count)
			}
		}
		output, runErr := command.CombinedOutput()
		if runErr != nil {
			t.Fatalf("child %s: %v\n%s", setting, runErr, output)
		}
		content, readErr := os.ReadFile(filepath.Join(reportDir, "probe.json"))
		if readErr != nil {
			t.Fatal(readErr)
		}
		var probe qlbT05ProcessProbe
		if decodeErr := json.Unmarshal(content, &probe); decodeErr != nil {
			t.Fatal(decodeErr)
		}
		if probe.Setting != setting || probe.ReportDir != reportDir || probe.PID != command.Process.Pid || probe.PID == os.Getpid() || processes[probe.PID] {
			t.Fatalf("cache settings must run in distinct fresh processes: %+v", probe)
		}
		processes[probe.PID] = true
	}
}

func TestQLBT05ChildProcessProbe(t *testing.T) {
	if os.Getenv(qlbT05ProcessProbeEnv) != "1" {
		t.Skip("only executed by the process-isolation test")
	}
	reportDir := os.Getenv(envReportDir)
	if createErr := os.MkdirAll(reportDir, 0o755); createErr != nil {
		t.Fatal(createErr)
	}
	content, encodeErr := json.Marshal(qlbT05ProcessProbe{Setting: os.Getenv(qlbT05ChildEnv), ReportDir: reportDir, PID: os.Getpid()})
	if encodeErr != nil {
		t.Fatal(encodeErr)
	}
	if writeErr := os.WriteFile(filepath.Join(reportDir, "probe.json"), content, 0o600); writeErr != nil {
		t.Fatal(writeErr)
	}
}
