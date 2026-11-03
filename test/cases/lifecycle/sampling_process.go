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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

type sampledLifecycleProcess struct {
	command      *exec.Cmd
	done         chan error
	exited       chan struct{}
	waitErr      error
	progressFile string
	logPath      string
	cleanupOnce  sync.Once
}

type sampledProcessCursor struct {
	Part  string `json:"part"`
	Block int    `json:"block"`
	Row   int    `json:"row"`
}

type sampledProcessCheckpoint struct {
	ManifestSHA256 string               `json:"manifest_sha256"`
	Cursor         sampledProcessCursor `json:"cursor"`
	Sequence       uint64               `json:"sequence"`
	Delivered      bool                 `json:"delivered"`
}

type sampledArtifactIdentity struct {
	SHA256        string `json:"sha256"`
	Inode         uint64 `json:"inode"`
	ModifiedNanos int64  `json:"modified_nanos"`
	Bytes         int64  `json:"bytes"`
}

func lifecycleNativeBuildContext() (string, []string) {
	root, err := os.Getwd()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for {
		if _, statErr := os.Stat(filepath.Join(root, "go.mod")); statErr == nil {
			break
		}
		parent := filepath.Dir(root)
		gomega.Expect(parent).NotTo(gomega.Equal(root), "native lifecycle tests require the repository module checkout")
		root = parent
	}
	var flags []string
	if info, valid := debug.ReadBuildInfo(); valid {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "-race", "-trimpath":
				if setting.Value == "true" {
					flags = append(flags, setting.Key)
				}
			case "-tags":
				if setting.Value != "" {
					flags = append(flags, "-tags", setting.Value)
				}
			}
		}
	}
	return root, flags
}

func buildSampledLifecycleBinary(directory string) string {
	root, buildFlags := lifecycleNativeBuildContext()
	binary := filepath.Join(directory, "banyand-lifecycle")
	args := append([]string{"build", "-o", binary}, buildFlags...)
	args = append(args, "./banyand/cmd/lifecycle")
	command := exec.Command("go", args...)
	command.Dir = root
	command.Env = append(os.Environ(), "CGO_ENABLED=1")
	output, err := command.CombinedOutput()
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "native lifecycle host must match sampler build flags: %s", output)
	return binary
}

func startSampledLifecycle(directory, binary, pluginDir string) *sampledLifecycleProcess {
	gomega.Expect(os.MkdirAll(directory, 0o700)).To(gomega.Succeed())
	log, err := os.CreateTemp(directory, "lifecycle-process-*.log")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	progress := filepath.Join(directory, "progress.json")
	args := []string{
		"--grpc-addr=" + SharedContext.DataAddr,
		"--stream-root-path=" + SharedContext.SrcDir,
		"--measure-root-path=" + SharedContext.SrcDir,
		"--trace-root-path=" + SharedContext.SrcDir,
		"--progress-file=" + progress, "--report-dir=" + filepath.Join(directory, "reports"),
		"--node-labels=type=hot", "--trace-sampling-enabled", "--trace-sampling-trusted-dir=" + pluginDir,
		"--row-replay-max-batch-rows=1",
	}
	args = append(args, SharedContext.MetadataFlags...)
	command := exec.Command(binary, args...)
	command.Dir = directory
	command.Env = append(os.Environ(), "POD_NAME=localhost")
	command.Stdout = log
	command.Stderr = log
	gomega.Expect(command.Start()).To(gomega.Succeed())
	process := &sampledLifecycleProcess{command: command, done: make(chan error, 1), exited: make(chan struct{}), progressFile: progress, logPath: log.Name()}
	go func() {
		process.waitErr = command.Wait()
		_ = log.Close()
		process.done <- process.waitErr
		close(process.exited)
	}()
	ginkgo.DeferCleanup(process.cleanup)
	return process
}

func (process *sampledLifecycleProcess) cleanup() {
	process.cleanupOnce.Do(func() {
		select {
		case <-process.exited:
			return
		default:
		}
		_ = process.command.Process.Kill()
		gomega.Eventually(process.exited, 10*time.Second).Should(gomega.BeClosed(), "lifecycle child cleanup must reap process")
	})
}

func readSampledProcessCheckpoint(path string) sampledProcessCheckpoint {
	payload, err := os.ReadFile(path)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var record struct {
		Checkpoint sampledProcessCheckpoint `json:"checkpoint"`
	}
	gomega.Expect(json.Unmarshal(payload, &record)).To(gomega.Succeed())
	return record.Checkpoint
}

func stopAtSampledCheckpoint(process *sampledLifecycleProcess, minimum uint64) (string, sampledProcessCheckpoint) {
	root := filepath.Join(process.progressFile+".sampling", recoveredTraceGroup)
	var artifact string
	gomega.Eventually(func() bool {
		select {
		case <-process.exited:
			ginkgo.Fail(fmt.Sprintf("lifecycle child exited before partial delivery: %v; log=%s", process.waitErr, process.logPath))
		default:
		}
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return nil
			}
			if entry.IsDir() && strings.HasPrefix(entry.Name(), ".preparing-") {
				return filepath.SkipDir
			}
			if entry.IsDir() || entry.Name() != "checkpoint.json" {
				return nil
			}
			payload, readErr := os.ReadFile(path)
			if readErr != nil {
				return nil
			}
			var record struct {
				Checkpoint sampledProcessCheckpoint `json:"checkpoint"`
			}
			if json.Unmarshal(payload, &record) == nil && record.Checkpoint.Sequence > minimum && !record.Checkpoint.Delivered {
				artifact = filepath.Dir(path)
			}
			return nil
		})
		return artifact != ""
	}, 90*time.Second, time.Millisecond).Should(gomega.BeTrue(), "a durable incomplete delivery checkpoint must appear; log=%s", process.logPath)
	gomega.Expect(process.command.Process.Signal(syscall.SIGSTOP)).To(gomega.Succeed())
	gomega.Eventually(func() bool {
		status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", process.command.Process.Pid))
		return err == nil && strings.Contains(string(status), "State:\tT")
	}, time.Second, time.Millisecond).Should(gomega.BeTrue(), "lifecycle child must stop before capturing durable state")
	checkpoint := readSampledProcessCheckpoint(filepath.Join(artifact, "checkpoint.json"))
	gomega.Expect(checkpoint.Sequence).To(gomega.BeNumerically(">", minimum))
	gomega.Expect(checkpoint.Delivered).To(gomega.BeFalse(), "fixture must leave a retained suffix unsent before SIGKILL")
	return artifact, checkpoint
}

func preparedFileIdentity(directory string) map[string]sampledArtifactIdentity {
	result := make(map[string]sampledArtifactIdentity)
	for _, name := range []string{"manifest.json", "decisions", "offsets"} {
		path := filepath.Join(directory, name)
		file, err := os.Open(path)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		digest := sha256.New()
		_, err = io.Copy(digest, file)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(file.Close()).To(gomega.Succeed())
		info, statErr := os.Stat(path)
		gomega.Expect(statErr).NotTo(gomega.HaveOccurred())
		native, ok := info.Sys().(*syscall.Stat_t)
		gomega.Expect(ok).To(gomega.BeTrue())
		result[name] = sampledArtifactIdentity{SHA256: hex.EncodeToString(digest.Sum(nil)), Inode: native.Ino, ModifiedNanos: info.ModTime().UnixNano(), Bytes: info.Size()}
	}
	return result
}

func killSampledLifecycle(process *sampledLifecycleProcess) {
	gomega.Expect(process.command.Process.Kill()).To(gomega.Succeed())
	gomega.Eventually(process.exited, 10*time.Second).Should(gomega.BeClosed(), "SIGKILL child must be reaped")
	gomega.Expect(process.waitErr).To(gomega.HaveOccurred(), "killed child cannot report successful migration completion")
}
