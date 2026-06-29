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

package setup

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	testflags "github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

const localhost = "127.0.0.1"

// ResolveBanyandBinary returns the path to the banyand server binary.
// It returns the value of the BANYAND_BIN environment variable if set and the
// file exists. Otherwise it resolves a path relative to this source file's
// location: two directories up from pkg/test/setup reaches the module root,
// and then descends into banyand/build/bin/dev/banyand-server. An error is
// returned when neither source can produce an existing file; callers should
// treat the error as a signal to skip the test.
func ResolveBanyandBinary() (string, error) {
	if envBin := os.Getenv("BANYAND_BIN"); envBin != "" {
		if _, statErr := os.Stat(envBin); statErr == nil {
			return envBin, nil
		}
		return "", fmt.Errorf("BANYAND_BIN=%q does not exist", envBin)
	}

	// runtime.Caller(0) returns the path to this source file at compile time.
	_, srcFile, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("runtime.Caller failed; set BANYAND_BIN to the banyand server binary path")
	}
	// pkg/test/setup  →  ../../../  →  module root
	moduleRoot := filepath.Join(filepath.Dir(srcFile), "..", "..", "..")
	binPath := filepath.Join(moduleRoot, "banyand", "build", "bin", "dev", "banyand-server")
	absPath, absErr := filepath.Abs(binPath)
	if absErr != nil {
		return "", fmt.Errorf("failed to resolve binary path: %w", absErr)
	}
	if _, statErr := os.Stat(absPath); statErr != nil {
		return "", fmt.Errorf(
			"banyand binary not found at %q (build it with `make -C banyand banyand-server`) or set BANYAND_BIN: %w",
			absPath, statErr,
		)
	}
	return absPath, nil
}

// ExternalCMD launches binPath as a separate OS process with the given flags,
// writing its combined stdout+stderr to a file at logPath.
// It returns a teardown closure that sends SIGTERM to the process and waits up
// to 30 seconds for it to exit, then SIGKILLs any survivor. The closure is
// safe to call exactly once.
func ExternalCMD(binPath, logPath string, flags ...string) (func(), error) {
	logFile, createErr := os.Create(logPath) //nolint:gosec // logPath is caller-controlled
	if createErr != nil {
		return nil, fmt.Errorf("failed to create log file %q: %w", logPath, createErr)
	}

	cmd := exec.Command(binPath, flags...) //nolint:gosec // G204: binPath is resolved by ResolveBanyandBinary
	cmd.Stdout = logFile
	cmd.Stderr = logFile

	if startErr := cmd.Start(); startErr != nil {
		_ = logFile.Close()
		return nil, fmt.Errorf("failed to start %q: %w", binPath, startErr)
	}

	var once sync.Once
	teardown := func() {
		once.Do(func() {
			defer func() { _ = logFile.Close() }()

			if signalErr := cmd.Process.Signal(syscall.SIGTERM); signalErr != nil {
				// Process already gone; best-effort.
				_ = cmd.Wait()
				return
			}

			waitCh := make(chan error, 1)
			go func() { waitCh <- cmd.Wait() }()

			select {
			case <-waitCh:
			case <-time.After(30 * time.Second):
				_ = cmd.Process.Signal(syscall.SIGKILL)
				<-waitCh
			}
		})
	}
	return teardown, nil
}

// standaloneFlags assembles the flag list for an external standalone server,
// mirroring the flag block in standaloneServerWithAuth (setup.go:377-396).
// The returned slice does NOT include the leading "standalone" subcommand.
func standaloneFlags(config *ClusterConfig, path string, ports []int) []string {
	addr := fmt.Sprintf("%s:%d", host, ports[0])
	schemaPort := ports[4]
	schemaAddr := fmt.Sprintf("%s:%d", localhost, schemaPort)
	ff := []string{
		"--logging-env=dev",
		"--logging-level=" + testflags.LogLevel,
		"--grpc-host=" + host,
		fmt.Sprintf("--grpc-port=%d", ports[0]),
		"--http-host=" + host,
		fmt.Sprintf("--http-port=%d", ports[1]),
		"--http-grpc-addr=" + addr,
		"--stream-root-path=" + path,
		"--measure-root-path=" + path,
		"--property-root-path=" + path,
		"--trace-root-path=" + path,
		"--schema-server-root-path=" + path,
		"--schema-registry-mode=" + config.SchemaRegistry.Mode,
		"--node-discovery-mode=" + config.NodeDiscovery.Mode,
		"--node-host-provider=flag",
		"--node-host=" + localhost,
		"--schema-server-grpc-host=" + localhost,
		fmt.Sprintf("--schema-server-grpc-port=%d", schemaPort),
	}
	if config.NodeDiscovery.Mode == ModeFile && config.NodeDiscovery.FileWriter != nil {
		ff = append(ff, fmt.Sprintf("--node-discovery-file-path=%s", config.NodeDiscovery.FileWriter.Path()))
	}
	config.AddSchemaServerAddr(schemaAddr)
	return ff
}

// ExternalStandalone launches an external standalone banyand server, waits for
// it to pass the gRPC and HTTP readiness gates, and returns the gRPC address,
// HTTP address, and a teardown closure.
// ports must contain at least 5 elements: [grpc, http, unused, unused, schema].
// Additional flags are appended after the assembled flag block.
func ExternalStandalone(config *ClusterConfig, binPath, path, logDir string, ports []int, flags ...string) (grpcAddr, httpAddr string, closeFn func()) {
	if config == nil {
		config = newDefaultClusterConfig()
	}
	grpcAddr = fmt.Sprintf("%s:%d", host, ports[0])
	httpAddr = fmt.Sprintf("%s:%d", host, ports[1])

	ff := standaloneFlags(config, path, ports)
	ff = append(ff, flags...)

	cmdFlags := append([]string{"standalone"}, ff...)

	logPath := filepath.Join(logDir, "standalone.log")
	teardown, launchErr := ExternalCMD(binPath, logPath, cmdFlags...)
	gomega.Expect(launchErr).NotTo(gomega.HaveOccurred())

	gomega.Eventually(
		helpers.HealthCheck(grpcAddr, 10*time.Second, 10*time.Second,
			grpclib.WithTransportCredentials(insecure.NewCredentials())),
		testflags.EventuallyTimeout).Should(gomega.Succeed())
	gomega.Eventually(helpers.HTTPHealthCheck(httpAddr, ""), testflags.EventuallyTimeout).Should(gomega.Succeed())

	closeFn = teardown
	return grpcAddr, httpAddr, closeFn
}

// dataNodeFlags assembles the flag list for an external data node,
// mirroring the flag block in startDataNode (setup.go:568-600).
// The returned slice does NOT include the leading "data" subcommand.
// ports must contain at least 3 elements: [grpc, gossip, http]; a 4th element
// is used for the schema server when runSchemaServer is true.
func dataNodeFlags(config *ClusterConfig, dataDir string, ports []int) []string {
	nodeHost := localhost
	runSchemaServer := len(ports) >= 4

	ff := []string{
		"--grpc-host=" + host,
		fmt.Sprintf("--grpc-port=%d", ports[0]),
		fmt.Sprintf("--property-repair-gossip-grpc-port=%d", ports[1]),
		fmt.Sprintf("--http-port=%d", ports[2]),
		"--stream-root-path=" + dataDir,
		"--measure-root-path=" + dataDir,
		"--property-root-path=" + dataDir,
		"--trace-root-path=" + dataDir,
		"--schema-server-root-path=" + dataDir,
		"--node-host-provider", "flag",
		"--node-host", nodeHost,
		"--logging-modules", "trace,sidx,property-schema-registry",
		"--logging-levels", "debug,debug,debug",
		"--schema-registry-mode=" + config.SchemaRegistry.Mode,
		"--node-discovery-mode=" + config.NodeDiscovery.Mode,
	}
	if config.NodeDiscovery.Mode == ModeFile && config.NodeDiscovery.FileWriter != nil {
		ff = append(ff, fmt.Sprintf("--node-discovery-file-path=%s", config.NodeDiscovery.FileWriter.Path()))
	}
	if runSchemaServer {
		schemaPort := ports[3]
		schemaAddr := fmt.Sprintf("%s:%d", nodeHost, schemaPort)
		ff = append(ff,
			"--schema-server-grpc-host="+nodeHost,
			fmt.Sprintf("--schema-server-grpc-port=%d", schemaPort),
		)
		config.AddSchemaServerAddr(schemaAddr)
	}
	return ff
}

// ExternalDataNode launches an external banyand data node, waits for it to
// pass the gRPC readiness gate, and returns the gRPC address and teardown closure.
// ports must contain at least 3 elements: [grpc, gossip, http]; pass 4 to also
// start the schema server (ports[3]).
// Additional flags are appended after the assembled flag block.
func ExternalDataNode(config *ClusterConfig, binPath, dataDir, logDir string, ports []int, flags ...string) (grpcAddr string, closeFn func()) {
	if config == nil {
		config = newDefaultClusterConfig()
	}
	grpcAddr = fmt.Sprintf("%s:%d", host, ports[0])

	ff := dataNodeFlags(config, dataDir, ports)
	ff = append(ff, flags...)

	cmdFlags := append([]string{"data"}, ff...)

	logPath := filepath.Join(logDir, fmt.Sprintf("data-%d.log", ports[0]))
	teardown, launchErr := ExternalCMD(binPath, logPath, cmdFlags...)
	gomega.Expect(launchErr).NotTo(gomega.HaveOccurred())

	gomega.Eventually(
		helpers.HealthCheck(grpcAddr, 10*time.Second, 10*time.Second,
			grpclib.WithTransportCredentials(insecure.NewCredentials())),
		testflags.EventuallyTimeout).Should(gomega.Succeed())

	if config.NodeDiscovery.FileWriter != nil {
		nodeAddr := fmt.Sprintf("%s:%d", localhost, ports[0])
		config.NodeDiscovery.FileWriter.AddNode(nodeAddr, nodeAddr)
	}

	closeFn = func() {
		if config.NodeDiscovery.FileWriter != nil {
			nodeAddr := fmt.Sprintf("%s:%d", localhost, ports[0])
			config.NodeDiscovery.FileWriter.RemoveNode(nodeAddr)
		}
		teardown()
	}
	return grpcAddr, closeFn
}

// liaisonFlags assembles the flag list for an external liaison node,
// mirroring the flag block in startLiaisonNode (setup.go:696-719).
// The returned slice does NOT include the leading "liaison" subcommand.
// ports must contain at least 3 elements: [grpc, http, liaison-server].
func liaisonFlags(config *ClusterConfig, path string, ports []int) []string {
	grpcAddr := fmt.Sprintf("%s:%d", host, ports[0])
	nodeHost := localhost
	ff := []string{
		"--grpc-host=" + host,
		fmt.Sprintf("--grpc-port=%d", ports[0]),
		"--http-host=" + host,
		fmt.Sprintf("--http-port=%d", ports[1]),
		"--liaison-server-grpc-host=" + host,
		fmt.Sprintf("--liaison-server-grpc-port=%d", ports[2]),
		"--http-grpc-addr=" + grpcAddr,
		"--node-host-provider", "flag",
		"--node-host", nodeHost,
		"--stream-root-path=" + path,
		"--measure-root-path=" + path,
		"--trace-root-path=" + path,
		"--stream-flush-timeout=500ms",
		"--measure-flush-timeout=500ms",
		"--trace-flush-timeout=500ms",
		"--stream-sync-interval=1s",
		"--measure-sync-interval=1s",
		"--trace-sync-interval=1s",
		"--logging-modules", "trace,sidx,property-schema-registry",
		"--logging-levels", "debug,debug,debug",
		"--schema-registry-mode=" + config.SchemaRegistry.Mode,
		"--node-discovery-mode=" + config.NodeDiscovery.Mode,
	}
	if config.NodeDiscovery.Mode == ModeFile && config.NodeDiscovery.FileWriter != nil {
		ff = append(ff, fmt.Sprintf("--node-discovery-file-path=%s", config.NodeDiscovery.FileWriter.Path()))
	}
	return ff
}

// ExternalLiaisonNode launches an external banyand liaison node, waits for it
// to pass the HTTP readiness gate, and returns the gRPC address and teardown
// closure. ports must contain at least 3 elements: [grpc, http, liaison-server].
// Additional flags are appended after the assembled flag block.
func ExternalLiaisonNode(config *ClusterConfig, binPath, path, logDir string, ports []int, flags ...string) (grpcAddr string, closeFn func()) {
	if config == nil {
		config = newDefaultClusterConfig()
	}
	grpcAddr = fmt.Sprintf("%s:%d", host, ports[0])
	httpAddr := fmt.Sprintf("%s:%d", host, ports[1])

	ff := liaisonFlags(config, path, ports)
	ff = append(ff, flags...)

	cmdFlags := append([]string{"liaison"}, ff...)

	logPath := filepath.Join(logDir, fmt.Sprintf("liaison-%d.log", ports[0]))
	teardown, launchErr := ExternalCMD(binPath, logPath, cmdFlags...)
	gomega.Expect(launchErr).NotTo(gomega.HaveOccurred())

	gomega.Eventually(helpers.HTTPHealthCheck(httpAddr, ""), testflags.EventuallyTimeout).Should(gomega.Succeed())

	if config.NodeDiscovery.FileWriter != nil {
		nodeAddr := fmt.Sprintf("%s:%d", localhost, ports[2])
		config.NodeDiscovery.FileWriter.AddNode(nodeAddr, nodeAddr)
	}

	closeFn = func() {
		if config.NodeDiscovery.FileWriter != nil {
			nodeAddr := fmt.Sprintf("%s:%d", localhost, ports[2])
			config.NodeDiscovery.FileWriter.RemoveNode(nodeAddr)
		}
		teardown()
	}
	return grpcAddr, closeFn
}
