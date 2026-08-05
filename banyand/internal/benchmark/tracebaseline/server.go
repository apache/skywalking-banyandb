// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership. The ASF licenses this
// file to you under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package tracebaseline

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/controller"
	"github.com/apache/skywalking-banyandb/banyand/internal/benchmark/tracefixture"
	storagetrace "github.com/apache/skywalking-banyandb/banyand/trace"
)

// ServerOptions configures the measured data-node process.
//
// ExecutionIdentity holds the harness-recorded fields forwarded by the
// wrapper; the readiness gate (SAME TEST BOUNDARY) enforces the harness
// requirement that ImageDigest, CloneMethod, BinarySHA256, and (for
// retain-all pipeline runs) PluginSHA256 are populated. The server itself
// tolerates empty values so out-of-harness invocations (e.g. local debug)
// remain runnable; the readiness gate, not the server, is the enforcement
// boundary.
type ServerOptions struct {
	ExpectedLedger    map[string]string
	ExecutionIdentity ExecutionIdentity
	ScheduleSHA256    string
	ProfileDir        string
	Commit            string
	FixtureSHA256     string
	Root              string
	RunID             string
	Mode              string
	OutputPath        string
	SocketPath        string
	Acceleration      float64
	ExpectedRows      uint64
	MaxInputPartID    uint64
	Attribution       bool
}

type benchmarkServer struct {
	receiver        *storagetrace.BenchmarkPartReceiver
	primaryProfile  *phaseProfiler
	cooldownProfile *phaseProfiler
	options         ServerOptions
	report          RunReport
	mu              sync.Mutex
}

type phaseProfiler struct {
	file   *os.File
	active bool
}

// Serve runs the measured data-node control plane on a Unix socket.
func Serve(ctx context.Context, options ServerOptions) (serveResultErr error) {
	if options.Root == "" || options.SocketPath == "" || options.OutputPath == "" {
		return fmt.Errorf("root, socket path, and output path are required")
	}
	if mkdirErr := os.MkdirAll(options.ProfileDir, 0o755); mkdirErr != nil {
		return fmt.Errorf("cannot create profile directory: %w", mkdirErr)
	}
	eventPath := filepath.Join(options.ProfileDir, "merge-events.jsonl")
	eventFile, eventErr := os.Create(eventPath)
	if eventErr != nil {
		return fmt.Errorf("cannot create merge event file: %w", eventErr)
	}
	receiver, receiverErr := storagetrace.NewBenchmarkMergeReceiver( //nolint:contextcheck // The storage constructor has no context parameter.
		options.Root, storagetrace.BenchmarkMergeReceiverOptions{
			LogicalNow: time.Unix(0, 1), MergeGrace: 2 * time.Hour, EventWriter: eventFile, MaxInputPartID: options.MaxInputPartID,
			Attribution: options.Attribution,
		})
	if receiverErr != nil {
		return errors.Join(fmt.Errorf("cannot open measured merge receiver: %w", receiverErr), eventFile.Close())
	}
	server := &benchmarkServer{receiver: receiver, options: options}
	defer func() {
		serveResultErr = errors.Join(serveResultErr, server.stopProfiles(), receiver.Close(), eventFile.Close())
	}()
	server.report = RunReport{
		Version: 1, RunID: options.RunID, Mode: options.Mode, Acceleration: options.Acceleration,
		FixtureSHA256: options.FixtureSHA256, ScheduleSHA256: options.ScheduleSHA256, ExpectedRows: options.ExpectedRows,
		ExpectedLedger: options.ExpectedLedger,
	}
	server.report.Environment = readEnvironment(options.Commit, options)
	if removeErr := os.RemoveAll(options.SocketPath); removeErr != nil {
		return fmt.Errorf("cannot remove stale benchmark socket: %w", removeErr)
	}
	listener, listenErr := net.Listen("unix", options.SocketPath)
	if listenErr != nil {
		return fmt.Errorf("cannot listen on benchmark socket: %w", listenErr)
	}
	if chmodErr := os.Chmod(options.SocketPath, 0o666); chmodErr != nil {
		return errors.Join(fmt.Errorf("cannot expose benchmark socket: %w", chmodErr), listener.Close())
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/health", server.health)
	mux.HandleFunc("/measurement/start", server.startMeasurement)
	mux.HandleFunc("/publish", server.publish)
	mux.HandleFunc("/idle", server.idle)
	mux.HandleFunc("/primary/end", server.endPrimary)
	mux.HandleFunc("/cooldown/run", server.runCooldown)
	mux.HandleFunc("/report", server.writeReport)
	httpServer := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	serveErrCh := make(chan error, 1)
	go func() { serveErrCh <- httpServer.Serve(listener) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancelShutdown := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancelShutdown()
		if shutdownErr := httpServer.Shutdown(shutdownCtx); shutdownErr != nil {
			return fmt.Errorf("cannot shut down benchmark server: %w", shutdownErr)
		}
	case serveErr := <-serveErrCh:
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			return fmt.Errorf("benchmark server failed: %w", serveErr)
		}
	}
	return nil
}

func (bs *benchmarkServer) health(responseWriter http.ResponseWriter, _ *http.Request) {
	responseWriter.WriteHeader(http.StatusNoContent)
}

func (bs *benchmarkServer) startMeasurement(responseWriter http.ResponseWriter, _ *http.Request) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if !bs.report.Primary.StartedAt.IsZero() {
		http.Error(responseWriter, "measurement already started", http.StatusConflict)
		return
	}
	primary := PhaseResult{Name: "primary", StartedAt: time.Now(), Start: readResourceSnapshot()}
	profile, profileErr := startPhaseProfiler(filepath.Join(bs.options.ProfileDir, "primary-cpu.pprof"))
	if profileErr != nil {
		http.Error(responseWriter, profileErr.Error(), http.StatusInternalServerError)
		return
	}
	bs.report.Primary = primary
	bs.primaryProfile = profile
	responseWriter.WriteHeader(http.StatusNoContent)
}

func (bs *benchmarkServer) publish(responseWriter http.ResponseWriter, request *http.Request) {
	var publish PublishRequest
	if decodeErr := json.NewDecoder(request.Body).Decode(&publish); decodeErr != nil {
		http.Error(responseWriter, decodeErr.Error(), http.StatusBadRequest)
		return
	}
	partName := fmt.Sprintf("%016x", publish.PartID)
	corePath := filepath.Join(bs.options.Root, partName)
	indexPaths := map[string]string{
		LedgerLatency:   filepath.Join(bs.options.Root, "sidx", LedgerLatency, partName),
		LedgerStartTime: filepath.Join(bs.options.Root, "sidx", LedgerStartTime, partName),
	}
	if publishErr := bs.receiver.PublishExistingPart(publish.PartID, corePath, indexPaths, publish.LogicalNow); publishErr != nil {
		http.Error(responseWriter, publishErr.Error(), http.StatusInternalServerError)
		return
	}
	if bs.options.Mode == ModeSerial {
		waitCtx, cancelWait := context.WithTimeout(request.Context(), 5*time.Minute)
		defer cancelWait()
		if waitErr := bs.receiver.WaitForMergeIdle(waitCtx); waitErr != nil {
			http.Error(responseWriter, waitErr.Error(), http.StatusInternalServerError)
			return
		}
	}
	status, statusErr := bs.receiver.MergeStatus()
	if statusErr != nil {
		http.Error(responseWriter, statusErr.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(responseWriter, status)
}

func (bs *benchmarkServer) idle(responseWriter http.ResponseWriter, request *http.Request) {
	waitCtx, cancelWait := context.WithTimeout(request.Context(), 5*time.Minute)
	defer cancelWait()
	if waitErr := bs.receiver.WaitForMergeIdle(waitCtx); waitErr != nil {
		http.Error(responseWriter, waitErr.Error(), http.StatusInternalServerError)
		return
	}
	responseWriter.WriteHeader(http.StatusNoContent)
}

func (bs *benchmarkServer) endPrimary(responseWriter http.ResponseWriter, request *http.Request) {
	var boundary PublishRequest
	if decodeErr := json.NewDecoder(request.Body).Decode(&boundary); decodeErr != nil {
		http.Error(responseWriter, decodeErr.Error(), http.StatusBadRequest)
		return
	}
	drainStarted := time.Now()
	waitCtx, cancelWait := context.WithTimeout(request.Context(), 10*time.Minute)
	defer cancelWait()
	if advanceErr := bs.receiver.AdvanceMergeTime(waitCtx, boundary.LogicalNow, storagetrace.BenchmarkMergePhaseDrain); advanceErr != nil {
		profileErr := bs.stopPrimaryProfile()
		http.Error(responseWriter, errors.Join(advanceErr, profileErr).Error(), http.StatusInternalServerError)
		return
	}
	bs.mu.Lock()
	profileErr := bs.primaryProfile.stop()
	bs.primaryProfile = nil
	bs.report.Primary.DrainNanos = time.Since(drainStarted).Nanoseconds()
	bs.report.Primary.FinishedAt = time.Now()
	bs.report.Primary.WallNanos = bs.report.Primary.FinishedAt.Sub(bs.report.Primary.StartedAt).Nanoseconds()
	bs.report.Primary.End = readResourceSnapshot()
	runtimeProfileErr := writeRuntimeProfiles(bs.options.ProfileDir, "primary")
	bs.mu.Unlock()
	if phaseErr := errors.Join(profileErr, runtimeProfileErr); phaseErr != nil {
		http.Error(responseWriter, phaseErr.Error(), http.StatusInternalServerError)
		return
	}
	responseWriter.WriteHeader(http.StatusNoContent)
}

func (bs *benchmarkServer) runCooldown(responseWriter http.ResponseWriter, request *http.Request) {
	var boundary PublishRequest
	if decodeErr := json.NewDecoder(request.Body).Decode(&boundary); decodeErr != nil {
		http.Error(responseWriter, decodeErr.Error(), http.StatusBadRequest)
		return
	}
	bs.mu.Lock()
	bs.report.Cooldown = PhaseResult{Name: "cooldown", StartedAt: time.Now(), Start: readResourceSnapshot()}
	profile, profileErr := startPhaseProfiler(filepath.Join(bs.options.ProfileDir, "cooldown-cpu.pprof"))
	if profileErr != nil {
		bs.mu.Unlock()
		http.Error(responseWriter, profileErr.Error(), http.StatusInternalServerError)
		return
	}
	bs.cooldownProfile = profile
	bs.mu.Unlock()
	waitCtx, cancelWait := context.WithTimeout(request.Context(), 10*time.Minute)
	defer cancelWait()
	if advanceErr := bs.receiver.AdvanceMergeTime(waitCtx, boundary.LogicalNow, storagetrace.BenchmarkMergePhaseCooldown); advanceErr != nil {
		stopProfileErr := bs.stopCooldownProfile()
		http.Error(responseWriter, errors.Join(advanceErr, stopProfileErr).Error(), http.StatusInternalServerError)
		return
	}
	bs.mu.Lock()
	profileErr = bs.cooldownProfile.stop()
	bs.cooldownProfile = nil
	bs.report.Cooldown.FinishedAt = time.Now()
	bs.report.Cooldown.WallNanos = bs.report.Cooldown.FinishedAt.Sub(bs.report.Cooldown.StartedAt).Nanoseconds()
	bs.report.Cooldown.DrainNanos = bs.report.Cooldown.WallNanos
	bs.report.Cooldown.End = readResourceSnapshot()
	runtimeProfileErr := writeRuntimeProfiles(bs.options.ProfileDir, "cooldown")
	bs.mu.Unlock()
	if phaseErr := errors.Join(profileErr, runtimeProfileErr); phaseErr != nil {
		http.Error(responseWriter, phaseErr.Error(), http.StatusInternalServerError)
		return
	}
	responseWriter.WriteHeader(http.StatusNoContent)
}

func (bs *benchmarkServer) writeReport(responseWriter http.ResponseWriter, request *http.Request) {
	inventory, inventoryErr := bs.receiver.MergeInventory()
	if inventoryErr != nil {
		http.Error(responseWriter, inventoryErr.Error(), http.StatusInternalServerError)
		return
	}
	mergeReport, mergeErr := bs.receiver.MergeRecordingReport()
	if mergeErr != nil {
		http.Error(responseWriter, mergeErr.Error(), http.StatusInternalServerError)
		return
	}
	ledgerCtx, cancelLedger := context.WithTimeout(request.Context(), 10*time.Minute)
	defer cancelLedger()
	actualLedger, ledgerErr := tracefixture.LogicalLedgerChecksums(ledgerCtx, bs.receiver)
	if ledgerErr != nil {
		http.Error(responseWriter, ledgerErr.Error(), http.StatusInternalServerError)
		return
	}
	bs.mu.Lock()
	bs.report.Inventory = inventory
	bs.report.Merges = mergeReport
	bs.report.ActualLedger = actualLedger
	bs.report.LedgerVerified = equalLedgerChecksums(bs.report.ExpectedLedger, actualLedger)
	bs.report.SamplingCalls = 0
	for eventIdx := range mergeReport.Events {
		event := &mergeReport.Events[eventIdx]
		bs.report.SamplingCalls += event.PluginCalls
	}
	bs.report.HotMerges, bs.report.MatureMerges = countMergeTemperatures(mergeReport.Events)
	bs.report.Correct = inventory.CoreRows == bs.report.ExpectedRows && inventory.IndexRows[LedgerLatency] == bs.report.ExpectedRows &&
		inventory.IndexRows[LedgerStartTime] == bs.report.ExpectedRows && bs.report.SamplingCalls == 0 && bs.report.LedgerVerified
	bs.report.LogicalWriteAmplification = logicalWriteAmplification(mergeReport)
	report := bs.report
	bs.mu.Unlock()
	reportData, marshalErr := json.MarshalIndent(report, "", "  ")
	if marshalErr != nil {
		http.Error(responseWriter, marshalErr.Error(), http.StatusInternalServerError)
		return
	}
	if writeErr := os.WriteFile(bs.options.OutputPath, reportData, 0o600); writeErr != nil {
		http.Error(responseWriter, writeErr.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(responseWriter, report)
}

func countMergeTemperatures(events []storagetrace.BenchmarkMergeEvent) (hotMerges, matureMerges int) {
	for eventIdx := range events {
		event := &events[eventIdx]
		if event.HotInputParts > 0 {
			hotMerges++
		}
		if event.MatureInputParts > 0 {
			matureMerges++
		}
	}
	return hotMerges, matureMerges
}

// logicalWriteAmplification reports logical compressed bytes written by core
// and secondary-index merges divided by the corresponding selected input
// bytes. Process I/O counters and final shard inventory are not part of this
// logical metric.
func logicalWriteAmplification(report storagetrace.BenchmarkMergeReport) float64 {
	var inputBytes, outputBytes uint64
	for eventIdx := range report.Events {
		event := &report.Events[eventIdx]
		inputBytes += event.InputBytes
		outputBytes += event.OutputBytes
		for childIdx := range event.Children {
			child := &event.Children[childIdx]
			inputBytes += child.InputBytes
			outputBytes += child.OutputBytes
		}
	}
	if inputBytes == 0 {
		return 0
	}
	return float64(outputBytes) / float64(inputBytes)
}

func equalLedgerChecksums(expected, actual map[string]string) bool {
	if len(expected) == 0 || len(expected) != len(actual) {
		return false
	}
	for ledgerName, expectedChecksum := range expected {
		if actual[ledgerName] != expectedChecksum {
			return false
		}
	}
	return true
}

func writeJSON(responseWriter http.ResponseWriter, value any) {
	responseWriter.Header().Set("Content-Type", "application/json")
	if encodeErr := json.NewEncoder(responseWriter).Encode(value); encodeErr != nil {
		http.Error(responseWriter, encodeErr.Error(), http.StatusInternalServerError)
	}
}

func startPhaseProfiler(path string) (*phaseProfiler, error) {
	profileFile, createErr := os.Create(path)
	if createErr != nil {
		return nil, fmt.Errorf("cannot create CPU profile %q: %w", path, createErr)
	}
	if startErr := pprof.StartCPUProfile(profileFile); startErr != nil {
		return nil, errors.Join(fmt.Errorf("cannot start CPU profile %q: %w", path, startErr), profileFile.Close())
	}
	return &phaseProfiler{file: profileFile, active: true}, nil
}

func (pp *phaseProfiler) stop() error {
	if pp == nil || !pp.active {
		return nil
	}
	pprof.StopCPUProfile()
	pp.active = false
	if closeErr := pp.file.Close(); closeErr != nil {
		return fmt.Errorf("cannot close CPU profile %q: %w", pp.file.Name(), closeErr)
	}
	return nil
}

func (bs *benchmarkServer) stopPrimaryProfile() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	profileErr := bs.primaryProfile.stop()
	bs.primaryProfile = nil
	return profileErr
}

func (bs *benchmarkServer) stopCooldownProfile() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	profileErr := bs.cooldownProfile.stop()
	bs.cooldownProfile = nil
	return profileErr
}

func (bs *benchmarkServer) stopProfiles() error {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	primaryErr := bs.primaryProfile.stop()
	bs.primaryProfile = nil
	cooldownErr := bs.cooldownProfile.stop()
	bs.cooldownProfile = nil
	return errors.Join(primaryErr, cooldownErr)
}

func writeRuntimeProfiles(root, phase string) error {
	var profileErrs []error
	for _, profileName := range []string{"heap", "allocs", "block", "mutex"} {
		profile := pprof.Lookup(profileName)
		if profile == nil {
			continue
		}
		profileFile, createErr := os.Create(filepath.Join(root, phase+"-"+profileName+".pprof"))
		if createErr != nil {
			profileErrs = append(profileErrs, fmt.Errorf("cannot create %s %s profile: %w", phase, profileName, createErr))
			continue
		}
		writeErr := profile.WriteTo(profileFile, 0)
		closeErr := profileFile.Close()
		if profileErr := errors.Join(writeErr, closeErr); profileErr != nil {
			profileErrs = append(profileErrs, fmt.Errorf("cannot write %s %s profile: %w", phase, profileName, profileErr))
		}
	}
	return errors.Join(profileErrs...)
}

func readEnvironment(commit string, options ServerOptions) Environment {
	identity, _ := controller.CurrentResourceIdentity(os.Getpid())
	hostname, _ := os.Hostname()
	kernelData, _ := os.ReadFile("/proc/sys/kernel/osrelease")
	filesystem := options.ExecutionIdentity.Filesystem
	storageDevice := options.ExecutionIdentity.StorageDevice
	if filesystem == "" || storageDevice == "" {
		detectedFilesystem, detectedStorageDevice := detectFilesystemAndDevice(options.Root)
		if filesystem == "" {
			filesystem = detectedFilesystem
		}
		if storageDevice == "" {
			storageDevice = detectedStorageDevice
		}
	}
	return Environment{
		Commit: commit, GoVersion: runtime.Version(), Kernel: strings.TrimSpace(string(kernelData)), CgroupVersion: "2",
		CPUSet: readTextFile("/sys/fs/cgroup/cpuset.cpus.effective"), MemoryMax: readTextFile("/sys/fs/cgroup/memory.max"),
		MemorySwapMax: readTextFile("/sys/fs/cgroup/memory.swap.max"), PIDsMax: readTextFile("/sys/fs/cgroup/pids.max"),
		GOMAXPROCS:     runtime.GOMAXPROCS(0),
		OneShardOnly:   true,
		DataNodePID:    os.Getpid(),
		DataNodeCgroup: hostname + ":" + identity.Cgroup,
		Filesystem:     filesystem,
		StorageDevice:  storageDevice,
		ImageDigest:    options.ExecutionIdentity.ImageDigest,
		CloneMethod:    options.ExecutionIdentity.CloneMethod,
		BinarySHA256:   options.ExecutionIdentity.BinarySHA256,
		PluginSHA256:   options.ExecutionIdentity.PluginSHA256,
	}
}

// mountInfoReader returns the raw contents of /proc/self/mountinfo. Tests can
// substitute the function to exercise mount parsing without touching the host
// filesystem.
var mountInfoReader = func() ([]byte, error) { return os.ReadFile("/proc/self/mountinfo") }

// detectFilesystemAndDevice reports the filesystem type and underlying device
// for the mount that backs the given path. It parses /proc/self/mountinfo so the
// measured process reflects its own view of the data-root mount. The longest
// matching mount point wins so /run/data is attributed to its actual bind mount
// rather than to the root filesystem. Returns empty strings when the path is
// not mounted or the info file is unreadable. readEnvironment uses this live
// detection only when the harness did not record the host filesystem or device.
func detectFilesystemAndDevice(path string) (fstype, device string) {
	return detectFilesystemAndDeviceWithReader(path)
}

// detectFilesystemAndDeviceWithReader is the indirection-friendly form used by
// tests; production callers go through detectFilesystemAndDevice which reads
// the live /proc/self/mountinfo.
func detectFilesystemAndDeviceWithReader(path string) (fstype, device string) {
	if path == "" {
		return "", ""
	}
	data, readErr := mountInfoReader()
	if readErr != nil {
		return "", ""
	}
	absPath, absErr := filepath.Abs(path)
	if absErr != nil {
		return "", ""
	}
	absPath = filepath.Clean(absPath)
	var bestMountPoint string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		// mountinfo format: mount_id parent_major:minor root mount_point options - fstype source super_options
		if len(fields) < 10 {
			continue
		}
		mountPoint := filepath.Clean(fields[4])
		if !pathWithinMount(absPath, mountPoint) {
			continue
		}
		if len(mountPoint) <= len(bestMountPoint) {
			continue
		}
		bestMountPoint = mountPoint
		for sepIdx := 6; sepIdx < len(fields)-1; sepIdx++ {
			if fields[sepIdx] == "-" {
				if sepIdx+2 < len(fields) {
					fstype = fields[sepIdx+1]
					device = fields[sepIdx+2]
				}
				break
			}
		}
	}
	if bestMountPoint == "" {
		return "", ""
	}
	return fstype, device
}

func pathWithinMount(path, mountPoint string) bool {
	if mountPoint == string(filepath.Separator) {
		return filepath.IsAbs(path)
	}
	return path == mountPoint || strings.HasPrefix(path, mountPoint+string(filepath.Separator))
}

func readResourceSnapshot() ResourceSnapshot {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	result := ResourceSnapshot{At: time.Now(), Allocated: memory.TotalAlloc, Allocations: memory.Mallocs, HeapBytes: memory.HeapAlloc}
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) == nil {
		result.CPUNanos = usage.Utime.Nano() + usage.Stime.Nano()
		result.RSSBytes = uint64(usage.Maxrss) * 1024
	}
	ioFile, ioErr := os.Open("/proc/self/io")
	if ioErr == nil {
		scanner := bufio.NewScanner(ioFile)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) != 2 {
				continue
			}
			value, valueErr := strconv.ParseUint(fields[1], 10, 64)
			if valueErr != nil {
				continue
			}
			switch strings.TrimSuffix(fields[0], ":") {
			case "read_bytes":
				result.ReadBytes = value
			case "write_bytes":
				result.WriteBytes = value
			}
		}
		_ = ioFile.Close()
	}
	cpuStat := readKeyValueFile("/sys/fs/cgroup/cpu.stat")
	result.CgroupCPUUsec = cpuStat["usage_usec"]
	result.CgroupPeak, _ = strconv.ParseUint(readTextFile("/sys/fs/cgroup/memory.peak"), 10, 64)
	return result
}

func readTextFile(path string) string {
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(data))
}

func readKeyValueFile(path string) map[string]uint64 {
	result := make(map[string]uint64)
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		return result
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		value, parseErr := strconv.ParseUint(fields[1], 10, 64)
		if parseErr == nil {
			result[fields[0]] = value
		}
	}
	return result
}
