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

package transfer

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/spf13/pflag"

	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exporter"
)

// ExportRun is the outcome of one RunExport call.
type ExportRun struct {
	Err error
	// Log is what the exporter printed to its log writer (stderr for bydbctl).
	Log string
	Out string
}

// ExitCode is the process exit code bydbctl would return: 0 on success.
func (r ExportRun) ExitCode() int {
	if r.Err == nil {
		return 0
	}
	return exporter.ExitCodeFor(r.Err)
}

// SessionID returns the session id the run announced, or "".
func (r ExportRun) SessionID() string {
	const prefix = "export session "
	for _, line := range strings.Split(r.Out, "\n") {
		if strings.HasPrefix(line, prefix) && strings.HasSuffix(line, " created") {
			return strings.TrimSuffix(strings.TrimPrefix(line, prefix), " created")
		}
	}
	return ""
}

// lineWriter collects the exporter's log and calls hook with every line, synchronously, so
// a test can act at a precise point of the export (after the session is created, while a
// node streams, ...).
type lineWriter struct {
	hook    func(line string)
	partial string
	buf     bytes.Buffer
	mu      sync.Mutex
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	w.buf.Write(p)
	text := w.partial + string(p)
	lines := strings.Split(text, "\n")
	w.partial = lines[len(lines)-1]
	w.mu.Unlock()
	if w.hook != nil {
		for _, l := range lines[:len(lines)-1] {
			w.hook(l)
		}
	}
	return len(p), nil
}

func (w *lineWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// DefaultExportOptions are the bydbctl defaults of an export into output, read from the
// option tags, with a parallelism of 4.
func DefaultExportOptions(output string) config.Export {
	var opts config.Export
	fs := pflag.NewFlagSet("export", pflag.ContinueOnError)
	config.Bind(fs, &opts)
	_, err := config.Resolve(fs, nil, &opts)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	opts.Output = output
	opts.Parallelism = 4
	return opts
}

// preflightTimeout bounds connecting to the liaisons; a target still merging an import can
// answer slowly.
const preflightTimeout = 30 * time.Second

// RunExport connects to the liaisons at addrs the way bydbctl does (Preflight, FetchGroups)
// and runs exporter.RunExport with opts. hook, when not nil, sees every line of the log and
// of the output as it is printed, on the printing goroutine.
func RunExport(ctx context.Context, addr string, opts config.Export, hook func(line string), moreAddrs ...string) ExportRun {
	logW := &lineWriter{hook: hook}
	preflightCtx, cancel := context.WithTimeout(ctx, preflightTimeout)
	defer cancel()
	// A preflight failure is returned, not asserted, so a caller retrying inside Eventually
	// (a busy target right after an import) can try again.
	liaisons, cluster, err := exporter.Preflight(preflightCtx, append([]string{addr}, moreAddrs...), exporter.ConnectOptions{Timeout: 10 * time.Second, Log: logW})
	if err != nil {
		return ExportRun{Err: err, Log: logW.String()}
	}
	defer func() { _ = liaisons.Close() }()
	groups, err := exporter.FetchGroups(preflightCtx, liaisons.Primary())
	if err != nil {
		return ExportRun{Err: err, Log: logW.String()}
	}
	outW := &lineWriter{hook: hook}
	err = exporter.RunExport(ctx, exporter.Env{Log: logW, Out: outW, Cluster: cluster, Liaisons: liaisons, Groups: groups}, opts)
	run := ExportRun{Err: err, Log: logW.String(), Out: outW.String()}
	ginkgo.GinkgoWriter.Printf("export into %s: err=%v\n%s%s", opts.Output, err, run.Log, run.Out)
	return run
}
