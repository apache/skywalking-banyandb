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
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/spf13/pflag"

	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/transfer/cluster"
	"github.com/apache/skywalking-banyandb/pkg/transfer/config"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
)

// ImportRun is the outcome of one RunImport call.
type ImportRun struct {
	Err    error
	Result *importer.Result
	// Log is what the importer printed to its log writer (stderr for bydbctl); Out is the plan.
	Log string
	Out string
}

// ExitCode is the process exit code bydbctl would return: 0 on success.
func (r ImportRun) ExitCode() int {
	if r.Err == nil {
		return 0
	}
	return exitcode.ExitCodeFor(r.Err)
}

// DefaultImportOptions are the bydbctl defaults of an import of input, read from the option
// tags, with --verify-timeout at least the suite's eventually timeout.
func DefaultImportOptions(input string) config.Import {
	var opts config.Import
	fs := pflag.NewFlagSet("import", pflag.ContinueOnError)
	config.Bind(fs, &opts)
	_, err := config.Resolve(fs, nil, &opts)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	opts.Input = input
	opts.VerifyTimeout = max(opts.VerifyTimeout, flags.EventuallyTimeout)
	return opts
}

// RunImport connects to the liaison at addr the way bydbctl does (Preflight) and runs
// importer.RunImport with opts. hook, when not nil, sees every line of the log and of the
// output as it is printed, and every acknowledged batch as "batch <work item> seq=<n>
// rows=<r>" right after its acknowledgement, on the pushing goroutine, so a test can stop a
// node in the middle of an import.
func RunImport(ctx context.Context, addr string, opts config.Import, hook func(event string)) ImportRun {
	logW := &lineWriter{hook: hook}
	preflightCtx, cancel := context.WithTimeout(ctx, flags.EventuallyTimeout)
	defer cancel()
	liaisons, info, err := cluster.Preflight(preflightCtx, []string{addr}, cluster.ConnectOptions{Timeout: 10 * time.Second, Log: logW})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	defer func() { _ = liaisons.Close() }()
	outW := &lineWriter{hook: hook}
	res, err := importer.RunImport(ctx, importer.Env{Log: logW, Out: outW, Cluster: info, Liaisons: liaisons, Hook: hook}, opts)
	run := ImportRun{Err: err, Result: res, Log: logW.String(), Out: outW.String()}
	ginkgo.GinkgoWriter.Printf("import of %s: err=%v\n%s%s", opts.Input, err, run.Out, run.Log)
	return run
}
