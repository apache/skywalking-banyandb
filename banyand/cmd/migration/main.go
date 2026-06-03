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

// Package main is the CLI entry point of the BanyanDB measure-migration
// tool. The root command exposes three subcommands:
//
//	copy    - file-level direct copy of measure parts into a data pod's
//	          measure root, with row-level grid alignment against each
//	          target stage's SegmentInterval.
//	verify  - read-only inspection of a previous copy run: source vs
//	          target row counts, target segment grid alignment, sidx
//	          doc counts. Takes the same --copy-config plan as `copy`.
//	analyze - row-level (seriesID, timestamp) duplicate analysis for
//	          one (entry, group): within-part dup rows plus the
//	          src-vs-target multiset diff that explains any gap
//	          reported by `verify`. Read-only.
//
// See banyand/cmd/migration/README.md for the full operator runbook.
package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // registers /debug/pprof/* handlers on http.DefaultServeMux; opt-in via --pprof-addr
	"os"

	"github.com/spf13/cobra"
)

func main() {
	var pprofAddr string
	root := &cobra.Command{
		Use:   "migration",
		Short: "BanyanDB measure data migration tool",
		Long: `migration moves measure data from a backup snapshot, or from a live
data PVC mount, into a target measure-data root.

  copy    - copy backup/live measure part files directly into the target,
            aligning rows to each entry stage's SegmentInterval. Fast,
            preserves all tag content, requires running inside the target
            pod (or on a host that owns the target directory exclusively).
  verify  - re-read the same plan and report per-(entry, group) source
            vs target row counts, target segment grid alignment, and
            sidx doc counts. Read-only; no exit code on mismatch.
  analyze - row-level (seriesID, timestamp) duplicate analysis for one
            (entry, group): scans src parts and the target group, reports
            within-part dup rows and the src-vs-target multiset diff to
            explain any row-count gap reported by 'verify'. Read-only.`,
		PersistentPreRun: func(_ *cobra.Command, _ []string) {
			if pprofAddr == "" {
				return
			}
			go func() {
				log.Printf("pprof listening on http://%s/debug/pprof/", pprofAddr)
				if err := http.ListenAndServe(pprofAddr, nil); err != nil { //nolint:gosec // operator-supplied debug endpoint, no public ACL needed
					log.Printf("pprof server exited: %v", err)
				}
			}()
		},
	}

	root.PersistentFlags().StringVar(&pprofAddr, "pprof-addr", "",
		"if non-empty, expose net/http/pprof on this host:port (e.g. 127.0.0.1:6060)")

	root.AddCommand(newCopyCmd())
	root.AddCommand(newVerifyCmd())
	root.AddCommand(newAnalyzeCmd())

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
