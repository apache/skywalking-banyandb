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

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/transfer/importer"
)

// runTTLRows prints how many rows of the import decision table of artifact A are expired
// now, as "ttlExpected: <n>", for expected/import-dryrun.yaml:
//
//	checker ttl-rows <artifact A>
//
// Which units are older than the destination's TTL depends on the day the e2e runs (the source's
// hour/day metrics may or may not have moved to warm), so the expectation is computed with the
// importer's own TTL gate (importer.Decide): the destination's groups are A's (the import
// creates them from A, and plan-dst.yaml overwrites the pre-created sw_records with A's), and
// --force-ttl is set, as in import-dryrun.sh. It exits 2 when the artifact cannot be read.
func runTTLRows(args []string) int {
	if len(args) != 1 {
		usage()
	}
	art, err := importer.OpenArtifact(context.Background(), args[0], false)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ttl-rows:", err)
		return 2
	}
	target := importer.NewTargetState()
	target.Schemas = art.Schemas
	plan := importer.Decide(importer.Inputs{Now: time.Now(), Artifact: art, Target: target, Flags: importer.Flags{ForceTTL: true}})
	fmt.Printf("ttlExpected: %d\n", ttlTableRows(plan))
	return 0
}

// ttlTableRows counts the decision table rows whose STATUS is expired: the importer renders one row per
// (catalog, group, target stage), marked when any of its work items is expired.
func ttlTableRows(plan *importer.Plan) int {
	rows := map[string]struct{}{}
	for _, e := range plan.Entries {
		if e.TTLExpired {
			rows[e.Item.CatalogName()+"/"+e.Item.Group+"/"+e.TargetStage] = struct{}{}
		}
	}
	for _, r := range plan.Rejected {
		if r.Gate == importer.GateTTL {
			rows[r.Item.CatalogName()+"/"+r.Item.Group+"/"+r.TargetStage] = struct{}{}
		}
	}
	return len(rows)
}
