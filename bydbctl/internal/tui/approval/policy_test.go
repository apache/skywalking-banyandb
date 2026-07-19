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

package approval

import "testing"

func TestIsReadOnlyBYDBQL(t *testing.T) {
	readQuery := "SELECT endpoint FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10"
	if !IsReadOnlyBYDBQL(readQuery) {
		t.Fatalf("expected select query to be read-only")
	}
	topNQuery := "SHOW TOP 10 FROM MEASURE endpoint_latency IN sw_metrics TIME > '-30m' ORDER BY DESC"
	if !IsReadOnlyBYDBQL(topNQuery) {
		t.Fatalf("expected show top query to be read-only")
	}
	if IsReadOnlyBYDBQL("CREATE MEASURE foo IN production") {
		t.Fatal("mutating statement must not be read-only")
	}
	if IsReadOnlyBYDBQL("not a query") {
		t.Fatal("unparseable statement must not be read-only")
	}
}

func TestExecutionPolicyAutoApprove(t *testing.T) {
	readQuery := "SELECT endpoint FROM MEASURE service_latency IN production TIME > '-30m' LIMIT 10"
	writeQuery := "CREATE MEASURE foo IN production"
	if PolicyAskEveryTime.AutoApprove(SourceManual, false, readQuery) {
		t.Fatal("default policy should require approval for every query")
	}
	if PolicyAskEveryTime.AutoApprove(SourceManual, false, writeQuery) {
		t.Fatal("default policy should require approval for mutating queries")
	}
	if !PolicyAutoProbe.AutoApprove(SourceAgentProbe, true, readQuery) {
		t.Fatal("auto_probe should auto-approve read probes")
	}
	if PolicyAutoProbe.AutoApprove(SourceAgentTool, false, readQuery) {
		t.Fatal("auto_probe should require approval for full execution")
	}
	if PolicyAutoProbe.AutoApprove(SourceManual, false, writeQuery) {
		t.Fatal("auto_probe should not auto-approve mutating manual execute")
	}
	if PolicyTrustSession.AutoApprove(SourceManual, false, writeQuery) {
		t.Fatal("trust_session must never auto-approve a mutating statement")
	}
	if !PolicyTrustSession.AutoApprove(SourceManual, false, readQuery) {
		t.Fatal("trust_session should auto-approve read-only execution")
	}
}

func TestExecutionPolicyNext(t *testing.T) {
	if PolicyAskEveryTime.Next() != PolicyAutoProbe {
		t.Fatalf("unexpected next policy: %s", PolicyAskEveryTime.Next())
	}
	if PolicyTrustSession.Next() != PolicyAskEveryTime {
		t.Fatalf("unexpected next policy: %s", PolicyTrustSession.Next())
	}
}
