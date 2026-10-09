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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations
// under the License.

package nidx03rollback

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	casesmeasuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
)

// envEnable gates every test in this package: it never runs in CI. Building
// and running a whole previous release from source, through a real gRPC
// standalone server, on both sides of the comparison, takes real wall time
// this package's own doc comment (previous_release.go) explains is the
// point -- this is a rollback *proof*, not a unit test.
const envEnable = "NIDX03_ROLLBACK"

// nidx03Measure/nidx03Group are the real, pre-registered index-mode measure
// NIDX-03 §1's "Index-mode Measure reads and writes" names, reused exactly
// as every other integration suite in this repo uses it (see
// pkg/test/measure/testdata/measures/service_traffic.json and
// .../groups/index_mode.json): tag_families [id, service_id, name,
// short_name, service_group, layer], entity [id], index rules on
// [service_id, layer]. Reusing the established schema instead of inventing
// one means this proof runs through the real schema validation, index-rule
// binding and index-mode write/query path every other test in this repo
// already exercises -- not a hand-built index.Document.
const (
	nidx03Measure = "service_traffic"
	nidx03Group   = "index_mode"
)

// TestNIDX03Rollback is the §12 item 2 rollback proof's gRPC-level case: it
// writes real index-mode Measure data (including an update of a live
// entity) through a real new-code standalone server's real gRPC Write path,
// captures a fixed set of query results, stops that server, starts the
// previous release's own server binary against the same data directory, and
// asserts the previous release's own Query RPC returns byte-identical JSON.
//
// NIDX-03 §1: rollback is a file property. This proves the file, not a
// library call -- both sides are real `banyand` server processes speaking
// the real wire protocol, exactly as an operator's rollback would.
func TestNIDX03Rollback(t *testing.T) {
	if os.Getenv(envEnable) == "" {
		t.Skip("one-off rollback proof; set NIDX03_ROLLBACK=1 to run it (builds and runs a previous-release binary from source)")
	}
	// pkg/test/setup is written for Ginkgo suites and uses bare gomega.Expect
	// assertions internally; registering t here (the documented non-Ginkgo
	// usage: https://onsi.github.io/gomega/#using-gomega-with-golangs-testing-package)
	// is what lets this plain *testing.T reuse it instead of panicking on
	// the first internal assertion.
	gomega.RegisterTestingT(t)

	previousBinary := PreviousReleaseBinary(t)

	dataDir, dataDirCleanup, err := test.NewSpace()
	require.NoError(t, err)
	defer dataDirCleanup()
	discoveryDir, discoveryCleanup, err := test.NewSpace()
	require.NoError(t, err)
	defer discoveryCleanup()

	// --- Phase 1: write with the current (native-only) code. ---
	ports, err := test.AllocateFreePorts(5)
	require.NoError(t, err)
	discoveryWriter := setup.NewDiscoveryFileWriter(discoveryDir)
	config := setup.PropertyClusterConfig(discoveryWriter)
	addr, _, closeNew := setup.ClosableStandalone(config, dataDir, ports)

	conn, err := grpchelper.Conn(addr, 10*time.Second, grpclib.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	baseTime := time.Now().Truncate(time.Millisecond)
	// Both JSON data files live in test/cases/measure/data/testdata/ (that
	// package's own //go:embed root, see casesmeasuredata.Write), prefixed
	// nidx03_rollback_ to stand out among that package's own fixtures.
	casesmeasuredata.Write(conn, nidx03Measure, nidx03Group, "nidx03_rollback_service_traffic_insert.json", baseTime, time.Minute)
	// The update: same entity ("nidx03-svc-01"), new tag values, written
	// after the insert above. Index-mode's Update is upsert by entity (NIDX-03
	// §2.1): the live document becomes service_group/layer's new values, not
	// a second row, so the capture below the proves the update actually
	// landed by asserting the field values it changed, not the update's
	// mere presence.
	casesmeasuredata.Write(conn, nidx03Measure, nidx03Group, "nidx03_rollback_service_traffic_update.json", baseTime.Add(time.Minute), time.Minute)
	// Give the index-mode write (buffered up to the measure flush timeout)
	// time to become queryable before capturing results and stopping the
	// server; polling until present would add little here since every
	// capture query below already tolerates -- and documents -- a short
	// stabilization wait via require.Eventually.
	newResults := captureAll(t, conn, 10*time.Second)
	require.NoError(t, conn.Close())
	closeNew()

	// --- Phase 2: reopen the same directory with the previous release. ---
	previousAddr, closePrevious := startPreviousRelease(t, previousBinary, dataDir, discoveryWriter.Path(), ports)
	defer closePrevious()
	previousConn, err := grpchelper.Conn(previousAddr, 10*time.Second, grpclib.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer previousConn.Close()
	previousResults := captureAll(t, previousConn, 10*time.Second)

	// --- Phase 3: automated equality, not a manual diff. ---
	newJSON, err := json.MarshalIndent(newResults, "", "  ")
	require.NoError(t, err)
	previousJSON, err := json.MarshalIndent(previousResults, "", "  ")
	require.NoError(t, err)
	require.JSONEq(t, string(newJSON), string(previousJSON),
		"the previous release (%s) must return the same query results the new binary's own data produced", PreviousReleaseTag)

	// Hand-derived constants, so this is not new-code-vs-itself only: the
	// previous release's own answer must also match literal expectations
	// pinned independently of either binary's current behavior.
	require.Equal(t, []string{"nidx03-svc-01", "nidx03-svc-02"}, previousResults.ByServiceGroupA, "criteria capture")
	require.Equal(t, []string{"nidx03-svc-02", "nidx03-svc-03", "nidx03-svc-01"}, previousResults.ByLayerSorted,
		"sorted by layer ascending: svc-02 (2) < svc-03 (3) < svc-01 (updated to 99)")
	require.Equal(t, "NIDX-03 rollback service 01 (updated)", previousResults.UpdatedName, "the update must have overwritten the insert's name field")
	require.Equal(t, int64(99), previousResults.UpdatedLayer, "the update must have overwritten the insert's layer field")
}

// nidx03Capture is the canonical, JSON-comparable shape both the new binary
// and the previous release's own Query RPC produce for the same fixed set
// of requests. Field order is the JSON key order the byte-for-byte (here,
// require.JSONEq) comparison depends on; reordering it is harmless for
// JSONEq (which compares structurally) but is kept stable for readability.
type nidx03Capture struct {
	UpdatedName     string   `json:"updatedName"`
	ByServiceGroupA []string `json:"byServiceGroupA"`
	ByLayerSorted   []string `json:"byLayerSorted"`
	UpdatedLayer    int64    `json:"updatedLayer"`
}

// nidx03WideTimeRange covers any baseTime this package's fixtures use (the
// query API requires TimeRange even for an index-mode measure, where it is
// not otherwise meaningful -- see design §3.4 -- so this is deliberately
// wide rather than tied to a specific write's timestamp).
func nidx03WideTimeRange() *modelv1.TimeRange {
	// Millisecond-aligned: the API validator rejects sub-millisecond
	// precision on both writes and query TimeRange (test/CLAUDE.md).
	now := time.Now().Truncate(time.Millisecond)
	return &modelv1.TimeRange{
		Begin: timestamppb.New(now.AddDate(-1, 0, 0)),
		End:   timestamppb.New(now.AddDate(1, 0, 0)),
	}
}

func captureAll(t *testing.T, conn *grpclib.ClientConn, timeout time.Duration) nidx03Capture {
	t.Helper()
	client := measurev1.NewMeasureServiceClient(conn)
	ctx := context.Background()

	// Criteria filter: id = nidx03-svc-01 OR id = nidx03-svc-02 -- an
	// entity-tag match selecting exactly those two of the fixture's three
	// entities.
	var byGroupA []string
	require.Eventually(t, func() bool {
		resp, queryErr := client.Query(ctx, &measurev1.QueryRequest{
			Groups:    []string{nidx03Group},
			Name:      nidx03Measure,
			TimeRange: nidx03WideTimeRange(),
			Criteria: orCriteria(
				condition("id", modelv1.Condition_BINARY_OP_EQ, strValue("nidx03-svc-01")),
				condition("id", modelv1.Condition_BINARY_OP_EQ, strValue("nidx03-svc-02")),
			),
			TagProjection: tagProjection("id"),
			Limit:         100,
		})
		if queryErr != nil {
			return false
		}
		byGroupA = extractTagValues(resp, "id")
		sort.Strings(byGroupA)
		return len(byGroupA) == 2
	}, timeout, 100*time.Millisecond, "id IN (nidx03-svc-01, nidx03-svc-02) must settle to exactly 2 entities")

	// Index-rule sort: every nidx03-rollback entity (service_id is either of
	// the two groups this fixture's 3 entities use), ordered by the "layer"
	// index rule ascending. nidx03-svc-01 was updated to layer=99, so it
	// must now sort last, proving the sort reads the live (post-update)
	// value, not a stale one.
	resp, queryErr := client.Query(ctx, &measurev1.QueryRequest{
		Groups:    []string{nidx03Group},
		Name:      nidx03Measure,
		TimeRange: nidx03WideTimeRange(),
		Criteria: orCriteria(
			condition("id", modelv1.Condition_BINARY_OP_EQ, strValue("nidx03-svc-01")),
			condition("id", modelv1.Condition_BINARY_OP_EQ, strValue("nidx03-svc-02")),
			condition("id", modelv1.Condition_BINARY_OP_EQ, strValue("nidx03-svc-03")),
		),
		TagProjection: tagProjection("id"),
		OrderBy:       &modelv1.QueryOrder{IndexRuleName: "layer", Sort: modelv1.Sort_SORT_ASC},
		Limit:         100,
	})
	require.NoError(t, queryErr)
	byLayer := extractTagValues(resp, "id")

	// Exact + projection of the updated value: id = nidx03-svc-01, projecting
	// name and layer (both changed by the update).
	resp, queryErr = client.Query(ctx, &measurev1.QueryRequest{
		Groups:        []string{nidx03Group},
		Name:          nidx03Measure,
		TimeRange:     nidx03WideTimeRange(),
		Criteria:      andCriteria(condition("id", modelv1.Condition_BINARY_OP_EQ, strValue("nidx03-svc-01"))),
		TagProjection: tagProjection("name", "layer"),
		Limit:         1,
	})
	require.NoError(t, queryErr)
	require.Len(t, resp.GetDataPoints(), 1, "exact id match must return exactly the updated entity")
	updatedName := tagString(resp.GetDataPoints()[0], "name")
	updatedLayer := tagInt(resp.GetDataPoints()[0], "layer")

	return nidx03Capture{
		ByServiceGroupA: byGroupA,
		ByLayerSorted:   byLayer,
		UpdatedName:     updatedName,
		UpdatedLayer:    updatedLayer,
	}
}

func andCriteria(conditions ...*modelv1.Condition) *modelv1.Criteria {
	if len(conditions) == 1 {
		return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: conditions[0]}}
	}
	result := &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: conditions[0]}}
	for _, c := range conditions[1:] {
		result = &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
			Op:    modelv1.LogicalExpression_LOGICAL_OP_AND,
			Left:  result,
			Right: &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: c}},
		}}}
	}
	return result
}

func orCriteria(conditions ...*modelv1.Condition) *modelv1.Criteria {
	result := &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: conditions[0]}}
	for _, c := range conditions[1:] {
		result = &modelv1.Criteria{Exp: &modelv1.Criteria_Le{Le: &modelv1.LogicalExpression{
			Op:    modelv1.LogicalExpression_LOGICAL_OP_OR,
			Left:  result,
			Right: &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: c}},
		}}}
	}
	return result
}

//nolint:unparam // general-purpose Condition builder; this file's own fixture only ever filters on "id", not a reason to narrow the helper.
func condition(name string, op modelv1.Condition_BinaryOp, value *modelv1.TagValue) *modelv1.Condition {
	return &modelv1.Condition{Name: name, Op: op, Value: value}
}

func strValue(s string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: s}}}
}

func tagProjection(names ...string) *modelv1.TagProjection {
	return &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: "default", Tags: names}}}
}

func extractTagValues(resp *measurev1.QueryResponse, tagName string) []string {
	var out []string
	for _, dp := range resp.GetDataPoints() {
		out = append(out, tagString(dp, tagName))
	}
	return out
}

func tagString(dp *measurev1.DataPoint, tagName string) string {
	for _, family := range dp.GetTagFamilies() {
		for _, tag := range family.GetTags() {
			if tag.GetKey() == tagName {
				return tag.GetValue().GetStr().GetValue()
			}
		}
	}
	return ""
}

func tagInt(dp *measurev1.DataPoint, tagName string) int64 {
	for _, family := range dp.GetTagFamilies() {
		for _, tag := range family.GetTags() {
			if tag.GetKey() == tagName {
				return tag.GetValue().GetInt().GetValue()
			}
		}
	}
	return 0
}
