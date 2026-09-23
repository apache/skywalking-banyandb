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

// Package nativelog holds the integration cases for native self-storage of a
// node's own log events. The standalone and distributed suites run the same
// cases: each suite starts its own nodes and describes them in SharedContext.
package nativelog

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

const (
	// The group and stream are the feature's public contract, so they are
	// named here rather than imported from the implementation.
	logGroup  = "_monitoring_log"
	logStream = "log"

	// rootModule is the module the root logger stamps. The line every server
	// role logs before it knows its own identity comes from the root logger.
	rootModule    = "ROOT"
	startupPrefix = "CPU Number:"

	// initialized is logged when a stream group's storage opens, through the
	// real logger of that group, under the group's name as the module.
	initialized = "initialized"

	// steadyRounds and steadyInterval shape the continuous case: a node logs
	// a line every interval, and every line must end up queryable.
	steadyRounds   = 12
	steadyInterval = 500 * time.Millisecond

	// queryLimit is the highest a stream query takes here: above about a
	// thousand rows the query is refused by the memory budget.
	queryLimit = 900

	droppedPublishFailed = `banyandb_logging_native_log_dropped_total{reason="publish_failed"}`
	writtenTotal         = "banyandb_logging_native_log_written_total"
)

var (
	// NativeFlags turns native logging on with the thresholds the cases
	// assume: the console at error and native storage at info, so an info line
	// is stored and not printed.
	NativeFlags = []string{"--logging-native-enabled", "--logging-native-level=info", "--logging-level=error"}

	// DisabledFlags leaves native logging off, its default, and prints info
	// lines, so a case can see that a node logged a line it did not store.
	DisabledFlags = []string{"--logging-level=info"}

	// readOnlyFlags makes the stream storage refuse every write. Watermark 0 is
	// the storage's explicit read-only mode, so no disk has to be filled.
	readOnlyFlags = []string{"--stream-retention-high-watermark=0", "--stream-retention-low-watermark=0"}

	nonceSeq atomic.Uint64
)

// Producer is a node whose own log events the cases check.
type Producer struct {
	// Conn reaches the node's data: the node itself for a standalone, the
	// liaison for a cluster.
	Conn     *grpc.ClientConn
	NodeID   string
	NodeType string
	// LogPath is the node's captured stdout and stderr.
	LogPath string
}

// RecoveryNode starts the node the recovery case runs on. Each call starts a
// new process on the same data directory, with NativeFlags and extraFlags,
// and returns the node, its metrics URL and a function that stops it
// gracefully.
type RecoveryNode func(extraFlags ...string) (node Producer, metricsURL string, stop func())

// Context is what a suite provides to the cases.
type Context struct {
	// Disabled is a producer started with DisabledFlags.
	Disabled Producer
	// Recovery starts the dedicated node the recovery case restarts. It has
	// its own data directory, because read-only mode starts forced retention
	// cleanup, which would delete the data of the other cases.
	Recovery RecoveryNode
	// Native are producers started with NativeFlags.
	Native      []Producer
	Distributed bool
}

// SharedContext is set by the suite before the specs run.
var SharedContext Context

// RequireBinary returns the server binary the suites launch. A missing binary
// skips the suite on a developer machine and fails it in CI: the CI prepare
// job builds the binary, so a skip there would report a pass for tests that
// never ran.
func RequireBinary() string {
	bin, err := setup.ResolveBanyandBinary()
	if err == nil {
		return bin
	}
	msg := fmt.Sprintf("banyand server binary unavailable (%v); build it with `make -C banyand banyand-server`", err)
	if os.Getenv("CI") != "" {
		g.Fail(msg)
	}
	g.Skip(msg)
	return ""
}

// errTruncated reports that the limit cut the result. A case that expects no
// row must fail on it instead of reading it as an empty stream.
var errTruncated = errors.New("truncated result")

// AwaitQueryable waits until a query of the log stream through conn succeeds.
// A cluster query fails while any data node does not know the stream yet. A
// truncated answer is still an answer, so it ends the wait.
func AwaitQueryable(conn *grpc.ClientConn) {
	gm.Eventually(func() error {
		_, err := query(conn, nil, nil)
		if errors.Is(err, errTruncated) {
			return nil
		}
		return err
	}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
}

var _ = g.Describe("Native self-stored logs", func() {
	g.It("TC1 stores a node's own events with its identity and structured fields", func() {
		native := SharedContext.Native
		gm.Expect(native).NotTo(gm.BeEmpty())
		name, module := createNonceGroup(native[0].Conn)
		for _, p := range native {
			gm.Eventually(func(inner gm.Gomega) {
				r := findInitialized(inner, p, module)
				inner.Expect(r.tags["level"]).To(gm.Equal("info"))
				inner.Expect(r.tags["node_type"]).To(gm.Equal(p.NodeType))
				var fields map[string]any
				inner.Expect(json.Unmarshal(r.fields, &fields)).To(gm.Succeed(), "fields is not a JSON object")
				inner.Expect(fields["path"]).To(gm.ContainSubstring(name),
					"the structured field of the log line was not kept")
			}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
		}
	})

	g.It("TC2 keeps each data node's events attributed to that node", func() {
		if !SharedContext.Distributed {
			g.Skip("attribution across data nodes needs a cluster")
		}
		native := SharedContext.Native
		ids := make([]string, 0, len(native)+1)
		want := make([]string, 0, len(native))
		for _, p := range native {
			ids = append(ids, p.NodeID)
			want = append(want, p.NodeID)
		}
		// The node with native logging off opens the group too, so the query
		// names it: a row under its id would be a line attributed to the
		// wrong node, or stored from a node that must store nothing.
		ids = append(ids, SharedContext.Disabled.NodeID)
		_, module := createNonceGroup(native[0].Conn)
		gm.Eventually(func(inner gm.Gomega) {
			rows, err := query(native[0].Conn, nodeIn(ids...), initializedOf(module))
			inner.Expect(err).NotTo(gm.HaveOccurred())
			got := make([]string, 0, len(rows))
			for _, r := range rows {
				inner.Expect(r.tags["node_type"]).To(gm.Equal("data"))
				got = append(got, r.tags["node_id"])
			}
			inner.Expect(got).To(gm.ConsistOf(want), "each data node must store exactly its own line, under its own id")
		}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
	})

	g.It("TC3a stores an info line that the console, at error, does not print", func() {
		native := SharedContext.Native
		_, module := createNonceGroup(native[0].Conn)
		for _, p := range native {
			gm.Eventually(func(inner gm.Gomega) {
				findInitialized(inner, p, module)
			}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
			// The console writes before the native sink admits, so once the row
			// is stored the console has already made its decision.
			gm.Expect(logged(p.LogPath, module)).To(gm.BeFalse(),
				"%s printed an info line although its console level is error", p.NodeID)
		}
	})

	g.It("TC3b stores nothing from a node with native logging off, its default", func() {
		d := SharedContext.Disabled
		_, module := createNonceGroup(d.Conn)
		gm.Eventually(func() bool { return logged(d.LogPath, module) }, flags.EventuallyTimeout, time.Second).
			Should(gm.BeTrue(), "the node did not log the line, so its absence from storage would prove nothing")
		if !SharedContext.Distributed {
			// A standalone without the flag never creates the log group.
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := databasev1.NewGroupRegistryServiceClient(d.Conn).
				Get(ctx, &databasev1.GroupRegistryServiceGetRequest{Group: logGroup})
			gm.Expect(status.Code(err)).To(gm.Equal(codes.NotFound), "%s exists on a node with native logging off: %v", logGroup, err)
			return
		}
		// In a cluster the group exists, created by the other data nodes, which
		// also stored the same line; that is the control that the query works.
		gm.Eventually(func(inner gm.Gomega) {
			findInitialized(inner, SharedContext.Native[0], module)
		}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
		gm.Consistently(func(inner gm.Gomega) {
			rows, err := query(d.Conn, nodeEq(d.NodeID), byModule(module))
			inner.Expect(err).NotTo(gm.HaveOccurred())
			inner.Expect(rows).To(gm.BeEmpty(), "a node with native logging off stored its own line")
		}, 5*time.Second, time.Second).Should(gm.Succeed())
	})

	g.It("TC4a stores startup events under the node's identity", func() {
		for _, p := range SharedContext.Native {
			gm.Eventually(func(inner gm.Gomega) {
				rows, err := query(p.Conn, nodeEq(p.NodeID), byModule(rootModule))
				inner.Expect(err).NotTo(gm.HaveOccurred())
				var found bool
				for _, r := range rows {
					found = found || strings.HasPrefix(r.tags["message"], startupPrefix)
				}
				// Filtered by node_id, so a hit is also a startup line stamped
				// with the identity the node learned only after the line was
				// admitted.
				inner.Expect(found).To(gm.BeTrue(), "no %q line stored for %s", startupPrefix, p.NodeID)
			}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
		}
	})

	g.It("TC5 counts refused writes and resumes collection after recovery", func() {
		// Phase 1: the storage refuses every write.
		node, metricsURL, stop := SharedContext.Recovery(readOnlyFlags...)
		stopped := false
		defer func() {
			if !stopped {
				stop()
			}
		}()
		_, moduleA := createNonceGroup(node.Conn)
		gm.Eventually(func(inner gm.Gomega) {
			v, ok := metric(metricsURL, droppedPublishFailed)
			inner.Expect(ok).To(gm.BeTrue(), "%s not published", droppedPublishFailed)
			inner.Expect(v).To(gm.BeNumerically(">", 0))
		}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
		written, ok := metric(metricsURL, writtenTotal)
		gm.Expect(ok).To(gm.BeTrue(), "%s not published", writtenTotal)
		gm.Expect(written).To(gm.BeZero(), "batches were counted as written while the storage refused them")
		// Checked before the restart: reopening the group logs its line again.
		rows, err := query(node.Conn, nodeEq(node.NodeID), byModule(moduleA))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(rows).To(gm.BeEmpty(), "a line was stored while the storage refused writes")
		stop()
		stopped = true

		// Phase 2: the storage accepts writes again. The counters restart with
		// the process, so recovery is shown by a new line being stored.
		node, _, stop = SharedContext.Recovery()
		defer stop()
		_, moduleB := createNonceGroup(node.Conn)
		gm.Eventually(func(inner gm.Gomega) {
			findInitialized(inner, node, moduleB)
		}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
	})

	g.It("TC6 keeps storing while a node logs continuously", func() {
		native := SharedContext.Native
		modules := make([]string, 0, steadyRounds)
		for i := 0; i < steadyRounds; i++ {
			_, module := createNonceGroup(native[0].Conn)
			modules = append(modules, module)
			time.Sleep(steadyInterval)
		}
		for _, p := range native {
			gm.Eventually(func(inner gm.Gomega) {
				rows, err := query(p.Conn, nodeEq(p.NodeID), func(r row) bool { return r.tags["message"] == initialized })
				inner.Expect(err).NotTo(gm.HaveOccurred())
				stored := make(map[string]bool, len(rows))
				for _, r := range rows {
					stored[r.tags["module"]] = true
				}
				missing := make([]string, 0, len(modules))
				for _, module := range modules {
					if !stored[module] {
						missing = append(missing, module)
					}
				}
				inner.Expect(missing).To(gm.BeEmpty(),
					"%s stored %d of %d lines it logged over %s; check native_log_dropped_total for the reason",
					p.NodeID, len(modules)-len(missing), len(modules), time.Duration(steadyRounds)*steadyInterval)
			}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
		}
	})
})

// createNonceGroup creates a stream group with a unique name. Opening its
// storage makes every node that stores data log `initialized` through its real
// logger, with the group's name, upper-cased, as the module.
func createNonceGroup(conn *grpc.ClientConn) (name, module string) {
	name = fmt.Sprintf("nl_%d_%d", time.Now().UnixNano(), nonceSeq.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := databasev1.NewGroupRegistryServiceClient(conn).Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
		Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: name},
			Catalog:  commonv1.Catalog_CATALOG_STREAM,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum:        1,
				SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
				Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			},
		},
	})
	gm.Expect(err).NotTo(gm.HaveOccurred())
	return name, strings.ToUpper(name)
}

// findInitialized returns the stored `initialized` line of module from p.
func findInitialized(inner gm.Gomega, p Producer, module string) row {
	rows, err := query(p.Conn, nodeEq(p.NodeID), initializedOf(module))
	inner.Expect(err).NotTo(gm.HaveOccurred())
	inner.Expect(rows).NotTo(gm.BeEmpty(), "no %q line of %s stored for %s", initialized, module, p.NodeID)
	return rows[0]
}

type row struct {
	tags   map[string]string
	fields []byte
}

// query reads the log stream. The server criteria MUST name only entity tags,
// node_id and level: the engine applies any other criteria after the limit
// cuts the scan, so a module filter on a stream that holds more rows than the
// limit returns nothing although the row is stored. Measured on a node with
// 506 rows: a module filter with limit 100 found the oldest marker and missed
// the two later ones, which the same filter with limit 900 returned. Every
// other condition is therefore applied here, by keep.
//
// A full result is refused, because a case that expects no row cannot tell a
// truncated read from an empty one.
func query(conn *grpc.ClientConn, entity *modelv1.Criteria, keep func(row) bool) ([]row, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The query validator rejects a time with a sub-millisecond remainder.
	now := time.Now().Truncate(time.Millisecond)
	resp, err := streamv1.NewStreamServiceClient(conn).Query(ctx, &streamv1.QueryRequest{
		Groups:    []string{logGroup},
		Name:      logStream,
		TimeRange: &modelv1.TimeRange{Begin: timestamppb.New(now.Add(-time.Hour)), End: timestamppb.New(now.Add(time.Hour))},
		Criteria:  entity,
		Limit:     queryLimit,
		Projection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: "searchable", Tags: []string{"node_id", "node_type", "module", "level", "message"}},
			{Name: "data", Tags: []string{"fields"}},
		}},
	})
	if err != nil {
		return nil, err
	}
	if len(resp.GetElements()) >= queryLimit {
		return nil, fmt.Errorf("%w: the query returned its limit of %d rows", errTruncated, queryLimit)
	}
	rows := make([]row, 0, len(resp.GetElements()))
	for _, e := range resp.GetElements() {
		r := row{tags: map[string]string{}}
		for _, family := range e.GetTagFamilies() {
			for _, tag := range family.GetTags() {
				if s := tag.GetValue().GetStr(); s != nil {
					r.tags[tag.GetKey()] = s.GetValue()
				}
				if b := tag.GetValue().GetBinaryData(); b != nil {
					r.fields = b
				}
			}
		}
		if keep == nil || keep(r) {
			rows = append(rows, r)
		}
	}
	return rows, nil
}

// byModule keeps the lines a nonce group produced.
func byModule(module string) func(row) bool {
	return func(r row) bool { return r.tags["module"] == module }
}

// initializedOf keeps the `initialized` line of one nonce group.
func initializedOf(module string) func(row) bool {
	return func(r row) bool { return r.tags["module"] == module && r.tags["message"] == initialized }
}

// nodeEq selects one node. node_id is an entity tag, so the engine applies it
// before the limit.
func nodeEq(id string) *modelv1.Criteria {
	return condition("node_id", modelv1.Condition_BINARY_OP_EQ,
		&modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: id}}})
}

// nodeIn selects several nodes. An entity tag accepts only equality and set
// membership.
func nodeIn(ids ...string) *modelv1.Criteria {
	return condition("node_id", modelv1.Condition_BINARY_OP_IN,
		&modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: ids}}})
}

func condition(name string, op modelv1.Condition_BinaryOp, value *modelv1.TagValue) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{Name: name, Op: op, Value: value}}}
}

// logged reports whether the node's captured output contains needle.
func logged(path, needle string) bool {
	b, err := os.ReadFile(path)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	return strings.Contains(string(b), needle)
}

// metric reads one series from a Prometheus text endpoint.
func metric(url, series string) (float64, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, false
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, series+" ") {
			continue
		}
		v, parseErr := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, series)), 64)
		return v, parseErr == nil
	}
	return 0, false
}
