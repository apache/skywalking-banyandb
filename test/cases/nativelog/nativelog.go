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
// node's own log events. A suite starts one node at a time and describes how
// to start it in SharedContext.
package nativelog

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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

	// readOnlyFlags make the stream storage refuse every write. Watermark 0 is
	// the storage's explicit read-only mode, so no disk has to be filled.
	readOnlyFlags = []string{"--stream-retention-high-watermark=0", "--stream-retention-low-watermark=0"}

	nonceSeq atomic.Uint64
)

// Node is a running server the cases query.
type Node struct {
	// Conn reaches the node's data.
	Conn       *grpc.ClientConn
	NodeID     string
	NodeType   string
	MetricsURL string
}

// Starter starts one node with extraFlags and returns it with a function that
// stops it. Only one node runs at a time, because the native sink and its
// configuration are process-global: a second node in the same process takes
// the sink over, and a node with native logging off stops collection for the
// whole process.
type Starter func(extraFlags ...string) (Node, func())

// Context is what a suite provides to the cases.
type Context struct {
	// Start starts a node with native logging on, on a data directory of its
	// own.
	Start Starter
	// StartDisabled starts a node with native logging off, its default.
	StartDisabled Starter
	// Restart starts a node with native logging on, always on the same data
	// directory, so the recovery case can stop one and start the next on the
	// data of the first.
	Restart Starter
	// Console reports what the console has printed in this process.
	Console func() string
	// Distributed tells the cases that Start returns a data node whose rows
	// are read through a liaison, and that the suite owns the node's lifetime.
	Distributed bool
}

// SharedContext is set by the suite before the specs run.
var SharedContext Context

// AwaitQueryable waits until a query of the log stream through conn succeeds.
// A cluster query fails while the liaison or a data node does not know the
// stream yet. A truncated answer is still an answer, so it ends the wait.
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
		node, stop := SharedContext.Start()
		defer stop()
		name, module := createNonceGroup(node.Conn)
		gm.Eventually(func(inner gm.Gomega) {
			r := findInitialized(inner, node, module)
			inner.Expect(r.tags["level"]).To(gm.Equal("info"))
			inner.Expect(r.tags["node_type"]).To(gm.Equal(node.NodeType))
			var fields map[string]any
			inner.Expect(json.Unmarshal(r.fields, &fields)).To(gm.Succeed(), "fields is not a JSON object")
			inner.Expect(fields["path"]).To(gm.ContainSubstring(name),
				"the structured field of the log line was not kept")
		}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
	})

	g.It("TC3a stores an info line that the console, at error, does not print", func() {
		node, stop := SharedContext.Start()
		defer stop()
		_, module := createNonceGroup(node.Conn)
		gm.Eventually(func(inner gm.Gomega) {
			findInitialized(inner, node, module)
		}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
		// The console writes before the native sink admits, so once the row is
		// stored the console has already made its decision.
		gm.Expect(SharedContext.Console()).NotTo(gm.ContainSubstring(module),
			"the node printed an info line although its console level is error")
	})

	g.It("TC3b stores nothing from a node with native logging off, its default", func() {
		if SharedContext.Distributed {
			// A node with native logging off would turn the sink off for every
			// node in the process, and the group exists anyway, created by the
			// node that has native logging on.
			g.Skip("a node with native logging off cannot run beside one that has it on")
		}
		node, stop := SharedContext.StartDisabled()
		defer stop()
		_, module := createNonceGroup(node.Conn)
		gm.Eventually(func() string { return SharedContext.Console() }, flags.EventuallyTimeout, time.Second).
			Should(gm.ContainSubstring(module),
				"the node did not log the line, so its absence from storage would prove nothing")
		// A node without the flag never creates the log group.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := databasev1.NewGroupRegistryServiceClient(node.Conn).
			Get(ctx, &databasev1.GroupRegistryServiceGetRequest{Group: logGroup})
		gm.Expect(status.Code(err)).To(gm.Equal(codes.NotFound),
			"%s exists on a node with native logging off: %v", logGroup, err)
	})

	g.It("TC4a stores startup events under the node's identity", func() {
		node, stop := SharedContext.Start()
		defer stop()
		gm.Eventually(func(inner gm.Gomega) {
			rows, err := query(node.Conn, nodeEq(node.NodeID), byModule(rootModule))
			inner.Expect(err).NotTo(gm.HaveOccurred())
			var found bool
			for _, r := range rows {
				found = found || strings.HasPrefix(r.tags["message"], startupPrefix)
			}
			// Filtered by node_id, so a hit is also a startup line stamped with
			// the identity the node learned only after the line was admitted.
			inner.Expect(found).To(gm.BeTrue(), "no %q line stored for %s", startupPrefix, node.NodeID)
		}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
	})

	g.It("TC5 counts refused writes and resumes collection after recovery", func() {
		if SharedContext.Distributed {
			g.Skip("the recovery case restarts the only producer, which the cluster suite keeps running")
		}
		// Phase 1: the storage refuses every write.
		node, stop := SharedContext.Restart(readOnlyFlags...)
		stopped := false
		defer func() {
			if !stopped {
				stop()
			}
		}()
		// The process keeps one metrics registry, so a counter carries what
		// earlier cases left behind; every assertion here is on the change.
		droppedBefore, _ := metric(node.MetricsURL, droppedPublishFailed)
		writtenBefore, ok := metric(node.MetricsURL, writtenTotal)
		gm.Expect(ok).To(gm.BeTrue(), "%s not published", writtenTotal)
		_, moduleA := createNonceGroup(node.Conn)
		gm.Eventually(func(inner gm.Gomega) {
			v, found := metric(node.MetricsURL, droppedPublishFailed)
			inner.Expect(found).To(gm.BeTrue(), "%s not published", droppedPublishFailed)
			inner.Expect(v).To(gm.BeNumerically(">", droppedBefore))
		}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
		written, ok := metric(node.MetricsURL, writtenTotal)
		gm.Expect(ok).To(gm.BeTrue(), "%s not published", writtenTotal)
		gm.Expect(written).To(gm.Equal(writtenBefore), "batches were counted as written while the storage refused them")
		// Checked before the restart: reopening the group logs its line again.
		rows, err := query(node.Conn, nodeEq(node.NodeID), byModule(moduleA))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(rows).To(gm.BeEmpty(), "a line was stored while the storage refused writes")
		stop()
		stopped = true

		// Phase 2: the storage accepts writes again, on the same data.
		node, stop = SharedContext.Restart()
		defer stop()
		_, moduleB := createNonceGroup(node.Conn)
		gm.Eventually(func(inner gm.Gomega) {
			findInitialized(inner, node, moduleB)
		}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
	})

	g.It("TC6 keeps storing while a node logs continuously", func() {
		node, stop := SharedContext.Start()
		defer stop()
		modules := make([]string, 0, steadyRounds)
		for i := 0; i < steadyRounds; i++ {
			_, module := createNonceGroup(node.Conn)
			modules = append(modules, module)
			time.Sleep(steadyInterval)
		}
		gm.Eventually(func(inner gm.Gomega) {
			rows, err := query(node.Conn, nodeEq(node.NodeID), func(r row) bool { return r.tags["message"] == initialized })
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
				node.NodeID, len(modules)-len(missing), len(modules), time.Duration(steadyRounds)*steadyInterval)
		}, flags.EventuallyTimeout, time.Second).Should(gm.Succeed())
	})
})

// createNonceGroup creates a stream group with a unique name. Opening its
// storage makes the node log `initialized` through its real logger, with the
// group's name, upper-cased, as the module.
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

// findInitialized returns the stored `initialized` line of module.
func findInitialized(inner gm.Gomega, node Node, module string) row {
	rows, err := query(node.Conn, nodeEq(node.NodeID), initializedOf(module))
	inner.Expect(err).NotTo(gm.HaveOccurred())
	inner.Expect(rows).NotTo(gm.BeEmpty(), "no %q line of %s stored for %s", initialized, module, node.NodeID)
	return rows[0]
}

type row struct {
	tags   map[string]string
	fields []byte
}

// errTruncated reports that the limit cut the result. A case that expects no
// row must fail on it instead of reading it as an empty stream.
var errTruncated = errors.New("truncated result")

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
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name:  "node_id",
		Op:    modelv1.Condition_BINARY_OP_EQ,
		Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: id}}},
	}}}
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
