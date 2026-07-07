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

// Package migration_test holds end-to-end migration tests that drive the full
// pipeline through external gRPC services only: register schema -> write ->
// snapshot -> RunCopy -> RunVerify -> serve the migrated tree -> query back.
// The tests live in an external test package because the stream / measure
// executors import this package.
package migration_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	backupsnapshot "github.com/apache/skywalking-banyandb/banyand/backup/snapshot"
	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

// ── Shared fixture: data layout and expected counts ──────────────────────────
//
// Both the stream and the measure test write the same shape of data:
//
//	batch A — e2eFastCount rows on a single day of one DAY×2 source segment
//	          (bucket A): the whole part stays inside one DAY×1 target segment,
//	          so migration takes the FAST path (whole-part byte copy);
//	batch B — e2eSlowCount rows straddling the midnight inside ANOTHER DAY×2
//	          source segment (bucket B): the part spans two DAY×1 target
//	          segments, so migration takes the SLOW path (row re-bucketing),
//	          which also forces the stream element-index rebuild
//	          (one source shard feeding two target segments).
//
// Writing the batches into two different source segments (instead of relying
// on flush timing to split parts) keeps the part layout deterministic on slow
// CI machines: rows of different segments can never share a part.
//
// The indexed `duration` tag is BOUND to the series (instance-0 always writes
// e2eDurationHi, instance-1 always e2eDurationLo). measure indexes non-entity
// tags at SERIES granularity (one sidx doc per series, holding a snapshot of
// the tag), so a per-row varying value would make the indexed filter's result
// depend on write order; a series-stable value keeps row-level and
// series-level filtering semantics identical for both catalogs.
const (
	e2eStage      = "daily"
	e2eRuleName   = "duration"
	e2eRuleID     = 7
	e2eDurationHi = int64(777)
	e2eDurationLo = int64(42)
	e2eInstance0  = "instance-0"
	e2eFastCount  = 12 // batch A rows (single-day part -> fast path)
	e2eSlowCount  = 10 // batch B rows (cross-day part -> slow path)
	e2eTotalCount = e2eFastCount + e2eSlowCount
	e2eDay2Count  = 5  // batch B rows whose timestamp falls on bucket B's day2
	e2eHiTotal    = 11 // instance-0 rows: batch A i%2==0 -> 6; batch B j%2==0 -> 5
	e2eHiDay2     = 2  // batch B j in {6, 8}
	e2eInst0Total = 11 // same series as e2eHiTotal (duration is series-bound)

	streamGroup  = "mig_stream"
	streamName   = "logs"
	streamFamily = "searchable"
)

// e2eRow is one row to write: a unique ID suffix plus the tag values shared by
// the stream and measure fixtures.
type e2eRow struct {
	ts       time.Time
	id       string
	instance string
	duration int64
}

// e2eDays picks the two DAY×2 source buckets. Segment grids are anchored on
// the local-time epoch grid (matching the server's own bucketing); both
// buckets sit strictly in the past (no future-timestamp writes) yet within the
// 7-day TTL.
func e2eDays() (aDay, bDay1, bDay2 time.Time) {
	grid := storage.IntervalRule{Unit: storage.DAY, Num: 2}
	current := grid.Standard(time.Now())
	bDay1 = current.AddDate(0, 0, -2)
	bDay2 = bDay1.AddDate(0, 0, 1)
	aDay = current.AddDate(0, 0, -4)
	return aDay, bDay1, bDay2
}

// e2eBatchA returns the fast-path rows: all on aDay's late morning.
func e2eBatchA(aDay time.Time) []e2eRow {
	out := make([]e2eRow, 0, e2eFastCount)
	for i := 0; i < e2eFastCount; i++ {
		duration := e2eDurationLo
		if i%2 == 0 {
			duration = e2eDurationHi
		}
		out = append(out, e2eRow{
			id:       "a" + strconv.Itoa(i),
			ts:       aDay.Add(10*time.Hour + time.Duration(i)*time.Minute),
			instance: "instance-" + strconv.Itoa(i%2),
			duration: duration,
		})
	}
	return out
}

// e2eBatchB returns the slow-path rows: j∈[0,5) lands on bDay1 (23:55..23:59),
// j∈[5,10) on bDay2 (00:00..00:04).
func e2eBatchB(bDay1 time.Time) []e2eRow {
	out := make([]e2eRow, 0, e2eSlowCount)
	for j := 0; j < e2eSlowCount; j++ {
		duration := e2eDurationLo
		if j%2 == 0 {
			duration = e2eDurationHi
		}
		out = append(out, e2eRow{
			id:       "b" + strconv.Itoa(j),
			ts:       bDay1.Add(23*time.Hour + 55*time.Minute + time.Duration(j)*time.Minute),
			instance: "instance-" + strconv.Itoa(j%2),
			duration: duration,
		})
	}
	return out
}

// startE2EStandalone boots a standalone server rooted at root with NO
// preloaded schema; the test registers schemas over the registry gRPC API.
func startE2EStandalone(t *testing.T, root string, kind schema.Kind, flushFlag string) (string, func()) {
	t.Helper()
	tmpDir, tmpDirCleanup, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	dfWriter := setup.NewDiscoveryFileWriter(tmpDir)
	config := setup.PropertyClusterConfig(dfWriter)
	config.AddLoadedKinds(kind)
	ports, err := test.AllocateFreePorts(5)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr, _, closeFn := setup.ClosableStandaloneWithSchemaLoaders(config, root, ports, nil, flushFlag)
	return addr, func() {
		closeFn()
		tmpDirCleanup()
	}
}

func dialE2E(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return conn
}

// registerE2EGroup creates one group; the source side passes the DAY×2 default
// interval plus the DAY×1 "daily" stage, the target side a DAY×1 default.
func registerE2EGroup(t *testing.T, conn *grpc.ClientConn, group string, catalog commonv1.Catalog,
	segInterval *commonv1.IntervalRule, stages []*commonv1.LifecycleStage,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := databasev1.NewGroupRegistryServiceClient(conn).Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
		Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: group},
			Catalog:  catalog,
			ResourceOpts: &commonv1.ResourceOpts{
				ShardNum:        1,
				SegmentInterval: segInterval,
				Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
				Stages:          stages,
			},
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func e2eDay2Interval() *commonv1.IntervalRule {
	return &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 2}
}

func e2eDay1Interval() *commonv1.IntervalRule {
	return &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1}
}

func e2eDailyStage() []*commonv1.LifecycleStage {
	return []*commonv1.LifecycleStage{{
		Name:            e2eStage,
		ShardNum:        1,
		SegmentInterval: e2eDay1Interval(),
		Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		NodeSelector:    "type=daily",
	}}
}

// registerE2EIndexRule registers the INVERTED duration rule and its binding.
// The explicit metadata ID keeps the index term keys identical between the
// source instance (whose schema drives the migration's index rebuild) and the
// target instance (whose schema drives query-time term lookups).
func registerE2EIndexRule(t *testing.T, conn *grpc.ClientConn, group, subjectName string, catalog commonv1.Catalog) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := databasev1.NewIndexRuleRegistryServiceClient(conn).Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{
		IndexRule: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: e2eRuleName, Group: group, Id: e2eRuleID},
			Tags:     []string{"duration"},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	_, err = databasev1.NewIndexRuleBindingRegistryServiceClient(conn).Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
		IndexRuleBinding: &databasev1.IndexRuleBinding{
			Metadata: &commonv1.Metadata{Name: subjectName + "-binding", Group: group},
			Subject:  &databasev1.Subject{Catalog: catalog, Name: subjectName},
			Rules:    []string{e2eRuleName},
			BeginAt:  timestamppb.New(time.Now().AddDate(-1, 0, 0)),
			ExpireAt: timestamppb.New(time.Now().AddDate(100, 0, 0)),
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

// awaitRows polls fn until it reports at least want rows; fn errors mean "not
// ready yet" (registry writes propagate to the serving modules asynchronously).
func awaitRows(t *testing.T, want int, fn func() (int, error)) {
	t.Helper()
	test.EventuallyConsistently(func() int {
		n, err := fn()
		if err != nil {
			return -1
		}
		return n
	}, flags.EventuallyTimeout).WithPolling(200*time.Millisecond).
		Should(gomega.BeNumerically(">=", want), "rows must become queryable")
}

// expectRows polls fn until it returns exactly want rows — every first query
// of a kind may race the asynchronous schema/index propagation on a freshly
// registered instance, so plain one-shot assertions are CI-hostile.
func expectRows(t *testing.T, want int, explain string, fn func() (int, error)) {
	t.Helper()
	test.EventuallyConsistently(func() int {
		n, err := fn()
		if err != nil {
			return -1
		}
		return n
	}, flags.EventuallyTimeout).WithPolling(200*time.Millisecond).
		Should(gomega.Equal(want), explain)
}

var e2ePartDirPattern = regexp.MustCompile(`^[0-9a-f]{16}$`)

// awaitPartsOnDisk blocks until the source segment holds at least one flushed
// part — the deterministic "this batch is durable" signal that replaces
// sleep-based flush timing (essential on slow CI machines, and it makes the
// snapshot content independent of memtable state).
func awaitPartsOnDisk(t *testing.T, catalogDataRoot, group string, segStart time.Time) {
	t.Helper()
	segDir := filepath.Join(catalogDataRoot, group, "seg-"+segStart.Format("20060102"), "shard-0")
	test.EventuallyConsistently(func() int {
		entries, err := os.ReadDir(segDir)
		if err != nil {
			return -1
		}
		parts := 0
		for _, e := range entries {
			if e.IsDir() && e2ePartDirPattern.MatchString(e.Name()) {
				parts++
			}
		}
		return parts
	}, flags.EventuallyTimeout).WithPolling(200*time.Millisecond).
		Should(gomega.BeNumerically(">=", 1), "a flushed part must appear under %s", segDir)
}

// takeE2ESnapshot snapshots the given catalog plus schema-property and returns
// the catalog snapshot dir (children are group dirs) and the `_schema` dir.
func takeE2ESnapshot(t *testing.T, conn *grpc.ClientConn, root string, catalog commonv1.Catalog, group string) (string, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := databasev1.NewSnapshotServiceClient(conn).Snapshot(ctx, &databasev1.SnapshotRequest{})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(resp.GetSnapshots()).NotTo(gomega.BeEmpty())

	var catalogDir, schemaDir string
	for _, snp := range resp.GetSnapshots() {
		dir, dirErr := backupsnapshot.Dir(snp, root, root, root, root, root)
		gomega.Expect(dirErr).NotTo(gomega.HaveOccurred())
		if strings.HasPrefix(snp.GetName(), backupsnapshot.SchemaPropertyCatalogName+"/") {
			schemaDir = filepath.Join(dir, schema.SchemaGroup)
			continue
		}
		if snp.GetCatalog() == catalog {
			catalogDir = dir
		}
	}
	gomega.Expect(catalogDir).NotTo(gomega.BeEmpty(), "catalog snapshot dir not found")
	gomega.Expect(schemaDir).NotTo(gomega.BeEmpty(), "schema-property _schema snapshot dir not found")
	entries, err := os.ReadDir(catalogDir)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	var sawGroup bool
	for _, e := range entries {
		if e.IsDir() && e.Name() == group {
			sawGroup = true
		}
	}
	gomega.Expect(sawGroup).To(gomega.BeTrue(), "snapshot %s missing group %s", catalogDir, group)
	return catalogDir, schemaDir
}

// e2ePlan builds the one-entry live plan that migrates the snapshot into the
// DAY×1 "daily" stage.
func e2ePlan(snapshotDir, schemaDir, group, target string) *migration.CopyPlan {
	return &migration.CopyPlan{
		Source: migration.CopySource{Live: &migration.LiveSource{
			SchemaPropertyPath: schemaDir,
			Stages: map[string][]migration.LiveStageNode{
				e2eStage: {{Node: "node-0", Root: snapshotDir}},
			},
		}},
		Groups: []string{group},
		Entries: []migration.CopyEntry{
			{Stage: e2eStage, Target: target, Nodes: []string{"node-0"}},
		},
	}
}

func durationHiEq() *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: "duration", Op: modelv1.Condition_BINARY_OP_EQ,
		Value: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: e2eDurationHi}}},
	}}}
}

func instanceEq(v string) *modelv1.Criteria {
	return &modelv1.Criteria{Exp: &modelv1.Criteria_Condition{Condition: &modelv1.Condition{
		Name: "instance", Op: modelv1.Condition_BINARY_OP_EQ,
		Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}},
	}}}
}

// TestMigrationStream proves the full stream migration pipeline with BOTH copy
// paths in one pass: fast (single-day part, byte copy — including its element
// index) and slow (cross-day part, row re-bucketing plus element-index
// rebuild), then queries the migrated tree back over gRPC.
func TestMigrationStream(t *testing.T) {
	gomega.RegisterTestingT(t)
	gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
	aDay, bDay1, bDay2 := e2eDays()

	// Source standalone: DAY×2 grid + DAY×1 "daily" stage, schema over gRPC.
	srcRoot, srcRootCleanup, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer srcRootCleanup()
	srcAddr, srcClose := startE2EStandalone(t, srcRoot, schema.KindStream, "--stream-flush-timeout=3s")
	defer srcClose()
	srcConn := dialE2E(t, srcAddr)
	defer func() { _ = srcConn.Close() }()

	registerE2EGroup(t, srcConn, streamGroup, commonv1.Catalog_CATALOG_STREAM, e2eDay2Interval(), e2eDailyStage())
	registerStreamSchema(t, srcConn)
	registerE2EIndexRule(t, srcConn, streamGroup, streamName, commonv1.Catalog_CATALOG_STREAM)
	awaitRows(t, 0, func() (int, error) { return tryStreamCount(srcConn, aDay, bDay2) })

	// Write the two batches; wait for each source segment to hold a flushed
	// part before moving on so the snapshot is complete and deterministic.
	writeStreamRows(t, srcConn, e2eBatchA(aDay))
	awaitPartsOnDisk(t, filepath.Join(srcRoot, "stream", "data"), streamGroup, aDay)
	writeStreamRows(t, srcConn, e2eBatchB(bDay1))
	awaitPartsOnDisk(t, filepath.Join(srcRoot, "stream", "data"), streamGroup, bDay1)
	awaitRows(t, e2eTotalCount, func() (int, error) { return tryStreamCount(srcConn, aDay, bDay2) })

	snapshotDir, schemaDir := takeE2ESnapshot(t, srcConn, srcRoot, commonv1.Catalog_CATALOG_STREAM, streamGroup)

	// Migrate into the DAY×1 stage: batch A fast, batch B slow.
	tgtRoot, tgtRootCleanup, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer tgtRootCleanup()
	staging, stagingCleanup, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer stagingCleanup()

	plan := e2ePlan(snapshotDir, schemaDir, streamGroup, filepath.Join(tgtRoot, "stream", "data"))
	executors := []migration.CatalogExecutor{stream.NewMigrationExecutor()}
	cls, err := plan.ClassifyGroups(executors)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	res, err := plan.RunCopy(context.Background(), staging, cls, executors)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	t.Logf("stream RunCopy: rows=%d segs=%d srcParts=%d targetParts=%d fast=%d slow=%d slowRows=%d",
		res.Totals.Rows, res.Totals.Segments, res.Totals.SourceParts, res.Totals.TargetParts,
		stream.FastPathHits(), stream.SlowPathHits(), stream.SlowPathRows())
	gomega.Expect(res.Totals.Rows).To(gomega.Equal(int64(e2eTotalCount)))
	gomega.Expect(stream.FastPathHits()).To(gomega.BeNumerically(">=", 1), "batch A must take the fast path")
	gomega.Expect(stream.SlowPathHits()).To(gomega.BeNumerically(">=", 1), "batch B must take the slow path")
	gomega.Expect(stream.SlowPathRows()).To(gomega.Equal(int64(e2eSlowCount)))

	// Verify: exact row parity (stream never deduplicates) and a non-empty
	// element index in all THREE day segments — bucket A's via byte copy,
	// bucket B's two via the multi-target rebuild.
	var srcRows, tgtRows uint64
	segIdxDocs := map[string]uint64{}
	verifyErr := plan.RunVerify(context.Background(), cls, executors, func(report any) {
		r, ok := report.(stream.EntryGroupReport)
		if !ok {
			return
		}
		srcRows += r.SrcRows
		for _, seg := range r.TargetSegs {
			tgtRows += seg.Rows
			for _, sh := range seg.Shards {
				if sh.IdxOpened {
					segIdxDocs[seg.Seg] += sh.IdxDocCount
				}
			}
		}
	})
	gomega.Expect(verifyErr).NotTo(gomega.HaveOccurred())
	gomega.Expect(srcRows).To(gomega.Equal(uint64(e2eTotalCount)))
	gomega.Expect(tgtRows).To(gomega.Equal(srcRows), "stream never deduplicates: target rows must equal source rows")
	gomega.Expect(segIdxDocs).To(gomega.HaveLen(3), "rows must land in three DAY×1 segments")
	for seg, docs := range segIdxDocs {
		gomega.Expect(docs).To(gomega.BeNumerically(">", 0), "segment %s must carry a non-empty element index", seg)
	}

	// Serve the migrated tree with the DAY×1 flavor of the schema and assert
	// every query shape over gRPC.
	tgtAddr, tgtClose := startE2EStandalone(t, tgtRoot, schema.KindStream, "--stream-flush-timeout=3s")
	defer tgtClose()
	tgtConn := dialE2E(t, tgtAddr)
	defer func() { _ = tgtConn.Close() }()
	registerE2EGroup(t, tgtConn, streamGroup, commonv1.Catalog_CATALOG_STREAM, e2eDay1Interval(), nil)
	registerStreamSchema(t, tgtConn)
	registerE2EIndexRule(t, tgtConn, streamGroup, streamName, commonv1.Catalog_CATALOG_STREAM)
	awaitRows(t, e2eTotalCount, func() (int, error) { return tryStreamCount(tgtConn, aDay, bDay2) })

	fullBegin, fullEnd := aDay.Add(-time.Hour), bDay2.AddDate(0, 0, 1).Add(-time.Millisecond)
	count := func(begin, end time.Time, criteria *modelv1.Criteria) func() (int, error) {
		return func() (int, error) {
			elems, qErr := tryStreamQuery(tgtConn, begin, end, criteria)
			return len(elems), qErr
		}
	}
	expectRows(t, e2eTotalCount, "all migrated rows", count(fullBegin, fullEnd, nil))
	// Per-window splits (TimeRange bounds are BOTH inclusive, hence the -1ms).
	expectRows(t, e2eFastCount, "fast-path rows must stay on bucket A's day",
		count(aDay, aDay.AddDate(0, 0, 1).Add(-time.Millisecond), nil))
	expectRows(t, e2eSlowCount-e2eDay2Count, "slow-path day1 rows",
		count(bDay1, bDay2.Add(-time.Millisecond), nil))
	expectRows(t, e2eDay2Count, "slow-path rows re-bucketed into day2",
		count(bDay2, bDay2.AddDate(0, 0, 1).Add(-time.Millisecond), nil))
	// INVERTED index filter — overall and inside the rebuilt day2 segment.
	expectRows(t, e2eHiTotal, "INVERTED duration filter over the full range",
		count(fullBegin, fullEnd, durationHiEq()))
	expectRows(t, e2eHiDay2, "INVERTED filter must hit the rebuilt day2 element index",
		count(bDay2, bDay2.AddDate(0, 0, 1).Add(-time.Millisecond), durationHiEq()))
	// Entity query (series index / union sidx path).
	expectRows(t, e2eInst0Total, "entity query via the union sidx",
		count(fullBegin, fullEnd, instanceEq(e2eInstance0)))
}

// registerStreamSchema registers the test stream: entity [instance], one
// searchable family with trace_id / instance / duration.
func registerStreamSchema(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := databasev1.NewStreamRegistryServiceClient(conn).Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
		Stream: &databasev1.Stream{
			Metadata: &commonv1.Metadata{Name: streamName, Group: streamGroup},
			Entity:   &databasev1.Entity{TagNames: []string{"instance"}},
			TagFamilies: []*databasev1.TagFamilySpec{{
				Name: streamFamily,
				Tags: []*databasev1.TagSpec{
					{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "instance", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
				},
			}},
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func writeStreamRows(t *testing.T, conn *grpc.ClientConn, rows []e2eRow) {
	t.Helper()
	getResp, err := databasev1.NewStreamRegistryServiceClient(conn).Get(context.Background(),
		&databasev1.StreamRegistryServiceGetRequest{Metadata: &commonv1.Metadata{Name: streamName, Group: streamGroup}})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	md := getResp.GetStream().GetMetadata()

	writeClient, err := streamv1.NewStreamServiceClient(conn).Write(context.Background())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, r := range rows {
		gomega.Expect(writeClient.Send(&streamv1.WriteRequest{
			Metadata:  md,
			MessageId: uint64(time.Now().UnixNano()),
			Element: &streamv1.ElementValue{
				ElementId: r.id,
				Timestamp: timestamppb.New(r.ts.Truncate(time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "trace-" + r.id}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: r.instance}}},
					{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: r.duration}}},
				}}},
			},
		})).To(gomega.Succeed())
	}
	gomega.Expect(writeClient.CloseSend()).To(gomega.Succeed())
	for {
		resp, recvErr := writeClient.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		gomega.Expect(recvErr).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp.GetStatus()).To(gomega.Equal(modelv1.Status_STATUS_SUCCEED.String()))
	}
}

func tryStreamQuery(conn *grpc.ClientConn, begin, end time.Time, criteria *modelv1.Criteria) ([]*streamv1.Element, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := streamv1.NewStreamServiceClient(conn).Query(ctx, &streamv1.QueryRequest{
		Groups: []string{streamGroup},
		Name:   streamName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(begin.Truncate(time.Millisecond)),
			End:   timestamppb.New(end.Truncate(time.Millisecond)),
		},
		Limit:    1000,
		Criteria: criteria,
		Projection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{
			Name: streamFamily,
			Tags: []string{"trace_id", "instance", "duration"},
		}}},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetElements(), nil
}

func tryStreamCount(conn *grpc.ClientConn, aDay, bDay2 time.Time) (int, error) {
	elems, err := tryStreamQuery(conn, aDay.Add(-time.Hour), bDay2.AddDate(0, 0, 1).Add(-time.Millisecond), nil)
	if err != nil {
		return 0, err
	}
	return len(elems), nil
}
