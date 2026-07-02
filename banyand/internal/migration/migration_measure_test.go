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

package migration_test

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

const (
	measureGroup  = "mig_measure"
	measureName   = "metrics"
	measureFamily = "default"
)

// TestMigrationMeasure proves the full measure migration pipeline with BOTH
// copy paths in one pass — fast (single-day part, byte copy) and slow
// (cross-day part, row re-bucketing) — then queries the migrated tree back
// over gRPC. The fixture keeps every (series, timestamp) pair unique, so the
// slow path's per-chunk deduplication never drops a row and the row parity
// check stays exact.
func TestMigrationMeasure(t *testing.T) {
	gomega.RegisterTestingT(t)
	gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
	aDay, bDay1, bDay2 := e2eDays()

	// Source standalone: DAY×2 grid + DAY×1 "daily" stage, schema over gRPC.
	srcRoot, srcRootCleanup, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer srcRootCleanup()
	srcAddr, srcClose := startE2EStandalone(t, srcRoot, schema.KindMeasure, "--measure-flush-timeout=3s")
	defer srcClose()
	srcConn := dialE2E(t, srcAddr)
	defer func() { _ = srcConn.Close() }()

	registerE2EGroup(t, srcConn, measureGroup, commonv1.Catalog_CATALOG_MEASURE, e2eDay2Interval(), e2eDailyStage())
	registerMeasureSchema(t, srcConn)
	registerE2EIndexRule(t, srcConn, measureGroup, measureName, commonv1.Catalog_CATALOG_MEASURE)
	awaitRows(t, 0, func() (int, error) { return tryMeasureCount(srcConn, aDay, bDay2) })

	// Write the two batches; wait for each source segment to hold a flushed
	// part before moving on so the snapshot is complete and deterministic.
	writeMeasureRows(t, srcConn, e2eBatchA(aDay))
	awaitPartsOnDisk(t, filepath.Join(srcRoot, "measure", "data"), measureGroup, aDay)
	writeMeasureRows(t, srcConn, e2eBatchB(bDay1))
	awaitPartsOnDisk(t, filepath.Join(srcRoot, "measure", "data"), measureGroup, bDay1)
	awaitRows(t, e2eTotalCount, func() (int, error) { return tryMeasureCount(srcConn, aDay, bDay2) })

	snapshotDir, schemaDir := takeE2ESnapshot(t, srcConn, srcRoot, commonv1.Catalog_CATALOG_MEASURE, measureGroup)

	// Migrate into the DAY×1 stage: batch A fast, batch B slow.
	tgtRoot, tgtRootCleanup, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer tgtRootCleanup()
	staging, stagingCleanup, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer stagingCleanup()

	plan := e2ePlan(snapshotDir, schemaDir, measureGroup, filepath.Join(tgtRoot, "measure", "data"))
	executors := []migration.CatalogExecutor{measure.NewMigrationExecutor()}
	cls, err := plan.ClassifyGroups(executors)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	res, err := plan.RunCopy(context.Background(), staging, cls, executors)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	t.Logf("measure RunCopy: rows=%d segs=%d srcParts=%d targetParts=%d fast=%d slow=%d slowRows=%d",
		res.Totals.Rows, res.Totals.Segments, res.Totals.SourceParts, res.Totals.TargetParts,
		measure.FastPathHits(), measure.SlowPathHits(), measure.SlowPathRows())
	gomega.Expect(res.Totals.Rows).To(gomega.Equal(int64(e2eTotalCount)))
	gomega.Expect(measure.FastPathHits()).To(gomega.BeNumerically(">=", 1), "batch A must take the fast path")
	gomega.Expect(measure.SlowPathHits()).To(gomega.BeNumerically(">=", 1), "batch B must take the slow path")
	gomega.Expect(measure.SlowPathRows()).To(gomega.Equal(int64(e2eSlowCount)))

	// Verify: exact row parity (every (series, ts) is unique, so the slow
	// path's chunk dedup drops nothing) and the broadcast union sidx present
	// in all THREE day segments.
	var srcRows, tgtRows uint64
	segSidxDocs := map[string]uint64{}
	verifyErr := plan.RunVerify(context.Background(), cls, executors, func(report any) {
		r, ok := report.(measure.EntryGroupReport)
		if !ok {
			return
		}
		srcRows += r.SrcRows
		for _, seg := range r.TargetSegs {
			tgtRows += seg.Rows
			if seg.SidxOpened {
				segSidxDocs[seg.Seg] += seg.SidxDocCount
			}
		}
	})
	gomega.Expect(verifyErr).NotTo(gomega.HaveOccurred())
	gomega.Expect(srcRows).To(gomega.Equal(uint64(e2eTotalCount)))
	gomega.Expect(tgtRows).To(gomega.Equal(srcRows), "unique (series, ts) pairs: no slow-path dedup may drop rows")
	gomega.Expect(segSidxDocs).To(gomega.HaveLen(3), "rows must land in three DAY×1 segments")
	for seg, docs := range segSidxDocs {
		gomega.Expect(docs).To(gomega.BeNumerically(">", 0), "segment %s must carry the broadcast union sidx", seg)
	}

	// Serve the migrated tree with the DAY×1 flavor of the schema and assert
	// every query shape over gRPC.
	tgtAddr, tgtClose := startE2EStandalone(t, tgtRoot, schema.KindMeasure, "--measure-flush-timeout=3s")
	defer tgtClose()
	tgtConn := dialE2E(t, tgtAddr)
	defer func() { _ = tgtConn.Close() }()
	registerE2EGroup(t, tgtConn, measureGroup, commonv1.Catalog_CATALOG_MEASURE, e2eDay1Interval(), nil)
	registerMeasureSchema(t, tgtConn)
	registerE2EIndexRule(t, tgtConn, measureGroup, measureName, commonv1.Catalog_CATALOG_MEASURE)
	awaitRows(t, e2eTotalCount, func() (int, error) { return tryMeasureCount(tgtConn, aDay, bDay2) })

	fullBegin, fullEnd := aDay.Add(-time.Hour), bDay2.AddDate(0, 0, 1).Add(-time.Millisecond)
	count := func(begin, end time.Time, criteria *modelv1.Criteria) func() (int, error) {
		return func() (int, error) {
			dps, qErr := tryMeasureQuery(tgtConn, begin, end, criteria)
			return len(dps), qErr
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
	// Tag condition over the whole range and inside the slow-path day2 segment.
	expectRows(t, e2eHiTotal, "indexed duration filter over the full range",
		count(fullBegin, fullEnd, durationHiEq()))
	expectRows(t, e2eHiDay2, "indexed duration filter inside the day2 segment",
		count(bDay2, bDay2.AddDate(0, 0, 1).Add(-time.Millisecond), durationHiEq()))
	// Entity query (series index / broadcast union sidx path).
	expectRows(t, e2eInst0Total, "entity query via the broadcast union sidx",
		count(fullBegin, fullEnd, instanceEq(e2eInstance0)))
}

// registerMeasureSchema registers the test measure: entity [instance], one
// tag family with trace_id / instance / duration plus one int field.
func registerMeasureSchema(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := databasev1.NewMeasureRegistryServiceClient(conn).Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
		Measure: &databasev1.Measure{
			Metadata: &commonv1.Metadata{Name: measureName, Group: measureGroup},
			Entity:   &databasev1.Entity{TagNames: []string{"instance"}},
			TagFamilies: []*databasev1.TagFamilySpec{{
				Name: measureFamily,
				Tags: []*databasev1.TagSpec{
					{Name: "trace_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "instance", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "duration", Type: databasev1.TagType_TAG_TYPE_INT},
				},
			}},
			Fields: []*databasev1.FieldSpec{{
				Name:              "value",
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			}},
			Interval: "1m",
		},
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
}

func writeMeasureRows(t *testing.T, conn *grpc.ClientConn, rows []e2eRow) {
	t.Helper()
	getResp, err := databasev1.NewMeasureRegistryServiceClient(conn).Get(context.Background(),
		&databasev1.MeasureRegistryServiceGetRequest{Metadata: &commonv1.Metadata{Name: measureName, Group: measureGroup}})
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	md := getResp.GetMeasure().GetMetadata()

	writeClient, err := measurev1.NewMeasureServiceClient(conn).Write(context.Background())
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, r := range rows {
		gomega.Expect(writeClient.Send(&measurev1.WriteRequest{
			Metadata:  md,
			MessageId: uint64(time.Now().UnixNano()),
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(r.ts.Truncate(time.Millisecond)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "trace-" + r.id}}},
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: r.instance}}},
					{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: r.duration}}},
				}}},
				Fields: []*modelv1.FieldValue{
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: r.duration}}},
				},
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

func tryMeasureQuery(conn *grpc.ClientConn, begin, end time.Time, criteria *modelv1.Criteria) ([]*measurev1.DataPoint, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := measurev1.NewMeasureServiceClient(conn).Query(ctx, &measurev1.QueryRequest{
		Groups: []string{measureGroup},
		Name:   measureName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(begin.Truncate(time.Millisecond)),
			End:   timestamppb.New(end.Truncate(time.Millisecond)),
		},
		Limit:    1000,
		Criteria: criteria,
		TagProjection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{
			Name: measureFamily,
			Tags: []string{"trace_id", "instance", "duration"},
		}}},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetDataPoints(), nil
}

func tryMeasureCount(conn *grpc.ClientConn, aDay, bDay2 time.Time) (int, error) {
	dps, err := tryMeasureQuery(conn, aDay.Add(-time.Hour), bDay2.AddDate(0, 0, 1).Add(-time.Millisecond), nil)
	if err != nil {
		return 0, err
	}
	return len(dps), nil
}
