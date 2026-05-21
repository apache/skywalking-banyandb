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

package measure_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/blugelabs/bluge"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/banyand/metadata/service"
	obsservice "github.com/apache/skywalking-banyandb/banyand/observability/services"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/query"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

// MigrationCopy end-to-end: write through the live measure service (so the
// schema goes through the metadata registry and parts land on disk via the
// normal write → flush path), then run MigrationCopy on the live dataPath,
// inspect the migrated target tree at both the **file level**
// (EnumerateGroupTarget) AND at the **service level** by re-opening the
// target tree from a second live measure service and calling
// CollectDataInfo on it — that path mirrors what banyandb's runtime does
// after Step 14 of the in-cluster swap (MIGRATION.md Part 2), so the test
// catches misalignment that pure on-disk inspection might miss.

const defaultTagFamily = "default"

var _ = Describe("MigrationCopy end-to-end (live service)", func() {
	const (
		group       = "e2e_two_day_migration"
		measureName = "e2e_two_day_metric"
	)

	It("migrates a live measure tree and CollectDataInfo on a re-opened target matches the source", func() {
		ctx := context.TODO()
		twoDayInterval := storage.IntervalRule{Unit: storage.DAY, Num: 2}

		// Use ginkgo-owned temp dirs for source + target rootPaths so the
		// source data survives the deliberate mid-test source-service
		// shutdown (which must happen before MigrationCopy runs so every
		// per-seg seriesIndex commits its bluge writer to disk).
		workspace := GinkgoT().TempDir()
		sourceRoot := filepath.Join(workspace, "source")
		targetRoot := filepath.Join(workspace, "target")
		Expect(os.MkdirAll(sourceRoot, 0o755)).To(Succeed())

		// Bring up the source measure service. Wait for the preloaded
		// sw_metric group so we know the schema pipeline is live, then
		// register our own DAY×2 group + measure.
		svcs, deferFn := setUpMigrationTarget(sourceRoot)
		sourceDown := func() {
			if deferFn != nil {
				deferFn()
				deferFn = nil
			}
		}
		defer sourceDown()
		Eventually(func() bool {
			_, ok := svcs.measure.LoadGroup("sw_metric")
			return ok
		}).WithTimeout(30 * time.Second).Should(BeTrue())
		registerMigrationE2EGroup(svcs, group, measureName)
		Eventually(func() bool {
			_, ok := svcs.measure.LoadGroup(group)
			return ok
		}).WithTimeout(30 * time.Second).Should(BeTrue())

		// Pick two timestamps that land in DIFFERENT DAY×2 buckets:
		// align "now" down to the current 2-day bucket, then step back 2
		// and 4 buckets so the source produces exactly two on-disk segs.
		nowBucket := twoDayInterval.Standard(time.Now().UTC())
		day1 := nowBucket.AddDate(0, 0, -4).Add(8 * time.Hour)
		day2 := nowBucket.AddDate(0, 0, -2).Add(8 * time.Hour)
		writeMigrationE2EPoints(svcs, group, measureName, day1, 3, 0)
		writeMigrationE2EPoints(svcs, group, measureName, day2, 3, 3)

		// Wait until the flusher has materialized two segments on disk.
		var sourceInfo *databasev1.DataInfo
		Eventually(func() int {
			info, err := svcs.measure.CollectDataInfo(ctx, group)
			if err != nil || info == nil {
				return 0
			}
			sourceInfo = info
			return len(info.SegmentInfo)
		}).WithTimeout(60*time.Second).WithPolling(time.Second).Should(Equal(2),
			"expected 2 DAY×2 segments on disk after writes are flushed")

		// Capture sourceDataPath + dump the live registry into a synthetic
		// schema-property bluge catalog BEFORE shutting down the source
		// (the registry calls in seedMigrationE2ESchemaProperty require a
		// running service).
		targetDataPath := filepath.Join(targetRoot, "measure", "data")
		staging := filepath.Join(workspace, "staging")
		schemaPropertyRoot := filepath.Join(workspace, "schema-property")
		seedMigrationE2ESchemaProperty(svcs, schemaPropertyRoot, group, measureName)
		sourceDataPath := svcs.measure.(interface{ GetDataPath() string }).GetDataPath()

		// Wait until every per-seg bluge sidx dir has a `.snp` snapshot
		// file. Measure's flushTimeout (5s) drives PersisterNapTime in
		// the underlying inverted.Store, and the persister exits without
		// writing the `.snp` if the writer is closed before that nap
		// elapses — so we poll the disk until persistence is confirmed
		// before tearing the service down. (Production tolerates the
		// missing-snapshot case via mergeOneSourceSidxInto, but the test
		// wants the strict version so the migrated sidx is never empty.)
		sourceGroupRoot := filepath.Join(sourceDataPath, group)
		Eventually(func() bool {
			return allSidxDirsHaveSnapshot(sourceGroupRoot)
		}).WithTimeout(60*time.Second).WithPolling(time.Second).Should(BeTrue(),
			"every <seg>/sidx/ under %s should carry a committed .snp before migration runs",
			sourceGroupRoot)

		// Stop the source service so every tsTable + per-segment
		// seriesIndex commits its bluge writer to disk. Migration must
		// run against a quiescent tree (the in-cluster runbook scales
		// the data StatefulSets to 0 for the same reason) — otherwise
		// the migrated target's `sidx/` carries no committed snapshot
		// and the runtime can't resolve series IDs at query time.
		sourceDown()

		cfg := measure.DirectCopyConfig{
			SchemaPropertyPath: schemaPropertyRoot,
			SidxStagingDir:     staging,
			Groups:             []string{group},
			Entries: []measure.DirectCopyEntry{
				{
					Stage:  measure.StageHot,
					Target: targetDataPath,
					Nodes:  []string{"node-0"},
					Source: []string{sourceDataPath},
				},
			},
		}
		res, err := measure.MigrationCopy(ctx, cfg)
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Segments).To(Equal(len(sourceInfo.SegmentInfo)),
			"copy res should land the same number of segments as the source")

		// File-level inspect: enumerate every target seg and cross-check
		// startTime against the source CollectDataInfo and endTime
		// against the on-disk metadata file.
		targetGroupRoot := filepath.Join(targetDataPath, group)
		reports, err := measure.EnumerateGroupTarget(targetGroupRoot, twoDayInterval, fs.NewLocalFileSystem())
		Expect(err).NotTo(HaveOccurred())
		Expect(reports).To(HaveLen(len(sourceInfo.SegmentInfo)))

		sourceByStart := map[string]*databasev1.SegmentInfo{}
		for _, si := range sourceInfo.SegmentInfo {
			sourceByStart[si.TimeRangeStart] = si
		}
		for _, r := range reports {
			Expect(r.Aligned).To(BeTrue(), "seg %s should align to DAY×2 grid", r.Seg)
			startRFC := r.StartTime.Format(time.RFC3339Nano)
			si, ok := sourceByStart[startRFC]
			Expect(ok).To(BeTrue(),
				"target seg %s startTime %s does not match any source segment (have: %v)",
				r.Seg, startRFC, keysOf(sourceByStart))
			Expect(r.Rows).To(BeNumerically(">=", uint64(1)),
				"seg %s should carry at least one row", r.Seg)

			raw, readErr := os.ReadFile(filepath.Join(targetGroupRoot, r.Seg, storage.SegmentMetadataFilename))
			Expect(readErr).NotTo(HaveOccurred(), "read %s/metadata", r.Seg)
			var meta storage.SegmentMetadata
			Expect(json.Unmarshal(raw, &meta)).To(Succeed())
			Expect(meta.Version).To(Equal(storage.CurrentSegmentVersion))
			Expect(meta.EndTime).To(Equal(si.TimeRangeEnd),
				"seg %s endTime mismatch: target=%s, source=%s",
				r.Seg, meta.EndTime, si.TimeRangeEnd)

			// Standard alignment invariant: the segment must span exactly
			// ONE bucket on the IntervalRule grid — start = Standard(start)
			// AND end = NextTime(start) — which is what banyandb's
			// segmentController guarantees at runtime. Asserting this on
			// the migrated target catches grid drift the per-seg
			// EnumerateGroupTarget Aligned bit alone won't flag.
			Expect(twoDayInterval.Standard(r.StartTime)).To(Equal(r.StartTime),
				"seg %s start %s is not aligned to its own DAY×2 standard %s",
				r.Seg, r.StartTime, twoDayInterval.Standard(r.StartTime))
			endTime, parseErr := time.Parse(time.RFC3339Nano, meta.EndTime)
			Expect(parseErr).NotTo(HaveOccurred(), "parse seg %s endTime %q", r.Seg, meta.EndTime)
			Expect(endTime).To(Equal(twoDayInterval.NextTime(r.StartTime)),
				"seg %s [%s, %s) is not exactly one DAY×2 bucket wide (expected end %s)",
				r.Seg, r.StartTime, endTime, twoDayInterval.NextTime(r.StartTime))
		}

		// Service-level inspect: re-open the migrated target from a fresh
		// measure service (which is what banyandb does after the in-cluster
		// data-dir swap in MIGRATION.md Part 2 Step 14 → Step 16). The
		// new service must surface the exact same segment count / time
		// ranges via CollectDataInfo, otherwise a query against the
		// migrated cluster would see segment-boundary inconsistencies.
		svcs2, deferFn2 := setUpMigrationTarget(targetRoot)
		defer deferFn2()
		Eventually(func() bool {
			_, ok := svcs2.measure.LoadGroup("sw_metric")
			return ok
		}).WithTimeout(30 * time.Second).Should(BeTrue())
		registerMigrationE2EGroup(svcs2, group, measureName)
		Eventually(func() bool {
			_, ok := svcs2.measure.LoadGroup(group)
			return ok
		}).WithTimeout(30 * time.Second).Should(BeTrue())
		Eventually(func() error {
			_, err := svcs2.measure.Measure(&commonv1.Metadata{Name: measureName, Group: group})
			return err
		}).WithTimeout(30*time.Second).Should(Succeed(),
			"re-opened service should resolve the measure before the query runs")

		var targetInfo *databasev1.DataInfo
		Eventually(func() int {
			info, ierr := svcs2.measure.CollectDataInfo(ctx, group)
			if ierr != nil || info == nil {
				return 0
			}
			targetInfo = info
			return len(info.SegmentInfo)
		}).WithTimeout(30*time.Second).WithPolling(time.Second).Should(
			Equal(len(sourceInfo.SegmentInfo)),
			"re-opened service should rediscover the same number of segments")

		// Cross-check every TimeRangeStart / TimeRangeEnd against the
		// source CollectDataInfo so any segment grid drift fails loudly.
		targetByStart := map[string]*databasev1.SegmentInfo{}
		for _, si := range targetInfo.SegmentInfo {
			targetByStart[si.TimeRangeStart] = si
		}
		for start, src := range sourceByStart {
			tgt, ok := targetByStart[start]
			Expect(ok).To(BeTrue(),
				"source segment with TimeRangeStart=%s missing from target CollectDataInfo (have: %v)",
				start, keysOf(targetByStart))
			Expect(tgt.TimeRangeEnd).To(Equal(src.TimeRangeEnd),
				"segment %s: target TimeRangeEnd=%s != source TimeRangeEnd=%s",
				start, tgt.TimeRangeEnd, src.TimeRangeEnd)
		}

		// Final check: drive a real measure query through the re-opened
		// service's pipeline so we exercise the runtime's part-merging /
		// block-scanning path against the migrated parts. A working
		// query proves the migrated tree is not just on-disk-shaped but
		// actually decodable by banyandb's reader.
		queryBegin := day1.Add(-time.Hour)
		queryEnd := day2.Add(time.Hour)
		dps := queryMigrationE2E(svcs2, group, measureName, queryBegin, queryEnd)
		Expect(dps).To(HaveLen(6),
			"query against the migrated target should return all 6 written data points")
		seenEntities := map[string]struct{}{}
		for _, dp := range dps {
			for _, tf := range dp.TagFamilies {
				if tf.Name != defaultTagFamily {
					continue
				}
				for _, tag := range tf.Tags {
					if tag.Key == "entity_id" {
						if sv := tag.Value.GetStr(); sv != nil {
							seenEntities[sv.Value] = struct{}{}
						}
					}
				}
			}
		}
		Expect(seenEntities).To(HaveLen(6),
			"every entity-id written into the source should appear in the post-migration query (got %v)",
			seenEntities)
	})
})

// queryMigrationE2E issues a measurev1.QueryRequest through the supplied
// services bundle's local pipeline and returns the resulting data points.
// Used to prove the migrated tree is queryable end-to-end through the
// runtime's part-merging / block-scanning path, not just on-disk shaped.
func queryMigrationE2E(svcs *services, group, measureName string, begin, end time.Time) []*measurev1.DataPoint {
	req := &measurev1.QueryRequest{
		Groups: []string{group},
		Name:   measureName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(begin),
			End:   timestamppb.New(end),
		},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: defaultTagFamily, Tags: []string{"id", "entity_id"}},
			},
		},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{
			Names: []string{"total", "value"},
		},
	}
	feat, err := svcs.pipeline.Publish(context.Background(), data.TopicMeasureQuery,
		bus.NewMessage(bus.MessageID(time.Now().UnixNano()), req))
	Expect(err).NotTo(HaveOccurred())
	msg, err := feat.Get()
	Expect(err).NotTo(HaveOccurred())
	switch d := msg.Data().(type) {
	case *measurev1.QueryResponse:
		return d.DataPoints
	case *common.Error:
		Fail(fmt.Sprintf("measure query returned error: %s", d.Error()))
	default:
		Fail(fmt.Sprintf("expected *measurev1.QueryResponse, got %T", d))
	}
	return nil
}

// allSidxDirsHaveSnapshot returns true when every `<groupRoot>/seg-*/sidx`
// directory carries at least one `.snp` file (bluge's snapshot marker).
// Used to wait deterministically for the per-seg seriesIndex persister to
// fire before MigrationCopy scans the source — see the call site for
// rationale.
func allSidxDirsHaveSnapshot(groupRoot string) bool {
	segs, err := os.ReadDir(groupRoot)
	if err != nil {
		return false
	}
	saw := 0
	for _, seg := range segs {
		if !seg.IsDir() || !strings.HasPrefix(seg.Name(), "seg-") {
			continue
		}
		sidxDir := filepath.Join(groupRoot, seg.Name(), "sidx")
		entries, readErr := os.ReadDir(sidxDir)
		if readErr != nil {
			return false
		}
		hasSnap := false
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".snp") {
				hasSnap = true
				break
			}
		}
		if !hasSnap {
			return false
		}
		saw++
	}
	return saw > 0
}

func keysOf(m map[string]*databasev1.SegmentInfo) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// registerMigrationE2EGroup creates a CATALOG_MEASURE group with
// SegmentInterval = DAY × 2 and one matching measure under it, using the
// metadata registry of the supplied services bundle. The same call is
// made on both the source service and the post-migration re-opened
// service so each one's schemaRepo discovers the on-disk segments.
func registerMigrationE2EGroup(svcs *services, group, measureName string) {
	ctx := context.TODO()
	_, err := svcs.metadataService.GroupRegistry().CreateGroup(ctx, &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: group},
		Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 2},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 30},
		},
	})
	if err != nil && !isAlreadyExistsErr(err) {
		Expect(err).NotTo(HaveOccurred())
	}
	_, err = svcs.metadataService.MeasureRegistry().CreateMeasure(ctx, &databasev1.Measure{
		Metadata: &commonv1.Metadata{Name: measureName, Group: group},
		Entity:   &databasev1.Entity{TagNames: []string{"entity_id"}},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: defaultTagFamily, Tags: []*databasev1.TagSpec{
				{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
			}},
		},
		Fields: []*databasev1.FieldSpec{
			{
				Name:              "total",
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			},
			{
				Name:              "value",
				FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			},
		},
	})
	if err != nil && !isAlreadyExistsErr(err) {
		Expect(err).NotTo(HaveOccurred())
	}
}

func isAlreadyExistsErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return msg != "" && (containsAny(msg, "already exists", "AlreadyExists"))
}

func containsAny(s string, needles ...string) bool {
	for _, n := range needles {
		if n == "" {
			continue
		}
		for i := 0; i+len(n) <= len(s); i++ {
			if s[i:i+len(n)] == n {
				return true
			}
		}
	}
	return false
}

// writeMigrationE2EPoints emits `count` data points spaced 1s apart starting
// at `baseTime`, each carrying a unique entity_id so the writes spread
// across all configured shards.
func writeMigrationE2EPoints(svcs *services, group, name string, baseTime time.Time, count, entityOffset int) {
	bp := svcs.pipeline.NewBatchPublisher(5 * time.Second)
	defer bp.Close()
	for i := 0; i < count; i++ {
		idx := entityOffset + i
		iStr := strconv.Itoa(idx)
		req := &measurev1.WriteRequest{
			Metadata: &commonv1.Metadata{Name: name, Group: group},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(baseTime.Add(time.Duration(i) * time.Second)),
				TagFamilies: []*modelv1.TagFamilyForWrite{{
					Tags: []*modelv1.TagValue{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "id-" + iStr}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity-" + iStr}}},
					},
				}},
				Fields: []*modelv1.FieldValue{
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(100 + idx)}}},
					{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: int64(idx)}}},
				},
			},
		}
		bp.Publish(context.TODO(), data.TopicMeasureWrite,
			bus.NewMessage(bus.MessageID(idx), &measurev1.InternalWriteRequest{
				EntityValues: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity-" + iStr}}},
				},
				Request: req,
			}))
	}
}

// seedMigrationE2ESchemaProperty writes a synthetic schema-property bluge
// catalog at <root>/shard-0/ containing the live registry's Group + Measure
// docs. The on-disk shape is exactly what walkSchemaPropertyShard reads in
// production.
func seedMigrationE2ESchemaProperty(svcs *services, root, group, measureName string) {
	ctx := context.TODO()
	grpProto, err := svcs.metadataService.GroupRegistry().GetGroup(ctx, group)
	Expect(err).NotTo(HaveOccurred())
	measureProto, err := svcs.metadataService.MeasureRegistry().GetMeasure(ctx,
		&commonv1.Metadata{Name: measureName, Group: group})
	Expect(err).NotTo(HaveOccurred())

	shardPath := filepath.Join(root, "shard-0")
	Expect(os.MkdirAll(shardPath, storage.DirPerm)).To(Succeed())
	w, err := bluge.OpenWriter(bluge.DefaultConfig(shardPath))
	Expect(err).NotTo(HaveOccurred())
	defer func() { Expect(w.Close()).To(Succeed()) }()

	batch := bluge.NewBatch()
	grpJSON, err := protojson.Marshal(grpProto)
	Expect(err).NotTo(HaveOccurred())
	batch.Insert(migrationE2EBlugeDoc("group/"+group, schema.KindGroup.String(), "", string(grpJSON)))
	measureJSON, err := protojson.Marshal(measureProto)
	Expect(err).NotTo(HaveOccurred())
	batch.Insert(migrationE2EBlugeDoc("measure/"+group+"/"+measureName,
		schema.KindMeasure.String(), group, string(measureJSON)))
	Expect(w.Batch(batch)).To(Succeed())
}

// migrationE2EBlugeDoc wraps one inner-proto JSON in a propertyv1.Property
// and returns a bluge doc whose `_source` field carries the Property JSON.
func migrationE2EBlugeDoc(id, kind, group, sourceJSON string) *bluge.Document {
	tags := []*modelv1.Tag{
		{Key: "source", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: sourceJSON}}}},
	}
	if group != "" {
		tags = append(tags, &modelv1.Tag{
			Key:   "group",
			Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: group}}},
		})
	}
	prop := &propertyv1.Property{
		Id:       id,
		Metadata: &commonv1.Metadata{Name: kind, ModRevision: 1},
		Tags:     tags,
	}
	propJSON, err := protojson.Marshal(prop)
	Expect(err).NotTo(HaveOccurred())
	return bluge.NewDocument(id).AddField(bluge.NewStoredOnlyField("_source", propJSON))
}

// setUpMigrationTarget mirrors setUp() from measure_suite_test.go but pins
// the measure root path to an existing on-disk tree (the migration target)
// so a fresh standalone service can re-open it. The metadata service is
// fresh so the caller must re-register every group it cares about; the
// preloaded fixtures (sw_metric et al.) come up automatically as in setUp().
func setUpMigrationTarget(measureRootPath string) (*services, func()) {
	pipeline := queue.Local()
	metadataService, err := service.NewService()
	Expect(err).NotTo(HaveOccurred())
	metricSvc := obsservice.NewMetricService(metadataService, pipeline, "test", nil)
	pm := protector.NewMemory(metricSvc)
	measureService, err := measure.NewStandalone(metadataService, pipeline, nil, metricSvc, pm)
	Expect(err).NotTo(HaveOccurred())
	preloadSvc := &preloadMeasureService{metaSvc: metadataService}
	querySvc, err := query.NewService(context.TODO(), nil, measureService, nil, metadataService, pipeline, metricSvc)
	Expect(err).NotTo(HaveOccurred())

	metaPath, metaDeferFunc, err := test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	schemaPorts, err := test.AllocateFreePorts(1)
	Expect(err).NotTo(HaveOccurred())
	flags := []string{
		"--schema-server-root-path=" + metaPath,
		fmt.Sprintf("--schema-server-grpc-port=%d", schemaPorts[0]),
		"--schema-server-grpc-host=127.0.0.1",
		"--measure-root-path=" + measureRootPath,
	}
	moduleDeferFunc := test.SetupModules(flags, pipeline, metadataService, preloadSvc, measureService, querySvc)
	return &services{
			measure:         measureService,
			metadataService: metadataService,
			pipeline:        pipeline,
		}, func() {
			moduleDeferFunc()
			metaDeferFunc()
		}
}
