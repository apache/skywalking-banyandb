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
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blugelabs/bluge"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/internal/migration"
	"github.com/apache/skywalking-banyandb/banyand/internal/storage"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/index"
)

// End-to-end migration of an INDEX-MODE measure that carries an index rule.
//
// Index-mode measures store no columnar parts: every data point is one inverted
// document under <seg>/sidx/, upserted by series per segment. The migration must
// rebuild each doc (stored tags + regenerated index-only entity fields) AND
// restore the index rule so the indexed tag stays searchable. This test drives
// the whole path through the LIVE service over gRPC-equivalent pipelines:
//
//	register group + index-mode measure + index rule + binding
//	-> write points (indexed `service_name` + entity `id`) across two segments
//	-> baseline: source answers a full scan AND an indexed-tag filter
//	-> migrate the quiescent tree
//	-> re-open the target and assert it answers BOTH queries identically.
//
// Every assertion that depends on asynchronous schema propagation, flushing or
// segment discovery is wrapped in Eventually so a slow / low-CPU runner retries
// instead of failing on a transient empty read.
// Fixed identifiers for the index-mode e2e fixture. Package-level so the query
// helper can reference the group/measure directly (they are invariant for this
// spec) without taking them as always-constant parameters.
const (
	idxModeGroup       = "e2e_indexmode_migration"
	idxModeMeasureName = "e2e_indexmode_metric"
)

var _ = Describe("migration.RunCopy index-mode end-to-end (live service)", func() {
	const (
		group       = idxModeGroup
		measureName = idxModeMeasureName
		ruleName    = "e2e_indexmode_service_name_rule"
		bindingName = "e2e_indexmode_binding"
		idxTag      = "service_name"
		entityTag   = "id"
		pointCount  = 6 // 3 per segment, unique entity id + service_name each
	)

	It("merges two source segments through the rebuild (slow) path and the re-opened target answers full-scan + indexed-tag queries identically to the source", func() {
		ctx := context.TODO()
		// Source writes on a DAY×1 grid (group default); the migration re-grids to
		// DAY×2 (the "hot" lifecycle stage), so the two adjacent 1-day source
		// segments MERGE into a single 2-day target segment. A merge makes every
		// source ineligible for the whole-sidx byte copy, forcing the rebuild
		// (slow) path — which this test asserts both by content and by Bytes == 0.
		twoDay := storage.IntervalRule{Unit: storage.DAY, Num: 2}
		oneDayProto := &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1}
		twoDayProto := &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 2}
		ttlProto := &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 30}
		// Source: DAY×1 write grid + a "hot" stage that re-grids to DAY×2 at migration.
		sourceOpts := &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: oneDayProto,
			Ttl:             ttlProto,
			Stages: []*commonv1.LifecycleStage{{
				Name: "hot", ShardNum: 1, SegmentInterval: twoDayProto, Ttl: ttlProto, NodeSelector: "type=hot",
			}},
		}
		// Target serves the migrated DAY×2 segments.
		targetOpts := &commonv1.ResourceOpts{ShardNum: 1, SegmentInterval: twoDayProto, Ttl: ttlProto}

		workspace := GinkgoT().TempDir()
		sourceRoot := filepath.Join(workspace, "source")
		targetRoot := filepath.Join(workspace, "target")

		// ── Source: bring up the live service, register the index-mode schema. ──
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
		registerIndexModeE2E(svcs, sourceOpts, group, measureName, ruleName, bindingName, idxTag, entityTag)
		Eventually(func() bool {
			_, ok := svcs.measure.LoadGroup(group)
			return ok
		}).WithTimeout(30 * time.Second).Should(BeTrue())
		// Gate writes on the index rule being APPLIED, not merely on the measure
		// resolving. The binding event arrives on a separate channel after the
		// measure loads, so a measure can resolve before its index-rule locators
		// exist — and a point written in that window lands UN-indexed permanently,
		// which no later wait can repair. GetIndexRules() reflects the same index
		// schema the write path consults, so it is the deterministic ready signal.
		Eventually(func() []string {
			return measureIndexRuleNames(svcs, group, measureName)
		}).WithTimeout(30*time.Second).WithPolling(time.Second).Should(ContainElement(ruleName),
			"source must apply the index rule before writes, or points land un-indexed")

		// Two timestamps in DIFFERENT 1-day buckets but the SAME 2-day bucket, so
		// the source flushes two 1-day segments that the migration merges into one.
		bucket := twoDay.Standard(time.Now().UTC().AddDate(0, 0, -6))
		day1 := bucket.Add(8 * time.Hour)
		day2 := bucket.Add(32 * time.Hour)
		writeIndexModeE2EPoints(svcs, group, measureName, day1, pointCount/2, 0)
		writeIndexModeE2EPoints(svcs, group, measureName, day2, pointCount/2, pointCount/2)

		// Wait until both 1-day source segments have materialized on disk.
		Eventually(func() int {
			info, err := svcs.measure.CollectDataInfo(ctx, group)
			if err != nil || info == nil {
				return 0
			}
			return len(info.SegmentInfo)
		}).WithTimeout(60*time.Second).WithPolling(time.Second).Should(Equal(2),
			"expected 2 DAY×1 source segments on disk after the writes are flushed")

		queryBegin := day1.Add(-time.Hour)
		queryEnd := day2.Add(time.Hour)

		// Baseline (pre-migration): the source must answer a full scan and an
		// indexed-tag filter. Capturing the SOURCE's full-scan content here lets the
		// post-migration assertions compare target == source field-for-field, and
		// isolates a migration regression from an indexing one — if the source
		// itself cannot serve the indexed query, the data was never indexed.
		//
		// srcContent is captured from the SAME query result that satisfies the
		// assertion, so the oracle is never a separate, racy re-query.
		var srcContent map[string]string
		Eventually(func() map[string]string {
			dps := queryIndexModeE2E(svcs, queryBegin, queryEnd, nil)
			srcContent = idxModeContentByID(dps)
			return idxModeIDToTag(dps, idxTag)
		}).WithTimeout(30*time.Second).WithPolling(time.Second).Should(Equal(expectedIDToTag(pointCount)),
			"source full scan should return every point with its indexed tag value")
		Eventually(func() []string {
			return idxModeEntities(queryIndexModeE2E(svcs, queryBegin, queryEnd, eqCriteria(idxTag, "svc-2")))
		}).WithTimeout(30*time.Second).WithPolling(time.Second).Should(ConsistOf("id-2"),
			"source indexed-tag filter service_name=svc-2 should return exactly id-2 (proves indexing)")

		// Dump the live registry (group + measure + index rule) into a synthetic
		// schema-property catalog the migration reads, then wait for every sidx to
		// snapshot and stop the source so the tree is quiescent for the copy.
		targetDataPath := filepath.Join(targetRoot, "measure", "data")
		staging := filepath.Join(workspace, "staging")
		schemaPropertyRoot := filepath.Join(workspace, "schema-property")
		seedIndexModeE2ESchemaProperty(svcs, schemaPropertyRoot, group, measureName, ruleName)
		sourceDataPath := svcs.measure.(interface{ GetDataPath() string }).GetDataPath()
		sourceGroupRoot := filepath.Join(sourceDataPath, group)
		Eventually(func() bool {
			return allSidxDirsHaveSnapshot(sourceGroupRoot)
		}).WithTimeout(60*time.Second).WithPolling(time.Second).Should(BeTrue(),
			"every <seg>/sidx/ under %s should carry a committed .snp before migration runs", sourceGroupRoot)
		sourceDown()

		// ── Migrate the quiescent tree. ──
		plan := &migration.CopyPlan{
			Source: migration.CopySource{Live: &migration.LiveSource{
				SchemaPropertyPath: schemaPropertyRoot,
				Stages: map[string][]migration.LiveStageNode{
					"hot": {{Node: "node-0", Root: sourceDataPath}},
				},
			}},
			Groups: []string{group},
			Entries: []migration.CopyEntry{
				{Stage: "hot", Target: targetDataPath, Nodes: []string{"node-0"}},
			},
		}
		executors := []migration.CatalogExecutor{measure.NewMigrationExecutor()}
		cls, err := plan.ClassifyGroups(executors)
		Expect(err).NotTo(HaveOccurred())
		res, err := plan.RunCopy(ctx, staging, cls, executors)
		Expect(err).NotTo(HaveOccurred())

		// Slow-path proof: index-mode records copied Bytes ONLY on the whole-sidx
		// byte-copy fast path; the rebuild path writes docs via UpdateSeriesBatch and
		// adds zero bytes. Two sources merging into one target make both ineligible
		// for byte-copy, so a pure rebuild run reports Bytes == 0 with all rows written.
		Expect(res.Totals.Rows).To(Equal(int64(pointCount)), "every source doc should be rewritten")
		Expect(res.Totals.Bytes).To(BeZero(),
			"the two source segments merge into one target segment, so every source takes the rebuild (slow) path — a byte-copy would have recorded copied bytes")

		// ── Target: re-open from a fresh service (DAY×2 grid), re-register, and
		// assert it answers the SAME queries as the source. ──
		svcs2, deferFn2 := setUpMigrationTarget(targetRoot)
		defer deferFn2()
		Eventually(func() bool {
			_, ok := svcs2.measure.LoadGroup("sw_metric")
			return ok
		}).WithTimeout(30 * time.Second).Should(BeTrue())
		registerIndexModeE2E(svcs2, targetOpts, group, measureName, ruleName, bindingName, idxTag, entityTag)
		Eventually(func() bool {
			_, ok := svcs2.measure.LoadGroup(group)
			return ok
		}).WithTimeout(30 * time.Second).Should(BeTrue())
		// Wait for the index rule to be APPLIED (not just the measure resolved) so
		// the indexed-tag filter below is asserting migration fidelity, not racing
		// the target's own binding propagation.
		Eventually(func() []string {
			return measureIndexRuleNames(svcs2, group, measureName)
		}).WithTimeout(30*time.Second).WithPolling(time.Second).Should(ContainElement(ruleName),
			"re-opened service must apply the index rule before the indexed query runs")

		// The two 1-day source segments collapsed into a single 2-day target segment.
		Eventually(func() int {
			info, ierr := svcs2.measure.CollectDataInfo(ctx, group)
			if ierr != nil || info == nil {
				return 0
			}
			return len(info.SegmentInfo)
		}).WithTimeout(30*time.Second).WithPolling(time.Second).Should(Equal(1),
			"the merge should leave exactly one DAY×2 target segment")

		// Content equality: the migrated target answers the full scan with the EXACT
		// same per-series content (every tag value + timestamp) as the source query.
		Eventually(func() map[string]string {
			return idxModeContentByID(queryIndexModeE2E(svcs2, queryBegin, queryEnd, nil))
		}).WithTimeout(30*time.Second).WithPolling(time.Second).Should(Equal(srcContent),
			"post-migration full scan content must match the source query content field-for-field")

		// Indexed-tag filter: the index rule survived the migration, so the
		// migrated sidx is still searchable by the indexed tag.
		Eventually(func() []string {
			return idxModeEntities(queryIndexModeE2E(svcs2, queryBegin, queryEnd, eqCriteria(idxTag, "svc-2")))
		}).WithTimeout(30*time.Second).WithPolling(time.Second).Should(ConsistOf("id-2"),
			"post-migration indexed-tag filter service_name=svc-2 should still return exactly id-2")
	})
})

// registerIndexModeE2E creates a CATALOG_MEASURE group, an index-mode measure
// whose `default` family holds an entity tag plus one searchable string tag, and
// an INVERTED index rule (+ binding) on that searchable tag. Re-callable on both
// the source and the re-opened target service (AlreadyExists is tolerated). opts
// carries the group's segment grid + lifecycle stages, so the source can write on
// one grid while the target serves the migrated (re-gridded) grid.
func registerIndexModeE2E(svcs *services, opts *commonv1.ResourceOpts, group, measureName, ruleName, bindingName, idxTag, entityTag string) {
	ctx := context.TODO()
	createOK := func(err error) {
		if err != nil && !isAlreadyExistsErr(err) {
			Expect(err).NotTo(HaveOccurred())
		}
	}
	_, err := svcs.metadataService.GroupRegistry().CreateGroup(ctx, &commonv1.Group{
		Metadata:     &commonv1.Metadata{Name: group},
		Catalog:      commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: opts,
	})
	createOK(err)
	_, err = svcs.metadataService.MeasureRegistry().CreateMeasure(ctx, &databasev1.Measure{
		Metadata:  &commonv1.Metadata{Name: measureName, Group: group},
		IndexMode: true,
		Entity:    &databasev1.Entity{TagNames: []string{entityTag}},
		TagFamilies: []*databasev1.TagFamilySpec{
			{Name: defaultTagFamily, Tags: []*databasev1.TagSpec{
				{Name: entityTag, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: idxTag, Type: databasev1.TagType_TAG_TYPE_STRING},
			}},
		},
	})
	createOK(err)
	_, err = svcs.metadataService.IndexRuleRegistry().CreateIndexRule(ctx, &databasev1.IndexRule{
		Metadata: &commonv1.Metadata{Name: ruleName, Group: group},
		Tags:     []string{idxTag},
		Type:     databasev1.IndexRule_TYPE_INVERTED,
		Analyzer: index.AnalyzerKeyword,
	})
	createOK(err)
	_, err = svcs.metadataService.IndexRuleBindingRegistry().CreateIndexRuleBinding(ctx, &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: bindingName, Group: group},
		Rules:    []string{ruleName},
		Subject:  &databasev1.Subject{Catalog: commonv1.Catalog_CATALOG_MEASURE, Name: measureName},
		BeginAt:  timestamppb.New(time.Now().Add(-time.Hour)),
		ExpireAt: timestamppb.New(time.Now().Add(365 * 24 * time.Hour)),
	})
	createOK(err)
}

// writeIndexModeE2EPoints publishes `count` index-mode points starting at
// baseTime (1s apart), each with a unique entity id-<n> and searchable tag
// value svc-<n> so every point is its own series and the indexed filter is
// selective.
func writeIndexModeE2EPoints(svcs *services, group, name string, baseTime time.Time, count, offset int) {
	bp := svcs.pipeline.NewBatchPublisher(5 * time.Second)
	defer bp.Close()
	for i := 0; i < count; i++ {
		idx := offset + i
		idVal := "id-" + strconv.Itoa(idx)
		svcVal := "svc-" + strconv.Itoa(idx)
		req := &measurev1.WriteRequest{
			Metadata: &commonv1.Metadata{Name: name, Group: group},
			DataPoint: &measurev1.DataPointValue{
				Timestamp: timestamppb.New(baseTime.Add(time.Duration(i) * time.Second)),
				// Index-mode stores _version only when non-zero (like _timestamp);
				// a real data point always carries one. Use a unique, monotonic
				// version per series so the upsert/merge keeps a deterministic doc.
				Version: int64(idx + 1),
				TagFamilies: []*modelv1.TagFamilyForWrite{{
					Tags: []*modelv1.TagValue{
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: idVal}}},
						{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: svcVal}}},
					},
				}},
			},
		}
		bp.Publish(context.TODO(), data.TopicMeasureWrite,
			bus.NewMessage(bus.MessageID(idx), &measurev1.InternalWriteRequest{
				EntityValues: []*modelv1.TagValue{
					{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: idVal}}},
				},
				Request: req,
			}))
	}
}

// seedIndexModeE2ESchemaProperty writes the live Group + Measure + IndexRule into
// a synthetic schema-property bluge catalog so the migration restores the
// index-mode schema (and the rule that keeps the indexed tag searchable).
func seedIndexModeE2ESchemaProperty(svcs *services, root, group, measureName, ruleName string) {
	ctx := context.TODO()
	grpProto, err := svcs.metadataService.GroupRegistry().GetGroup(ctx, group)
	Expect(err).NotTo(HaveOccurred())
	measureProto, err := svcs.metadataService.MeasureRegistry().GetMeasure(ctx,
		&commonv1.Metadata{Name: measureName, Group: group})
	Expect(err).NotTo(HaveOccurred())
	ruleProto, err := svcs.metadataService.IndexRuleRegistry().GetIndexRule(ctx,
		&commonv1.Metadata{Name: ruleName, Group: group})
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
	ruleJSON, err := protojson.Marshal(ruleProto)
	Expect(err).NotTo(HaveOccurred())
	batch.Insert(migrationE2EBlugeDoc("index-rule/"+group+"/"+ruleName,
		schema.KindIndexRule.String(), group, string(ruleJSON)))
	Expect(w.Batch(batch)).To(Succeed())
}

// queryIndexModeE2E runs a measure query (optionally filtered) through the
// service pipeline and returns the data points.
func queryIndexModeE2E(svcs *services, begin, end time.Time, criteria *modelv1.Criteria) []*measurev1.DataPoint {
	req := &measurev1.QueryRequest{
		Groups:    []string{idxModeGroup},
		Name:      idxModeMeasureName,
		TimeRange: &modelv1.TimeRange{Begin: timestamppb.New(begin), End: timestamppb.New(end)},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: defaultTagFamily, Tags: []string{"id", "service_name"}},
			},
		},
		Criteria: criteria,
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
		Fail(fmt.Sprintf("index-mode measure query returned error: %s", d.Error()))
	default:
		Fail(fmt.Sprintf("expected *measurev1.QueryResponse, got %T", d))
	}
	return nil
}

func eqCriteria(tag, value string) *modelv1.Criteria {
	return &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: &modelv1.Condition{
				Name:  tag,
				Op:    modelv1.Condition_BINARY_OP_EQ,
				Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}},
			},
		},
	}
}

// measureIndexRuleNames returns the names of the index rules currently applied
// to the measure, or nil if it has not resolved yet. This reads the same index
// schema the write path uses, so "the rule appears here" is the deterministic
// signal that subsequent writes/queries are indexed.
func measureIndexRuleNames(svcs *services, group, measureName string) []string {
	m, err := svcs.measure.Measure(&commonv1.Metadata{Name: measureName, Group: group})
	if err != nil || m == nil {
		return nil
	}
	var names []string
	for _, r := range m.GetIndexRules() {
		names = append(names, r.GetMetadata().GetName())
	}
	return names
}

// idxModeContentByID keys each data point by its entity id and maps it to a
// stable digest of its FULL content — every tag value, every field value, and
// the timestamp, sorted so the digest is order-independent. Comparing the source
// and target digests asserts the migration preserved every field of every point.
func idxModeContentByID(dps []*measurev1.DataPoint) map[string]string {
	out := make(map[string]string, len(dps))
	for _, dp := range dps {
		var id string
		parts := make([]string, 0, 4)
		for _, tf := range dp.GetTagFamilies() {
			for _, tag := range tf.GetTags() {
				v := canonValue(tag.GetValue())
				parts = append(parts, "t:"+tf.GetName()+"."+tag.GetKey()+"="+v)
				if tag.GetKey() == "id" {
					if sv := tag.GetValue().GetStr(); sv != nil {
						id = sv.GetValue()
					}
				}
			}
		}
		for _, f := range dp.GetFields() {
			parts = append(parts, "f:"+f.GetName()+"="+canonValue(f.GetValue()))
		}
		parts = append(parts, "@"+strconv.FormatInt(dp.GetTimestamp().AsTime().UnixNano(), 10))
		sort.Strings(parts)
		out[id] = strings.Join(parts, "|")
	}
	return out
}

// canonValue renders any proto value (tag or field) as a stable JSON string.
func canonValue(m proto.Message) string {
	b, err := protojson.Marshal(m)
	Expect(err).NotTo(HaveOccurred())
	return string(b)
}

// idxModeIDToTag maps each data point's entity id to its indexed tag value.
func idxModeIDToTag(dps []*measurev1.DataPoint, idxTag string) map[string]string {
	out := map[string]string{}
	for _, dp := range dps {
		var id, tagVal string
		for _, tf := range dp.TagFamilies {
			for _, tag := range tf.Tags {
				switch tag.Key {
				case "id":
					if sv := tag.Value.GetStr(); sv != nil {
						id = sv.Value
					}
				case idxTag:
					if sv := tag.Value.GetStr(); sv != nil {
						tagVal = sv.Value
					}
				}
			}
		}
		if id != "" {
			out[id] = tagVal
		}
	}
	return out
}

// idxModeEntities returns the sorted entity ids of the data points.
func idxModeEntities(dps []*measurev1.DataPoint) []string {
	out := make([]string, 0, len(dps))
	for _, dp := range dps {
		for _, tf := range dp.TagFamilies {
			for _, tag := range tf.Tags {
				if tag.Key == "id" {
					if sv := tag.Value.GetStr(); sv != nil {
						out = append(out, sv.Value)
					}
				}
			}
		}
	}
	return out
}

// expectedIDToTag builds the id-<n> -> svc-<n> map the writes produce.
func expectedIDToTag(count int) map[string]string {
	out := make(map[string]string, count)
	for i := 0; i < count; i++ {
		out["id-"+strconv.Itoa(i)] = "svc-" + strconv.Itoa(i)
	}
	return out
}
