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

package lifecycle_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/backup/lifecycle"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
)

// orphanRec mirrors one archived JSONL line (the subset this suite asserts on).
// The archive writer lives in banyand/backup/lifecycle (different package); this
// is a read-side view of its self-describing record.
type orphanRec struct {
	Fields    map[string]orphanVal `json:"fields"`
	Tags      map[string]orphanVal `json:"tags"`
	Group     string               `json:"group"`
	Catalog   string               `json:"catalog"`
	Measure   string               `json:"measure"`
	Timestamp string               `json:"timestamp"`
	ElementID string               `json:"element_id"`
	Entity    []string             `json:"entity"`
	TimeNanos int64                `json:"timestamp_unix_nano"`
	Version   int64                `json:"version"`
}

type orphanVal struct {
	Value interface{} `json:"value"`
	Type  string      `json:"type"`
}

type orphanManifest struct {
	Measures []struct {
		Measure string `json:"measure"`
		Rows    int    `json:"rows"`
	} `json:"measures"`
	TotalRows int `json:"total_rows"`
}

// dayInterval/coprime grid clone of sw_cross_segment: a 2-day source segment
// straddles the 3-day warm boundary, so crossSegmentTimestamps() lands rows on
// the row-replay path — the only path where orphan archiving happens.
func orphanStages() *commonv1.ResourceOpts {
	return &commonv1.ResourceOpts{
		ShardNum:        1,
		SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 2},
		Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 5},
		Stages: []*commonv1.LifecycleStage{{
			Name:            "warm",
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 3},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 10},
			NodeSelector:    "type=warm",
		}},
	}
}

// drainWriteResult closes a client-streaming write and returns an error on any
// non-success ack or non-EOF stream error (the error-returning sibling of
// drainWriteAcks, so the whole write can be retried under Eventually until a
// freshly-created schema has propagated to the liaison).
func drainWriteResult[R interface{ GetStatus() string }](recv func() (R, error), closeSend func() error) error {
	if err := closeSend(); err != nil {
		return err
	}
	for {
		resp, recvErr := recv()
		if errors.Is(recvErr, io.EOF) {
			return nil
		}
		if recvErr != nil {
			return recvErr
		}
		if s := resp.GetStatus(); s != "" && s != "STATUS_SUCCEED" {
			return fmt.Errorf("write ack status: %s", s)
		}
	}
}

// runLifecycleMigrationWithArchive runs one real hot->warm lifecycle migration
// (the actual lifecycle command) with the orphan archive policy enabled. archiveSubdir
// is the relative subdir (under each catalog's root path) the archive lands in.
func runLifecycleMigrationWithArchive(progressFile, reportDir, archiveSubdir string) {
	lifecycleCmd := lifecycle.NewCommand()
	args := []string{
		"--grpc-addr", SharedContext.DataAddr,
		"--stream-root-path", SharedContext.SrcDir,
		"--measure-root-path", SharedContext.SrcDir,
		"--trace-root-path", SharedContext.SrcDir,
		"--progress-file", progressFile,
		"--report-dir", reportDir,
		"--migration-orphan-policy", "archive",
		"--migration-orphan-archive-subdir", archiveSubdir,
	}
	args = append(args, SharedContext.MetadataFlags...)
	lifecycleCmd.SetArgs(args)
	gomega.Expect(lifecycleCmd.Execute()).To(gomega.Succeed())
}

// readOrphanArchive gunzips every part-*.jsonl.gz under groupArchiveDir (the
// <catalog-root>/<subdir>/<group> directory) and returns the decoded records plus
// the summed manifest row count.
func readOrphanArchive(groupArchiveDir string) (recs []orphanRec, manifestRows int) {
	root := groupArchiveDir
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		switch {
		case strings.HasSuffix(d.Name(), ".jsonl.gz"):
			f, openErr := os.Open(path)
			gomega.Expect(openErr).NotTo(gomega.HaveOccurred())
			defer f.Close()
			gr, gzErr := gzip.NewReader(f)
			gomega.Expect(gzErr).NotTo(gomega.HaveOccurred(), "archive file %s must be a valid gzip", path)
			defer gr.Close()
			data, readErr := io.ReadAll(gr)
			gomega.Expect(readErr).NotTo(gomega.HaveOccurred())
			dec := json.NewDecoder(bytes.NewReader(data))
			for dec.More() {
				var rec orphanRec
				gomega.Expect(dec.Decode(&rec)).To(gomega.Succeed(), "archive line in %s must be valid JSON", path)
				recs = append(recs, rec)
			}
		case d.Name() == "manifest.json":
			raw, readErr := os.ReadFile(path)
			gomega.Expect(readErr).NotTo(gomega.HaveOccurred())
			var m orphanManifest
			gomega.Expect(json.Unmarshal(raw, &m)).To(gomega.Succeed())
			manifestRows += m.TotalRows
		}
		return nil
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "walk archive dir %s", root)
	return recs, manifestRows
}

var _ = ginkgo.Describe("Lifecycle orphan-schema archive", ginkgo.Ordered, func() {
	ginkgo.It("measure: deleted-schema rows are archived + source removed; sibling measure migrates", func() {
		const (
			group       = "orphan_e2e_measure"
			toDelete    = "orphan_deleted_measure"
			keep        = "orphan_kept_measure"
			leftValue   = int64(200)
			rightValue  = int64(300)
			keepLeftVal = int64(11)
			keepRightV  = int64(22)
		)
		_, leftTS, rightTS := crossSegmentTimestamps()

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		conn, dialErr := grpchelper.Conn(SharedContext.LiaisonAddr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(dialErr).NotTo(gomega.HaveOccurred())
		defer func() { _ = conn.Close() }()
		groupClient := databasev1.NewGroupRegistryServiceClient(conn)
		measureReg := databasev1.NewMeasureRegistryServiceClient(conn)
		writeClient := measurev1.NewMeasureServiceClient(conn)

		ginkgo.By("creating group + two measures (one to delete, one to keep)")
		_, err := groupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: group}, Catalog: commonv1.Catalog_CATALOG_MEASURE, ResourceOpts: orphanStages(),
		}})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		createMeasure := func(name string) {
			_, cErr := measureReg.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{Measure: &databasev1.Measure{
				Metadata: &commonv1.Metadata{Name: name, Group: group},
				Entity:   &databasev1.Entity{TagNames: []string{"id"}},
				TagFamilies: []*databasev1.TagFamilySpec{{Name: "default", Tags: []*databasev1.TagSpec{
					{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING},
				}}},
				Fields: []*databasev1.FieldSpec{{
					Name: "value", FieldType: databasev1.FieldType_FIELD_TYPE_INT,
					EncodingMethod: databasev1.EncodingMethod_ENCODING_METHOD_GORILLA, CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
				}},
			}})
			gomega.Expect(cErr).NotTo(gomega.HaveOccurred())
		}
		createMeasure(toDelete)
		createMeasure(keep)

		ginkgo.By("writing rows on the straddling (row-replay) segment to both measures")
		// The write goes through the liaison, which routes by schema. A freshly
		// created measure reaches the liaison via schema sync slightly after the
		// registry returns, so retry the whole write until the liaison has it; a
		// not-yet-synced write is rejected at routing (nothing is stored), so the
		// retry lands exactly the two rows once.
		writeMeasure := func(name string, lv, rv int64) func() error {
			return func() error {
				ws, wErr := writeClient.Write(ctx)
				if wErr != nil {
					return wErr
				}
				send := func(ts time.Time, val int64, first bool) error {
					req := &measurev1.WriteRequest{DataPoint: &measurev1.DataPointValue{
						Timestamp: timestamppb.New(ts), Version: ts.UnixNano(),
						TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "ent-1"}}},
						}}},
						Fields: []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: val}}}},
					}, MessageId: uint64(time.Now().UnixNano())}
					if first {
						req.Metadata = &commonv1.Metadata{Group: group, Name: name}
					}
					return ws.Send(req)
				}
				if sendErr := send(leftTS, lv, true); sendErr != nil {
					return sendErr
				}
				if sendErr := send(rightTS, rv, false); sendErr != nil {
					return sendErr
				}
				return drainWriteResult(ws.Recv, ws.CloseSend)
			}
		}
		gomega.Eventually(writeMeasure(toDelete, leftValue, rightValue), flags.EventuallyTimeout).Should(gomega.Succeed())
		gomega.Eventually(writeMeasure(keep, keepLeftVal, keepRightV), flags.EventuallyTimeout).Should(gomega.Succeed())
		time.Sleep(flags.ConsistentlyTimeout)

		ginkgo.By("deleting the schema of " + toDelete + " (its on-disk data becomes orphan)")
		_, delErr := measureReg.Delete(ctx, &databasev1.MeasureRegistryServiceDeleteRequest{Metadata: &commonv1.Metadata{Name: toDelete, Group: group}})
		gomega.Expect(delErr).NotTo(gomega.HaveOccurred())

		// Gate on the registry reflecting the intended post-delete state before
		// migrating: the kept measure must be resolvable and the deleted one gone, so
		// the migration's per-row schema lookup classifies each correctly (a not-yet-
		// propagated kept schema would otherwise be misread as orphan).
		ginkgo.By("waiting until the registry settles (kept measure resolvable, deleted measure gone)")
		gomega.Eventually(func() error {
			if _, e := measureReg.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: &commonv1.Metadata{Name: keep, Group: group}}); e != nil {
				return fmt.Errorf("kept measure %s not resolvable yet: %w", keep, e)
			}
			if _, e := measureReg.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: &commonv1.Metadata{Name: toDelete, Group: group}}); e == nil {
				return fmt.Errorf("deleted measure %s still resolvable", toDelete)
			}
			return nil
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())

		ginkgo.By("running the real lifecycle migration with orphan archive enabled")
		tmpDir, err := os.MkdirTemp("", "orphan-e2e-measure")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer os.RemoveAll(tmpDir)
		// The archive lands in <measure-root-path>/<subdir>/<group>; the test points
		// the measure root at SrcDir, so read it back from there.
		const archiveSubdir = "orphan-archive"
		groupArchiveDir := filepath.Join(SharedContext.SrcDir, archiveSubdir, group)
		defer os.RemoveAll(groupArchiveDir)
		runLifecycleMigrationWithArchive(filepath.Join(tmpDir, "progress.json"), filepath.Join(tmpDir, "report"), archiveSubdir)

		ginkgo.By("verifying every archived record is correct and complete")
		recs, manifestRows := readOrphanArchive(groupArchiveDir)
		gomega.Expect(recs).To(gomega.HaveLen(2), "exactly the deleted measure's two rows must be archived")
		gomega.Expect(manifestRows).To(gomega.Equal(2), "manifest total_rows must equal the archived rows")
		gotValues := map[int64]bool{}
		for _, r := range recs {
			gomega.Expect(r.Measure).To(gomega.Equal(toDelete), "only the deleted measure may be archived")
			gomega.Expect(r.Group).To(gomega.Equal(group))
			gomega.Expect(r.Catalog).To(gomega.Equal("measure"))
			gomega.Expect(r.Entity).To(gomega.ContainElement("ent-1"), "entity value must be preserved")
			fv, ok := r.Fields["value"]
			gomega.Expect(ok).To(gomega.BeTrue(), "archived measure record must carry its field")
			gomega.Expect(fv.Type).To(gomega.Equal("INT64"))
			gotValues[int64(fv.Value.(float64))] = true
			gomega.Expect(r.Version).To(gomega.BeNumerically(">", 0))
		}
		gomega.Expect(gotValues).To(gomega.Equal(map[int64]bool{leftValue: true, rightValue: true}),
			"archived field values must equal exactly what was written")

		ginkgo.By("verifying the orphan source segment was deleted")
		srcGroupRoot := filepath.Join(SharedContext.SrcDir, "measure", "data", group)
		gomega.Eventually(func() bool { return noSegDirs(srcGroupRoot) }, flags.EventuallyTimeout).
			Should(gomega.BeTrue(), "source segment(s) under %s must be deleted after migration", srcGroupRoot)

		ginkgo.By("verifying the kept measure migrated and is queryable on the warm stage")
		queryClient := measurev1.NewMeasureServiceClient(conn)
		test.EventuallyConsistently(func() error {
			resp, qErr := queryClient.Query(ctx, &measurev1.QueryRequest{
				Groups: []string{group}, Name: keep,
				TimeRange:       &modelv1.TimeRange{Begin: timestamppb.New(leftTS.Add(-time.Hour)), End: timestamppb.New(rightTS.Add(time.Hour))},
				TagProjection:   &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: "default", Tags: []string{"id"}}}},
				FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
				Stages:          []string{"warm"}, Limit: 100,
			})
			if qErr != nil {
				return qErr
			}
			if len(resp.DataPoints) != 2 {
				return fmt.Errorf("want 2 kept rows on warm, got %d", len(resp.DataPoints))
			}
			return nil
		}, flags.EventuallyTimeout).Should(gomega.Succeed())
	})

	ginkgo.It("stream: deleted-schema elements are archived + source removed; sibling stream migrates", func() {
		const (
			group    = "orphan_e2e_stream"
			toDelete = "orphan_deleted_stream"
			keep     = "orphan_kept_stream"
		)
		_, leftTS, rightTS := crossSegmentTimestamps()

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		conn, dialErr := grpchelper.Conn(SharedContext.LiaisonAddr, 10*time.Second,
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		gomega.Expect(dialErr).NotTo(gomega.HaveOccurred())
		defer func() { _ = conn.Close() }()
		groupClient := databasev1.NewGroupRegistryServiceClient(conn)
		streamReg := databasev1.NewStreamRegistryServiceClient(conn)
		writeClient := streamv1.NewStreamServiceClient(conn)

		ginkgo.By("creating group + two streams (one to delete, one to keep)")
		_, err := groupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{Group: &commonv1.Group{
			Metadata: &commonv1.Metadata{Name: group}, Catalog: commonv1.Catalog_CATALOG_STREAM, ResourceOpts: orphanStages(),
		}})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		createStream := func(name string) {
			_, cErr := streamReg.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{Stream: &databasev1.Stream{
				Metadata: &commonv1.Metadata{Name: name, Group: group},
				Entity:   &databasev1.Entity{TagNames: []string{"entity_id"}},
				// "searchable" is the queryable/indexed tag family for streams. Mirror
				// the proven cross_segment_log shape: a non-entity tag (business_id)
				// alongside the entity tag (entity_id), so time-range queries resolve.
				TagFamilies: []*databasev1.TagFamilySpec{{Name: "searchable", Tags: []*databasev1.TagSpec{
					{Name: "business_id", Type: databasev1.TagType_TAG_TYPE_STRING},
					{Name: "entity_id", Type: databasev1.TagType_TAG_TYPE_STRING},
				}}},
			}})
			gomega.Expect(cErr).NotTo(gomega.HaveOccurred())
		}
		createStream(toDelete)
		createStream(keep)

		ginkgo.By("writing elements on the straddling (row-replay) segment to both streams")
		// Retry the whole write until the liaison has synced the freshly created
		// stream schema (see the measure case for the rationale).
		writeStream := func(name string) func() error {
			return func() error {
				ws, wErr := writeClient.Write(ctx)
				if wErr != nil {
					return wErr
				}
				send := func(ts time.Time, eid, biz, entity string, first bool) error {
					req := &streamv1.WriteRequest{Element: &streamv1.ElementValue{
						ElementId: eid, Timestamp: timestamppb.New(ts),
						TagFamilies: []*modelv1.TagFamilyForWrite{{Tags: []*modelv1.TagValue{
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: biz}}},
							{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: entity}}},
						}}},
					}, MessageId: uint64(time.Now().UnixNano())}
					if first {
						req.Metadata = &commonv1.Metadata{Group: group, Name: name}
					}
					return ws.Send(req)
				}
				if sendErr := send(leftTS, name+"-L", "biz-1", "ent-L", true); sendErr != nil {
					return sendErr
				}
				if sendErr := send(rightTS, name+"-R", "biz-1", "ent-R", false); sendErr != nil {
					return sendErr
				}
				return drainWriteResult(ws.Recv, ws.CloseSend)
			}
		}
		gomega.Eventually(writeStream(toDelete), flags.EventuallyTimeout).Should(gomega.Succeed())
		gomega.Eventually(writeStream(keep), flags.EventuallyTimeout).Should(gomega.Succeed())
		time.Sleep(flags.ConsistentlyTimeout)

		ginkgo.By("deleting the schema of " + toDelete)
		_, delErr := streamReg.Delete(ctx, &databasev1.StreamRegistryServiceDeleteRequest{Metadata: &commonv1.Metadata{Name: toDelete, Group: group}})
		gomega.Expect(delErr).NotTo(gomega.HaveOccurred())

		// Gate on the registry reflecting the intended post-delete state before
		// migrating: the kept stream must be resolvable and the deleted one gone, so
		// the migration's per-row schema lookup classifies each correctly (a not-yet-
		// propagated kept schema would otherwise be misread as orphan).
		ginkgo.By("waiting until the registry settles (kept stream resolvable, deleted stream gone)")
		gomega.Eventually(func() error {
			if _, e := streamReg.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{Metadata: &commonv1.Metadata{Name: keep, Group: group}}); e != nil {
				return fmt.Errorf("kept stream %s not resolvable yet: %w", keep, e)
			}
			if _, e := streamReg.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{Metadata: &commonv1.Metadata{Name: toDelete, Group: group}}); e == nil {
				return fmt.Errorf("deleted stream %s still resolvable", toDelete)
			}
			return nil
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())

		ginkgo.By("running the real lifecycle migration with orphan archive enabled")
		tmpDir, err := os.MkdirTemp("", "orphan-e2e-stream")
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		defer os.RemoveAll(tmpDir)
		// The archive lands in <stream-root-path>/<subdir>/<group>; the test points
		// the stream root at SrcDir, so read it back from there.
		const archiveSubdir = "orphan-archive"
		groupArchiveDir := filepath.Join(SharedContext.SrcDir, archiveSubdir, group)
		defer os.RemoveAll(groupArchiveDir)
		runLifecycleMigrationWithArchive(filepath.Join(tmpDir, "progress.json"), filepath.Join(tmpDir, "report"), archiveSubdir)

		ginkgo.By("verifying every archived stream record is correct (element_id present, no fields)")
		recs, manifestRows := readOrphanArchive(groupArchiveDir)
		gomega.Expect(recs).To(gomega.HaveLen(2))
		gomega.Expect(manifestRows).To(gomega.Equal(2))
		for _, r := range recs {
			gomega.Expect(r.Measure).To(gomega.Equal(toDelete), "only the deleted stream may be archived")
			gomega.Expect(r.Group).To(gomega.Equal(group))
			gomega.Expect(r.Catalog).To(gomega.Equal("stream"))
			gomega.Expect(r.ElementID).NotTo(gomega.BeEmpty(), "stream archive record must carry element_id")
			gomega.Expect(r.Fields).To(gomega.BeEmpty(), "stream archive record must not carry fields")
		}

		ginkgo.By("verifying the orphan source segment was deleted")
		srcGroupRoot := filepath.Join(SharedContext.SrcDir, "stream", "data", group)
		gomega.Eventually(func() bool { return noSegDirs(srcGroupRoot) }, flags.EventuallyTimeout).
			Should(gomega.BeTrue(), "source segment(s) under %s must be deleted after migration", srcGroupRoot)

		ginkgo.By("verifying the kept stream migrated and is queryable on the warm stage")
		queryClient := streamv1.NewStreamServiceClient(conn)
		test.EventuallyConsistently(func() error {
			resp, qErr := queryClient.Query(ctx, &streamv1.QueryRequest{
				Groups: []string{group}, Name: keep,
				TimeRange:  &modelv1.TimeRange{Begin: timestamppb.New(leftTS.Add(-time.Hour)), End: timestamppb.New(rightTS.Add(time.Hour))},
				Projection: &modelv1.TagProjection{TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: "searchable", Tags: []string{"business_id", "entity_id"}}}},
				// OrderBy with no index-rule name + SORT_UNSPECIFIED means "by event
				// time"; stream queries need it to select the time index.
				OrderBy: &modelv1.QueryOrder{Sort: modelv1.Sort_SORT_UNSPECIFIED},
				Stages:  []string{"warm"},
			})
			if qErr != nil {
				return qErr
			}
			if len(resp.Elements) != 2 {
				return fmt.Errorf("want 2 kept elements on warm, got %d", len(resp.Elements))
			}
			return nil
		}, flags.EventuallyTimeout).Should(gomega.Succeed())
	})
})

// noSegDirs reports whether root has no seg-* directories left (only a lock file
// or nothing), i.e. the migration deleted the source segment(s).
func noSegDirs(root string) bool {
	entries, err := os.ReadDir(root)
	if err != nil {
		return true // group dir gone entirely is also "deleted"
	}
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "seg-") {
			return false
		}
	}
	return true
}
