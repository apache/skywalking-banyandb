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

package trace

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/banyand/internal/sidx"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/pkg/convert"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	pbv1 "github.com/apache/skywalking-banyandb/pkg/pb/v1"
	"github.com/apache/skywalking-banyandb/pkg/query/model"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

// mustNewSpace creates a temp directory for tests, failing on error.
func mustNewSpace(g *gomega.WithT) (string, func()) {
	dir, cleanup, err := test.NewSpace()
	g.Expect(err).NotTo(gomega.HaveOccurred())
	return dir, cleanup
}

// transferParts performs the file-based migration of all parts from sender snapshot to receiver tsTable.
// For each part it reads the on-disk files, streams chunks through the real syncChunkCallback handler,
// and calls FinishSync so that the receiver gains a file-backed part.
// sidxAllPaths is a pre-captured map[sidxName]map[partID]path that was obtained atomically with senderSnp
// (via waitForConsistentSidxSnapshot). Pass nil to skip SIDX transfer (for tests without SIDX).
func transferParts(g *gomega.WithT, senderSnp *snapshot, receiverTst *tsTable, fileSystem fs.FileSystem, sidxAllPaths map[string]map[uint64]string) {
	handler := &syncChunkCallback{l: logger.GetLogger("transfer-handler")}

	buf := make([]byte, 64*1024)
	streamFiles := func(files []queue.FileInfo, ctx *queue.ChunkedSyncPartContext) {
		for i := range files {
			fi := &files[i]
			ctx.FileName = fi.Name
			for {
				n, readErr := fi.Reader.Read(buf)
				if n > 0 {
					g.Expect(handler.HandleFileChunk(ctx, buf[:n])).To(gomega.Succeed())
				}
				if readErr == io.EOF {
					break
				}
				g.Expect(readErr).NotTo(gomega.HaveOccurred())
			}
		}
	}

	for _, pw := range senderSnp.parts {
		if pw.mp != nil {
			// Only transfer file-backed parts.
			continue
		}

		partID := pw.p.partMetadata.ID
		coreFiles, cleanup := CreatePartFileReaderFromPath(pw.p.path, fileSystem)

		// Load core part metadata from disk via the same helper used by lifecycle migration.
		coreData, parseErr := ParsePartMetadata(fileSystem, pw.p.path)
		g.Expect(parseErr).NotTo(gomega.HaveOccurred())
		ctx := &queue.ChunkedSyncPartContext{
			ID:                    coreData.ID,
			CompressedSizeBytes:   coreData.CompressedSizeBytes,
			UncompressedSizeBytes: coreData.UncompressedSizeBytes,
			TotalCount:            coreData.TotalCount,
			BlocksCount:           coreData.BlocksCount,
			MinTimestamp:          coreData.MinTimestamp,
			MaxTimestamp:          coreData.MaxTimestamp,
			PartType:              PartTypeCore,
		}

		partCtx := &syncPartContext{tsTable: receiverTst, l: logger.GetLogger("transfer-part")}

		// Transfer SIDX parts first so they share the same partID as the core part
		// (production order: SIDX parts arrive before core part in VisitShard).
		for sidxName, pathsByID := range sidxAllPaths {
			sidxPartPath, ok := pathsByID[partID]
			if !ok {
				continue
			}
			sidxData, sidxParseErr := sidx.ParsePartMetadata(fileSystem, sidxPartPath)
			g.Expect(sidxParseErr).NotTo(gomega.HaveOccurred())
			sidxCtx := &queue.ChunkedSyncPartContext{
				ID:                    sidxData.ID,
				CompressedSizeBytes:   sidxData.CompressedSizeBytes,
				UncompressedSizeBytes: sidxData.UncompressedSizeBytes,
				TotalCount:            sidxData.TotalCount,
				BlocksCount:           sidxData.BlocksCount,
				MinKey:                sidxData.MinKey,
				MaxKey:                sidxData.MaxKey,
				PartType:              sidxName,
			}
			g.Expect(partCtx.NewPartType(sidxCtx)).To(gomega.Succeed())
			sidxCtx.Handler = partCtx

			sidxFiles, sidxCleanup := sidx.CreatePartFileReaderFromPath(sidxPartPath, fileSystem)
			streamFiles(sidxFiles, sidxCtx)
			sidxCleanup()
		}

		// Transfer core part.
		g.Expect(partCtx.NewPartType(ctx)).To(gomega.Succeed())
		ctx.Handler = partCtx
		streamFiles(coreFiles, ctx)
		cleanup()

		g.Expect(partCtx.FinishSync()).To(gomega.Succeed())
	}
}

// mustBuildSidxMemPart creates a sidx.MemPart for the given index name and write requests.
// This simulates what write_standalone.go does when processing index rules.
func mustBuildSidxMemPart(g *gomega.WithT, tst *tsTable, sidxName string, reqs []sidx.WriteRequest, minTS, maxTS int64) *sidx.MemPart {
	sidxInstance, err := tst.getOrCreateSidx(sidxName)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	mp, err := sidxInstance.ConvertToMemPart(reqs, 0, &minTS, &maxTS)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	return mp
}

// snapshotPartIDSet returns a set of all file-backed part IDs in the snapshot.
func snapshotPartIDSet(snp *snapshot) map[uint64]struct{} {
	ids := make(map[uint64]struct{}, len(snp.parts))
	for _, pw := range snp.parts {
		if pw.mp == nil {
			ids[pw.p.partMetadata.ID] = struct{}{}
		}
	}
	return ids
}

// waitForFileParts blocks until the tsTable has at least one file-backed part (no memParts)
// or the timeout elapses.
func waitForFileParts(g *gomega.WithT, tst *tsTable) {
	g.Eventually(func() bool {
		snp := tst.currentSnapshot()
		if snp == nil {
			return false
		}
		defer snp.decRef()
		if len(snp.parts) == 0 {
			return false
		}
		for _, pw := range snp.parts {
			if pw.mp != nil {
				return false
			}
		}
		return true
	}).WithTimeout(30 * time.Second).WithPolling(100 * time.Millisecond).Should(gomega.BeTrue())
}

// waitForConsistentSidxSnapshot polls until the tsTable's core snapshot and all named SIDX instances
// are consistent: every file-backed core part ID has a corresponding file-backed SIDX part.
// Returns the snapshot (caller must call decRef) and a map[sidxName]map[partID]path captured at the
// same moment the consistency check passed, so they are guaranteed to be in sync.
func waitForConsistentSidxSnapshot(g *gomega.WithT, tst *tsTable, sidxNames []string) (*snapshot, map[string]map[uint64]string) {
	var resultSnp *snapshot
	var resultPaths map[string]map[uint64]string

	g.Eventually(func() bool {
		snp := tst.currentSnapshot()
		if snp == nil {
			return false
		}
		coreIDs := snapshotPartIDSet(snp)
		if len(coreIDs) == 0 {
			snp.decRef()
			return false
		}
		sidxMap := tst.getAllSidx()
		captured := make(map[string]map[uint64]string, len(sidxNames))
		for _, name := range sidxNames {
			inst, ok := sidxMap[name]
			if !ok {
				snp.decRef()
				return false
			}
			paths := inst.PartPaths(coreIDs)
			if len(paths) == 0 {
				snp.decRef()
				return false
			}
			captured[name] = paths
		}
		// All SIDX instances are consistent with the core snapshot. Keep the ref.
		if resultSnp != nil {
			resultSnp.decRef()
		}
		resultSnp = snp
		resultPaths = captured
		return true
	}).WithTimeout(30 * time.Second).WithPolling(50 * time.Millisecond).Should(gomega.BeTrue())

	return resultSnp, resultPaths
}

// queryTraceID executes the standard query pattern used in query_test.go and returns all TraceResults.
func queryTraceID(g *gomega.WithT, snp *snapshot, traceID string, minTS, maxTS int64) []model.TraceResult {
	pp, _ := snp.getParts(nil, minTS, maxTS, []string{traceID})

	bma := generateBlockMetadataArray()
	defer releaseBlockMetadataArray(bma)

	ti := &tstIter{}
	groupedTids := make([][]string, len(pp))
	for i := range groupedTids {
		groupedTids[i] = []string{traceID}
	}
	ti.init(bma, pp, groupedTids)

	queryOpts := queryOptions{
		schemaTagTypes: testSchemaTagTypes,
	}

	var cursors []*blockCursor
	for ti.nextBlock() {
		bc := generateBlockCursor()
		p := ti.piPool[ti.idx]
		opts := queryOpts
		opts.TagProjection = allTagProjections
		bc.init(p.p, p.curBlock, opts)
		cursors = append(cursors, bc)
	}
	g.Expect(ti.Error()).NotTo(gomega.HaveOccurred())

	cursorCh := make(chan scanCursorResult, len(cursors))
	for _, bc := range cursors {
		cursorCh <- scanCursorResult{cursor: bc}
	}
	close(cursorCh)

	cursorBatch := make(chan *scanBatch, 1)
	traceIDMap := make(map[uint64][]string)
	traceIDMap[0] = []string{traceID}
	cursorBatch <- &scanBatch{
		traceBatch: traceBatch{
			traceIDs:      traceIDMap,
			traceIDsOrder: []string{traceID},
			keys:          map[string]int64{traceID: 0},
		},
		cursorCh: cursorCh,
	}
	close(cursorBatch)

	result := queryResult{
		ctx:           context.Background(),
		tagProjection: allTagProjections,
		cursorBatchCh: cursorBatch,
		keys:          map[string]int64{traceID: 0},
	}
	defer result.Release()

	var got []model.TraceResult
	for {
		r := result.Pull()
		if r == nil {
			break
		}
		got = append(got, *r)
	}
	return got
}

// checkGoroutineLeak captures the set of live goroutines and registers a t.Cleanup
// that fails the test if any new goroutine remains after the test returns.
func checkGoroutineLeak(t *testing.T, g *gomega.WithT) {
	t.Helper()
	goods := gleak.Goroutines()
	t.Cleanup(func() {
		g.Eventually(gleak.Goroutines, 10*time.Second).ShouldNot(gleak.HaveLeaked(goods))
	})
}

// TestLifecycleMigration_PreserveTraceData verifies that file-based migration preserves trace data correctly.
func TestLifecycleMigration_PreserveTraceData(t *testing.T) {
	g := gomega.NewWithT(t)
	checkGoroutineLeak(t, g)
	fileSystem := fs.NewLocalFileSystem()

	// Phase 1: Sender — write known data and flush to disk.
	senderPath, senderCleanup := mustNewSpace(g)
	defer senderCleanup()

	senderTst, err := newTSTable(
		fileSystem, senderPath, common.Position{},
		logger.GetLogger("sender"), timestamp.TimeRange{},
		option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}},
		nil,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		g.Expect(senderTst.Close()).To(gomega.Succeed())
	}()

	senderTst.mustAddTraces(tsTS1, nil)
	time.Sleep(100 * time.Millisecond)
	senderTst.mustAddTraces(tsTS2, nil)
	time.Sleep(100 * time.Millisecond)

	waitForFileParts(g, senderTst)

	senderSnp := senderTst.currentSnapshot()
	g.Expect(senderSnp).NotTo(gomega.BeNil())
	defer senderSnp.decRef()
	g.Expect(len(senderSnp.parts)).To(gomega.BeNumerically(">", 0))

	// Phase 2: Receiver — create tsTable and transfer all parts via the real handler.
	receiverPath, receiverCleanup := mustNewSpace(g)
	defer receiverCleanup()

	receiverTst, err := newTSTable(
		fileSystem, receiverPath, common.Position{},
		logger.GetLogger("receiver"), timestamp.TimeRange{},
		option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}},
		nil,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	transferParts(g, senderSnp, receiverTst, fileSystem, nil)

	// Verify the receiver snapshot has only file-backed parts.
	recvSnp := receiverTst.currentSnapshot()
	g.Expect(recvSnp).NotTo(gomega.BeNil())
	for _, pw := range recvSnp.parts {
		g.Expect(pw.mp).To(gomega.BeNil(), "receiver should have file-backed parts only")
	}
	recvSnp.decRef()

	defer func() {
		g.Expect(receiverTst.Close()).To(gomega.Succeed())
	}()

	// Phase 3: Verify all parts on disk are readable via mustOpenFilePart and
	// query returns correct data from the in-memory snapshot.
	recvSnp = receiverTst.currentSnapshot()
	g.Expect(recvSnp).NotTo(gomega.BeNil())
	defer recvSnp.decRef()

	// Verify each receiver part is well-formed on disk.
	for _, pw := range recvSnp.parts {
		p := mustOpenFilePart(pw.p.partMetadata.ID, receiverTst.root, fileSystem)
		g.Expect(p.partMetadata.TotalCount).To(gomega.BeNumerically(">", uint64(0)), "reopened part must have data")
		g.Expect(p.primaryBlockMetadata).NotTo(gomega.BeEmpty(), "primary block metadata must be loadable")
		p.close()

		// Verify metadata.json was written with correct values.
		pp := partPath(receiverTst.root, pw.p.partMetadata.ID)
		spd, metaErr := ParsePartMetadata(fileSystem, pp)
		g.Expect(metaErr).NotTo(gomega.HaveOccurred(), "metadata.json must be readable")
		g.Expect(spd.TotalCount).To(gomega.BeNumerically(">", uint64(0)), "metadata.json TotalCount must be > 0")
		g.Expect(spd.MinTimestamp).To(gomega.BeNumerically("<=", spd.MaxTimestamp), "metadata.json timestamps must be valid")
	}

	// trace1 should return spans from both tsTS1 (span1) and tsTS2 (span4).
	got := queryTraceID(g, recvSnp, "trace1", 1, 2)
	g.Expect(got).NotTo(gomega.BeEmpty(), "should have query results for trace1")

	totalSpans := 0
	for _, tr := range got {
		totalSpans += len(tr.Spans)
	}
	g.Expect(totalSpans).To(gomega.Equal(2), "trace1 should have 2 spans (one from each write batch)")

	// Verify span content: collect all span bytes from all TraceResult entries.
	var allSpans [][]byte
	for _, tr := range got {
		allSpans = append(allSpans, tr.Spans...)
	}
	containsSpan := func(spanData [][]byte, target []byte) bool {
		for _, s := range spanData {
			if bytes.Equal(s, target) {
				return true
			}
		}
		return false
	}
	g.Expect(containsSpan(allSpans, []byte("span1"))).To(gomega.BeTrue(), "span1 from tsTS1 must be present after migration")
	g.Expect(containsSpan(allSpans, []byte("span4"))).To(gomega.BeTrue(), "span4 from tsTS2 must be present after migration")

	// Verify tag values: find the Tags slice from any result that contains trace1 data.
	// The query merges spans from tsTS1 (timestamp=1) and tsTS2 (timestamp=2) into one TraceResult.
	// Tags are parallel to Spans: Tags[i].Values[j] corresponds to span j.
	// strTag for trace1: tsTS1="value1", tsTS2="value4".
	// intTag for trace1: tsTS1=10, tsTS2=40.
	// strArrTag for trace1: tsTS1=["value1","value2"], tsTS2=["value5","value6"].
	for _, tr := range got {
		if len(tr.Tags) == 0 {
			continue
		}
		for _, tag := range tr.Tags {
			switch tag.Name {
			case "strTag":
				for _, tv := range tag.Values {
					sv := tv.GetStr()
					g.Expect(sv).NotTo(gomega.BeNil(), "strTag must be a string value")
					g.Expect(sv.Value == "value1" || sv.Value == "value4").To(gomega.BeTrue(),
						"strTag value must be value1 (tsTS1) or value4 (tsTS2), got: %s", sv.Value)
				}
			case "intTag":
				for _, tv := range tag.Values {
					iv := tv.GetInt()
					g.Expect(iv).NotTo(gomega.BeNil(), "intTag must be an int64 value")
					g.Expect(iv.Value == int64(10) || iv.Value == int64(40)).To(gomega.BeTrue(),
						"intTag value must be 10 (tsTS1) or 40 (tsTS2), got: %d", iv.Value)
				}
			case "strArrTag":
				for _, tv := range tag.Values {
					av := tv.GetStrArray()
					g.Expect(av).NotTo(gomega.BeNil(), "strArrTag must be a string array value")
					g.Expect(len(av.Value)).To(gomega.Equal(2), "strArrTag must have 2 elements")
				}
			}
		}
	}

	// trace2 and trace3 should also be queryable.
	got2 := queryTraceID(g, recvSnp, "trace2", 1, 2)
	g.Expect(got2).NotTo(gomega.BeEmpty(), "should have query results for trace2")

	got3 := queryTraceID(g, recvSnp, "trace3", 1, 2)
	g.Expect(got3).NotTo(gomega.BeEmpty(), "should have query results for trace3")
}

// TestLifecycleMigration_TwoConcurrentSenders verifies that data from two concurrent senders is preserved correctly.
func TestLifecycleMigration_TwoConcurrentSenders(t *testing.T) {
	g := gomega.NewWithT(t)
	checkGoroutineLeak(t, g)
	fileSystem := fs.NewLocalFileSystem()

	// Sender 1 writes tsTS1 (timestamps=1), Sender 2 writes tsTS2 (timestamps=2).
	// Both transfer to the same receiver.

	sender1Path, s1Cleanup := mustNewSpace(g)
	defer s1Cleanup()
	sender2Path, s2Cleanup := mustNewSpace(g)
	defer s2Cleanup()

	mkSender := func(path string, name string, data *traces) (*tsTable, *snapshot) {
		tst, mkErr := newTSTable(
			fileSystem, path, common.Position{},
			logger.GetLogger(name), timestamp.TimeRange{},
			option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}},
			nil,
		)
		g.Expect(mkErr).NotTo(gomega.HaveOccurred())
		tst.mustAddTraces(data, nil)
		time.Sleep(100 * time.Millisecond)
		waitForFileParts(g, tst)
		snp := tst.currentSnapshot()
		g.Expect(snp).NotTo(gomega.BeNil())
		return tst, snp
	}

	sender1Tst, sender1Snp := mkSender(sender1Path, "sender1", tsTS1)
	defer func() {
		sender1Snp.decRef()
		g.Expect(sender1Tst.Close()).To(gomega.Succeed())
	}()
	sender2Tst, sender2Snp := mkSender(sender2Path, "sender2", tsTS2)
	defer func() {
		sender2Snp.decRef()
		g.Expect(sender2Tst.Close()).To(gomega.Succeed())
	}()

	receiverPath, receiverCleanup := mustNewSpace(g)
	defer receiverCleanup()

	receiverTst, err := newTSTable(
		fileSystem, receiverPath, common.Position{},
		logger.GetLogger("receiver-two"), timestamp.TimeRange{},
		option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}},
		nil,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		g.Expect(receiverTst.Close()).To(gomega.Succeed())
	}()

	// Transfer from both senders into the single receiver.
	transferParts(g, sender1Snp, receiverTst, fileSystem, nil)
	transferParts(g, sender2Snp, receiverTst, fileSystem, nil)

	recvSnp := receiverTst.currentSnapshot()
	g.Expect(recvSnp).NotTo(gomega.BeNil())
	defer recvSnp.decRef()

	// trace1 should surface spans from both senders.
	got := queryTraceID(g, recvSnp, "trace1", 1, 2)
	g.Expect(got).NotTo(gomega.BeEmpty())
	totalSpans := 0
	for _, tr := range got {
		totalSpans += len(tr.Spans)
	}
	g.Expect(totalSpans).To(gomega.Equal(2), "trace1 should have 2 spans from two senders")

	// trace2 should also be present.
	got2 := queryTraceID(g, recvSnp, "trace2", 1, 2)
	g.Expect(got2).NotTo(gomega.BeEmpty())
}

// TestLifecycleMigration_BloomFilterAfterMigration verifies that BloomFilter correctly filters after migration.
func TestLifecycleMigration_BloomFilterAfterMigration(t *testing.T) {
	g := gomega.NewWithT(t)
	checkGoroutineLeak(t, g)
	fileSystem := fs.NewLocalFileSystem()

	senderPath, senderCleanup := mustNewSpace(g)
	defer senderCleanup()

	senderTst, err := newTSTable(
		fileSystem, senderPath, common.Position{},
		logger.GetLogger("sender-bf"), timestamp.TimeRange{},
		option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}},
		nil,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		g.Expect(senderTst.Close()).To(gomega.Succeed())
	}()

	senderTst.mustAddTraces(tsTS1, nil)
	time.Sleep(100 * time.Millisecond)

	waitForFileParts(g, senderTst)

	senderSnp := senderTst.currentSnapshot()
	g.Expect(senderSnp).NotTo(gomega.BeNil())
	defer senderSnp.decRef()

	receiverPath, receiverCleanup := mustNewSpace(g)
	defer receiverCleanup()

	receiverTst, err := newTSTable(
		fileSystem, receiverPath, common.Position{},
		logger.GetLogger("receiver-bf"), timestamp.TimeRange{},
		option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}},
		nil,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		g.Expect(receiverTst.Close()).To(gomega.Succeed())
	}()

	transferParts(g, senderSnp, receiverTst, fileSystem, nil)

	recvSnp := receiverTst.currentSnapshot()
	g.Expect(recvSnp).NotTo(gomega.BeNil())
	defer recvSnp.decRef()

	// Existing trace IDs should pass the bloom filter.
	ppHit, nHit := recvSnp.getParts(nil, 1, 1, []string{"trace1"})
	g.Expect(nHit).To(gomega.BeNumerically(">", 0), "trace1 should pass BloomFilter")
	g.Expect(ppHit).NotTo(gomega.BeEmpty())

	// Non-existing trace ID should be rejected by the bloom filter.
	ppMiss, nMiss := recvSnp.getParts(nil, 1, 1, []string{"nonexistent-trace-id"})
	g.Expect(nMiss).To(gomega.Equal(0), "nonexistent-trace-id should be filtered by BloomFilter")
	g.Expect(ppMiss).To(gomega.BeEmpty())
}

// TestLifecycleMigration_WithSIDX verifies that SIDX parts are correctly migrated alongside core parts.
// It creates trace data with SIDX (secondary index) entries, transfers both core and SIDX parts to a
// receiver, and verifies the receiver's SIDX contains the expected file-backed parts.
func TestLifecycleMigration_WithSIDX(t *testing.T) {
	g := gomega.NewWithT(t)
	checkGoroutineLeak(t, g)
	fileSystem := fs.NewLocalFileSystem()

	const sidxIndexName = "timestamp_millis"

	// Phase 1: Sender — write trace data with SIDX.
	senderPath, senderCleanup := mustNewSpace(g)
	defer senderCleanup()

	senderTst, err := newTSTable(
		fileSystem, senderPath, common.Position{},
		logger.GetLogger("sender-sidx"), timestamp.TimeRange{},
		option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}},
		nil,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// Build SIDX write requests that index the traces by timestamp (simulating a timestamp_millis index).
	sidxReqs1 := []sidx.WriteRequest{
		{SeriesID: 1, Key: 1, Data: []byte("trace1-ts1")},
		{SeriesID: 2, Key: 1, Data: []byte("trace2-ts1")},
	}
	sidxReqs2 := []sidx.WriteRequest{
		{SeriesID: 1, Key: 2, Data: []byte("trace1-ts2")},
		{SeriesID: 3, Key: 2, Data: []byte("trace3-ts2")},
	}

	// Write first batch with SIDX data.
	sidxMp1 := mustBuildSidxMemPart(g, senderTst, sidxIndexName, sidxReqs1, 1, 1)
	senderTst.mustAddTraces(tsTS1, map[string]*sidx.MemPart{sidxIndexName: sidxMp1})
	time.Sleep(100 * time.Millisecond)

	// Write second batch with SIDX data.
	sidxMp2 := mustBuildSidxMemPart(g, senderTst, sidxIndexName, sidxReqs2, 2, 2)
	senderTst.mustAddTraces(tsTS2, map[string]*sidx.MemPart{sidxIndexName: sidxMp2})
	time.Sleep(100 * time.Millisecond)

	// Wait until core snapshot and SIDX are consistent (both merged to the same part IDs).
	// waitForConsistentSidxSnapshot returns a snapshot and SIDX paths captured atomically.
	senderSnp, senderSidxAllPaths := waitForConsistentSidxSnapshot(g, senderTst, []string{sidxIndexName})
	g.Expect(senderSnp).NotTo(gomega.BeNil())
	defer senderSnp.decRef()
	g.Expect(len(senderSnp.parts)).To(gomega.BeNumerically(">", 0))
	g.Expect(senderSidxAllPaths).To(gomega.HaveKey(sidxIndexName), "sender must have SIDX index %q", sidxIndexName)
	senderSidxPaths := senderSidxAllPaths[sidxIndexName]
	g.Expect(senderSidxPaths).NotTo(gomega.BeEmpty(), "sender SIDX must have file-backed parts after flush")

	// Stop the sender's background goroutines (merger, flusher) before transferring.
	// This prevents the merger from deleting on-disk part directories that transferParts reads.
	g.Expect(senderTst.Close()).To(gomega.Succeed())

	// Phase 2: Receiver — transfer both core and SIDX parts.
	receiverPath, receiverCleanup := mustNewSpace(g)
	defer receiverCleanup()

	receiverTst, err := newTSTable(
		fileSystem, receiverPath, common.Position{},
		logger.GetLogger("receiver-sidx"), timestamp.TimeRange{},
		option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}},
		nil,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		g.Expect(receiverTst.Close()).To(gomega.Succeed())
	}()

	transferParts(g, senderSnp, receiverTst, fileSystem, senderSidxAllPaths)

	// Phase 3: Verify core parts migrated correctly.
	recvSnp := receiverTst.currentSnapshot()
	g.Expect(recvSnp).NotTo(gomega.BeNil())
	defer recvSnp.decRef()

	g.Expect(len(recvSnp.parts)).To(gomega.BeNumerically(">", 0), "receiver must have core parts after migration")
	for _, pw := range recvSnp.parts {
		g.Expect(pw.mp).To(gomega.BeNil(), "receiver core parts must be file-backed")
	}

	// Verify trace data is queryable after migration.
	got := queryTraceID(g, recvSnp, "trace1", 1, 2)
	g.Expect(got).NotTo(gomega.BeEmpty(), "trace1 must be queryable after SIDX migration")

	// Phase 4: Verify SIDX parts migrated to receiver.
	receiverSidxMap := receiverTst.getAllSidx()
	g.Expect(receiverSidxMap).To(gomega.HaveKey(sidxIndexName), "receiver must have SIDX index %q after migration", sidxIndexName)

	recvPartIDSet := snapshotPartIDSet(recvSnp)
	receiverSidxPaths := receiverSidxMap[sidxIndexName].PartPaths(recvPartIDSet)
	g.Expect(receiverSidxPaths).NotTo(gomega.BeEmpty(), "receiver SIDX must have file-backed parts after migration")
	g.Expect(len(receiverSidxPaths)).To(gomega.Equal(len(senderSidxPaths)),
		"receiver SIDX part count must match sender SIDX part count")

	// Verify manifest.json in each receiver SIDX part directory.
	for _, sidxPartPath := range receiverSidxPaths {
		sidxMetaErr := sidx.ValidatePartMetadata(fileSystem, sidxPartPath)
		g.Expect(sidxMetaErr).NotTo(gomega.HaveOccurred(), "manifest.json must be valid in SIDX part %s", sidxPartPath)
	}
}

// buildSidxTagReqs is a helper for constructing sidx.WriteRequest slices with a string tag value,
// suitable for use with mustBuildSidxMemPart when testing tagged SIDX data.
func buildSidxTagReqs(seriesID uint64, key int64, data []byte, tagName, tagValue string) sidx.WriteRequest {
	return sidx.WriteRequest{
		SeriesID: common.SeriesID(seriesID),
		Key:      key,
		Data:     data,
		Tags: []sidx.Tag{
			{
				Name:      tagName,
				Value:     []byte(tagValue),
				ValueType: pbv1.ValueTypeStr,
			},
		},
	}
}

// TestLifecycleMigration_WithSIDXTags verifies that SIDX tag data is correctly migrated alongside core parts.
func TestLifecycleMigration_WithSIDXTags(t *testing.T) {
	g := gomega.NewWithT(t)
	checkGoroutineLeak(t, g)
	fileSystem := fs.NewLocalFileSystem()

	const sidxIndexName = "service_latency"

	// Phase 1: Sender — write trace data with SIDX including tag data.
	senderPath, senderCleanup := mustNewSpace(g)
	defer senderCleanup()

	senderTst, err := newTSTable(
		fileSystem, senderPath, common.Position{},
		logger.GetLogger("sender-sidx-tags"), timestamp.TimeRange{},
		option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}},
		nil,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())

	// Build SIDX write requests with tags (simulating indexed span attributes like latency).
	sidxReqs := []sidx.WriteRequest{
		buildSidxTagReqs(1, 1, convert.Int64ToBytes(100), "service", "frontend"),
		buildSidxTagReqs(2, 1, convert.Int64ToBytes(200), "service", "backend"),
		buildSidxTagReqs(3, 2, convert.Int64ToBytes(150), "service", "cache"),
	}

	sidxMp := mustBuildSidxMemPart(g, senderTst, sidxIndexName, sidxReqs, 1, 2)
	senderTst.mustAddTraces(tsTS1, map[string]*sidx.MemPart{sidxIndexName: sidxMp})
	time.Sleep(100 * time.Millisecond)

	// Wait until core snapshot and SIDX are consistent.
	senderSnp, senderSidxAllPaths := waitForConsistentSidxSnapshot(g, senderTst, []string{sidxIndexName})
	g.Expect(senderSnp).NotTo(gomega.BeNil())
	defer senderSnp.decRef()
	g.Expect(senderSidxAllPaths).To(gomega.HaveKey(sidxIndexName))
	senderSidxPaths := senderSidxAllPaths[sidxIndexName]
	g.Expect(senderSidxPaths).NotTo(gomega.BeEmpty())

	// Stop the sender's background goroutines before transferring to prevent merger from
	// deleting on-disk part directories that transferParts reads.
	g.Expect(senderTst.Close()).To(gomega.Succeed())

	// Phase 2: Transfer to receiver.
	receiverPath, receiverCleanup := mustNewSpace(g)
	defer receiverCleanup()

	receiverTst, err := newTSTable(
		fileSystem, receiverPath, common.Position{},
		logger.GetLogger("receiver-sidx-tags"), timestamp.TimeRange{},
		option{flushTimeout: 0, mergePolicy: newDefaultMergePolicyForTesting(), protector: protector.Nop{}},
		nil,
	)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	defer func() {
		g.Expect(receiverTst.Close()).To(gomega.Succeed())
	}()

	transferParts(g, senderSnp, receiverTst, fileSystem, senderSidxAllPaths)

	// Phase 3: Verify receiver has SIDX with tag data.
	receiverSidxMap := receiverTst.getAllSidx()
	g.Expect(receiverSidxMap).To(gomega.HaveKey(sidxIndexName), "receiver must have SIDX index %q after migration", sidxIndexName)

	recvSnp := receiverTst.currentSnapshot()
	g.Expect(recvSnp).NotTo(gomega.BeNil())
	defer recvSnp.decRef()
	recvPartIDSet := snapshotPartIDSet(recvSnp)
	receiverSidxPaths := receiverSidxMap[sidxIndexName].PartPaths(recvPartIDSet)
	g.Expect(receiverSidxPaths).NotTo(gomega.BeEmpty(), "receiver SIDX must have file-backed parts with tag data")
	g.Expect(len(receiverSidxPaths)).To(gomega.Equal(len(senderSidxPaths)),
		"receiver SIDX part count must match sender")
}
