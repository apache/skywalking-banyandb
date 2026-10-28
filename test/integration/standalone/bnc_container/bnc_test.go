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

package bnc_container_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	"github.com/apache/skywalking-banyandb/pkg/transfer/format/bnc"
	casesmeasuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	casespropertydata "github.com/apache/skywalking-banyandb/test/cases/property/data"
	casesstreamdata "github.com/apache/skywalking-banyandb/test/cases/stream/data"
	casestracedata "github.com/apache/skywalking-banyandb/test/cases/trace/data"
)

const (
	writeInterval     = 500 * time.Millisecond
	topNSourceMeasure = "service_instance_cpm_minute"
	// leaseFile is the lease of an export session, the one file of a session snapshot that is
	// not engine data.
	leaseFile = ".lease"
	// segMetadataFile is the segment metadata, which an export writes as is beside the volumes.
	segMetadataFile = "metadata"
	shardDirPrefix  = "shard-"
	segDirPrefix    = "seg-"
	sidxDir         = "sidx"
)

var (
	dataDir   string
	conn      *grpc.ClientConn
	stopFunc  func()
	startTime time.Time

	volumeName      = regexp.MustCompile(`^(.+)-([0-9]{5})\.bnc$`)
	standaloneFlags = []string{"--stream-flush-timeout=500ms", "--measure-flush-timeout=500ms", "--trace-flush-timeout=500ms"}
)

var _ = ginkgo.BeforeSuite(func() {
	gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
	var deferFn func()
	var err error
	dataDir, deferFn, err = test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ports, err := test.AllocateFreePorts(5)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	addr, _, closeFn := setup.ClosableStandalone(nil, dataDir, ports, standaloneFlags...)
	stopFunc = func() {
		closeFn()
		deferFn()
	}
	conn, err = grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	startTime = timestamp.NowMilli()
	casesstreamdata.Write(conn, transfertest.StreamName, timestamp.NowMilli(), writeInterval)
	ns := timestamp.NowMilli().UnixNano()
	measureBase := time.Unix(0, ns-ns%int64(time.Minute))
	casesmeasuredata.Write(conn, transfertest.MeasureName, transfertest.MeasureGroup, transfertest.MeasureFile, measureBase, writeInterval)
	casesmeasuredata.Write(conn, transfertest.IndexModeName, transfertest.IndexModeGroup, transfertest.IndexModeFile, measureBase, writeInterval)
	casesmeasuredata.Write(conn, topNSourceMeasure, transfertest.MeasureGroup, topNSourceMeasure+"_data.json", measureBase, time.Minute)
	casestracedata.Write(conn, transfertest.TraceName, timestamp.NowMilli(), writeInterval)
	writeLargeSpans(conn, timestamp.NowMilli())
	casespropertydata.Write(conn, "sw1")
	transfertest.WriteTombstone(conn)
	if measureBase.Before(startTime) {
		startTime = measureBase
	}
})

// largeSpans traces with spans of largeSpanSize random bytes make the trace span file far larger
// than bnc.MinFileSize, so a volume limit splits it across several volumes.
const (
	largeSpans    = 4
	largeSpanSize = 150 << 10
)

// writeLargeSpans writes largeSpans spans of random bytes to the suite's trace, checking every
// write status.
func writeLargeSpans(c *grpc.ClientConn, base time.Time) {
	rng := rand.New(rand.NewPCG(1, 2)) //nolint:gosec // a fixed seed keeps failures reproducible
	str := func(v string) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: v}}}
	}
	num := func(v int64) *modelv1.TagValue {
		return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: v}}}
	}
	gomega.Eventually(func(g gomega.Gomega) {
		ctx, cancel := context.WithTimeout(context.Background(), flags.EventuallyTimeout)
		defer cancel()
		stream, err := tracev1.NewTraceServiceClient(c).Write(ctx)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		for i := 0; i < largeSpans; i++ {
			span := make([]byte, largeSpanSize)
			for j := range span {
				span[j] = byte(rng.Uint32())
			}
			ts := &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(base.Add(time.Duration(i) * time.Millisecond))}}
			g.Expect(stream.Send(&tracev1.WriteRequest{
				Metadata: &commonv1.Metadata{Name: transfertest.TraceName, Group: transfertest.TraceGroup},
				Tags: []*modelv1.TagValue{
					str(fmt.Sprintf("bnc_large_%d", i)), num(1), str("large_service"), str("large_instance"), str("/large"),
					num(int64(i)), str(fmt.Sprintf("bnc_large_span_%d", i)), ts,
				},
				Span:    span,
				Version: 1,
			})).To(gomega.Succeed())
		}
		g.Expect(stream.CloseSend()).To(gomega.Succeed())
		var acks int
		for {
			resp, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}
			g.Expect(recvErr).NotTo(gomega.HaveOccurred())
			g.Expect(resp.GetStatus()).To(gomega.Equal(modelv1.Status_STATUS_SUCCEED.String()))
			acks++
		}
		g.Expect(acks).To(gomega.Equal(largeSpans))
	}, flags.EventuallyTimeout).Should(gomega.Succeed())
}

var _ = ginkgo.AfterSuite(func() {
	if conn != nil {
		gomega.Expect(conn.Close()).To(gomega.Succeed())
	}
	if stopFunc != nil {
		stopFunc()
	}
})

// fileSink writes the volumes of VolumeWriters as files under root and records the summary of
// each by its relative path.
type fileSink struct {
	file *os.File
	sums map[string]bnc.Summary
	root string
	cur  string
}

func (s *fileSink) StartFile(path string, _ uint32) error {
	full := filepath.Join(s.root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(full, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	s.file, s.cur = f, path
	return nil
}

func (s *fileSink) Write(p []byte) error {
	_, err := s.file.Write(p)
	return err
}

func (s *fileSink) FinishFile(sum bnc.Summary) error {
	if err := s.file.Close(); err != nil {
		return err
	}
	s.sums[s.cur] = sum
	return nil
}

// listMembers lists every file under dir as members with slash-separated relative paths.
func listMembers(dir string) []bnc.Member {
	var out []bnc.Member
	gomega.ExpectWithOffset(1, filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		out = append(out, bnc.Member{Path: filepath.ToSlash(rel), Size: info.Size()})
		return nil
	})).To(gomega.Succeed())
	return out
}

// pack writes members of dir as the volumes prefix-NNNNN.bnc, numbered from 1 as
// CountVolumes announces, each at most maxFileSize bytes on disk.
func pack(sink *fileSink, dir, prefix string, members []bnc.Member, maxFileSize uint64) {
	if len(members) == 0 {
		return
	}
	v, err := bnc.NewVolumeWriter(sink, prefix, maxFileSize, func(p string) (io.ReadCloser, error) {
		return os.Open(filepath.Join(dir, filepath.FromSlash(p)))
	})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	for _, m := range members {
		gomega.ExpectWithOffset(1, v.Add(context.Background(), m)).To(gomega.Succeed())
	}
	gomega.ExpectWithOffset(1, v.Close()).To(gomega.Succeed())
	count, err := bnc.CountVolumes(members, maxFileSize)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	for i := 1; i <= int(count); i++ {
		rel := fmt.Sprintf("%s-%05d.bnc", prefix, i)
		gomega.ExpectWithOffset(1, sink.sums).To(gomega.HaveKey(rel))
		info, statErr := os.Stat(filepath.Join(sink.root, filepath.FromSlash(rel)))
		gomega.ExpectWithOffset(1, statErr).NotTo(gomega.HaveOccurred())
		gomega.ExpectWithOffset(1, uint64(info.Size())).To(gomega.Equal(sink.sums[rel].Bytes))
		if maxFileSize > 0 {
			gomega.ExpectWithOffset(1, uint64(info.Size())).To(gomega.BeNumerically("<=", maxFileSize), "%s exceeds the limit", rel)
		}
	}
	gomega.ExpectWithOffset(1, sink.sums).NotTo(gomega.HaveKey(fmt.Sprintf("%s-%05d.bnc", prefix, count+1)))
}

// exportCatalog packs one catalog of a session snapshot into out the way an export lays it out:
// a property shard into <group>/shard-<N>-NNNNN.bnc; a segment's shard directory into
// <group>/seg-<suffix>/shard-<N>-NNNNN.bnc, its index into sidx-NNNNN.bnc and its metadata as is.
func exportCatalog(sink *fileSink, catalog, snapshot string, maxFileSize uint64) {
	groups, err := os.ReadDir(snapshot)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, g := range groups {
		if !g.IsDir() {
			gomega.Expect(g.Name()).To(gomega.Equal(leaseFile), "a session snapshot holds group directories and its lease")
			continue
		}
		groupDir := filepath.Join(snapshot, g.Name())
		entries, readErr := os.ReadDir(groupDir)
		gomega.Expect(readErr).NotTo(gomega.HaveOccurred())
		for _, e := range entries {
			gomega.Expect(e.IsDir()).To(gomega.BeTrue(), "unexpected file %s/%s", groupDir, e.Name())
			src := filepath.Join(groupDir, e.Name())
			prefix := catalog + "/" + g.Name() + "/" + e.Name()
			if catalog == "property" {
				gomega.Expect(e.Name()).To(gomega.HavePrefix(shardDirPrefix))
				pack(sink, src, prefix, bnc.OrderIndexDir(listMembers(src)), maxFileSize)
				continue
			}
			gomega.Expect(e.Name()).To(gomega.HavePrefix(segDirPrefix))
			exportSegment(sink, src, prefix, maxFileSize)
		}
	}
}

func exportSegment(sink *fileSink, segDir, prefix string, maxFileSize uint64) {
	entries, err := os.ReadDir(segDir)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	for _, e := range entries {
		src := filepath.Join(segDir, e.Name())
		switch {
		case e.IsDir() && strings.HasPrefix(e.Name(), shardDirPrefix):
			pack(sink, src, prefix+"/"+e.Name(), bnc.OrderShard(listMembers(src)), maxFileSize)
		case e.IsDir() && e.Name() == sidxDir:
			pack(sink, src, prefix+"/"+e.Name(), bnc.OrderIndexDir(listMembers(src)), maxFileSize)
		case !e.IsDir() && e.Name() == segMetadataFile:
			gomega.Expect(copyFile(src, filepath.Join(sink.root, filepath.FromSlash(prefix), segMetadataFile))).To(gomega.Succeed())
		default:
			ginkgo.Fail("unexpected segment entry " + src)
		}
	}
}

// importAll opens the volumes of every container under out from disk as one VolumeSet, checks
// them against the summaries their writer reported and writes their files into
// <restored>/<catalog>/data/<group>/..., the layout of a data directory. It returns the most
// volumes a single file spans.
func importAll(out, restored string, sums map[string]bnc.Summary) int {
	containers := map[string][]string{} // container prefix -> volume paths
	gomega.Expect(filepath.WalkDir(out, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(out, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		catalog, rest, _ := strings.Cut(rel, "/")
		if d.Name() == segMetadataFile {
			return copyFile(p, filepath.Join(restored, catalog, "data", filepath.FromSlash(rest)))
		}
		m := volumeName.FindStringSubmatch(rel)
		gomega.Expect(m).NotTo(gomega.BeNil(), "unexpected file %s", rel)
		containers[m[1]] = append(containers[m[1]], p)
		return nil
	})).To(gomega.Succeed())
	var opened, widest int
	for prefix, paths := range containers {
		// WalkDir lists names in order, so the volumes come in sequence order.
		volumes := make([]*bnc.Container, 0, len(paths))
		pieces := map[string]int{} // volumes holding a piece of each file
		for _, p := range paths {
			c, err := bnc.OpenFile(p)
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), p)
			for _, e := range c.Entries() {
				pieces[e.Path]++
			}
			volumes = append(volumes, c)
		}
		widest = max(widest, slices.Max(slices.Collect(maps.Values(pieces))))
		set, err := bnc.NewVolumeSet(volumes)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), prefix)
		sumsOf, err := set.Verify(context.Background())
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), prefix)
		for i, sum := range sumsOf {
			gomega.Expect(sum).To(gomega.Equal(sums[fmt.Sprintf("%s-%05d.bnc", prefix, i+1)]), "%s volume %d reads back as written", prefix, i+1)
		}
		opened += len(paths)
		catalog, rest, _ := strings.Cut(prefix, "/")
		if catalog != "property" && filepath.Base(prefix) != sidxDir {
			layout := set.Shard()
			gomega.Expect(layout.Others).To(gomega.BeEmpty(), "%s: every engine file is covered by the grammar", prefix)
			gomega.Expect(layout.Orphans).To(gomega.BeEmpty(), "%s: a snapshot holds complete parts only", prefix)
		}
		dir := filepath.Join(restored, catalog, "data", filepath.FromSlash(rest))
		for _, f := range set.Files() {
			r, openErr := set.Open(f)
			gomega.Expect(openErr).NotTo(gomega.HaveOccurred())
			gomega.Expect(writeNewFile(filepath.Join(dir, filepath.FromSlash(f.Path)), r)).To(gomega.Succeed(), "%s holds %s once", prefix, f.Path)
		}
		gomega.Expect(set.Close()).To(gomega.Succeed())
	}
	gomega.Expect(opened).To(gomega.Equal(len(sums)), "every written volume is on disk")
	return widest
}

func copyFile(src, dst string) error {
	f, err := os.Open(src)
	if err != nil {
		return err
	}
	defer f.Close()
	return writeNewFile(dst, f)
}

// writeNewFile writes r to a file that must not exist yet.
func writeNewFile(path string, r io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	// A volume set reads with one ReadAt per Read: copy in large blocks.
	if _, err = io.CopyBuffer(f, struct{ io.Reader }{r}, make([]byte, 1<<20)); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// fullTree fingerprints every file under root, index directories included, leaving out the
// files named in skip.
func fullTree(root string, skip ...string) map[string]string {
	out := map[string]string{}
	gomega.ExpectWithOffset(1, filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || slices.Contains(skip, d.Name()) {
			return err
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		h := sha256.New()
		if _, err = io.Copy(h, f); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		out[filepath.ToSlash(rel)] = hex.EncodeToString(h.Sum(nil))
		return nil
	})).To(gomega.Succeed())
	return out
}

// queryItems runs a BydbQL query and returns its result items, each encoded deterministically
// and sorted, so two processes compare regardless of the order they return them in.
func queryItems(c *grpc.ClientConn, query string, params ...*modelv1.TagValue) []string {
	resp, err := bydbqlv1.NewBydbQLServiceClient(c).Query(context.Background(), &bydbqlv1.QueryRequest{Query: query, Params: params})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred(), query)
	var items []proto.Message
	switch r := resp.GetResult().(type) {
	case *bydbqlv1.QueryResponse_StreamResult:
		for _, e := range r.StreamResult.GetElements() {
			items = append(items, e)
		}
	case *bydbqlv1.QueryResponse_MeasureResult:
		for _, dp := range r.MeasureResult.GetDataPoints() {
			items = append(items, dp)
		}
	case *bydbqlv1.QueryResponse_TraceResult:
		for _, tr := range r.TraceResult.GetTraces() {
			items = append(items, tr)
		}
	case *bydbqlv1.QueryResponse_PropertyResult:
		for _, p := range r.PropertyResult.GetProperties() {
			items = append(items, p)
		}
	default:
		ginkgo.Fail(fmt.Sprintf("%s: unexpected result %T", query, r))
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		b, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(item)
		gomega.ExpectWithOffset(1, marshalErr).NotTo(gomega.HaveOccurred())
		out = append(out, string(b))
	}
	slices.Sort(out)
	return out
}

func timeParam(t time.Time) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(t)}}
}

var _ = ginkgo.Describe("BNC containers of a standalone export snapshot", ginkgo.Ordered, func() {
	ginkgo.BeforeAll(func() {
		ginkgo.By("Waiting for every catalog to be flushed")
		gomega.Eventually(func(g gomega.Gomega) {
			frames, planErr := transfertest.Plan(conn, &transferv1.PlanRequest{})
			g.Expect(planErr).NotTo(gomega.HaveOccurred())
			parts := map[commonv1.Catalog]int{}
			var docs uint64
			for _, f := range transfertest.UnitFrames(frames) {
				for _, u := range f.GetUnits() {
					for _, sh := range u.GetSegment().GetShards() {
						parts[u.GetSegment().GetUnit().GetCatalog()] += int(sh.GetPartsCount())
					}
					if p := u.GetProperty(); p.GetGroup() == transfertest.PropertyGroup {
						for _, sh := range p.GetShards() {
							docs += sh.GetDocCount()
						}
					}
				}
			}
			g.Expect(parts[commonv1.Catalog_CATALOG_STREAM]).To(gomega.BeNumerically(">", 0))
			g.Expect(parts[commonv1.Catalog_CATALOG_MEASURE]).To(gomega.BeNumerically(">", 0))
			g.Expect(parts[commonv1.Catalog_CATALOG_TRACE]).To(gomega.BeNumerically(">", 0))
			g.Expect(transfertest.IndexModeDocs(frames, transfertest.IndexModeGroup)).To(gomega.BeNumerically(">=", 3), "index-mode rows")
			// The property index commits on its own schedule: wait for the live property and the tombstone.
			g.Expect(docs).To(gomega.BeNumerically(">=", 2))
		}, flags.EventuallyTimeout, time.Second).Should(gomega.Succeed())
	})

	// Unlimited volumes, then volumes of at most bnc.MinFileSize bytes on disk: the trace span
	// file of the large spans is several times larger, so it is split across volumes. The
	// snapshot must come back whole either way.
	for _, maxFileSize := range []uint64{0, bnc.MinFileSize} {
		ginkgo.It(fmt.Sprintf("restores a session snapshot from .bnc files of at most %d bytes (0 = no limit) and serves the same data", maxFileSize), func() {
			frames := transfertest.MustPlan(conn, transfertest.CreateRequest(nil, false))
			id := frames[0].GetCreated().GetSessionId()
			gomega.Expect(id).NotTo(gomega.BeEmpty())
			defer transfertest.Release(conn, id)

			ginkgo.By("Packing every catalog of the session snapshot into .bnc files")
			out, restored := ginkgo.GinkgoT().TempDir(), ginkgo.GinkgoT().TempDir()
			sink := &fileSink{root: out, sums: map[string]bnc.Summary{}}
			for _, catalog := range transfertest.Catalogs {
				exportCatalog(sink, catalog, transfertest.SessionDir(dataDir, catalog, id), maxFileSize)
			}
			gomega.Expect(sink.sums).NotTo(gomega.BeEmpty())

			ginkgo.By("Reading the .bnc files back from disk into a data directory")
			widest := importAll(out, restored, sink.sums)
			if maxFileSize > 0 {
				gomega.Expect(widest).To(gomega.BeNumerically(">=", 3), "a file is split across three volumes or more")
			} else {
				gomega.Expect(widest).To(gomega.Equal(1))
			}
			for _, catalog := range transfertest.Catalogs {
				want := fullTree(transfertest.SessionDir(dataDir, catalog, id), leaseFile)
				gomega.Expect(want).NotTo(gomega.BeEmpty(), catalog)
				gomega.Expect(fullTree(filepath.Join(restored, catalog, "data"))).To(gomega.Equal(want),
					"the restored %s files equal the snapshot byte for byte", catalog)
			}

			ginkgo.By("Serving the restored data directory from a second standalone process")
			ports, err := test.AllocateFreePorts(5)
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			restoredAddr, _, closeRestored := setup.ClosableStandalone(nil, restored, ports, standaloneFlags...)
			defer closeRestored()
			restoredConn, err := grpchelper.Conn(restoredAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
			defer restoredConn.Close()

			begin, end := timeParam(startTime.Add(-time.Hour)), timeParam(time.Now().Add(time.Hour))
			queries := []string{
				fmt.Sprintf("SELECT * FROM STREAM %s IN %s TIME BETWEEN ? AND ? LIMIT 1000", transfertest.StreamName, transfertest.StreamGroup),
				fmt.Sprintf("SELECT * FROM MEASURE %s IN %s TIME BETWEEN ? AND ? LIMIT 1000", transfertest.MeasureName, transfertest.MeasureGroup),
				fmt.Sprintf("SELECT * FROM MEASURE %s IN %s TIME BETWEEN ? AND ? LIMIT 1000", topNSourceMeasure, transfertest.MeasureGroup),
				fmt.Sprintf("SELECT * FROM MEASURE %s IN %s TIME BETWEEN ? AND ? LIMIT 1000", transfertest.IndexModeName, transfertest.IndexModeGroup),
				fmt.Sprintf("SELECT * FROM TRACE %s IN %s TIME BETWEEN ? AND ? ORDER BY timestamp DESC LIMIT 1000", transfertest.TraceName, transfertest.TraceGroup),
			}
			for _, q := range queries {
				want := queryItems(conn, q, begin, end)
				gomega.Expect(want).NotTo(gomega.BeEmpty(), q)
				gomega.Eventually(func() []string { return queryItems(restoredConn, q, begin, end) }, flags.EventuallyTimeout, time.Second).
					Should(gomega.Equal(want), "the restored process answers %s like the source", q)
			}
			q := fmt.Sprintf("SELECT * FROM PROPERTY %s IN %s LIMIT 1000", transfertest.PropertyName, transfertest.PropertyGroup)
			want := queryItems(conn, q)
			gomega.Expect(want).NotTo(gomega.BeEmpty(), q)
			gomega.Eventually(func() []string { return queryItems(restoredConn, q) }, flags.EventuallyTimeout, time.Second).
				Should(gomega.Equal(want), "the restored process answers %s like the source", q)
		})
	}
})
