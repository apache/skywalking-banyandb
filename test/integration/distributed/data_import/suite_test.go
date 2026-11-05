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

package data_import_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	transferv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/transfer/v1"
	"github.com/apache/skywalking-banyandb/banyand/backup/lifecycle"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_property "github.com/apache/skywalking-banyandb/pkg/test/property"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	test_trace "github.com/apache/skywalking-banyandb/pkg/test/trace"
	transfertest "github.com/apache/skywalking-banyandb/pkg/test/transfer"
	casesmeasuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	integration_distributed "github.com/apache/skywalking-banyandb/test/integration/distributed"
)

func TestDataImport(t *testing.T) {
	gomega.RegisterFailHandler(ginkgo.Fail)
	ginkgo.RunSpecs(t, "Distributed Data Import Suite", ginkgo.Label(integration_distributed.Labels...))
}

const (
	// stagedGroup is a measure group with a hot and a warm stage. Its old rows are moved to
	// the warm node by a lifecycle migration before the export.
	stagedGroup   = "import_staged"
	stagedMeasure = "service_cpm_minute"
	// stagedRows is the number of rows of service_cpm_minute_data.json, written once per stage.
	stagedRows = 6
	// warmAge is how old the rows the lifecycle migration moves to the warm stage are: past
	// the hot TTL (2 days), inside the warm one.
	warmAge = 4 * 24 * time.Hour
	// targetStreamShards is the shard_num the targets pre-create the stream group with; the
	// source uses 2.
	targetStreamShards = 3
	hotLabel           = "type=hot"
	warmLabel          = "type=warm"
)

// dataNode is one data node of a test cluster.
type dataNode struct {
	closeFn func()
	addr    string
	dir     string
	// name is the node id the liaison registers the node under.
	name  string
	label string
	once  sync.Once
}

// stop closes the node once; a stopped node stays in the discovery file when the cluster
// keeps stopped nodes.
func (n *dataNode) stop() { n.once.Do(n.closeFn) }

// cluster is one test cluster: data nodes, one liaison and a connection to it.
type cluster struct {
	config       *setup.ClusterConfig
	discovery    *setup.DiscoveryFileWriter
	conn         *grpc.ClientConn
	closeLiaison func()
	cleanup      func()
	liaisonAddr  string
	nodes        []*dataNode
}

// namesOf returns the node names carrying label.
func (c *cluster) namesOf(label string) []string {
	var out []string
	for _, n := range c.nodes {
		if n.label == label {
			out = append(out, n.name)
		}
	}
	return out
}

func (c *cluster) stop() {
	if c == nil {
		return
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	if c.closeLiaison != nil {
		c.closeLiaison()
	}
	for _, n := range c.nodes {
		n.stop()
	}
	c.cleanup()
}

var (
	source *cluster
	// target1 is the main import target: 3 hot nodes and 1 warm node.
	target1 *cluster
	// target2 is a fresh target for the parallelism-1 import: 2 hot nodes and 1 warm node.
	target2     *cluster
	streamBase  time.Time
	measureBase time.Time
	traceBase   time.Time
	// exportRoot holds every export of the suite, so an export outlives the spec that made it.
	exportRoot string
)

// startCluster starts the data nodes with the given labels one after the other, runs the
// schema loaders, and starts a liaison writing the default stage to the hot nodes. Clusters
// must be started sequentially: the harness binds each node's schema registry by a
// process-global index.
func startCluster(labels []string, keepStopped bool, loaders ...func(ctx context.Context, registry schema.Registry) error) *cluster {
	tmpDir, cleanup, err := test.NewSpace()
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	c := &cluster{discovery: setup.NewDiscoveryFileWriter(tmpDir), cleanup: cleanup}
	c.config = setup.PropertyClusterConfig(c.discovery)
	c.config.NodeDiscovery.KeepStoppedNodes = keepStopped
	for _, label := range labels {
		ginkgo.By("Starting data node " + label)
		addr, dir, _, closeFn := setup.DataNodeWithAddrAndDir(c.config, "--node-labels", label,
			"--stream-flush-timeout=500ms", "--measure-flush-timeout=500ms", "--trace-flush-timeout=500ms")
		c.nodes = append(c.nodes, &dataNode{addr: addr, dir: dir, closeFn: closeFn, label: label, name: transfertest.CurrentNodeName(addr)})
	}
	setup.PreloadSchemaViaProperty(c.config, loaders...)
	ginkgo.By("Starting liaison node")
	c.liaisonAddr, c.closeLiaison = setup.LiaisonNode(c.config, "--data-node-selector", hotLabel)
	c.conn, err = grpchelper.Conn(c.liaisonAddr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	return c
}

// stagedGroupSchema is the hot+warm measure group: the warm stage has its own shard number.
func stagedGroupSchema() *commonv1.Group {
	day := func(n uint32) *commonv1.IntervalRule {
		return &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: n}
	}
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: stagedGroup},
		Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        2,
			SegmentInterval: day(1),
			Ttl:             day(2),
			Stages: []*commonv1.LifecycleStage{{
				Name:            "warm",
				ShardNum:        1,
				SegmentInterval: day(1),
				Ttl:             day(7),
				NodeSelector:    warmLabel,
			}},
		},
	}
}

// loadStagedGroup creates the staged group and a copy of sw_metric/service_cpm_minute in it.
func loadStagedGroup(ctx context.Context, registry schema.Registry) error {
	if _, err := registry.CreateGroup(ctx, stagedGroupSchema()); err != nil {
		return err
	}
	m, err := registry.GetMeasure(ctx, &commonv1.Metadata{Group: transfertest.MeasureGroup, Name: stagedMeasure})
	if err != nil {
		return err
	}
	m = proto.Clone(m).(*databasev1.Measure)
	m.Metadata = &commonv1.Metadata{Group: stagedGroup, Name: stagedMeasure}
	m.UpdatedAt = nil
	_, err = registry.CreateMeasure(ctx, m)
	return err
}

// loadTargetStreamGroup pre-creates the suite's stream group with another shard number, so
// the import meets a group conflict that only "ignore" resolves, and the rows are routed
// with the target's shard number.
func loadTargetStreamGroup(ctx context.Context, registry schema.Registry) error {
	_, err := registry.CreateGroup(ctx, &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: transfertest.StreamGroup},
		Catalog:  commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        targetStreamShards,
			Replicas:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 3},
		},
	})
	return err
}

var _ = ginkgo.SynchronizedBeforeSuite(func() []byte {
	gomega.Expect(logger.Init(logger.Logging{Env: "dev", Level: flags.LogLevel})).To(gomega.Succeed())
	var err error
	exportRoot, err = os.MkdirTemp("", "data-import-exports-")
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	ginkgo.By("Starting the source cluster")
	source = startCluster([]string{hotLabel, hotLabel, warmLabel}, false,
		test_stream.PreloadSchema, test_measure.PreloadSchema, test_trace.PreloadSchema, test_property.PreloadSchema, loadStagedGroup)
	ginkgo.By("Starting the first target cluster")
	target1 = startCluster([]string{hotLabel, hotLabel, hotLabel, warmLabel}, true, loadTargetStreamGroup)
	ginkgo.By("Starting the second target cluster")
	target2 = startCluster([]string{hotLabel, hotLabel, warmLabel}, true, loadTargetStreamGroup)

	ginkgo.By("Writing stream, measure, trace and property data to the source")
	conn := source.conn
	d := transfertest.WriteSuiteData(conn)
	streamBase, measureBase, traceBase = d.StreamBase, d.MeasureBase, d.TraceBase
	casesmeasuredata.Write(conn, stagedMeasure, stagedGroup, "service_cpm_minute_data.json", measureBase, transfertest.WriteInterval)
	casesmeasuredata.Write(conn, stagedMeasure, stagedGroup, "service_cpm_minute_data.json", measureBase.Add(-warmAge), transfertest.WriteInterval)

	ginkgo.By("Waiting for the staged rows to be flushed on the hot nodes")
	gomega.Eventually(func(g gomega.Gomega) {
		byStage := stagedRowsByStage(g, source)
		g.Expect(byStage[""]).To(gomega.Equal(uint64(2*stagedRows)), "hot rows of %s: %v", stagedGroup, byStage)
	}, time.Minute, time.Second).Should(gomega.Succeed())
	ginkgo.By("Migrating the old staged rows to the warm node")
	for _, n := range source.nodes {
		if n.label == hotLabel {
			runLifecycle(source, n)
		}
	}
	gomega.Eventually(func(g gomega.Gomega) {
		byStage := stagedRowsByStage(g, source)
		g.Expect(byStage).To(gomega.Equal(map[string]uint64{"": stagedRows, "warm": stagedRows}), "rows of %s per stage", stagedGroup)
	}, time.Minute, time.Second).Should(gomega.Succeed())
	ginkgo.By("Waiting for every catalog to be flushed on the source")
	gomega.Eventually(func(g gomega.Gomega) {
		frames, err := transfertest.Plan(conn, &transferv1.PlanRequest{})
		g.Expect(err).NotTo(gomega.HaveOccurred())
		g.Expect(transfertest.GroupRows(frames, commonv1.Catalog_CATALOG_STREAM, transfertest.StreamGroup)).To(gomega.Equal(uint64(10)), "stream on both replicas")
		// The written points on both replicas and at least one _top_n_result row.
		g.Expect(transfertest.GroupRows(frames, commonv1.Catalog_CATALOG_MEASURE, transfertest.MeasureGroup)).
			To(gomega.BeNumerically(">", 2*(transfertest.MeasureRows+transfertest.TopNSourcePoints)), "measure and Top-N results on both replicas")
		g.Expect(transfertest.GroupRows(frames, commonv1.Catalog_CATALOG_TRACE, transfertest.TraceGroup)).To(gomega.Equal(uint64(13)), "trace")
		g.Expect(transfertest.IndexModeDocs(frames, transfertest.IndexModeGroup)).To(gomega.BeNumerically(">=", 3), "index-mode rows")
		g.Expect(transfertest.PropertyDocs(frames, transfertest.PropertyGroup)).To(gomega.BeNumerically(">=", 4), "property")
	}, time.Minute, time.Second).Should(gomega.Succeed())
	return nil
}, func(_ []byte) {})

var _ = ginkgo.SynchronizedAfterSuite(func() {
	target2.stop()
	target1.stop()
	source.stop()
	gomega.Expect(os.RemoveAll(exportRoot)).To(gomega.Succeed())
}, func() {})

// runLifecycle runs one lifecycle migration on a hot data node of c, moving its segments
// past the hot TTL to the next stage.
func runLifecycle(c *cluster, n *dataNode) {
	dir := ginkgo.GinkgoT().TempDir()
	cmd := lifecycle.NewCommand()
	cmd.SetArgs([]string{
		"--grpc-addr", n.addr,
		"--stream-root-path", n.dir,
		"--measure-root-path", n.dir,
		"--trace-root-path", n.dir,
		"--progress-file", filepath.Join(dir, "progress.json"),
		"--report-dir", filepath.Join(dir, "report"),
		"--node-discovery-mode=file",
		"--node-discovery-file-path=" + c.discovery.Path(),
	})
	gomega.Expect(cmd.Execute()).To(gomega.Succeed())
}

// stagedRowsByStage sums the rows of the staged group per node stage ("" is the default
// stage) on c.
func stagedRowsByStage(g gomega.Gomega, c *cluster) map[string]uint64 {
	frames, err := transfertest.Plan(c.conn, &transferv1.PlanRequest{Selectors: []*transferv1.Selector{
		{Catalog: commonv1.Catalog_CATALOG_MEASURE, Groups: []string{stagedGroup}},
	}})
	g.Expect(err).NotTo(gomega.HaveOccurred())
	out := map[string]uint64{}
	for stage, byNode := range transfertest.RowsByStageNode(frames, commonv1.Catalog_CATALOG_MEASURE, stagedGroup) {
		out[stage] = sum(byNode)
	}
	return out
}
