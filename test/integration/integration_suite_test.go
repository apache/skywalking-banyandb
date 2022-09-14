package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/liaison/grpc"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/query"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
	test_measure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	test_stream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	cases_measure "github.com/apache/skywalking-banyandb/test/cases/measure"
	cases_stream "github.com/apache/skywalking-banyandb/test/cases/stream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Integration Suite")
}

const host = "127.0.0.1"

var (
	connection              *grpclib.ClientConn
	now                     time.Time
	gracefulStop, deferFunc func()
)
var _ = SynchronizedBeforeSuite(func() []byte {
	Expect(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "info",
	})).To(Succeed())
	var path string
	var err error
	path, deferFunc, err = test.NewSpace()
	Expect(err).NotTo(HaveOccurred())
	var ports []int
	ports, err = test.AllocateFreePorts(4)
	Expect(err).NotTo(HaveOccurred())
	addr := fmt.Sprintf("%s:%d", host, ports[0])
	flags := []string{
		"--addr=" + addr,
		"--stream-root-path=" + path,
		"--measure-root-path=" + path,
		"--metadata-root-path=" + path,
		fmt.Sprintf("--etcd-listen-client-url=http://%s:%d", host, ports[2]), fmt.Sprintf("--etcd-listen-peer-url=http://%s:%d", host, ports[3]),
	}
	gracefulStop = setup(flags)
	var conn *grpclib.ClientConn
	conn, err = grpclib.Dial(
		string(addr),
		grpclib.WithTransportCredentials(insecure.NewCredentials()),
	)
	Expect(err).NotTo(HaveOccurred())
	now = timestamp.NowMilli()
	interval := 500 * time.Millisecond
	cases_stream.Write(conn, "data.json", now, interval)
	cases_measure.Write(conn, "service_traffic", "sw_metric", "service_traffic_data.json", now, interval)
	cases_measure.Write(conn, "service_instance_traffic", "sw_metric", "service_instance_traffic_data.json", now, interval)
	cases_measure.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", now, interval)
	Expect(conn.Close()).To(Succeed())
	return []byte(addr)
}, func(address []byte) {
	var err error
	connection, err = grpclib.Dial(
		string(address),
		grpclib.WithTransportCredentials(insecure.NewCredentials()),
		grpclib.WithBlock(),
	)
	cases_stream.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	cases_measure.SharedContext = helpers.SharedContext{
		Connection: connection,
		BaseTime:   now,
	}
	Expect(err).NotTo(HaveOccurred())
})

var _ = SynchronizedAfterSuite(func() {
	if connection != nil {
		Expect(connection.Close()).To(Succeed())
	}
}, func() {
	gracefulStop()
	deferFunc()
})

func setup(flags []string) func() {
	// Init `Discovery` module
	repo, err := discovery.NewServiceRepo(context.Background())
	Expect(err).NotTo(HaveOccurred())
	// Init `Queue` module
	pipeline, err := queue.NewQueue(context.TODO(), repo)
	Expect(err).NotTo(HaveOccurred())
	// Init `Metadata` module
	metaSvc, err := metadata.NewService(context.TODO())
	Expect(err).NotTo(HaveOccurred())
	// Init `Stream` module
	streamSvc, err := stream.NewService(context.TODO(), metaSvc, repo, pipeline)
	Expect(err).NotTo(HaveOccurred())
	// Init `Measure` module
	measureSvc, err := measure.NewService(context.TODO(), metaSvc, repo, pipeline)
	Expect(err).NotTo(HaveOccurred())
	// Init `Query` module
	q, err := query.NewExecutor(context.TODO(), streamSvc, measureSvc, metaSvc, repo, pipeline)
	Expect(err).NotTo(HaveOccurred())
	tcp := grpc.NewServer(context.TODO(), pipeline, repo, metaSvc)

	return test.SetUpModules(
		flags,
		repo,
		pipeline,
		metaSvc,
		&preloadService{name: "stream", metaSvc: metaSvc},
		&preloadService{name: "measure", metaSvc: metaSvc},
		streamSvc,
		measureSvc,
		q,
		tcp,
	)
}

type preloadService struct {
	name    string
	metaSvc metadata.Service
}

func (p *preloadService) Name() string {
	return "preload-" + p.name
}

func (p *preloadService) PreRun() error {
	if p.name == "stream" {
		return test_stream.PreloadSchema(p.metaSvc.SchemaRegistry())
	}
	return test_measure.PreloadSchema(p.metaSvc.SchemaRegistry())
}
