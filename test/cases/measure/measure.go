package measure_test

import (
	"context"
	"embed"
	"encoding/json"
	"io"
	"time"

	"github.com/google/go-cmp/cmp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/yaml"

	common_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measure_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

var SharedContext helpers.SharedContext

//go:embed input/*.yaml
var inputFS embed.FS

//go:embed want/*.yaml
var wantFS embed.FS

var _ = DescribeTable("Scanning Measures", func(args helpers.Args) {
	i, err := inputFS.ReadFile("input/" + args.Input + ".yaml")
	Expect(err).NotTo(HaveOccurred())
	query := &measure_v1.QueryRequest{}
	helpers.UnmarshalYAML(i, query)
	query.TimeRange = helpers.TimeRange(args, SharedContext)
	c := measure_v1.NewMeasureServiceClient(SharedContext.Connection)
	ctx := context.Background()
	resp, err := c.Query(ctx, query)
	if args.WantErr {
		if err == nil {
			Fail("expect error")
		}
		return
	}
	Expect(err).NotTo(HaveOccurred(), query.String())
	if args.WantEmpty {
		Expect(resp.DataPoints).To(BeEmpty())
		return
	}
	if args.Want == "" {
		args.Want = args.Input
	}
	ww, err := wantFS.ReadFile("want/" + args.Want + ".yaml")
	Expect(err).NotTo(HaveOccurred())
	want := &measure_v1.QueryResponse{}
	helpers.UnmarshalYAML(ww, want)
	Expect(cmp.Equal(resp, want,
		protocmp.IgnoreUnknown(),
		protocmp.IgnoreFields(&measure_v1.DataPoint{}, "timestamp"),
		protocmp.Transform())).
		To(BeTrue(), func() string {
			j, err := protojson.Marshal(resp)
			if err != nil {
				return err.Error()
			}
			y, err := yaml.JSONToYAML(j)
			if err != nil {
				return err.Error()
			}
			return string(y)
		})
},
	Entry("all", helpers.Args{Input: "all", Duration: 1 * time.Hour}),
	Entry("filter by tag", helpers.Args{Input: "tag_filter", Duration: 1 * time.Hour}),
	Entry("filter by an unknown tag", helpers.Args{Input: "tag_filter_unknown", Duration: 1 * time.Hour, WantEmpty: true}),
	Entry("group and max", helpers.Args{Input: "group_max", Duration: 1 * time.Hour}),
	Entry("group without field", helpers.Args{Input: "group_no_field", Duration: 1 * time.Hour}),
	Entry("top 2", helpers.Args{Input: "top", Duration: 1 * time.Hour}),
	Entry("bottom 2", helpers.Args{Input: "bottom", Duration: 1 * time.Hour}),
	Entry("order by time asc", helpers.Args{Input: "order_asc", Duration: 1 * time.Hour}),
	Entry("order by time desc", helpers.Args{Input: "order_desc", Duration: 1 * time.Hour}),
	Entry("limit 3,2", helpers.Args{Input: "limit", Duration: 1 * time.Hour}),
	Entry("match a node", helpers.Args{Input: "match_node", Duration: 1 * time.Hour}),
	Entry("match nodes", helpers.Args{Input: "match_nodes", Duration: 1 * time.Hour}),
)

//go:embed testdata/*.json
var dataFS embed.FS

func loadData(md *common_v1.Metadata, measure measure_v1.MeasureService_WriteClient, dataFile string, baseTime time.Time, interval time.Duration) {
	var templates []interface{}
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(json.Unmarshal(content, &templates)).ShouldNot(HaveOccurred())
	for i, template := range templates {
		rawDataPointValue, errMarshal := json.Marshal(template)
		Expect(errMarshal).ShouldNot(HaveOccurred())
		dataPointValue := &measure_v1.DataPointValue{}
		Expect(protojson.Unmarshal(rawDataPointValue, dataPointValue)).ShouldNot(HaveOccurred())
		dataPointValue.Timestamp = timestamppb.New(baseTime.Add(time.Duration(i) * time.Minute))
		Expect(measure.Send(&measure_v1.WriteRequest{Metadata: md, DataPoint: dataPointValue})).
			Should(Succeed())
	}
}

func Write(conn *grpclib.ClientConn, name, group, dataFile string,
	baseTime time.Time, interval time.Duration) {
	c := measure_v1.NewMeasureServiceClient(conn)
	ctx := context.Background()
	writeClient, err := c.Write(ctx)
	Expect(err).NotTo(HaveOccurred())
	loadData(&common_v1.Metadata{
		Name:  name,
		Group: group,
	}, writeClient, dataFile, baseTime, interval)
	Expect(writeClient.CloseSend()).To(Succeed())
	Eventually(func() error {
		_, err := writeClient.Recv()
		return err
	}).Should(Equal(io.EOF))
}
