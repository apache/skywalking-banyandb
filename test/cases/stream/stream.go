package stream_test

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"io"
	"math"
	"strconv"
	"time"

	"github.com/google/go-cmp/cmp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sigs.k8s.io/yaml"

	common_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	model_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	stream_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"

	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var SharedContext helpers.SharedContext

//go:embed input/*.yaml
var inputFS embed.FS

//go:embed want/*.yaml
var wantFS embed.FS

var _ = DescribeTable("Scanning Streams", func(args helpers.Args) {
	i, err := inputFS.ReadFile("input/" + args.Input + ".yaml")
	Expect(err).NotTo(HaveOccurred())
	query := &stream_v1.QueryRequest{}
	helpers.UnmarshalYAML(i, query)
	query.TimeRange = helpers.TimeRange(args, SharedContext)
	c := stream_v1.NewStreamServiceClient(SharedContext.Connection)
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
		Expect(resp.Elements).To(BeEmpty())
		return
	}
	if args.Want == "" {
		args.Want = args.Input
	}
	ww, err := wantFS.ReadFile("want/" + args.Want + ".yaml")
	Expect(err).NotTo(HaveOccurred())
	want := &stream_v1.QueryResponse{}
	helpers.UnmarshalYAML(ww, want)
	Expect(cmp.Equal(resp, want,
		protocmp.IgnoreUnknown(),
		protocmp.IgnoreFields(&stream_v1.Element{}, "timestamp"),
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
	Entry("all elements", helpers.Args{Input: "all", Duration: 1 * time.Hour}),
	Entry("limit", helpers.Args{Input: "limit", Duration: 1 * time.Hour}),
	Entry("offset", helpers.Args{Input: "offset", Duration: 1 * time.Hour}),
	Entry("nothing", helpers.Args{Input: "all", WantEmpty: true}),
	Entry("invalid time range", helpers.Args{
		Input: "all",
		Begin: timestamppb.New(time.Unix(0, int64(math.MinInt64+time.Millisecond)).Truncate(time.Millisecond)),
		End:   timestamppb.New(time.Unix(0, math.MaxInt64).Truncate(time.Millisecond)),
	}),
	Entry("sort desc", helpers.Args{Input: "sort_desc", Duration: 1 * time.Hour}),
	Entry("global index", helpers.Args{Input: "global_index", Duration: 1 * time.Hour}),
	Entry("numeric local index: less", helpers.Args{Input: "less", Duration: 1 * time.Hour}),
	Entry("numeric local index: less and eq", helpers.Args{Input: "less_eq", Duration: 1 * time.Hour}),
	Entry("logical expression", helpers.Args{Input: "logical", Duration: 1 * time.Hour}),
	Entry("having", helpers.Args{Input: "having", Duration: 1 * time.Hour}),
	Entry("full text searching", helpers.Args{Input: "search", Duration: 1 * time.Hour}),
)

//go:embed testdata/*.json
var dataFS embed.FS

func loadData(stream stream_v1.StreamService_WriteClient, dataFile string, baseTime time.Time, interval time.Duration) {
	var templates []interface{}
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(json.Unmarshal(content, &templates)).ShouldNot(HaveOccurred())
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")
	for i, template := range templates {
		rawSearchTagFamily, errMarshal := json.Marshal(template)
		Expect(errMarshal).ShouldNot(HaveOccurred())
		searchTagFamily := &model_v1.TagFamilyForWrite{}
		Expect(protojson.Unmarshal(rawSearchTagFamily, searchTagFamily)).ShouldNot(HaveOccurred())
		e := &stream_v1.ElementValue{
			ElementId: strconv.Itoa(i),
			Timestamp: timestamppb.New(baseTime.Add(interval * time.Duration(i))),
			TagFamilies: []*model_v1.TagFamilyForWrite{
				{
					Tags: []*model_v1.TagValue{
						{
							Value: &model_v1.TagValue_BinaryData{
								BinaryData: bb,
							},
						},
					},
				},
			},
		}
		e.TagFamilies = append(e.TagFamilies, searchTagFamily)
		errInner := stream.Send(&stream_v1.WriteRequest{
			Metadata: &common_v1.Metadata{
				Name:  "sw",
				Group: "default",
			},
			Element: e,
		})
		Expect(errInner).ShouldNot(HaveOccurred())
	}
}

func Write(conn *grpclib.ClientConn, dataFile string, baseTime time.Time, interval time.Duration) {
	c := stream_v1.NewStreamServiceClient(conn)
	ctx := context.Background()
	writeClient, err := c.Write(ctx)
	Expect(err).NotTo(HaveOccurred())
	loadData(writeClient, dataFile, baseTime, interval)
	Expect(writeClient.CloseSend()).To(Succeed())
	Eventually(func() error {
		_, err := writeClient.Recv()
		return err
	}).Should(Equal(io.EOF))
}
