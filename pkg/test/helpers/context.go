package helpers

import (
	"time"

	. "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/yaml"

	model_v1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

type SharedContext struct {
	Connection *grpclib.ClientConn
	BaseTime   time.Time
}

type Args struct {
	Input     string
	Offset    time.Duration
	Duration  time.Duration
	Begin     *timestamppb.Timestamp
	End       *timestamppb.Timestamp
	Want      string
	WantEmpty bool
	WantErr   bool
}

func UnmarshalYAML(ii []byte, m proto.Message) {
	j, err := yaml.YAMLToJSON(ii)
	Expect(err).NotTo(HaveOccurred())
	Expect(protojson.Unmarshal(j, m)).To(Succeed())
}

func TimeRange(args Args, shardContext SharedContext) *model_v1.TimeRange {
	if args.Begin != nil && args.End != nil {
		return &model_v1.TimeRange{
			Begin: args.Begin,
			End:   args.End,
		}
	}
	b := shardContext.BaseTime.Add(args.Offset)
	return &model_v1.TimeRange{
		Begin: timestamppb.New(b),
		End:   timestamppb.New(b.Add(args.Duration)),
	}
}
