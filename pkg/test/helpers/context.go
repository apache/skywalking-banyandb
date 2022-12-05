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

package helpers

import (
	"time"

	"github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/yaml"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

// SharedContext is the context shared between test cases in the integration testing.
type SharedContext struct {
	Connection *grpclib.ClientConn
	BaseTime   time.Time
}

// Args is a wrapper seals all necessary info for table specs.
type Args struct {
	Begin     *timestamppb.Timestamp
	End       *timestamppb.Timestamp
	Input     string
	Want      string
	Offset    time.Duration
	Duration  time.Duration
	WantEmpty bool
	WantErr   bool
}

// UnmarshalYAML decodes YAML raw bytes to proto.Message.
func UnmarshalYAML(ii []byte, m proto.Message) {
	j, err := yaml.YAMLToJSON(ii)
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	gomega.Expect(protojson.Unmarshal(j, m)).To(gomega.Succeed())
}

// TimeRange returns a modelv1.TimeRange based on Args and SharedContext.
func TimeRange(args Args, shardContext SharedContext) *modelv1.TimeRange {
	if args.Begin != nil && args.End != nil {
		return &modelv1.TimeRange{
			Begin: args.Begin,
			End:   args.End,
		}
	}
	b := shardContext.BaseTime.Add(args.Offset)
	return &modelv1.TimeRange{
		Begin: timestamppb.New(b),
		End:   timestamppb.New(b.Add(args.Duration)),
	}
}
