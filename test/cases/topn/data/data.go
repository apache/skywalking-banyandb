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

// Package data contains integration test cases of the topN
package data

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

//go:embed input/*.yaml
var inputFS embed.FS

//go:embed want/*.yaml
var wantFS embed.FS

// VerifyFn verify whether the query response matches the wanted result
var VerifyFn = func(sharedContext helpers.SharedContext, args helpers.Args) {
	i, err := inputFS.ReadFile("input/" + args.Input + ".yaml")
	Expect(err).NotTo(HaveOccurred())
	query := &measure_v1.TopNRequest{}
	helpers.UnmarshalYAML(i, query)
	query.TimeRange = helpers.TimeRange(args, sharedContext)
	c := measure_v1.NewMeasureServiceClient(sharedContext.Connection)
	ctx := context.Background()
	resp, err := c.TopN(ctx, query)
	if args.WantErr {
		if err == nil {
			Fail("expect error")
		}
		return
	}
	Expect(err).NotTo(HaveOccurred(), query.String())
	if args.WantEmpty {
		Expect(resp.Lists).To(BeEmpty())
		return
	}
	if args.Want == "" {
		args.Want = args.Input
	}
	ww, err := wantFS.ReadFile("want/" + args.Want + ".yaml")
	Expect(err).NotTo(HaveOccurred())
	want := &measure_v1.TopNResponse{}
	helpers.UnmarshalYAML(ww, want)
	Expect(cmp.Equal(resp, want,
		protocmp.IgnoreUnknown(),
		protocmp.IgnoreFields(&measure_v1.TopNList{}, "timestamp"),
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
}

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
		dataPointValue.Timestamp = timestamppb.New(baseTime.Add(time.Duration(i) * interval))
		Expect(measure.Send(&measure_v1.WriteRequest{Metadata: md, DataPoint: dataPointValue})).
			Should(Succeed())
	}
}

// Write data into the server
func Write(conn *grpclib.ClientConn, name, group, dataFile string,
	baseTime time.Time, interval time.Duration,
) {
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
