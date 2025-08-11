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

// Package data contains integration test cases of the measure.
package data

import (
	"context"
	"embed"
	"encoding/json"
	"io"
	"time"

	"github.com/google/go-cmp/cmp"
	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
	"sigs.k8s.io/yaml"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

//go:embed input/*.yaml
var inputFS embed.FS

//go:embed want/*.yaml
var wantFS embed.FS

func verifyWithContext(ctx context.Context, innerGm gm.Gomega, sharedContext helpers.SharedContext, args helpers.Args) {
	i, err := inputFS.ReadFile("input/" + args.Input + ".yaml")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	query := &measurev1.QueryRequest{}
	helpers.UnmarshalYAML(i, query)
	query.TimeRange = helpers.TimeRange(args, sharedContext)
	query.Stages = args.Stages
	c := measurev1.NewMeasureServiceClient(sharedContext.Connection)
	resp, err := c.Query(ctx, query)
	if args.WantErr {
		if err == nil {
			g.Fail("expect error")
		}
		return
	}
	innerGm.Expect(err).NotTo(gm.HaveOccurred(), query.String())
	if args.WantEmpty {
		innerGm.Expect(resp.DataPoints).To(gm.BeEmpty())
		return
	}
	if args.Want == "" {
		args.Want = args.Input
	}
	ww, err := wantFS.ReadFile("want/" + args.Want + ".yaml")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	want := &measurev1.QueryResponse{}
	helpers.UnmarshalYAML(ww, want)
	for i := range resp.DataPoints {
		if resp.DataPoints[i].Timestamp != nil {
			innerGm.Expect(resp.DataPoints[i].Version).Should(gm.BeNumerically(">", 0))
			innerGm.Expect(resp.DataPoints[i].Sid).Should(gm.BeNumerically(">", 0))
		}
	}
	success := innerGm.Expect(cmp.Equal(resp, want,
		protocmp.IgnoreUnknown(),
		protocmp.IgnoreFields(&measurev1.DataPoint{}, "timestamp"),
		protocmp.IgnoreFields(&measurev1.DataPoint{}, "version"),
		protocmp.IgnoreFields(&measurev1.DataPoint{}, "sid"),
		protocmp.Transform())).
		To(gm.BeTrue(), func() string {
			var j []byte
			j, err = protojson.Marshal(resp)
			if err != nil {
				return err.Error()
			}
			var y []byte
			y, err = yaml.JSONToYAML(j)
			if err != nil {
				return err.Error()
			}
			return string(y)
		})
	if !success {
		return
	}
	query.Trace = true
	resp, err = c.Query(ctx, query)
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	innerGm.Expect(resp.Trace).NotTo(gm.BeNil())
	innerGm.Expect(resp.Trace.GetSpans()).NotTo(gm.BeEmpty())
}

// VerifyFn verify whether the query response matches the wanted result.
var VerifyFn = func(innerGm gm.Gomega, sharedContext helpers.SharedContext, args helpers.Args) {
	ctx := context.Background()
	verifyWithContext(ctx, innerGm, sharedContext, args)
}

// VerifyFnWithAuth verify whether the query response matches the wanted result with Auth.
var VerifyFnWithAuth = func(innerGm gm.Gomega, sharedContext helpers.SharedContext, args helpers.Args, username, password string) {
	ctx := metadata.AppendToOutgoingContext(context.Background(),
		"username", username,
		"password", password,
	)
	verifyWithContext(ctx, innerGm, sharedContext, args)
}

//go:embed testdata/*.json
var dataFS embed.FS

func loadData(md *commonv1.Metadata, measure measurev1.MeasureService_WriteClient, dataFile string, baseTime time.Time, interval time.Duration) {
	var templates []interface{}
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	gm.Expect(err).ShouldNot(gm.HaveOccurred())
	gm.Expect(json.Unmarshal(content, &templates)).ShouldNot(gm.HaveOccurred())
	for i, template := range templates {
		rawDataPointValue, errMarshal := json.Marshal(template)
		gm.Expect(errMarshal).ShouldNot(gm.HaveOccurred())
		dataPointValue := &measurev1.DataPointValue{}
		gm.Expect(protojson.Unmarshal(rawDataPointValue, dataPointValue)).ShouldNot(gm.HaveOccurred())
		dataPointValue.Timestamp = timestamppb.New(baseTime.Add(-time.Duration(len(templates)-i-1) * interval))
		gm.Expect(measure.Send(&measurev1.WriteRequest{Metadata: md, DataPoint: dataPointValue, MessageId: uint64(time.Now().UnixNano())})).
			Should(gm.Succeed())
	}
}

// Write data into the server.
func Write(conn *grpclib.ClientConn, name, group, dataFile string,
	baseTime time.Time, interval time.Duration,
) {
	WriteWithAuth(conn, name, group, dataFile, baseTime, interval, "", "")
}

// WriteWithAuth data into the server with Auth.
func WriteWithAuth(conn *grpclib.ClientConn, name, group, dataFile string,
	baseTime time.Time, interval time.Duration, username, password string,
) {
	ctx := context.Background()
	md := metadata.Pairs("username", username, "password", password)
	ctx = metadata.NewOutgoingContext(ctx, md)
	metadata := &commonv1.Metadata{
		Name:  name,
		Group: group,
	}

	schema := databasev1.NewMeasureRegistryServiceClient(conn)
	resp, err := schema.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{Metadata: metadata})
	gm.Expect(err).NotTo(gm.HaveOccurred())
	metadata = resp.GetMeasure().GetMetadata()

	c := measurev1.NewMeasureServiceClient(conn)
	writeClient, err := c.Write(ctx)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	loadData(metadata, writeClient, dataFile, baseTime, interval)
	gm.Expect(writeClient.CloseSend()).To(gm.Succeed())
	gm.Eventually(func() error {
		_, err := writeClient.Recv()
		return err
	}, flags.EventuallyTimeout).Should(gm.Equal(io.EOF))
}

// WriteOnly write data into the server and return the write client.
func WriteOnly(conn *grpclib.ClientConn, name, group, dataFile string,
	baseTime time.Time, interval time.Duration,
) measurev1.MeasureService_WriteClient {
	metadata := &commonv1.Metadata{
		Name:  name,
		Group: group,
	}

	schema := databasev1.NewMeasureRegistryServiceClient(conn)
	resp, err := schema.Get(context.Background(), &databasev1.MeasureRegistryServiceGetRequest{Metadata: metadata})
	gm.Expect(err).NotTo(gm.HaveOccurred())
	metadata = resp.GetMeasure().GetMetadata()

	c := measurev1.NewMeasureServiceClient(conn)
	ctx := context.Background()
	writeClient, err := c.Write(ctx)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	loadData(metadata, writeClient, dataFile, baseTime, interval)
	return writeClient
}
