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

// Package data includes utilities for property test data management and verification.
package data

import (
	"context"
	"embed"
	"fmt"
	"slices"
	"strings"

	"github.com/google/go-cmp/cmp"
	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"go.uber.org/mock/gomock"
	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
	"sigs.k8s.io/yaml"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	metadatapkg "github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/metadata/schema"
	"github.com/apache/skywalking-banyandb/pkg/bydbql"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

//go:embed input/*.yaml
var inputFS embed.FS

//go:embed want/*.yaml
var wantFS embed.FS

//go:embed testdata/*.json
var dataFS embed.FS

//go:embed input/*.ql
var qlFS embed.FS

// VerifyFn verify whether the query response matches the wanted result.
var VerifyFn = func(innerGm gm.Gomega, sharedContext helpers.SharedContext, args helpers.Args) {
	ctx := context.Background()
	verifyWithContext(ctx, innerGm, sharedContext, args)
}

func verifyWithContext(ctx context.Context, innerGm gm.Gomega, sharedContext helpers.SharedContext, args helpers.Args) {
	i, err := inputFS.ReadFile("input/" + args.Input + ".yaml")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	query := &propertyv1.QueryRequest{}
	helpers.UnmarshalYAML(i, query)
	verifyQLWithRequest(ctx, innerGm, args, query, sharedContext.Connection)
	c := propertyv1.NewPropertyServiceClient(sharedContext.Connection)
	resp, err := c.Query(ctx, query)
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	if args.WantErr {
		if err == nil {
			g.Fail("expected error")
		}
		return
	}
	innerGm.Expect(err).NotTo(gm.HaveOccurred(), query.String())
	if args.WantEmpty {
		innerGm.Expect(len(resp.GetProperties())).To(gm.BeEmpty(), query.String())
		return
	}
	if args.Want == "" {
		args.Want = args.Input
	}
	w, err := wantFS.ReadFile("want/" + args.Want + ".yaml")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	want := &propertyv1.QueryResponse{}
	helpers.UnmarshalYAML(w, want)
	innerGm.Expect(resp.GetProperties()).To(gm.HaveLen(len(want.GetProperties())), query.String())
	slices.SortFunc(want.GetProperties(), func(a, b *propertyv1.Property) int {
		return strings.Compare(a.Id, b.Id)
	})
	success := innerGm.Expect(cmp.Equal(resp, want,
		protocmp.IgnoreUnknown(),
		protocmp.IgnoreFields(&propertyv1.Property{}, "updated_at"),
		protocmp.IgnoreFields(&commonv1.Metadata{}, "create_revision", "mod_revision"),
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

func verifyQLWithRequest(ctx context.Context, innerGm gm.Gomega, args helpers.Args, yamlQuery *propertyv1.QueryRequest, conn *grpclib.ClientConn) {
	// if the test case expects an error, skip the QL verification.
	if args.WantErr {
		return
	}
	qlContent, err := qlFS.ReadFile("input/" + args.Input + ".ql")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())

	var qlQueryStr string
	for _, line := range strings.Split(string(qlContent), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if qlQueryStr != "" {
			qlQueryStr += " "
		}
		qlQueryStr += trimmed
	}

	ctrl := gomock.NewController(g.GinkgoT())
	defer ctrl.Finish()

	mockRepo := metadatapkg.NewMockRepo(ctrl)
	measure := schema.NewMockProperty(ctrl)
	measure.EXPECT().GetProperty(gomock.Any(), gomock.Any()).DoAndReturn(func(ctx context.Context, meatadata *commonv1.Metadata) (*databasev1.Property, error) {
		client := databasev1.NewPropertyRegistryServiceClient(conn)
		resp, getErr := client.Get(ctx, &databasev1.PropertyRegistryServiceGetRequest{Metadata: meatadata})
		if getErr != nil {
			return nil, getErr
		}
		return resp.Property, nil
	}).AnyTimes()
	mockRepo.EXPECT().PropertyRegistry().AnyTimes().Return(measure)

	parsed, errStrs := bydbql.ParseQuery(qlQueryStr)
	innerGm.Expect(errStrs).To(gm.BeNil())

	transformer := bydbql.NewTransformer(mockRepo)
	result, err := transformer.Transform(ctx, parsed)
	innerGm.Expect(err).NotTo(gm.HaveOccurred())

	qlQuery, ok := result.QueryRequest.(*propertyv1.QueryRequest)
	innerGm.Expect(ok).To(gm.BeTrue())

	equal := cmp.Equal(qlQuery, yamlQuery,
		protocmp.IgnoreUnknown(),
		protocmp.Transform())
	innerGm.Expect(equal).To(gm.BeTrue(), "QL:\n%s\nYAML:\n%s", qlQuery.String(), yamlQuery.String())

	bydbqlClient := bydbqlv1.NewBydbQLServiceClient(conn)
	bydbqlResp, err := bydbqlClient.Query(ctx, &bydbqlv1.QueryRequest{
		Query: qlQueryStr,
	})
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	innerGm.Expect(bydbqlResp).NotTo(gm.BeNil())
	_, ok = bydbqlResp.Result.(*bydbqlv1.QueryResponse_PropertyResult)
	innerGm.Expect(ok).To(gm.BeTrue())
}

// Write writes a property with the given name to the default group and name.
func Write(conn *grpclib.ClientConn, name string) {
	WriteToGroup(conn, "ui_menu", "sw", name)
}

// WriteToGroup writes a property with the given name to the specified group and name.
func WriteToGroup(conn *grpclib.ClientConn, name, group, fileName string) {
	metadata := &commonv1.Metadata{
		Name:  name,
		Group: group,
	}
	schema := databasev1.NewPropertyRegistryServiceClient(conn)
	resp, err := schema.Get(context.Background(), &databasev1.PropertyRegistryServiceGetRequest{Metadata: metadata})
	gm.Expect(err).NotTo(gm.HaveOccurred())
	metadata = resp.GetProperty().GetMetadata()

	c := propertyv1.NewPropertyServiceClient(conn)
	ctx := context.Background()
	var request propertyv1.ApplyRequest
	content, err := dataFS.ReadFile(fmt.Sprintf("testdata/%s.json", fileName))
	gm.Expect(err).ShouldNot(gm.HaveOccurred())
	gm.Expect(protojson.Unmarshal(content, &request)).ShouldNot(gm.HaveOccurred())
	request.Property.Metadata = metadata

	_, err = c.Apply(ctx, &request)
	gm.Expect(err).ShouldNot(gm.HaveOccurred())
}
