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
	grpclib "google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"
	"sigs.k8s.io/yaml"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"
)

//go:embed input/*.yaml
var inputFS embed.FS

//go:embed want/*.yaml
var wantFS embed.FS

//go:embed testdata/*.json
var dataFS embed.FS

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

func Write(conn *grpclib.ClientConn, name string) {
	WriteToGroup(conn, "ui_menu", "sw", name)
}

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

	_, err = c.Apply(ctx, &request)
	gm.Expect(err).ShouldNot(gm.HaveOccurred())
}
