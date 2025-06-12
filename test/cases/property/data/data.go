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

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/test/helpers"

	"github.com/google/go-cmp/cmp"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/testing/protocmp"

	"sigs.k8s.io/yaml"
)

//go:embed input/*.yaml
var inputFS embed.FS

//go:embed want/*.yaml
var wantFS embed.FS

// VerifyFn verify whether the query response matches the wanted result.
var VerifyFn = func(innerGm gm.Gomega, sharedContext helpers.SharedContext, args helpers.Args) {
	i, err := inputFS.ReadFile("input/" + args.Input + ".yaml")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	if args.Want == "" {
		args.Want = args.Input
	}
	ww, err := wantFS.ReadFile("want/" + args.Want + ".yaml")
	innerGm.Expect(err).NotTo(gm.HaveOccurred())

	switch args.Mode {
	case helpers.TestModeCreate:
		updateVerifier(i, ww, innerGm, sharedContext, args, true)
	case helpers.TestModeUpdate:
		updateVerifier(i, ww, innerGm, sharedContext, args, false)
	case helpers.TestModeDelete:
		deleteVerifier(i, ww, innerGm, sharedContext, args)
	case helpers.TestModeQuery:
		queryVerifier(i, ww, innerGm, sharedContext, args)
	}
}

func deleteVerifier(input []byte, want []byte, innerGm gm.Gomega, sharedContext helpers.SharedContext, args helpers.Args) {
	del := &propertyv1.DeleteRequest{}
	helpers.UnmarshalYAML(input, del)
	c := propertyv1.NewPropertyServiceClient(sharedContext.Connection)
	ctx := context.Background()
	resp, err := c.Delete(ctx, del)
	if args.WantErr {
		if err == nil {
			g.Fail("expected error")
		}
		return
	}
	innerGm.Expect(resp.Deleted).To(gm.BeTrue())
	queryRequest := &propertyv1.QueryRequest{
		Groups: []string{del.Group},
		Name:   del.Name,
		Ids:    []string{del.Id},
	}
	verifyQuery(want, ctx, c, queryRequest, args, innerGm)
}

func queryVerifier(input, want []byte, innerGm gm.Gomega, sharedContext helpers.SharedContext, args helpers.Args) {
	queryRequest := &propertyv1.QueryRequest{}
	helpers.UnmarshalYAML(input, queryRequest)
	c := propertyv1.NewPropertyServiceClient(sharedContext.Connection)
	ctx := context.Background()
	verifyQuery(want, ctx, c, queryRequest, args, innerGm)
}

func updateVerifier(input, want []byte, innerGm gm.Gomega, sharedContext helpers.SharedContext, args helpers.Args, create bool) {
	apply := &propertyv1.ApplyRequest{}
	helpers.UnmarshalYAML(input, apply)
	c := propertyv1.NewPropertyServiceClient(sharedContext.Connection)
	ctx := context.Background()
	resp, err := c.Apply(ctx, apply)
	if args.WantErr {
		if err == nil {
			g.Fail("expected error")
		}
		return
	}
	gm.Expect(err).NotTo(gm.HaveOccurred())
	gm.Expect(resp.Created).To(gm.Equal(create))
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	queryRequest := &propertyv1.QueryRequest{
		Groups: []string{apply.Property.Metadata.Group},
		Name:   apply.Property.Metadata.Name,
		Ids:    []string{apply.Property.Id},
	}
	if !verifyQuery(want, ctx, c, queryRequest, args, innerGm) {
		return
	}
	queryRequest.Trace = true
	q, err := c.Query(ctx, queryRequest)
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	innerGm.Expect(q.Trace).NotTo(gm.BeNil())
	innerGm.Expect(q.Trace.GetSpans()).NotTo(gm.BeEmpty())
}

func verifyQuery(want []byte, ctx context.Context, client propertyv1.PropertyServiceClient,
	queryRequest *propertyv1.QueryRequest, args helpers.Args, innerGm gm.Gomega) bool {
	query, err := client.Query(ctx, queryRequest)
	innerGm.Expect(err).NotTo(gm.HaveOccurred())
	if args.WantEmpty {
		innerGm.Expect(query.Properties).To(gm.BeEmpty())
		return true
	}
	wantResp := &propertyv1.QueryResponse{}
	helpers.UnmarshalYAML(want, wantResp)
	success := innerGm.Expect(cmp.Equal(query, wantResp,
		protocmp.IgnoreUnknown(),
		protocmp.IgnoreFields(&propertyv1.Property{}, "updated_at"),
		protocmp.IgnoreFields(&commonv1.Metadata{}, "id", "create_revision", "mod_revision"),
		protocmp.Transform())).
		To(gm.BeTrue(), func() string {
			var j []byte
			j, err = protojson.Marshal(query)
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
	return success
}
