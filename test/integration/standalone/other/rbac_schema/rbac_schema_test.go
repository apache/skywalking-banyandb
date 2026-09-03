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

package rbacschema_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	clientauth "github.com/apache/skywalking-banyandb/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

// The canonical fixture family of the #13994 design, narrowed to the actors issue #14015
// needs: an operator administrator, an exact-scope writer and reader on alpha, a wildcard
// reader, and an authenticated principal with no binding.
const schemaPolicy = `
users:
  - username: "bydb-admin"
    password: "admin-secret"
  - username: "bydb-writer-alpha"
    password: "writer-alpha-secret"
  - username: "bydb-writer-gamma"
    password: "writer-gamma-secret"
  - username: "bydb-reader-alpha"
    password: "reader-alpha-secret"
  - username: "bydb-reader-all"
    password: "reader-all-secret"
  - username: "bydb-unbound"
    password: "unbound-secret"
rbac:
  enabled: true
  bindings:
    - principal: "bydb-admin"
      role: "admin"
      groups: ["*"]
    - principal: "bydb-writer-alpha"
      role: "writer"
      groups: ["alpha"]
    - principal: "bydb-writer-gamma"
      role: "writer"
      groups: ["gamma"]
    - principal: "bydb-reader-alpha"
      role: "reader"
      groups: ["alpha"]
    - principal: "bydb-reader-all"
      role: "reader"
      groups: ["*"]
`

const (
	groupAlpha   = "alpha"
	groupBeta    = "beta"
	groupGamma   = "gamma"
	measureAlpha = "alpha_marker"
	measureBeta  = "beta_marker"
	// internalTopNResult is provisioned by the measure subsystem itself in every measure
	// group, exactly as test/cases/schema/clients.go already accounts for. It is not a
	// resource this fixture created, and authorization is a group decision rather than a
	// resource filter, so a list assertion names the fixture's own measures instead of
	// carrying an off-by-one for it.
	internalTopNResult = "_top_n_result"
)

type actor struct {
	name     string
	password string
}

var (
	adminActor       = actor{name: "bydb-admin", password: "admin-secret"}
	writerAlphaActor = actor{name: "bydb-writer-alpha", password: "writer-alpha-secret"}
	writerGammaActor = actor{name: "bydb-writer-gamma", password: "writer-gamma-secret"}
	readerAlphaActor = actor{name: "bydb-reader-alpha", password: "reader-alpha-secret"}
	readerAllActor   = actor{name: "bydb-reader-all", password: "reader-all-secret"}
	unboundActor     = actor{name: "bydb-unbound", password: "unbound-secret"}
)

func (a actor) ctx() context.Context {
	return metadata.NewOutgoingContext(context.Background(),
		metadata.Pairs("username", a.name, "password", a.password))
}

// measureGroup is the group body the fixture provisions. Both fixture groups are identical
// apart from their name, so a scope leak shows up as the wrong group answering, not as a
// resource that happens to be shaped differently.
func measureGroup(name string) *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: name},
		Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        1,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}
}

func measureSchema(group, name string) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Group: group, Name: name},
		Entity:   &databasev1.Entity{TagNames: []string{"id"}},
		TagFamilies: []*databasev1.TagFamilySpec{{
			Name: "default",
			Tags: []*databasev1.TagSpec{{Name: "id", Type: databasev1.TagType_TAG_TYPE_STRING}},
		}},
		Fields: []*databasev1.FieldSpec{{
			Name:              "value",
			FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
			EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
			CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
		}},
	}
}

// userMeasureNames names the measures a caller created, dropping the auto-provisioned TopN
// result entry.
func userMeasureNames(listed []*databasev1.Measure) []string {
	names := make([]string, 0, len(listed))
	for _, entry := range listed {
		if entry.GetMetadata().GetName() == internalTopNResult {
			continue
		}
		names = append(names, entry.GetMetadata().GetName())
	}
	return names
}

// httpCall issues one grpc-gateway request with Basic credentials and returns the status code
// and body. Both are needed: a boolean "did it succeed" assertion cannot tell 401 from 403,
// and the body is where a leaked group name would show up.
func httpCall(httpAddr, method, path string, a actor, body string) (int, string) {
	var reader io.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequestWithContext(context.Background(), method,
		fmt.Sprintf("http://%s%s", httpAddr, path), reader)
	gm.ExpectWithOffset(1, err).NotTo(gm.HaveOccurred())
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if a.name != "" {
		req.Header.Set("Authorization", clientauth.GenerateBasicAuthHeader(a.name, a.password))
	}
	resp, doErr := http.DefaultClient.Do(req)
	gm.ExpectWithOffset(1, doErr).NotTo(gm.HaveOccurred())
	defer func() {
		_ = resp.Body.Close()
	}()
	payload, readErr := io.ReadAll(resp.Body)
	gm.ExpectWithOffset(1, readErr).NotTo(gm.HaveOccurred())
	return resp.StatusCode, string(payload)
}

var _ = g.Describe("rbac-schema group-scoped schema authorization through the real liaison", func() {
	var (
		grpcAddr, httpAddr string
		conn               *grpclib.ClientConn
		groups             databasev1.GroupRegistryServiceClient
		measures           databasev1.MeasureRegistryServiceClient
		barrier            schemav1.SchemaBarrierServiceClient
		deferFn            func()
	)

	g.BeforeEach(func() {
		dataRoot, releaseSpace, spaceErr := test.NewSpace()
		gm.Expect(spaceErr).NotTo(gm.HaveOccurred())

		policyFile := filepath.Join(dataRoot, "security.yaml")
		gm.Expect(os.WriteFile(policyFile, []byte(schemaPolicy), 0o600)).To(gm.Succeed())

		ports, portErr := test.AllocateFreePorts(5)
		gm.Expect(portErr).NotTo(gm.HaveOccurred())

		var closeServer func()
		grpcAddr, httpAddr, closeServer = setup.EmptyClosableStandalone(nil, dataRoot, ports,
			"--auth-config-file="+policyFile)

		var dialErr error
		conn, dialErr = grpclib.NewClient(grpcAddr, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(dialErr).NotTo(gm.HaveOccurred())
		groups = databasev1.NewGroupRegistryServiceClient(conn)
		measures = databasev1.NewMeasureRegistryServiceClient(conn)
		barrier = schemav1.NewSchemaBarrierServiceClient(conn)

		deferFn = func() {
			_ = conn.Close()
			closeServer()
			releaseSpace()
		}

		// The administrator provisions the alpha/beta fixture before any role assertion, and
		// waits for it through the protected SchemaBarrier API rather than by sleeping. This
		// is itself part of the contract: a global administrator must be able to bootstrap the
		// groups and child schemas its deployment needs without holding cluster administration.
		for _, group := range []string{groupAlpha, groupBeta} {
			_, createErr := groups.Create(adminActor.ctx(), &databasev1.GroupRegistryServiceCreateRequest{
				Group: measureGroup(group),
			})
			gm.Expect(createErr).NotTo(gm.HaveOccurred(), "the administrator must be allowed to create group %s", group)
		}
		for group, name := range map[string]string{groupAlpha: measureAlpha, groupBeta: measureBeta} {
			_, createErr := measures.Create(adminActor.ctx(), &databasev1.MeasureRegistryServiceCreateRequest{
				Measure: measureSchema(group, name),
			})
			gm.Expect(createErr).NotTo(gm.HaveOccurred(), "the administrator must be allowed to create measure %s/%s", group, name)
		}
		_, barrierErr := barrier.AwaitSchemaApplied(adminActor.ctx(), &schemav1.AwaitSchemaAppliedRequest{
			Keys: []*schemav1.SchemaKey{
				{Kind: "group", Name: groupAlpha},
				{Kind: "group", Name: groupBeta},
				{Kind: "measure", Group: groupAlpha, Name: measureAlpha},
				{Kind: "measure", Group: groupBeta, Name: measureBeta},
			},
			MinRevisions: []int64{0, 0, 0, 0},
		})
		gm.Expect(barrierErr).NotTo(gm.HaveOccurred(), "the administrator must be allowed to await the fixture schema")
	})

	g.AfterEach(func() {
		deferFn()
	})

	// R1: an exact grant admits its own group and denies every other, a wildcard grant admits
	// both, and an unauthorized existence check is denied rather than answered false. The
	// expected codes are read off issue #14015's R1, not recomputed from the policy.
	g.It("scopes Group and registry reads to the caller's exact groups", func() {
		for _, a := range []actor{adminActor, writerAlphaActor, readerAlphaActor, readerAllActor} {
			resp, err := groups.Get(a.ctx(), &databasev1.GroupRegistryServiceGetRequest{Group: groupAlpha})
			gm.Expect(err).NotTo(gm.HaveOccurred(), "%s must read the alpha group", a.name)
			gm.Expect(resp.GetGroup().GetMetadata().GetName()).To(gm.Equal(groupAlpha))
		}
		for _, a := range []actor{writerAlphaActor, readerAlphaActor} {
			_, err := groups.Get(a.ctx(), &databasev1.GroupRegistryServiceGetRequest{Group: groupBeta})
			gm.Expect(status.Code(err)).To(gm.Equal(codes.PermissionDenied), "%s must be denied the beta group", a.name)
		}
		_, unboundErr := groups.Get(unboundActor.ctx(), &databasev1.GroupRegistryServiceGetRequest{Group: groupAlpha})
		gm.Expect(status.Code(unboundErr)).To(gm.Equal(codes.PermissionDenied), "an unbound principal must be denied")

		// An unauthorized Exist must be denied, never answered false: false would tell the
		// caller the resource is absent, which is a fact about a group it cannot see.
		existResp, existErr := groups.Exist(readerAlphaActor.ctx(), &databasev1.GroupRegistryServiceExistRequest{Group: groupBeta})
		gm.Expect(status.Code(existErr)).To(gm.Equal(codes.PermissionDenied),
			"Exist on an unauthorized group must be denied, got response %v", existResp)

		measureExist, measureExistErr := measures.Exist(readerAlphaActor.ctx(), &databasev1.MeasureRegistryServiceExistRequest{
			Metadata: &commonv1.Metadata{Group: groupBeta, Name: measureBeta},
		})
		gm.Expect(status.Code(measureExistErr)).To(gm.Equal(codes.PermissionDenied),
			"Exist on an unauthorized measure must be denied, got response %v", measureExist)

		// R2's three structural families over a real registry: List by direct group, Get by
		// metadata group, and the alpha reader denied both in beta.
		listAlpha, listAlphaErr := measures.List(readerAlphaActor.ctx(), &databasev1.MeasureRegistryServiceListRequest{Group: groupAlpha})
		gm.Expect(listAlphaErr).NotTo(gm.HaveOccurred(), "the alpha reader must list alpha measures")
		for _, listed := range listAlpha.GetMeasure() {
			gm.Expect(listed.GetMetadata().GetGroup()).To(gm.Equal(groupAlpha),
				"a group-scoped list must answer from the requested group only, got %s/%s",
				listed.GetMetadata().GetGroup(), listed.GetMetadata().GetName())
		}
		gm.Expect(userMeasureNames(listAlpha.GetMeasure())).To(gm.ConsistOf(measureAlpha),
			"the alpha reader's list must be the alpha fixture measure and nothing else the fixture created")

		_, listBetaErr := measures.List(readerAlphaActor.ctx(), &databasev1.MeasureRegistryServiceListRequest{Group: groupBeta})
		gm.Expect(status.Code(listBetaErr)).To(gm.Equal(codes.PermissionDenied), "the alpha reader must be denied beta measures")

		getAlpha, getAlphaErr := measures.Get(readerAlphaActor.ctx(), &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Group: groupAlpha, Name: measureAlpha},
		})
		gm.Expect(getAlphaErr).NotTo(gm.HaveOccurred(), "the alpha reader must get an alpha measure")
		gm.Expect(getAlpha.GetMeasure().GetMetadata().GetGroup()).To(gm.Equal(groupAlpha))
	})

	// R2: a request carrying no resolvable group is malformed, and authentication has already
	// happened, so the caller sees InvalidArgument rather than an authorization outcome.
	g.It("keeps validation precedence for requests carrying no group", func() {
		_, emptyGroupErr := groups.Get(adminActor.ctx(), &databasev1.GroupRegistryServiceGetRequest{})
		gm.Expect(status.Code(emptyGroupErr)).To(gm.Equal(codes.InvalidArgument),
			"an empty group on a Group point read must be InvalidArgument")

		_, nilBodyErr := measures.Create(adminActor.ctx(), &databasev1.MeasureRegistryServiceCreateRequest{})
		gm.Expect(status.Code(nilBodyErr)).To(gm.Equal(codes.InvalidArgument),
			"a create with no resource body must be InvalidArgument")

		_, nilMetadataErr := measures.Get(adminActor.ctx(), &databasev1.MeasureRegistryServiceGetRequest{})
		gm.Expect(status.Code(nilMetadataErr)).To(gm.Equal(codes.InvalidArgument),
			"a get with no metadata must be InvalidArgument")

		// The same holds when the caller would not have been allowed either way: a malformed
		// request must not become a channel for learning what a scope check would have said.
		_, unboundMalformedErr := groups.Get(unboundActor.ctx(), &databasev1.GroupRegistryServiceGetRequest{})
		gm.Expect(status.Code(unboundMalformedErr)).To(gm.Equal(codes.InvalidArgument),
			"an unbound principal's malformed request must be InvalidArgument too")
	})

	// R2/R3: Group upserts are scoped by Group.Metadata.Name, and a denied write leaves the
	// preloaded schema exactly as it was. The absence check runs as the administrator, so it
	// observes storage rather than the denied caller's own view of it.
	g.It("scopes Group upserts by the group body name and leaves denied writes without effect", func() {
		_, readerDeniedErr := groups.Create(readerAlphaActor.ctx(), &databasev1.GroupRegistryServiceCreateRequest{
			Group: measureGroup(groupGamma),
		})
		gm.Expect(status.Code(readerDeniedErr)).To(gm.Equal(codes.PermissionDenied), "a reader must not create a group")

		_, wrongScopeErr := groups.Create(writerAlphaActor.ctx(), &databasev1.GroupRegistryServiceCreateRequest{
			Group: measureGroup(groupGamma),
		})
		gm.Expect(status.Code(wrongScopeErr)).To(gm.Equal(codes.PermissionDenied),
			"the alpha writer must not create a group outside its scope")

		// An exact scope is exact: a name that merely starts with the bound one is a different
		// group and stays denied.
		_, prefixErr := groups.Create(writerGammaActor.ctx(), &databasev1.GroupRegistryServiceCreateRequest{
			Group: measureGroup(groupGamma + "-extra"),
		})
		gm.Expect(status.Code(prefixErr)).To(gm.Equal(codes.PermissionDenied),
			"a group whose name only shares a prefix with the bound scope must be denied")

		for _, absent := range []string{groupGamma, groupGamma + "-extra"} {
			_, absentErr := groups.Get(adminActor.ctx(), &databasev1.GroupRegistryServiceGetRequest{Group: absent})
			gm.Expect(absentErr).To(gm.HaveOccurred(), "denied create must leave %s absent", absent)
			gm.Expect(status.Code(absentErr)).NotTo(gm.Equal(codes.OK))
		}

		// A writer bound to a group that does not exist yet may bootstrap it, which is what
		// lets a service account initialize the groups it needs without cluster administration.
		_, bootstrapErr := groups.Create(writerGammaActor.ctx(), &databasev1.GroupRegistryServiceCreateRequest{
			Group: measureGroup(groupGamma),
		})
		gm.Expect(bootstrapErr).NotTo(gm.HaveOccurred(), "the gamma writer must be allowed to create its bound group")

		// Update is scoped by the same body name, so the gamma writer may update gamma and
		// nothing else.
		_, gammaUpdateErr := groups.Update(writerGammaActor.ctx(), &databasev1.GroupRegistryServiceUpdateRequest{
			Group: measureGroup(groupGamma),
		})
		gm.Expect(gammaUpdateErr).NotTo(gm.HaveOccurred(), "the gamma writer must be allowed to update its bound group")

		_, alphaUpdateErr := groups.Update(writerGammaActor.ctx(), &databasev1.GroupRegistryServiceUpdateRequest{
			Group: measureGroup(groupAlpha),
		})
		gm.Expect(status.Code(alphaUpdateErr)).To(gm.Equal(codes.PermissionDenied),
			"the gamma writer must be denied an update to the alpha group")

		// A denied registry mutation likewise leaves the resource untouched.
		_, updateDeniedErr := measures.Update(readerAlphaActor.ctx(), &databasev1.MeasureRegistryServiceUpdateRequest{
			Measure: measureSchema(groupAlpha, measureAlpha),
		})
		gm.Expect(status.Code(updateDeniedErr)).To(gm.Equal(codes.PermissionDenied), "a reader must not update a measure")

		_, deleteDeniedErr := measures.Delete(readerAlphaActor.ctx(), &databasev1.MeasureRegistryServiceDeleteRequest{
			Metadata: &commonv1.Metadata{Group: groupAlpha, Name: measureAlpha},
		})
		gm.Expect(status.Code(deleteDeniedErr)).To(gm.Equal(codes.PermissionDenied), "a reader must not delete a measure")

		stillThere, stillThereErr := measures.Get(adminActor.ctx(), &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Group: groupAlpha, Name: measureAlpha},
		})
		gm.Expect(stillThereErr).NotTo(gm.HaveOccurred(), "the denied mutations must have left the measure in place")
		gm.Expect(stillThere.GetMeasure().GetMetadata().GetName()).To(gm.Equal(measureAlpha))

		// The writer within scope then succeeds on the same resource, which is what proves the
		// denials above were authorization outcomes and not a broken fixture.
		deleted, deleteErr := measures.Delete(writerAlphaActor.ctx(), &databasev1.MeasureRegistryServiceDeleteRequest{
			Metadata: &commonv1.Metadata{Group: groupAlpha, Name: measureAlpha},
		})
		gm.Expect(deleteErr).NotTo(gm.HaveOccurred(), "the alpha writer must delete its own measure")
		gm.Expect(deleted.GetDeleted()).To(gm.BeTrue())
	})

	// R3: Group.List admits any principal holding schema:read somewhere, rejects one holding
	// it nowhere, and returns only the groups the caller's scopes cover. The expected lists
	// are written out from the fixture bindings.
	g.It("filters Group.List to the caller's visible groups", func() {
		for _, tc := range []struct {
			who  actor
			want []string
		}{
			{who: adminActor, want: []string{groupAlpha, groupBeta}},
			{who: readerAllActor, want: []string{groupAlpha, groupBeta}},
			{who: readerAlphaActor, want: []string{groupAlpha}},
			{who: writerAlphaActor, want: []string{groupAlpha}},
		} {
			resp, err := groups.List(tc.who.ctx(), &databasev1.GroupRegistryServiceListRequest{})
			gm.Expect(err).NotTo(gm.HaveOccurred(), "%s must be allowed Group.List", tc.who.name)
			names := make([]string, 0, len(resp.GetGroup()))
			for _, group := range resp.GetGroup() {
				names = append(names, group.GetMetadata().GetName())
			}
			for _, want := range tc.want {
				gm.Expect(names).To(gm.ContainElement(want), "%s must see %s", tc.who.name, want)
			}
			if len(tc.want) == 1 {
				gm.Expect(names).NotTo(gm.ContainElement(groupBeta), "%s must not see the beta group", tc.who.name)
			}
		}
		_, unboundErr := groups.List(unboundActor.ctx(), &databasev1.GroupRegistryServiceListRequest{})
		gm.Expect(status.Code(unboundErr)).To(gm.Equal(codes.PermissionDenied),
			"a principal with no schema:read grant anywhere must be denied Group.List")
	})

	// R4: a key wait needs every resolved group, a group-kind key takes its scope from the
	// key name, and the cluster-wide revision wait needs a wildcard schema read. Revision
	// zero is a real allow case, not a degenerate one.
	g.It("scopes SchemaBarrier key waits and requires a wildcard read for revision waits", func() {
		alphaKey := []*schemav1.SchemaKey{{Kind: "group", Name: groupAlpha}}
		bothKeys := []*schemav1.SchemaKey{
			{Kind: "group", Name: groupAlpha},
			{Kind: "measure", Group: groupBeta, Name: measureBeta},
		}

		applied, appliedErr := barrier.AwaitSchemaApplied(readerAlphaActor.ctx(), &schemav1.AwaitSchemaAppliedRequest{
			Keys: alphaKey, MinRevisions: []int64{0},
		})
		gm.Expect(appliedErr).NotTo(gm.HaveOccurred(), "the alpha reader must await an alpha key")
		gm.Expect(applied.GetApplied()).To(gm.BeTrue())

		_, mixedErr := barrier.AwaitSchemaApplied(readerAlphaActor.ctx(), &schemav1.AwaitSchemaAppliedRequest{
			Keys: bothKeys, MinRevisions: []int64{0, 0},
		})
		gm.Expect(status.Code(mixedErr)).To(gm.Equal(codes.PermissionDenied),
			"one forbidden key group must deny the whole wait")

		wildcardApplied, wildcardErr := barrier.AwaitSchemaApplied(readerAllActor.ctx(), &schemav1.AwaitSchemaAppliedRequest{
			Keys: bothKeys, MinRevisions: []int64{0, 0},
		})
		gm.Expect(wildcardErr).NotTo(gm.HaveOccurred(), "a wildcard reader must await both key groups")
		gm.Expect(wildcardApplied.GetApplied()).To(gm.BeTrue())

		_, deletedErr := barrier.AwaitSchemaDeleted(readerAlphaActor.ctx(), &schemav1.AwaitSchemaDeletedRequest{
			Keys: []*schemav1.SchemaKey{{Kind: "measure", Group: groupBeta, Name: measureBeta}},
		})
		gm.Expect(status.Code(deletedErr)).To(gm.Equal(codes.PermissionDenied),
			"the alpha reader must be denied a beta deletion wait")

		_, exactRevisionErr := barrier.AwaitRevisionApplied(readerAlphaActor.ctx(), &schemav1.AwaitRevisionAppliedRequest{
			MinRevision: 0,
		})
		gm.Expect(status.Code(exactRevisionErr)).To(gm.Equal(codes.PermissionDenied),
			"an exact-scope reader must be denied a cluster-wide revision wait")

		revisionApplied, revisionErr := barrier.AwaitRevisionApplied(readerAllActor.ctx(), &schemav1.AwaitRevisionAppliedRequest{
			MinRevision: 0,
		})
		gm.Expect(revisionErr).NotTo(gm.HaveOccurred(), "a wildcard reader must be allowed a revision wait")
		gm.Expect(revisionApplied.GetApplied()).To(gm.BeTrue())
	})

	// R4: every bound grpc-gateway route reaches the same decision its gRPC method does. The
	// gateway maps PermissionDenied to 403, InvalidArgument to 400, and the Basic-auth
	// middleware answers 401 itself, so 401, 400 and 403 must stay distinguishable.
	g.It("decides the bound HTTP schema routes exactly as direct gRPC does", func() {
		alphaPath := "/api/v1/group/schema/" + groupAlpha
		betaPath := "/api/v1/group/schema/" + groupBeta

		for _, a := range []actor{adminActor, readerAllActor, readerAlphaActor, writerAlphaActor} {
			gotStatus, body := httpCall(httpAddr, http.MethodGet, alphaPath, a, "")
			gm.Expect(gotStatus).To(gm.Equal(http.StatusOK), "%s must read the alpha group over HTTP: %s", a.name, body)
		}
		for _, a := range []actor{readerAlphaActor, writerAlphaActor} {
			gotStatus, body := httpCall(httpAddr, http.MethodGet, betaPath, a, "")
			gm.Expect(gotStatus).To(gm.Equal(http.StatusForbidden), "%s must be denied the beta group over HTTP: %s", a.name, body)
		}
		wrongPassword := actor{name: adminActor.name, password: "not-the-password"}
		gotStatus, _ := httpCall(httpAddr, http.MethodGet, alphaPath, wrongPassword, "")
		gm.Expect(gotStatus).To(gm.Equal(http.StatusUnauthorized), "bad credentials must stay 401, not 403")

		// Group.List over the gateway is filtered exactly as it is over gRPC, and the body is
		// inspected because that is where a hidden group would leak.
		listStatus, listBody := httpCall(httpAddr, http.MethodGet, "/api/v1/group/schema/lists", readerAlphaActor, "")
		gm.Expect(listStatus).To(gm.Equal(http.StatusOK))
		gm.Expect(listBody).To(gm.ContainSubstring(groupAlpha))
		gm.Expect(listBody).NotTo(gm.ContainSubstring(`"name":"`+groupBeta+`"`),
			"the alpha reader's Group.List body must not name the beta group")

		unboundStatus, _ := httpCall(httpAddr, http.MethodGet, "/api/v1/group/schema/lists", unboundActor, "")
		gm.Expect(unboundStatus).To(gm.Equal(http.StatusForbidden), "an unbound principal must get 403 from Group.List")

		// A registry read, a registry list and a registry write, each over its bound route.
		measureGetStatus, _ := httpCall(httpAddr, http.MethodGet,
			fmt.Sprintf("/api/v1/measure/schema/%s/%s", groupAlpha, measureAlpha), readerAlphaActor, "")
		gm.Expect(measureGetStatus).To(gm.Equal(http.StatusOK), "the alpha reader must get an alpha measure over HTTP")

		measureBetaStatus, _ := httpCall(httpAddr, http.MethodGet,
			fmt.Sprintf("/api/v1/measure/schema/%s/%s", groupBeta, measureBeta), readerAlphaActor, "")
		gm.Expect(measureBetaStatus).To(gm.Equal(http.StatusForbidden), "the alpha reader must be denied a beta measure over HTTP")

		measureListStatus, _ := httpCall(httpAddr, http.MethodGet,
			"/api/v1/measure/schema/lists/"+groupBeta, readerAlphaActor, "")
		gm.Expect(measureListStatus).To(gm.Equal(http.StatusForbidden), "the alpha reader must be denied a beta measure list over HTTP")

		createBody := fmt.Sprintf(
			`{"measure":{"metadata":{"group":%q,"name":"http_marker"},"entity":{"tagNames":["id"]},`+
				`"tagFamilies":[{"name":"default","tags":[{"name":"id","type":"TAG_TYPE_STRING"}]}],`+
				`"fields":[{"name":"value","fieldType":"FIELD_TYPE_INT",`+
				`"encodingMethod":"ENCODING_METHOD_GORILLA","compressionMethod":"COMPRESSION_METHOD_ZSTD"}]}}`,
			groupBeta)
		createStatus, createBodyOut := httpCall(httpAddr, http.MethodPost, "/api/v1/measure/schema", writerAlphaActor, createBody)
		gm.Expect(createStatus).To(gm.Equal(http.StatusForbidden),
			"the alpha writer must be denied a beta measure create over HTTP: %s", createBodyOut)

		absent, absentErr := measures.Exist(adminActor.ctx(), &databasev1.MeasureRegistryServiceExistRequest{
			Metadata: &commonv1.Metadata{Group: groupBeta, Name: "http_marker"},
		})
		gm.Expect(absentErr).NotTo(gm.HaveOccurred())
		gm.Expect(absent.GetHasMeasure()).To(gm.BeFalse(), "the denied HTTP create must have written nothing")

		deleteStatus, _ := httpCall(httpAddr, http.MethodDelete,
			fmt.Sprintf("/api/v1/measure/schema/%s/%s", groupBeta, measureBeta), writerAlphaActor, "")
		gm.Expect(deleteStatus).To(gm.Equal(http.StatusForbidden), "the alpha writer must be denied a beta measure delete over HTTP")

		stillThere, stillThereErr := measures.Exist(adminActor.ctx(), &databasev1.MeasureRegistryServiceExistRequest{
			Metadata: &commonv1.Metadata{Group: groupBeta, Name: measureBeta},
		})
		gm.Expect(stillThereErr).NotTo(gm.HaveOccurred())
		gm.Expect(stillThere.GetHasMeasure()).To(gm.BeTrue(), "the denied HTTP delete must have left the measure in place")
	})
})
