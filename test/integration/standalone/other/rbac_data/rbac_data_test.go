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

package rbacdata_test

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	grpclib "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	bydbqlv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/bydbql/v1"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	schemav1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/schema/v1"
	clientauth "github.com/apache/skywalking-banyandb/pkg/auth"
	"github.com/apache/skywalking-banyandb/pkg/test"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/test/setup"
)

// The canonical fixture family of the #13994 design narrowed to the actors issue #14016
// needs. Each group-scoped binding names both of its actor's alpha groups, because a measure
// and a property live in groups of different catalogs and the scope story is about the actor,
// not about the catalog.
const dataPolicy = `
users:
  - username: "bydb-admin"
    password: "admin-secret"
  - username: "bydb-writer-alpha"
    password: "writer-alpha-secret"
  - username: "bydb-reader-alpha"
    password: "reader-alpha-secret"
  - username: "bydb-reader-all"
    password: "reader-all-secret"
  - username: "bydb-monitor"
    password: "monitor-secret"
  - username: "bydb-unbound"
    password: "unbound-secret"
rbac:
  enabled: true
  roles:
    monitor:
      permissions: ["cluster:read"]
  bindings:
    - principal: "bydb-admin"
      role: "admin"
      groups: ["*"]
    - principal: "bydb-writer-alpha"
      role: "writer"
      groups: ["rbac-alpha", "rbac-alpha-prop"]
    - principal: "bydb-reader-alpha"
      role: "reader"
      groups: ["rbac-alpha", "rbac-alpha-prop"]
    - principal: "bydb-reader-all"
      role: "reader"
      groups: ["*"]
    - principal: "bydb-monitor"
      role: "monitor"
      groups: ["*"]
`

// revokedDataPolicy keeps the actors authenticatable while removing their data bindings.
// Rewriting the watched file to it lets the suite observe a revocation on an existing unary
// connection and an already-open write stream without reconnecting or restarting the process.
const revokedDataPolicy = `
users:
  - username: "bydb-admin"
    password: "admin-secret"
  - username: "bydb-writer-alpha"
    password: "writer-alpha-secret"
  - username: "bydb-reader-alpha"
    password: "reader-alpha-secret"
rbac:
  enabled: true
  bindings:
    - principal: "bydb-admin"
      role: "admin"
      groups: ["*"]
`

const (
	groupAlpha     = "rbac-alpha"
	groupBeta      = "rbac-beta"
	groupAlphaProp = "rbac-alpha-prop"
	groupBetaProp  = "rbac-beta-prop"
	fixtureMeasure = "service_cpm"
	fixtureProp    = "endpoint"
	// The marker each group's data carries. Making them group-unique is what turns a scope
	// leak into an observable value rather than a count that happens to be right.
	markerAlpha = "alpha-only-marker"
	markerBeta  = "beta-only-marker"
)

type actor struct {
	name     string
	password string
}

var (
	adminActor       = actor{name: "bydb-admin", password: "admin-secret"}
	writerAlphaActor = actor{name: "bydb-writer-alpha", password: "writer-alpha-secret"}
	readerAlphaActor = actor{name: "bydb-reader-alpha", password: "reader-alpha-secret"}
	readerAllActor   = actor{name: "bydb-reader-all", password: "reader-all-secret"}
	monitorActor     = actor{name: "bydb-monitor", password: "monitor-secret"}
	unboundActor     = actor{name: "bydb-unbound", password: "unbound-secret"}
)

func (a actor) ctx() context.Context {
	return metadata.NewOutgoingContext(context.Background(),
		metadata.Pairs("username", a.name, "password", a.password))
}

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

func propertyGroup(name string) *commonv1.Group {
	return &commonv1.Group{
		Metadata:     &commonv1.Metadata{Name: name},
		Catalog:      commonv1.Catalog_CATALOG_PROPERTY,
		ResourceOpts: &commonv1.ResourceOpts{ShardNum: 1},
	}
}

// measureSchema is identical in both fixture groups apart from the group it names, so a scope
// leak shows up as the wrong group answering rather than as a differently shaped resource.
func measureSchema(group string) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata: &commonv1.Metadata{Group: group, Name: fixtureMeasure},
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

func propertySchema(group string) *databasev1.Property {
	return &databasev1.Property{
		Metadata: &commonv1.Metadata{Group: group, Name: fixtureProp},
		Tags:     []*databasev1.TagSpec{{Name: "marker", Type: databasev1.TagType_TAG_TYPE_STRING}},
	}
}

func markerFrame(group, marker string, at time.Time, messageID uint64) *measurev1.WriteRequest {
	return &measurev1.WriteRequest{
		Metadata:  &commonv1.Metadata{Group: group, Name: fixtureMeasure},
		MessageId: messageID,
		DataPoint: &measurev1.DataPointValue{
			Timestamp: timestamppb.New(at),
			TagFamilies: []*modelv1.TagFamilyForWrite{{
				Tags: []*modelv1.TagValue{{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: marker}}}},
			}},
			Fields: []*modelv1.FieldValue{{Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: 1}}}},
		},
	}
}

func markerQuery(base time.Time, groups ...string) *measurev1.QueryRequest {
	return &measurev1.QueryRequest{
		Groups: groups,
		Name:   fixtureMeasure,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(base.Add(-time.Hour)),
			End:   timestamppb.New(base.Add(time.Hour)),
		},
		TagProjection: &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{{Name: "default", Tags: []string{"id"}}},
		},
		FieldProjection: &measurev1.QueryRequest_FieldProjection{Names: []string{"value"}},
	}
}

// markersIn reports every marker value the response carries, which is how a leak from another
// group is named rather than merely counted.
func markersIn(resp *measurev1.QueryResponse) []string {
	markers := make([]string, 0, len(resp.GetDataPoints()))
	for _, point := range resp.GetDataPoints() {
		for _, family := range point.GetTagFamilies() {
			for _, tag := range family.GetTags() {
				markers = append(markers, tag.GetValue().GetStr().GetValue())
			}
		}
	}
	return markers
}

func replacePolicy(path, content string) {
	nextPath := path + ".next"
	gm.ExpectWithOffset(1, os.WriteFile(nextPath, []byte(content), 0o600)).To(gm.Succeed())
	gm.ExpectWithOffset(1, os.Rename(nextPath, path)).To(gm.Succeed())
}

func metricValue(addr, name string, labels ...string) (float64, error) {
	resp, requestErr := http.Get(fmt.Sprintf("http://%s/metrics", addr))
	if requestErr != nil {
		return 0, fmt.Errorf("scrape metrics: %w", requestErr)
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("scrape metrics: status %d", resp.StatusCode)
	}
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, name) {
			continue
		}
		matched := true
		for _, label := range labels {
			if !strings.Contains(line, label) {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			return 0, fmt.Errorf("scrape metric %s: malformed sample %q", name, line)
		}
		value, parseErr := strconv.ParseFloat(fields[1], 64)
		if parseErr != nil {
			return 0, fmt.Errorf("parse metric %s: %w", name, parseErr)
		}
		return value, nil
	}
	if scanErr := scanner.Err(); scanErr != nil {
		return 0, fmt.Errorf("scan metrics: %w", scanErr)
	}
	return 0, nil
}

// sendMeasureFrames opens a real bidirectional write, sends every frame, closes the send side
// and returns the terminal status of the stream. A per-frame denial surfaces on Recv rather
// than on Send, so the send side has to be closed before the status can be read.
func sendMeasureFrames(a actor, client measurev1.MeasureServiceClient, frames ...*measurev1.WriteRequest) error {
	stream, openErr := client.Write(a.ctx())
	if openErr != nil {
		return openErr
	}
	for _, frame := range frames {
		if sendErr := stream.Send(frame); sendErr != nil {
			if errors.Is(sendErr, io.EOF) {
				break
			}
			return sendErr
		}
	}
	if closeErr := stream.CloseSend(); closeErr != nil {
		return closeErr
	}
	for {
		_, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			return nil
		}
		if recvErr != nil {
			return recvErr
		}
	}
}

// httpCall issues one grpc-gateway request with Basic credentials and returns the status code
// and body. Both are needed: a boolean "did it succeed" assertion cannot tell 401 from 403,
// and the body is where a leaked marker would show up.
func httpCall(httpAddr, method, path string, a actor, body string) (int, string) {
	var reader io.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	}
	req, reqErr := http.NewRequestWithContext(context.Background(), method,
		fmt.Sprintf("http://%s%s", httpAddr, path), reader)
	gm.ExpectWithOffset(1, reqErr).NotTo(gm.HaveOccurred())
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

var _ = g.Describe("rbac-data group-scoped data authorization through the real liaison", func() {
	var (
		httpAddr    string
		metricsAddr string
		policyFile  string
		conn        *grpclib.ClientConn
		groups      databasev1.GroupRegistryServiceClient
		measures    measurev1.MeasureServiceClient
		properties  propertyv1.PropertyServiceClient
		bydbql      bydbqlv1.BydbQLServiceClient
		baseTime    time.Time
		deferFn     func()
	)

	g.BeforeEach(func() {
		// The write validator rejects a data point whose timestamp is finer than a
		// millisecond, so an untruncated time.Now() would have every seeding frame refused
		// for its precision and no scope assertion below would be reached.
		baseTime = time.Now().Truncate(time.Millisecond)

		dataRoot, releaseSpace, spaceErr := test.NewSpace()
		gm.Expect(spaceErr).NotTo(gm.HaveOccurred())

		policyFile = filepath.Join(dataRoot, "security.yaml")
		gm.Expect(os.WriteFile(policyFile, []byte(dataPolicy), 0o600)).To(gm.Succeed())

		ports, portErr := test.AllocateFreePorts(5)
		gm.Expect(portErr).NotTo(gm.HaveOccurred())
		metricsAddr = fmt.Sprintf("127.0.0.1:%d", ports[2])

		var grpcAddr string
		var closeServer func()
		grpcAddr, httpAddr, closeServer = setup.EmptyClosableStandalone(nil, dataRoot, ports,
			"--auth-config-file="+policyFile,
			"--observability-listener-addr="+metricsAddr)

		var dialErr error
		conn, dialErr = grpclib.NewClient(grpcAddr, grpclib.WithTransportCredentials(insecure.NewCredentials()))
		gm.Expect(dialErr).NotTo(gm.HaveOccurred())
		groups = databasev1.NewGroupRegistryServiceClient(conn)
		measures = measurev1.NewMeasureServiceClient(conn)
		properties = propertyv1.NewPropertyServiceClient(conn)
		bydbql = bydbqlv1.NewBydbQLServiceClient(conn)

		deferFn = func() {
			_ = conn.Close()
			closeServer()
			releaseSpace()
		}

		// Schema provisioning is W-PR2's boundary and is already authorized, so it belongs in
		// setup. Seeding the data is this milestone's boundary and is asserted in the specs.
		measureRegistry := databasev1.NewMeasureRegistryServiceClient(conn)
		propertyRegistry := databasev1.NewPropertyRegistryServiceClient(conn)
		barrier := schemav1.NewSchemaBarrierServiceClient(conn)
		for _, group := range []*commonv1.Group{
			measureGroup(groupAlpha), measureGroup(groupBeta),
			propertyGroup(groupAlphaProp), propertyGroup(groupBetaProp),
		} {
			_, createErr := groups.Create(adminActor.ctx(), &databasev1.GroupRegistryServiceCreateRequest{Group: group})
			gm.Expect(createErr).NotTo(gm.HaveOccurred(),
				"the administrator must be allowed to create group %s", group.GetMetadata().GetName())
		}
		for _, group := range []string{groupAlpha, groupBeta} {
			_, createErr := measureRegistry.Create(adminActor.ctx(), &databasev1.MeasureRegistryServiceCreateRequest{
				Measure: measureSchema(group),
			})
			gm.Expect(createErr).NotTo(gm.HaveOccurred(), "the administrator must be allowed to create the %s measure", group)
		}
		for _, group := range []string{groupAlphaProp, groupBetaProp} {
			_, createErr := propertyRegistry.Create(adminActor.ctx(), &databasev1.PropertyRegistryServiceCreateRequest{
				Property: propertySchema(group),
			})
			gm.Expect(createErr).NotTo(gm.HaveOccurred(), "the administrator must be allowed to create the %s property schema", group)
		}
		_, barrierErr := barrier.AwaitSchemaApplied(adminActor.ctx(), &schemav1.AwaitSchemaAppliedRequest{
			Keys: []*schemav1.SchemaKey{
				{Kind: "group", Name: groupAlpha},
				{Kind: "group", Name: groupBeta},
				{Kind: "group", Name: groupAlphaProp},
				{Kind: "group", Name: groupBetaProp},
				{Kind: "measure", Group: groupAlpha, Name: fixtureMeasure},
				{Kind: "measure", Group: groupBeta, Name: fixtureMeasure},
			},
			MinRevisions: []int64{0, 0, 0, 0, 0, 0},
		})
		gm.Expect(barrierErr).NotTo(gm.HaveOccurred(), "the administrator must be allowed to await the fixture schema")
	})

	g.AfterEach(func() {
		deferFn()
	})

	// R1: every group a native read names must be granted, so an exact reader sees its own
	// group, is refused another, and is refused a request that mixes the two rather than being
	// served the part of it it is allowed. The expected codes are read off issue #14016's R1.
	g.It("serves a group-scoped reader its own markers and refuses every mixed request", func() {
		gm.Expect(sendMeasureFrames(adminActor, measures,
			markerFrame(groupAlpha, markerAlpha, baseTime, 1),
			markerFrame(groupBeta, markerBeta, baseTime, 2),
		)).To(gm.Succeed(), "the administrator must be allowed to seed both groups' markers")

		gm.Eventually(func() []string {
			resp, queryErr := measures.Query(adminActor.ctx(), markerQuery(baseTime, groupAlpha, groupBeta))
			if queryErr != nil {
				return nil
			}
			return markersIn(resp)
		}, flags.EventuallyTimeout).Should(gm.ConsistOf(markerAlpha, markerBeta),
			"the seeded markers must be readable by the administrator before scope is asserted")

		alphaResp, alphaErr := measures.Query(readerAlphaActor.ctx(), markerQuery(baseTime, groupAlpha))
		gm.Expect(alphaErr).NotTo(gm.HaveOccurred(), "the alpha reader must read its own group")
		gm.Expect(markersIn(alphaResp)).To(gm.ConsistOf(markerAlpha),
			"the alpha reader must see the alpha marker and nothing from beta")

		_, betaErr := measures.Query(readerAlphaActor.ctx(), markerQuery(baseTime, groupBeta))
		gm.Expect(status.Code(betaErr)).To(gm.Equal(codes.PermissionDenied), "the alpha reader must be refused beta")

		mixedResp, mixedErr := measures.Query(readerAlphaActor.ctx(), markerQuery(baseTime, groupAlpha, groupBeta))
		gm.Expect(status.Code(mixedErr)).To(gm.Equal(codes.PermissionDenied),
			"a request mixing an allowed and a forbidden group must be refused whole")
		gm.Expect(markersIn(mixedResp)).To(gm.BeEmpty(), "a refused multi-group read must return no partial result")

		bothResp, bothErr := measures.Query(readerAllActor.ctx(), markerQuery(baseTime, groupAlpha, groupBeta))
		gm.Expect(bothErr).NotTo(gm.HaveOccurred(), "the wildcard reader must read both groups")
		gm.Expect(markersIn(bothResp)).To(gm.ConsistOf(markerAlpha, markerBeta))

		for _, refused := range []actor{monitorActor, unboundActor} {
			_, refusedErr := measures.Query(refused.ctx(), markerQuery(baseTime, groupAlpha))
			gm.Expect(status.Code(refusedErr)).To(gm.Equal(codes.PermissionDenied),
				"%s holds no data:read grant and must be refused", refused.name)
		}

		// The gateway carries the same decision: the HTTP route and the direct gRPC method
		// share one authorizer, so 200 and 403 must fall exactly where OK and PermissionDenied
		// did above, and 401 must stay reserved for a credential failure.
		allowedBody := fmt.Sprintf(
			`{"groups":["%s"],"name":"%s","timeRange":{"begin":"%s","end":"%s"},`+
				`"tagProjection":{"tagFamilies":[{"name":"default","tags":["id"]}]},"fieldProjection":{"names":["value"]}}`,
			groupAlpha, fixtureMeasure, baseTime.Add(-time.Hour).UTC().Format(time.RFC3339), baseTime.Add(time.Hour).UTC().Format(time.RFC3339))
		forbiddenBody := fmt.Sprintf(
			`{"groups":["%s"],"name":"%s","timeRange":{"begin":"%s","end":"%s"},`+
				`"tagProjection":{"tagFamilies":[{"name":"default","tags":["id"]}]},"fieldProjection":{"names":["value"]}}`,
			groupBeta, fixtureMeasure, baseTime.Add(-time.Hour).UTC().Format(time.RFC3339), baseTime.Add(time.Hour).UTC().Format(time.RFC3339))

		allowedStatus, allowedPayload := httpCall(httpAddr, http.MethodPost, "/api/v1/measure/data", readerAlphaActor, allowedBody)
		gm.Expect(allowedStatus).To(gm.Equal(http.StatusOK), "the alpha reader's HTTP query returned %s", allowedPayload)
		gm.Expect(allowedPayload).To(gm.ContainSubstring(markerAlpha))
		gm.Expect(allowedPayload).NotTo(gm.ContainSubstring(markerBeta))

		forbiddenStatus, forbiddenPayload := httpCall(httpAddr, http.MethodPost, "/api/v1/measure/data", readerAlphaActor, forbiddenBody)
		gm.Expect(forbiddenStatus).To(gm.Equal(http.StatusForbidden), "the alpha reader's beta HTTP query returned %s", forbiddenPayload)
		gm.Expect(forbiddenPayload).NotTo(gm.ContainSubstring(markerBeta), "a refused HTTP query must leak no marker")
	})

	// R3: a write stream is opened only by a principal holding data:write somewhere, each
	// resource-bearing frame is decided against the group it resolves to, and a frame that is
	// refused never reaches storage — which an administrator's query afterwards is what proves.
	g.It("refuses a forbidden write frame mid-stream and stores nothing from it", func() {
		gm.Expect(sendMeasureFrames(writerAlphaActor, measures,
			markerFrame(groupAlpha, markerAlpha, baseTime, 1),
		)).To(gm.Succeed(), "the alpha writer must be allowed to write its own group")

		gm.Eventually(func() []string {
			resp, queryErr := measures.Query(adminActor.ctx(), markerQuery(baseTime, groupAlpha))
			if queryErr != nil {
				return nil
			}
			return markersIn(resp)
		}, flags.EventuallyTimeout).Should(gm.ConsistOf(markerAlpha), "the allowed frame must be stored")

		forbiddenErr := sendMeasureFrames(writerAlphaActor, measures,
			markerFrame(groupAlpha, markerAlpha, baseTime.Add(time.Second), 2),
			markerFrame(groupBeta, markerBeta, baseTime.Add(time.Second), 3),
		)
		gm.Expect(status.Code(forbiddenErr)).To(gm.Equal(codes.PermissionDenied),
			"a frame addressing a forbidden group must end the stream with PermissionDenied")

		betaResp, betaErr := measures.Query(adminActor.ctx(), markerQuery(baseTime.Add(time.Second), groupBeta))
		gm.Expect(betaErr).NotTo(gm.HaveOccurred(), "the administrator must be able to look for the refused frame")
		gm.Expect(markersIn(betaResp)).To(gm.BeEmpty(), "a refused frame must leave nothing in storage")

		readerErr := sendMeasureFrames(readerAlphaActor, measures, markerFrame(groupAlpha, markerAlpha, baseTime, 4))
		gm.Expect(status.Code(readerErr)).To(gm.Equal(codes.PermissionDenied),
			"a principal holding no data:write grant must be refused the write stream")
	})

	// R5: every data decision reads the current policy revision. A unary call on the existing
	// connection observes a live revocation, and the next frame on a write stream that was
	// admitted under the previous revision is denied before it can reach storage.
	g.It("applies live revocation to unary data calls and an already-open write stream", func() {
		writeStream, openErr := measures.Write(writerAlphaActor.ctx())
		gm.Expect(openErr).NotTo(gm.HaveOccurred())

		allowedFrame := markerFrame(groupAlpha, markerAlpha, baseTime, 1)
		gm.Expect(writeStream.Send(allowedFrame)).To(gm.Succeed(),
			"the writer must send an allowed frame before its grant is revoked")
		gm.Eventually(func() (float64, error) {
			return metricValue(metricsAddr, "banyandb_rbac_decisions_total",
				`decision="allow"`, `method="banyandb.measure.v1.MeasureService/Write"`,
				`permission="data:write"`, `reason="granted"`)
		}, flags.EventuallyTimeout).Should(gm.BeNumerically(">=", 1),
			"the open stream's first frame must be allowed under the original revision")

		_, allowedQueryErr := measures.Query(readerAlphaActor.ctx(), markerQuery(baseTime, groupAlpha))
		gm.Expect(allowedQueryErr).NotTo(gm.HaveOccurred(),
			"the reader must be allowed on the existing connection before its grant is revoked")

		replacePolicy(policyFile, revokedDataPolicy)
		gm.Eventually(func() (float64, error) {
			return metricValue(metricsAddr, "banyandb_rbac_policy_revision")
		}, flags.EventuallyTimeout).Should(gm.Equal(float64(2)),
			"the liaison must publish the replacement policy revision")
		gm.Eventually(func() codes.Code {
			_, queryErr := measures.Query(readerAlphaActor.ctx(), markerQuery(baseTime, groupAlpha))
			return status.Code(queryErr)
		}, flags.EventuallyTimeout).Should(gm.Equal(codes.PermissionDenied),
			"the existing unary connection must observe the revoked policy revision")

		deniedFrame := markerFrame(groupAlpha, "revoked-marker", baseTime.Add(time.Second), 2)
		gm.Expect(writeStream.Send(deniedFrame)).To(gm.Succeed(),
			"the transport must accept the next frame on the same open stream")
		for {
			_, receiveErr := writeStream.Recv()
			if receiveErr == nil {
				continue
			}
			gm.Expect(status.Code(receiveErr)).To(gm.Equal(codes.PermissionDenied),
				"the next frame on the already-open stream must use the revoked revision")
			break
		}

		_, deniedQueryErr := measures.Query(adminActor.ctx(), markerQuery(baseTime.Add(time.Second), groupAlpha))
		gm.Expect(deniedQueryErr).NotTo(gm.HaveOccurred(),
			"the administrator must be able to look for the frame denied after revocation")
		gm.Eventually(func() []string {
			stored, queryErr := measures.Query(adminActor.ctx(), markerQuery(baseTime.Add(time.Second), groupAlpha))
			if queryErr != nil {
				return nil
			}
			return markersIn(stored)
		}, flags.EventuallyTimeout).Should(gm.ConsistOf(markerAlpha),
			"the frame denied after revocation must leave no marker beyond the previously allowed frame")
	})

	// R2: a property mutation is decided by the group of the record it names, before the
	// handler runs. A refused mutation is proved to have had no effect by an administrator
	// reading the record afterwards, not by the status code alone.
	g.It("refuses a property mutation without changing the record", func() {
		alphaProperty := &propertyv1.Property{
			Metadata: &commonv1.Metadata{Group: groupAlphaProp, Name: fixtureProp},
			Id:       "1",
			Tags:     []*modelv1.Tag{{Key: "marker", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: markerAlpha}}}}},
		}
		betaProperty := &propertyv1.Property{
			Metadata: &commonv1.Metadata{Group: groupBetaProp, Name: fixtureProp},
			Id:       "1",
			Tags:     []*modelv1.Tag{{Key: "marker", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: markerBeta}}}}},
		}

		_, readerApplyErr := properties.Apply(readerAlphaActor.ctx(), &propertyv1.ApplyRequest{Property: alphaProperty})
		gm.Expect(status.Code(readerApplyErr)).To(gm.Equal(codes.PermissionDenied), "a reader must not apply a property")

		_, betaApplyErr := properties.Apply(writerAlphaActor.ctx(), &propertyv1.ApplyRequest{Property: betaProperty})
		gm.Expect(status.Code(betaApplyErr)).To(gm.Equal(codes.PermissionDenied),
			"the alpha writer must not apply a property in beta")

		absent, absentErr := properties.Query(adminActor.ctx(), &propertyv1.QueryRequest{
			Groups: []string{groupAlphaProp, groupBetaProp}, Name: fixtureProp, Ids: []string{"1"},
		})
		gm.Expect(absentErr).NotTo(gm.HaveOccurred())
		gm.Expect(absent.GetProperties()).To(gm.BeEmpty(), "a refused apply must leave the property absent")

		gm.Eventually(func() error {
			_, applyErr := properties.Apply(writerAlphaActor.ctx(), &propertyv1.ApplyRequest{Property: alphaProperty})
			return applyErr
		}, flags.EventuallyTimeout).Should(gm.Succeed(), "the alpha writer must apply its own group's property")

		_, readerDeleteErr := properties.Delete(readerAlphaActor.ctx(), &propertyv1.DeleteRequest{
			Group: groupAlphaProp, Name: fixtureProp, Id: "1",
		})
		gm.Expect(status.Code(readerDeleteErr)).To(gm.Equal(codes.PermissionDenied), "a reader must not delete a property")

		stillThere, stillErr := properties.Query(adminActor.ctx(), &propertyv1.QueryRequest{
			Groups: []string{groupAlphaProp}, Name: fixtureProp, Ids: []string{"1"},
		})
		gm.Expect(stillErr).NotTo(gm.HaveOccurred())
		gm.Expect(stillThere.GetProperties()).To(gm.HaveLen(1), "a refused delete must leave the property unchanged")

		deleted, deleteErr := properties.Delete(writerAlphaActor.ctx(), &propertyv1.DeleteRequest{
			Group: groupAlphaProp, Name: fixtureProp, Id: "1",
		})
		gm.Expect(deleteErr).NotTo(gm.HaveOccurred(), "the alpha writer must delete its own group's property")
		gm.Expect(deleted.GetDeleted()).To(gm.BeTrue())

		// The gateway route carries the same decision and the same absence of a side effect.
		httpStatus, httpPayload := httpCall(httpAddr, http.MethodDelete,
			fmt.Sprintf("/api/v1/property/data/%s/%s/1", groupBetaProp, fixtureProp), writerAlphaActor, "")
		gm.Expect(httpStatus).To(gm.Equal(http.StatusForbidden), "the alpha writer's beta HTTP delete returned %s", httpPayload)
	})

	// R4: a ByDBQL query is decided by the native request it transformed into. Its text, its
	// casing, its comments, its parameters and the route that carried it are not resources, so
	// none of them can address a group the decision does not see.
	g.It("decides a ByDBQL query by the groups of the request it transformed into", func() {
		gm.Expect(sendMeasureFrames(adminActor, measures,
			markerFrame(groupAlpha, markerAlpha, baseTime, 1),
			markerFrame(groupBeta, markerBeta, baseTime, 2),
		)).To(gm.Succeed(), "the administrator must be allowed to seed both groups' markers")

		allowed := fmt.Sprintf("SELECT * FROM MEASURE %s IN %s", fixtureMeasure, groupAlpha)
		_, allowedErr := bydbql.Query(readerAlphaActor.ctx(), &bydbqlv1.QueryRequest{Query: allowed})
		gm.Expect(status.Code(allowedErr)).NotTo(gm.Equal(codes.PermissionDenied),
			"the alpha reader's own-group ByDBQL query must not be refused, got %v", allowedErr)

		// Every query here transforms into a native request naming a group the alpha reader
		// does not hold, so the decision the handler takes on that request is the one that
		// answers the caller. Casing is not part of the resource, so the lowercase form is
		// refused on the same grounds as the uppercase one.
		for _, refused := range []string{
			fmt.Sprintf("SELECT * FROM MEASURE %s IN %s", fixtureMeasure, groupBeta),
			fmt.Sprintf("SELECT * FROM MEASURE %s IN %s, %s", fixtureMeasure, groupAlpha, groupBeta),
			fmt.Sprintf("select * from measure %s in %s", fixtureMeasure, groupBeta),
		} {
			resp, refusedErr := bydbql.Query(readerAlphaActor.ctx(), &bydbqlv1.QueryRequest{Query: refused})
			gm.Expect(status.Code(refusedErr)).To(gm.Equal(codes.PermissionDenied),
				"%q must be refused, got response %v", refused, resp)
			gm.Expect(resp.String()).NotTo(gm.ContainSubstring(markerBeta), "a refused ByDBQL query must leak no marker")
		}

		// A query whose text ByDBQL cannot parse is a separate matter: BanyanDB's grammar has
		// no comment syntax, and #13994 adds none, so the requirement this case carries is not
		// which code comes back but that no text-level trick gets a forbidden group served.
		// Appending the permitted group in what a SQL dialect would treat as a trailing
		// comment must not turn a beta query into an answer.
		commented := fmt.Sprintf("SELECT * FROM MEASURE %s IN %s -- IN %s", fixtureMeasure, groupBeta, groupAlpha)
		commentedResp, commentedErr := bydbql.Query(readerAlphaActor.ctx(), &bydbqlv1.QueryRequest{Query: commented})
		gm.Expect(commentedErr).To(gm.HaveOccurred(), "%q must not be served, got response %v", commented, commentedResp)
		gm.Expect(commentedResp.String()).NotTo(gm.ContainSubstring(markerBeta), "a refused ByDBQL query must leak no marker")

		_, monitorErr := bydbql.Query(monitorActor.ctx(), &bydbqlv1.QueryRequest{Query: allowed})
		gm.Expect(status.Code(monitorErr)).To(gm.Equal(codes.PermissionDenied),
			"a cluster monitor holds no data:read grant and must be refused")

		// The gateway route reaches the same handler and therefore the same decision.
		allowedStatus, allowedPayload := httpCall(httpAddr, http.MethodPost, "/api/v1/bydbql/query",
			readerAlphaActor, fmt.Sprintf(`{"query":%q}`, allowed))
		gm.Expect(allowedStatus).NotTo(gm.Equal(http.StatusForbidden), "the alpha reader's HTTP ByDBQL query returned %s", allowedPayload)

		forbiddenStatus, forbiddenPayload := httpCall(httpAddr, http.MethodPost, "/api/v1/bydbql/query",
			readerAlphaActor, fmt.Sprintf(`{"query":%q}`, fmt.Sprintf("SELECT * FROM MEASURE %s IN %s", fixtureMeasure, groupBeta)))
		gm.Expect(forbiddenStatus).To(gm.Equal(http.StatusForbidden), "the alpha reader's beta HTTP ByDBQL query returned %s", forbiddenPayload)
		gm.Expect(forbiddenPayload).NotTo(gm.ContainSubstring(markerBeta))
	})
})
