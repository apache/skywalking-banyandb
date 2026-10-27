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

package transfer

import (
	"context"
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	casesmeasuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	casespropertydata "github.com/apache/skywalking-banyandb/test/cases/property/data"
	casesstreamdata "github.com/apache/skywalking-banyandb/test/cases/stream/data"
	casestracedata "github.com/apache/skywalking-banyandb/test/cases/trace/data"
)

// WriteInterval spaces the points WriteSuiteData writes.
const WriteInterval = 500 * time.Millisecond

// WriteSuiteData writes MeasureRows points into MeasureName and TopNSourcePoints into
// TopNSourceMeasure, the source measure of a Top-N aggregation of MeasureGroup.
const (
	MeasureRows       = 6
	TopNSourceMeasure = "service_instance_cpm_minute"
	TopNSourcePoints  = 6
)

// WriteSuiteData writes the data the export and import suites share (test/cases/<catalog>/data):
// the stream, the measure, the index-mode measure, the Top-N source measure, the trace, the
// properties and the tombstone. It returns the base times the data was written with.
func WriteSuiteData(conn *grpc.ClientConn) SuiteData {
	d := SuiteData{Interval: WriteInterval}
	d.StreamBase = timestamp.NowMilli()
	casesstreamdata.Write(conn, StreamName, d.StreamBase, WriteInterval)
	ns := timestamp.NowMilli().UnixNano()
	d.MeasureBase = time.Unix(0, ns-ns%int64(time.Minute))
	casesmeasuredata.Write(conn, MeasureName, MeasureGroup, MeasureFile, d.MeasureBase, WriteInterval)
	casesmeasuredata.Write(conn, IndexModeName, IndexModeGroup, IndexModeFile, d.MeasureBase, WriteInterval)
	// A minute apart, the points of a Top-N aggregation's source measure span several windows,
	// so the Top-N processor closes all but the last and writes their _top_n_result rows.
	casesmeasuredata.Write(conn, TopNSourceMeasure, MeasureGroup, TopNSourceMeasure+"_data.json", d.MeasureBase, time.Minute)
	d.TraceBase = timestamp.NowMilli()
	casestracedata.Write(conn, TraceName, d.TraceBase, WriteInterval)
	casespropertydata.Write(conn, "sw1")
	WriteTombstone(conn)
	return d
}

// CurrentNodeName asks the data node at addr for the node id the liaison registers it under.
func CurrentNodeName(addr string) string {
	c, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	defer func() { _ = c.Close() }()
	cur, err := databasev1.NewNodeQueryServiceClient(c).GetCurrentNode(context.Background(), &databasev1.GetCurrentNodeRequest{})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	return cur.GetNode().GetMetadata().GetName()
}

// TombstoneProperty is the ui_menu property of group "sw" that WriteTombstone applies and
// deletes, so the property catalog holds a deleted document.
func TombstoneProperty() *propertyv1.Property {
	return &propertyv1.Property{
		Metadata: &commonv1.Metadata{Group: "sw", Name: "ui_menu"},
		Id:       "export-tombstone",
		Tags: []*modelv1.Tag{
			{Key: "menu_name", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "deleted"}}}},
			{Key: "configuration", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "gone"}}}},
			{Key: "update_time", Value: &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: 7}}}},
		},
	}
}

// WriteTombstone applies TombstoneProperty and deletes it again.
func WriteTombstone(conn *grpc.ClientConn) {
	ctx, cancel := callCtx()
	defer cancel()
	c := propertyv1.NewPropertyServiceClient(conn)
	p := TombstoneProperty()
	_, err := c.Apply(ctx, &propertyv1.ApplyRequest{Property: p, Strategy: propertyv1.ApplyRequest_STRATEGY_REPLACE})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	resp, err := c.Delete(ctx, &propertyv1.DeleteRequest{Group: p.GetMetadata().GetGroup(), Name: p.GetMetadata().GetName(), Id: p.GetId()})
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	gomega.ExpectWithOffset(1, resp.GetDeleted()).To(gomega.BeTrue())
}
