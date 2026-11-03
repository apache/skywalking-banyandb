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
	"github.com/onsi/gomega"
	"google.golang.org/grpc"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	propertyv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/property/v1"
)

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
