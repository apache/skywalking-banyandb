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

package schema

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	g "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

const (
	tagFamilyMoveSearchableFamily  = "searchable"
	tagFamilyMoveStorageOnlyFamily = "storage-only"
	tagFamilyMoveEntityTag         = "svc"
	tagFamilyMoveMovedTag          = "host"
	tagFamilyMoveStorageTag        = "region"
)

func tagFamilyMoveStreamGroup(name string) *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: name},
		Catalog:  commonv1.Catalog_CATALOG_STREAM,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        2,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}
}

func tagFamilyMoveInitialFamilies() []*databasev1.TagFamilySpec {
	return []*databasev1.TagFamilySpec{
		{
			Name: tagFamilyMoveSearchableFamily,
			Tags: []*databasev1.TagSpec{
				{Name: tagFamilyMoveEntityTag, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: tagFamilyMoveMovedTag, Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
		{
			Name: tagFamilyMoveStorageOnlyFamily,
			Tags: []*databasev1.TagSpec{
				{Name: tagFamilyMoveStorageTag, Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
	}
}

func tagFamilyMoveUpdatedFamilies() []*databasev1.TagFamilySpec {
	return []*databasev1.TagFamilySpec{
		{
			Name: tagFamilyMoveSearchableFamily,
			Tags: []*databasev1.TagSpec{
				{Name: tagFamilyMoveEntityTag, Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
		{
			Name: tagFamilyMoveStorageOnlyFamily,
			Tags: []*databasev1.TagSpec{
				{Name: tagFamilyMoveStorageTag, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: tagFamilyMoveMovedTag, Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
	}
}

func tagFamilyMoveMeasureGroup(name string) *commonv1.Group {
	return &commonv1.Group{
		Metadata: &commonv1.Metadata{Name: name},
		Catalog:  commonv1.Catalog_CATALOG_MEASURE,
		ResourceOpts: &commonv1.ResourceOpts{
			ShardNum:        2,
			SegmentInterval: &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 1},
			Ttl:             &commonv1.IntervalRule{Unit: commonv1.IntervalRule_UNIT_DAY, Num: 7},
		},
	}
}

func tagFamilyMoveMeasureSpec(groupName, measureName string) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata:    &commonv1.Metadata{Name: measureName, Group: groupName},
		Entity:      &databasev1.Entity{TagNames: []string{tagFamilyMoveEntityTag}},
		TagFamilies: tagFamilyMoveInitialFamilies(),
		Fields:      tagFamilyMoveMeasureFields(),
	}
}

func tagFamilyMoveStorageToSearchableMeasureSpec(groupName, measureName string) *databasev1.Measure {
	return &databasev1.Measure{
		Metadata:    &commonv1.Metadata{Name: measureName, Group: groupName},
		Entity:      &databasev1.Entity{TagNames: []string{tagFamilyMoveEntityTag}},
		TagFamilies: tagFamilyMoveStorageToSearchableInitialFamilies(),
		Fields:      tagFamilyMoveMeasureFields(),
	}
}

func tagFamilyMoveMeasureFields() []*databasev1.FieldSpec {
	return []*databasev1.FieldSpec{{
		Name:              "value",
		FieldType:         databasev1.FieldType_FIELD_TYPE_INT,
		EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
		CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
	}}
}

func tagFamilyMoveMeasureInitialWriteSpec() *measurev1.DataPointSpec {
	return &measurev1.DataPointSpec{
		TagFamilySpec: []*measurev1.TagFamilySpec{
			{Name: tagFamilyMoveSearchableFamily, TagNames: []string{tagFamilyMoveEntityTag, tagFamilyMoveMovedTag}},
			{Name: tagFamilyMoveStorageOnlyFamily, TagNames: []string{tagFamilyMoveStorageTag}},
		},
		FieldNames: []string{"value"},
	}
}

func tagFamilyMoveMeasureUpdatedWriteSpec() *measurev1.DataPointSpec {
	return &measurev1.DataPointSpec{
		TagFamilySpec: []*measurev1.TagFamilySpec{
			{Name: tagFamilyMoveSearchableFamily, TagNames: []string{tagFamilyMoveEntityTag}},
			{Name: tagFamilyMoveStorageOnlyFamily, TagNames: []string{tagFamilyMoveStorageTag, tagFamilyMoveMovedTag}},
		},
		FieldNames: []string{"value"},
	}
}

func tagFamilyMoveMeasureStorageToSearchableInitialWriteSpec() *measurev1.DataPointSpec {
	return &measurev1.DataPointSpec{
		TagFamilySpec: []*measurev1.TagFamilySpec{
			{Name: tagFamilyMoveSearchableFamily, TagNames: []string{tagFamilyMoveEntityTag}},
			{Name: tagFamilyMoveStorageOnlyFamily, TagNames: []string{tagFamilyMoveStorageTag, tagFamilyMoveMovedTag}},
		},
		FieldNames: []string{"value"},
	}
}

func tagFamilyMoveMeasureStorageToSearchableUpdatedWriteSpec() *measurev1.DataPointSpec {
	return &measurev1.DataPointSpec{
		TagFamilySpec: []*measurev1.TagFamilySpec{
			{Name: tagFamilyMoveSearchableFamily, TagNames: []string{tagFamilyMoveEntityTag, tagFamilyMoveMovedTag}},
			{Name: tagFamilyMoveStorageOnlyFamily, TagNames: []string{tagFamilyMoveStorageTag}},
		},
		FieldNames: []string{"value"},
	}
}

func tagFamilyMoveStreamSpec(groupName, streamName string) *databasev1.Stream {
	return &databasev1.Stream{
		Metadata:    &commonv1.Metadata{Name: streamName, Group: groupName},
		Entity:      &databasev1.Entity{TagNames: []string{tagFamilyMoveEntityTag}},
		TagFamilies: tagFamilyMoveInitialFamilies(),
	}
}

func tagFamilyMoveStorageToSearchableStreamSpec(groupName, streamName string) *databasev1.Stream {
	return &databasev1.Stream{
		Metadata:    &commonv1.Metadata{Name: streamName, Group: groupName},
		Entity:      &databasev1.Entity{TagNames: []string{tagFamilyMoveEntityTag}},
		TagFamilies: tagFamilyMoveStorageToSearchableInitialFamilies(),
	}
}

func tagFamilyMoveStorageToSearchableInitialFamilies() []*databasev1.TagFamilySpec {
	return []*databasev1.TagFamilySpec{
		{
			Name: tagFamilyMoveSearchableFamily,
			Tags: []*databasev1.TagSpec{
				{Name: tagFamilyMoveEntityTag, Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
		{
			Name: tagFamilyMoveStorageOnlyFamily,
			Tags: []*databasev1.TagSpec{
				{Name: tagFamilyMoveStorageTag, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: tagFamilyMoveMovedTag, Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
	}
}

func tagFamilyMoveStorageToSearchableUpdatedFamilies() []*databasev1.TagFamilySpec {
	return []*databasev1.TagFamilySpec{
		{
			Name: tagFamilyMoveSearchableFamily,
			Tags: []*databasev1.TagSpec{
				{Name: tagFamilyMoveEntityTag, Type: databasev1.TagType_TAG_TYPE_STRING},
				{Name: tagFamilyMoveMovedTag, Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
		{
			Name: tagFamilyMoveStorageOnlyFamily,
			Tags: []*databasev1.TagSpec{
				{Name: tagFamilyMoveStorageTag, Type: databasev1.TagType_TAG_TYPE_STRING},
			},
		},
	}
}

func tagFamilyMoveInitialWriteSpec() []*streamv1.TagFamilySpec {
	return []*streamv1.TagFamilySpec{
		{Name: tagFamilyMoveSearchableFamily, TagNames: []string{tagFamilyMoveEntityTag, tagFamilyMoveMovedTag}},
		{Name: tagFamilyMoveStorageOnlyFamily, TagNames: []string{tagFamilyMoveStorageTag}},
	}
}

func tagFamilyMoveUpdatedWriteSpec() []*streamv1.TagFamilySpec {
	return []*streamv1.TagFamilySpec{
		{Name: tagFamilyMoveSearchableFamily, TagNames: []string{tagFamilyMoveEntityTag}},
		{Name: tagFamilyMoveStorageOnlyFamily, TagNames: []string{tagFamilyMoveStorageTag, tagFamilyMoveMovedTag}},
	}
}

func tagFamilyMoveStorageToSearchableInitialWriteSpec() []*streamv1.TagFamilySpec {
	return []*streamv1.TagFamilySpec{
		{Name: tagFamilyMoveSearchableFamily, TagNames: []string{tagFamilyMoveEntityTag}},
		{Name: tagFamilyMoveStorageOnlyFamily, TagNames: []string{tagFamilyMoveStorageTag, tagFamilyMoveMovedTag}},
	}
}

func tagFamilyMoveStorageToSearchableUpdatedWriteSpec() []*streamv1.TagFamilySpec {
	return []*streamv1.TagFamilySpec{
		{Name: tagFamilyMoveSearchableFamily, TagNames: []string{tagFamilyMoveEntityTag, tagFamilyMoveMovedTag}},
		{Name: tagFamilyMoveStorageOnlyFamily, TagNames: []string{tagFamilyMoveStorageTag}},
	}
}

func tagFamilyMoveInitialLayout() map[string][]string {
	return map[string][]string{
		tagFamilyMoveSearchableFamily:  {tagFamilyMoveEntityTag, tagFamilyMoveMovedTag},
		tagFamilyMoveStorageOnlyFamily: {tagFamilyMoveStorageTag},
	}
}

func tagFamilyMoveUpdatedLayout() map[string][]string {
	return map[string][]string{
		tagFamilyMoveSearchableFamily:  {tagFamilyMoveEntityTag},
		tagFamilyMoveStorageOnlyFamily: {tagFamilyMoveStorageTag, tagFamilyMoveMovedTag},
	}
}

func tagFamilyMoveStorageToSearchableInitialLayout() map[string][]string {
	return map[string][]string{
		tagFamilyMoveSearchableFamily:  {tagFamilyMoveEntityTag},
		tagFamilyMoveStorageOnlyFamily: {tagFamilyMoveStorageTag, tagFamilyMoveMovedTag},
	}
}

func tagFamilyMoveStorageToSearchableUpdatedLayout() map[string][]string {
	return map[string][]string{
		tagFamilyMoveSearchableFamily:  {tagFamilyMoveEntityTag, tagFamilyMoveMovedTag},
		tagFamilyMoveStorageOnlyFamily: {tagFamilyMoveStorageTag},
	}
}

func expectTagFamilyLayout(tagFamilies []*databasev1.TagFamilySpec, expectedTagsByFamily map[string][]string) {
	gm.Expect(tagFamilies).Should(gm.HaveLen(len(expectedTagsByFamily)), "unexpected tag family count")
	actualTagsByFamily := make(map[string][]string, len(tagFamilies))
	for _, tagFamily := range tagFamilies {
		actualTagNames := make([]string, 0, len(tagFamily.GetTags()))
		for _, tag := range tagFamily.GetTags() {
			actualTagNames = append(actualTagNames, tag.GetName())
		}
		actualTagsByFamily[tagFamily.GetName()] = actualTagNames
	}
	gm.Expect(actualTagsByFamily).Should(gm.Equal(expectedTagsByFamily), "unexpected tag family layout")
}

func tagFamilyMoveTags(svc, host, region string, moved bool) []*modelv1.TagFamilyForWrite {
	if moved {
		return []*modelv1.TagFamilyForWrite{
			{Tags: []*modelv1.TagValue{tagFamilyMoveStringTag(svc)}},
			{Tags: []*modelv1.TagValue{tagFamilyMoveStringTag(region), tagFamilyMoveStringTag(host)}},
		}
	}
	return []*modelv1.TagFamilyForWrite{
		{Tags: []*modelv1.TagValue{tagFamilyMoveStringTag(svc), tagFamilyMoveStringTag(host)}},
		{Tags: []*modelv1.TagValue{tagFamilyMoveStringTag(region)}},
	}
}

func tagFamilyMoveStorageToSearchableTags(svc, host, region string, moved bool) []*modelv1.TagFamilyForWrite {
	if moved {
		return []*modelv1.TagFamilyForWrite{
			{Tags: []*modelv1.TagValue{tagFamilyMoveStringTag(svc), tagFamilyMoveStringTag(host)}},
			{Tags: []*modelv1.TagValue{tagFamilyMoveStringTag(region)}},
		}
	}
	return []*modelv1.TagFamilyForWrite{
		{Tags: []*modelv1.TagValue{tagFamilyMoveStringTag(svc)}},
		{Tags: []*modelv1.TagValue{tagFamilyMoveStringTag(region), tagFamilyMoveStringTag(host)}},
	}
}

func tagFamilyMoveStringTag(value string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}}
}

func tagFamilyMoveStringValue(tag *modelv1.TagValue) string {
	if tag == nil || tag.GetStr() == nil {
		return ""
	}
	return tag.GetStr().GetValue()
}

func tagFamilyMoveNull(tag *modelv1.TagValue) bool {
	if tag == nil {
		return true
	}
	_, ok := tag.GetValue().(*modelv1.TagValue_Null)
	return ok
}

func createTagFamilyMoveStreamIndexes(ctx context.Context, clients *Clients, groupName, streamName string) int64 {
	svcRuleName := streamName + "_svc_idx"
	hostRuleName := streamName + "_host_idx"
	_, createSvcRuleErr := clients.IndexRuleClient.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{
		IndexRule: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: svcRuleName, Group: groupName},
			Tags:     []string{tagFamilyMoveEntityTag},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
		},
	})
	gm.Expect(createSvcRuleErr).ShouldNot(gm.HaveOccurred())
	_, createHostRuleErr := clients.IndexRuleClient.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{
		IndexRule: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: hostRuleName, Group: groupName},
			Tags:     []string{tagFamilyMoveMovedTag},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
		},
	})
	gm.Expect(createHostRuleErr).ShouldNot(gm.HaveOccurred())
	bindingResp, createBindingErr := clients.IndexRuleBindingClient.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
		IndexRuleBinding: tagFamilyMoveIndexRuleBinding(groupName, streamName, []string{svcRuleName, hostRuleName}),
	})
	gm.Expect(createBindingErr).ShouldNot(gm.HaveOccurred())
	return bindingResp.GetModRevision()
}

func createTagFamilyMoveSvcIndex(ctx context.Context, clients *Clients, groupName, streamName string) int64 {
	svcRuleName := streamName + "_svc_idx"
	_, createSvcRuleErr := clients.IndexRuleClient.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{
		IndexRule: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: svcRuleName, Group: groupName},
			Tags:     []string{tagFamilyMoveEntityTag},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
		},
	})
	gm.Expect(createSvcRuleErr).ShouldNot(gm.HaveOccurred())
	bindingResp, createBindingErr := clients.IndexRuleBindingClient.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
		IndexRuleBinding: tagFamilyMoveIndexRuleBinding(groupName, streamName, []string{svcRuleName}),
	})
	gm.Expect(createBindingErr).ShouldNot(gm.HaveOccurred())
	return bindingResp.GetModRevision()
}

func tagFamilyMoveIndexRuleBinding(groupName, streamName string, rules []string) *databasev1.IndexRuleBinding {
	return &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: streamName + "_binding", Group: groupName},
		Rules:    rules,
		Subject: &databasev1.Subject{
			Catalog: commonv1.Catalog_CATALOG_STREAM,
			Name:    streamName,
		},
		BeginAt:  timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
		ExpireAt: timestamppb.New(time.Date(2121, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
}

func moveTagFamilyMoveStreamIndexes(ctx context.Context, clients *Clients, groupName, streamName string) int64 {
	svcRuleName := streamName + "_svc_idx"
	updateResp, updateErr := clients.IndexRuleBindingClient.Update(ctx, &databasev1.IndexRuleBindingRegistryServiceUpdateRequest{
		IndexRuleBinding: tagFamilyMoveIndexRuleBinding(groupName, streamName, []string{svcRuleName}),
	})
	gm.Expect(updateErr).ShouldNot(gm.HaveOccurred())
	return updateResp.GetModRevision()
}

func moveTagFamilyMoveStorageToSearchableIndexes(ctx context.Context, clients *Clients, groupName, streamName string) int64 {
	svcRuleName := streamName + "_svc_idx"
	hostRuleName := streamName + "_host_idx"
	_, createHostRuleErr := clients.IndexRuleClient.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{
		IndexRule: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: hostRuleName, Group: groupName},
			Tags:     []string{tagFamilyMoveMovedTag},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
		},
	})
	gm.Expect(createHostRuleErr).ShouldNot(gm.HaveOccurred())
	updateResp, updateErr := clients.IndexRuleBindingClient.Update(ctx, &databasev1.IndexRuleBindingRegistryServiceUpdateRequest{
		IndexRuleBinding: tagFamilyMoveIndexRuleBinding(groupName, streamName, []string{svcRuleName, hostRuleName}),
	})
	gm.Expect(updateErr).ShouldNot(gm.HaveOccurred())
	return updateResp.GetModRevision()
}

func createTagFamilyMoveMeasureIndexes(ctx context.Context, clients *Clients, groupName, measureName string) int64 {
	svcRuleName := measureName + "_svc_idx"
	hostRuleName := measureName + "_host_idx"
	_, createSvcRuleErr := clients.IndexRuleClient.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{
		IndexRule: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: svcRuleName, Group: groupName},
			Tags:     []string{tagFamilyMoveEntityTag},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
		},
	})
	gm.Expect(createSvcRuleErr).ShouldNot(gm.HaveOccurred())
	_, createHostRuleErr := clients.IndexRuleClient.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{
		IndexRule: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: hostRuleName, Group: groupName},
			Tags:     []string{tagFamilyMoveMovedTag},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
		},
	})
	gm.Expect(createHostRuleErr).ShouldNot(gm.HaveOccurred())
	bindingResp, createBindingErr := clients.IndexRuleBindingClient.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
		IndexRuleBinding: tagFamilyMoveMeasureIndexRuleBinding(groupName, measureName, []string{svcRuleName, hostRuleName}),
	})
	gm.Expect(createBindingErr).ShouldNot(gm.HaveOccurred())
	return bindingResp.GetModRevision()
}

func createTagFamilyMoveMeasureSvcIndex(ctx context.Context, clients *Clients, groupName, measureName string) int64 {
	svcRuleName := measureName + "_svc_idx"
	_, createSvcRuleErr := clients.IndexRuleClient.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{
		IndexRule: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: svcRuleName, Group: groupName},
			Tags:     []string{tagFamilyMoveEntityTag},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
		},
	})
	gm.Expect(createSvcRuleErr).ShouldNot(gm.HaveOccurred())
	bindingResp, createBindingErr := clients.IndexRuleBindingClient.Create(ctx, &databasev1.IndexRuleBindingRegistryServiceCreateRequest{
		IndexRuleBinding: tagFamilyMoveMeasureIndexRuleBinding(groupName, measureName, []string{svcRuleName}),
	})
	gm.Expect(createBindingErr).ShouldNot(gm.HaveOccurred())
	return bindingResp.GetModRevision()
}

func tagFamilyMoveMeasureIndexRuleBinding(groupName, measureName string, rules []string) *databasev1.IndexRuleBinding {
	return &databasev1.IndexRuleBinding{
		Metadata: &commonv1.Metadata{Name: measureName + "_binding", Group: groupName},
		Rules:    rules,
		Subject: &databasev1.Subject{
			Catalog: commonv1.Catalog_CATALOG_MEASURE,
			Name:    measureName,
		},
		BeginAt:  timestamppb.New(time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)),
		ExpireAt: timestamppb.New(time.Date(2121, 1, 1, 0, 0, 0, 0, time.UTC)),
	}
}

func moveTagFamilyMoveMeasureIndexes(ctx context.Context, clients *Clients, groupName, measureName string) int64 {
	svcRuleName := measureName + "_svc_idx"
	updateResp, updateErr := clients.IndexRuleBindingClient.Update(ctx, &databasev1.IndexRuleBindingRegistryServiceUpdateRequest{
		IndexRuleBinding: tagFamilyMoveMeasureIndexRuleBinding(groupName, measureName, []string{svcRuleName}),
	})
	gm.Expect(updateErr).ShouldNot(gm.HaveOccurred())
	return updateResp.GetModRevision()
}

func moveTagFamilyMoveMeasureStorageToSearchableIndexes(ctx context.Context, clients *Clients, groupName, measureName string) int64 {
	svcRuleName := measureName + "_svc_idx"
	hostRuleName := measureName + "_host_idx"
	_, createHostRuleErr := clients.IndexRuleClient.Create(ctx, &databasev1.IndexRuleRegistryServiceCreateRequest{
		IndexRule: &databasev1.IndexRule{
			Metadata: &commonv1.Metadata{Name: hostRuleName, Group: groupName},
			Tags:     []string{tagFamilyMoveMovedTag},
			Type:     databasev1.IndexRule_TYPE_INVERTED,
		},
	})
	gm.Expect(createHostRuleErr).ShouldNot(gm.HaveOccurred())
	updateResp, updateErr := clients.IndexRuleBindingClient.Update(ctx, &databasev1.IndexRuleBindingRegistryServiceUpdateRequest{
		IndexRuleBinding: tagFamilyMoveMeasureIndexRuleBinding(groupName, measureName, []string{svcRuleName, hostRuleName}),
	})
	gm.Expect(updateErr).ShouldNot(gm.HaveOccurred())
	return updateResp.GetModRevision()
}

func writeTagFamilyMoveStream(
	ctx context.Context,
	client streamv1.StreamServiceClient,
	groupName, streamName, elementID string,
	ts time.Time,
	tagFamilies []*modelv1.TagFamilyForWrite,
	spec []*streamv1.TagFamilySpec,
) string {
	writeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	writeClient, dialErr := client.Write(writeCtx)
	gm.Expect(dialErr).ShouldNot(gm.HaveOccurred())
	sendErr := writeClient.Send(&streamv1.WriteRequest{
		Metadata: &commonv1.Metadata{Name: streamName, Group: groupName},
		Element: &streamv1.ElementValue{
			ElementId:   elementID,
			Timestamp:   timestamppb.New(ts.Truncate(time.Millisecond)),
			TagFamilies: tagFamilies,
		},
		MessageId:     uint64(time.Now().UnixNano()),
		TagFamilySpec: spec,
	})
	gm.Expect(sendErr).ShouldNot(gm.HaveOccurred())
	closeErr := writeClient.CloseSend()
	gm.Expect(closeErr).ShouldNot(gm.HaveOccurred())
	var firstStatus string
	for {
		resp, recvErr := writeClient.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		gm.Expect(recvErr).ShouldNot(gm.HaveOccurred())
		if resp != nil && firstStatus == "" {
			firstStatus = resp.GetStatus()
		}
	}
	return firstStatus
}

func queryTagFamilyMoveStream(
	ctx context.Context,
	client streamv1.StreamServiceClient,
	groupName string,
	begin, end time.Time,
	movedProjection bool,
	expectedElements int,
	criteria *modelv1.Criteria,
) []*streamv1.Element {
	projection := &modelv1.TagProjection{
		TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: tagFamilyMoveSearchableFamily, Tags: []string{tagFamilyMoveEntityTag}},
			{Name: tagFamilyMoveStorageOnlyFamily, Tags: []string{tagFamilyMoveStorageTag, tagFamilyMoveMovedTag}},
		},
	}
	if !movedProjection {
		projection = &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: tagFamilyMoveSearchableFamily, Tags: []string{tagFamilyMoveEntityTag, tagFamilyMoveMovedTag}},
				{Name: tagFamilyMoveStorageOnlyFamily, Tags: []string{tagFamilyMoveStorageTag}},
			},
		}
	}
	var resp *streamv1.QueryResponse
	test.EventuallyConsistently(func() int {
		queryResp, queryErr := client.Query(ctx, &streamv1.QueryRequest{
			Groups: []string{groupName},
			Name:   "tfm_stream",
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(begin.Truncate(time.Millisecond)),
				End:   timestamppb.New(end.Truncate(time.Millisecond)),
			},
			Limit:      10,
			Criteria:   criteria,
			Projection: projection,
		})
		if queryErr != nil {
			return -1
		}
		resp = queryResp
		return len(queryResp.GetElements())
	}, 5*time.Second, 50*time.Millisecond).Should(gm.Equal(expectedElements))
	return resp.GetElements()
}

func queryStorageToSearchableMoveStream(
	ctx context.Context,
	client streamv1.StreamServiceClient,
	groupName string,
	begin, end time.Time,
	movedProjection bool,
	expectedElements int,
	criteria *modelv1.Criteria,
) []*streamv1.Element {
	projection := &modelv1.TagProjection{
		TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: tagFamilyMoveSearchableFamily, Tags: []string{tagFamilyMoveEntityTag, tagFamilyMoveMovedTag}},
			{Name: tagFamilyMoveStorageOnlyFamily, Tags: []string{tagFamilyMoveStorageTag}},
		},
	}
	if !movedProjection {
		projection = &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: tagFamilyMoveSearchableFamily, Tags: []string{tagFamilyMoveEntityTag}},
				{Name: tagFamilyMoveStorageOnlyFamily, Tags: []string{tagFamilyMoveStorageTag, tagFamilyMoveMovedTag}},
			},
		}
	}
	var resp *streamv1.QueryResponse
	test.EventuallyConsistently(func() int {
		queryResp, queryErr := client.Query(ctx, &streamv1.QueryRequest{
			Groups: []string{groupName},
			Name:   "tfm_storage_stream",
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(begin.Truncate(time.Millisecond)),
				End:   timestamppb.New(end.Truncate(time.Millisecond)),
			},
			Limit:      10,
			Criteria:   criteria,
			Projection: projection,
		})
		if queryErr != nil {
			return -1
		}
		resp = queryResp
		return len(queryResp.GetElements())
	}, 5*time.Second, 50*time.Millisecond).Should(gm.Equal(expectedElements))
	return resp.GetElements()
}

func writeTagFamilyMoveMeasure(
	ctx context.Context,
	client measurev1.MeasureServiceClient,
	groupName, measureName string,
	ts time.Time,
	tagFamilies []*modelv1.TagFamilyForWrite,
	spec *measurev1.DataPointSpec,
	fieldValue int64,
) string {
	writeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	writeClient, dialErr := client.Write(writeCtx)
	gm.Expect(dialErr).ShouldNot(gm.HaveOccurred())
	sendErr := writeClient.Send(&measurev1.WriteRequest{
		Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		DataPoint: &measurev1.DataPointValue{
			Timestamp:   timestamppb.New(ts.Truncate(time.Millisecond)),
			TagFamilies: tagFamilies,
			Fields: []*modelv1.FieldValue{{
				Value: &modelv1.FieldValue_Int{Int: &modelv1.Int{Value: fieldValue}},
			}},
		},
		MessageId:     uint64(time.Now().UnixNano()),
		DataPointSpec: spec,
	})
	gm.Expect(sendErr).ShouldNot(gm.HaveOccurred())
	closeErr := writeClient.CloseSend()
	gm.Expect(closeErr).ShouldNot(gm.HaveOccurred())
	var firstStatus string
	for {
		resp, recvErr := writeClient.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		gm.Expect(recvErr).ShouldNot(gm.HaveOccurred())
		if resp != nil && firstStatus == "" {
			firstStatus = resp.GetStatus()
		}
	}
	return firstStatus
}

func queryTagFamilyMoveMeasure(
	ctx context.Context,
	client measurev1.MeasureServiceClient,
	groupName string,
	begin, end time.Time,
	movedProjection bool,
	expectedDataPoints int,
	criteria *modelv1.Criteria,
) []*measurev1.DataPoint {
	projection := tagFamilyMoveMeasureProjection(movedProjection)
	var resp *measurev1.QueryResponse
	test.EventuallyConsistently(func() int {
		queryResp, queryErr := client.Query(ctx, &measurev1.QueryRequest{
			Groups: []string{groupName},
			Name:   "tfm_measure",
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(begin.Truncate(time.Millisecond)),
				End:   timestamppb.New(end.Truncate(time.Millisecond)),
			},
			Limit:         10,
			Criteria:      criteria,
			TagProjection: projection,
			FieldProjection: &measurev1.QueryRequest_FieldProjection{
				Names: []string{"value"},
			},
		})
		if queryErr != nil {
			return -1
		}
		resp = queryResp
		return len(queryResp.GetDataPoints())
	}, 5*time.Second, 50*time.Millisecond).Should(gm.Equal(expectedDataPoints))
	return resp.GetDataPoints()
}

func queryStorageToSearchableMoveMeasure(
	ctx context.Context,
	client measurev1.MeasureServiceClient,
	groupName string,
	begin, end time.Time,
	movedProjection bool,
	expectedDataPoints int,
	criteria *modelv1.Criteria,
) []*measurev1.DataPoint {
	projection := tagFamilyMoveMeasureStorageToSearchableProjection(movedProjection)
	var resp *measurev1.QueryResponse
	test.EventuallyConsistently(func() int {
		queryResp, queryErr := client.Query(ctx, &measurev1.QueryRequest{
			Groups: []string{groupName},
			Name:   "tfm_storage_measure",
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(begin.Truncate(time.Millisecond)),
				End:   timestamppb.New(end.Truncate(time.Millisecond)),
			},
			Limit:         10,
			Criteria:      criteria,
			TagProjection: projection,
			FieldProjection: &measurev1.QueryRequest_FieldProjection{
				Names: []string{"value"},
			},
		})
		if queryErr != nil {
			return -1
		}
		resp = queryResp
		return len(queryResp.GetDataPoints())
	}, 5*time.Second, 50*time.Millisecond).Should(gm.Equal(expectedDataPoints))
	return resp.GetDataPoints()
}

func tagFamilyMoveMeasureProjection(movedProjection bool) *modelv1.TagProjection {
	if movedProjection {
		return &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: tagFamilyMoveSearchableFamily, Tags: []string{tagFamilyMoveEntityTag}},
				{Name: tagFamilyMoveStorageOnlyFamily, Tags: []string{tagFamilyMoveStorageTag, tagFamilyMoveMovedTag}},
			},
		}
	}
	return &modelv1.TagProjection{
		TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: tagFamilyMoveSearchableFamily, Tags: []string{tagFamilyMoveEntityTag, tagFamilyMoveMovedTag}},
			{Name: tagFamilyMoveStorageOnlyFamily, Tags: []string{tagFamilyMoveStorageTag}},
		},
	}
}

func tagFamilyMoveMeasureStorageToSearchableProjection(movedProjection bool) *modelv1.TagProjection {
	if movedProjection {
		return &modelv1.TagProjection{
			TagFamilies: []*modelv1.TagProjection_TagFamily{
				{Name: tagFamilyMoveSearchableFamily, Tags: []string{tagFamilyMoveEntityTag, tagFamilyMoveMovedTag}},
				{Name: tagFamilyMoveStorageOnlyFamily, Tags: []string{tagFamilyMoveStorageTag}},
			},
		}
	}
	return &modelv1.TagProjection{
		TagFamilies: []*modelv1.TagProjection_TagFamily{
			{Name: tagFamilyMoveSearchableFamily, Tags: []string{tagFamilyMoveEntityTag}},
			{Name: tagFamilyMoveStorageOnlyFamily, Tags: []string{tagFamilyMoveStorageTag, tagFamilyMoveMovedTag}},
		},
	}
}

func expectMeasureHostCriteriaError(
	ctx context.Context,
	client measurev1.MeasureServiceClient,
	groupName, measureName, host string,
	begin, end time.Time,
	projection *modelv1.TagProjection,
) {
	_, queryErr := client.Query(ctx, &measurev1.QueryRequest{
		Groups: []string{groupName},
		Name:   measureName,
		TimeRange: &modelv1.TimeRange{
			Begin: timestamppb.New(begin.Truncate(time.Millisecond)),
			End:   timestamppb.New(end.Truncate(time.Millisecond)),
		},
		Limit:         10,
		Criteria:      tagFamilyMoveHostCriteria(host),
		TagProjection: projection,
		FieldProjection: &measurev1.QueryRequest_FieldProjection{
			Names: []string{"value"},
		},
	})
	gm.Expect(queryErr).Should(gm.HaveOccurred(), "measure criteria on host should fail while host has no index rule binding")
}

func expectMeasureMovedTagValue(dataPoints []*measurev1.DataPoint, svc, expectedHost string, expectNull bool) {
	for _, dataPoint := range dataPoints {
		if tagFamilyMoveStringValue(findProjectedTag(dataPoint.GetTagFamilies(), tagFamilyMoveSearchableFamily, tagFamilyMoveEntityTag)) != svc {
			continue
		}
		movedTag := findProjectedTag(dataPoint.GetTagFamilies(), tagFamilyMoveStorageOnlyFamily, tagFamilyMoveMovedTag)
		if expectNull {
			gm.Expect(tagFamilyMoveNull(movedTag)).Should(gm.BeTrue(), "pre-move measure data should be queryable but moved tag should be null")
			return
		}
		gm.Expect(tagFamilyMoveStringValue(movedTag)).Should(gm.Equal(expectedHost))
		return
	}
	g.Fail(fmt.Sprintf("measure row with svc %q not found", svc))
}

func expectMeasureIndexedMovedTagValue(dataPoints []*measurev1.DataPoint, svc, expectedHost string) {
	for _, dataPoint := range dataPoints {
		if tagFamilyMoveStringValue(findProjectedTag(dataPoint.GetTagFamilies(), tagFamilyMoveSearchableFamily, tagFamilyMoveEntityTag)) != svc {
			continue
		}
		movedTag := findProjectedTag(dataPoint.GetTagFamilies(), tagFamilyMoveSearchableFamily, tagFamilyMoveMovedTag)
		gm.Expect(tagFamilyMoveStringValue(movedTag)).Should(gm.Equal(expectedHost))
		return
	}
	g.Fail(fmt.Sprintf("measure row with svc %q not found", svc))
}

func expectMeasureSearchableMovedTagValue(dataPoints []*measurev1.DataPoint, svc, expectedHost string, expectNull bool) {
	for _, dataPoint := range dataPoints {
		if tagFamilyMoveStringValue(findProjectedTag(dataPoint.GetTagFamilies(), tagFamilyMoveSearchableFamily, tagFamilyMoveEntityTag)) != svc {
			continue
		}
		movedTag := findProjectedTag(dataPoint.GetTagFamilies(), tagFamilyMoveSearchableFamily, tagFamilyMoveMovedTag)
		if expectNull {
			gm.Expect(tagFamilyMoveNull(movedTag)).Should(gm.BeTrue(), "pre-move measure data should be queryable but moved tag should be null")
			return
		}
		gm.Expect(tagFamilyMoveStringValue(movedTag)).Should(gm.Equal(expectedHost))
		return
	}
	g.Fail(fmt.Sprintf("measure row with svc %q not found", svc))
}

func expectMeasureStorageOnlyMovedTagValue(dataPoints []*measurev1.DataPoint, svc, expectedHost string) {
	for _, dataPoint := range dataPoints {
		if tagFamilyMoveStringValue(findProjectedTag(dataPoint.GetTagFamilies(), tagFamilyMoveSearchableFamily, tagFamilyMoveEntityTag)) != svc {
			continue
		}
		movedTag := findProjectedTag(dataPoint.GetTagFamilies(), tagFamilyMoveStorageOnlyFamily, tagFamilyMoveMovedTag)
		gm.Expect(tagFamilyMoveStringValue(movedTag)).Should(gm.Equal(expectedHost))
		return
	}
	g.Fail(fmt.Sprintf("measure row with svc %q not found", svc))
}

func tagFamilyMoveHostCriteria(host string) *modelv1.Criteria {
	return &modelv1.Criteria{
		Exp: &modelv1.Criteria_Condition{
			Condition: &modelv1.Condition{
				Name:  tagFamilyMoveMovedTag,
				Op:    modelv1.Condition_BINARY_OP_EQ,
				Value: tagFamilyMoveStringTag(host),
			},
		},
	}
}

func expectMovedTagValue(elements []*streamv1.Element, svc, expectedHost string, expectNull bool) {
	for _, element := range elements {
		if tagFamilyMoveStringValue(findProjectedTag(element.GetTagFamilies(), tagFamilyMoveSearchableFamily, tagFamilyMoveEntityTag)) != svc {
			continue
		}
		movedTag := findProjectedTag(element.GetTagFamilies(), tagFamilyMoveStorageOnlyFamily, tagFamilyMoveMovedTag)
		if expectNull {
			gm.Expect(tagFamilyMoveNull(movedTag)).Should(gm.BeTrue(), "pre-move data should be queryable but moved tag should be null")
			return
		}
		gm.Expect(tagFamilyMoveStringValue(movedTag)).Should(gm.Equal(expectedHost))
		return
	}
	g.Fail(fmt.Sprintf("stream row with svc %q not found", svc))
}

func expectIndexedMovedTagValue(elements []*streamv1.Element, svc, expectedHost string) {
	for _, element := range elements {
		if tagFamilyMoveStringValue(findProjectedTag(element.GetTagFamilies(), tagFamilyMoveSearchableFamily, tagFamilyMoveEntityTag)) != svc {
			continue
		}
		movedTag := findProjectedTag(element.GetTagFamilies(), tagFamilyMoveSearchableFamily, tagFamilyMoveMovedTag)
		gm.Expect(tagFamilyMoveStringValue(movedTag)).Should(gm.Equal(expectedHost))
		return
	}
	g.Fail(fmt.Sprintf("stream row with svc %q not found", svc))
}

func expectSearchableMovedTagValue(elements []*streamv1.Element, svc, expectedHost string, expectNull bool) {
	for _, element := range elements {
		if tagFamilyMoveStringValue(findProjectedTag(element.GetTagFamilies(), tagFamilyMoveSearchableFamily, tagFamilyMoveEntityTag)) != svc {
			continue
		}
		movedTag := findProjectedTag(element.GetTagFamilies(), tagFamilyMoveSearchableFamily, tagFamilyMoveMovedTag)
		if expectNull {
			gm.Expect(tagFamilyMoveNull(movedTag)).Should(gm.BeTrue(), "pre-move data should be queryable but moved tag should be null")
			return
		}
		gm.Expect(tagFamilyMoveStringValue(movedTag)).Should(gm.Equal(expectedHost))
		return
	}
	g.Fail(fmt.Sprintf("stream row with svc %q not found", svc))
}

func expectStorageOnlyMovedTagValue(elements []*streamv1.Element, svc, expectedHost string) {
	for _, element := range elements {
		if tagFamilyMoveStringValue(findProjectedTag(element.GetTagFamilies(), tagFamilyMoveSearchableFamily, tagFamilyMoveEntityTag)) != svc {
			continue
		}
		movedTag := findProjectedTag(element.GetTagFamilies(), tagFamilyMoveStorageOnlyFamily, tagFamilyMoveMovedTag)
		gm.Expect(tagFamilyMoveStringValue(movedTag)).Should(gm.Equal(expectedHost))
		return
	}
	g.Fail(fmt.Sprintf("stream row with svc %q not found", svc))
}

func findProjectedTag(tagFamilies []*modelv1.TagFamily, familyName, tagName string) *modelv1.TagValue {
	for _, tagFamily := range tagFamilies {
		if tagFamily.GetName() != familyName {
			continue
		}
		for _, tag := range tagFamily.GetTags() {
			if tag.GetKey() == tagName {
				return tag.GetValue()
			}
		}
	}
	return nil
}

// Schema tag-family move tests.
var _ = g.Describe("Schema tag family move", func() {
	var (
		ctx     context.Context
		clients *Clients
	)

	g.BeforeEach(func() {
		ctx = context.Background()
		clients = NewClients(SharedContext.Connection)
	})

	g.It("keeps stream writes and queries working when a tag moves from searchable to storage-only", func() {
		groupName := fmt.Sprintf("tfm-stream-%d", time.Now().UnixNano())
		streamName := "tfm_stream"
		begin := time.Now().Add(-time.Second)
		preMoveTime := begin.Add(100 * time.Millisecond)
		inMoveTime := begin.Add(200 * time.Millisecond)
		postMoveTime := begin.Add(300 * time.Millisecond)
		end := begin.Add(time.Hour)

		g.By("Creating stream group")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: tagFamilyMoveStreamGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())
		defer func() {
			_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
		}()

		g.By("Creating stream with searchable and storage-only tag families")
		createResp, createStreamErr := clients.StreamRegClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
			Stream: tagFamilyMoveStreamSpec(groupName, streamName),
		})
		gm.Expect(createStreamErr).ShouldNot(gm.HaveOccurred())
		baseRevision := createResp.GetModRevision()

		g.By("Binding inverted indexes for the searchable tags")
		indexRevision := createTagFamilyMoveStreamIndexes(ctx, clients, groupName, streamName)
		gm.Expect(clients.AwaitRevision(ctx, max(baseRevision, indexRevision), 10*time.Second)).Should(gm.Succeed())

		g.By("Writing and querying stream data before the tag is moved")
		writeStatus := writeTagFamilyMoveStream(ctx, clients.StreamWriteClient, groupName, streamName, "stream-pre", preMoveTime,
			tagFamilyMoveTags("svc-pre", "host-pre", "region-pre", false), tagFamilyMoveInitialWriteSpec())
		gm.Expect(writeStatus).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()))
		beforeElements := queryTagFamilyMoveStream(ctx, clients.StreamWriteClient, groupName, begin, end, false, 1, nil)
		expectIndexedMovedTagValue(beforeElements, "svc-pre", "host-pre")
		indexedElements := queryTagFamilyMoveStream(ctx, clients.StreamWriteClient, groupName, begin, end, false, 1, tagFamilyMoveHostCriteria("host-pre"))
		expectIndexedMovedTagValue(indexedElements, "svc-pre", "host-pre")

		g.By("Moving host from searchable to storage-only and removing it from the searchable index binding")
		getResp, getStreamErr := clients.StreamRegClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: streamName, Group: groupName},
		})
		gm.Expect(getStreamErr).ShouldNot(gm.HaveOccurred())
		updatedStream := getResp.GetStream()
		expectTagFamilyLayout(updatedStream.GetTagFamilies(), tagFamilyMoveInitialLayout())
		updatedStream.TagFamilies = tagFamilyMoveUpdatedFamilies()
		updateResp, updateStreamErr := clients.StreamRegClient.Update(ctx, &databasev1.StreamRegistryServiceUpdateRequest{
			Stream: updatedStream,
		})
		gm.Expect(updateStreamErr).ShouldNot(gm.HaveOccurred())
		indexMoveRevision := moveTagFamilyMoveStreamIndexes(ctx, clients, groupName, streamName)

		g.By("Continuing writes with the old layout while the move is applying")
		writeInMoveStatus := writeTagFamilyMoveStream(ctx, clients.StreamWriteClient, groupName, streamName, "stream-in-move", inMoveTime,
			tagFamilyMoveTags("svc-in-move", "host-in-move", "region-in-move", false), tagFamilyMoveInitialWriteSpec())
		gm.Expect(writeInMoveStatus).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()))

		g.By("Waiting for the moved stream tag schema and index binding to be applied")
		targetRevision := max(updateResp.GetModRevision(), indexMoveRevision)
		gm.Expect(clients.AwaitRevision(ctx, targetRevision, 10*time.Second)).Should(gm.Succeed())

		g.By("Writing stream data with the new storage-only host layout")
		writePostMoveStatus := writeTagFamilyMoveStream(ctx, clients.StreamWriteClient, groupName, streamName, "stream-post", postMoveTime,
			tagFamilyMoveTags("svc-post", "host-post", "region-post", true), tagFamilyMoveUpdatedWriteSpec())
		gm.Expect(writePostMoveStatus).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()))

		g.By("Verifying the moved stream schema layout")
		afterResp, getAfterErr := clients.StreamRegClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: streamName, Group: groupName},
		})
		gm.Expect(getAfterErr).ShouldNot(gm.HaveOccurred())
		expectTagFamilyLayout(afterResp.GetStream().GetTagFamilies(), tagFamilyMoveUpdatedLayout())

		g.By("Querying stream data after the move")
		afterElements := queryTagFamilyMoveStream(ctx, clients.StreamWriteClient, groupName, begin, end, true, 3, nil)
		expectMovedTagValue(afterElements, "svc-pre", "", true)
		expectMovedTagValue(afterElements, "svc-in-move", "", true)
		expectMovedTagValue(afterElements, "svc-post", "host-post", false)
		storageOnlyCriteriaElements := queryTagFamilyMoveStream(ctx, clients.StreamWriteClient, groupName, begin, end, true, 1,
			tagFamilyMoveHostCriteria("host-post"))
		expectMovedTagValue(storageOnlyCriteriaElements, "svc-post", "host-post", false)
	})

	g.It("keeps stream writes and queries working when a tag moves from storage-only to searchable", func() {
		groupName := fmt.Sprintf("tfm-stream-storage-%d", time.Now().UnixNano())
		streamName := "tfm_storage_stream"
		begin := time.Now().Add(-time.Second)
		preMoveTime := begin.Add(100 * time.Millisecond)
		inMoveTime := begin.Add(200 * time.Millisecond)
		postMoveTime := begin.Add(300 * time.Millisecond)
		end := begin.Add(time.Hour)

		g.By("Creating stream group")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: tagFamilyMoveStreamGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())
		defer func() {
			_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
		}()

		g.By("Creating stream with host in the storage-only tag family")
		createResp, createStreamErr := clients.StreamRegClient.Create(ctx, &databasev1.StreamRegistryServiceCreateRequest{
			Stream: tagFamilyMoveStorageToSearchableStreamSpec(groupName, streamName),
		})
		gm.Expect(createStreamErr).ShouldNot(gm.HaveOccurred())
		baseRevision := createResp.GetModRevision()

		g.By("Binding an inverted index for the searchable entity tag")
		indexRevision := createTagFamilyMoveSvcIndex(ctx, clients, groupName, streamName)
		gm.Expect(clients.AwaitRevision(ctx, max(baseRevision, indexRevision), 10*time.Second)).Should(gm.Succeed())

		g.By("Writing and querying stream data before the storage-only tag is moved")
		writeStatus := writeTagFamilyMoveStream(ctx, clients.StreamWriteClient, groupName, streamName, "stream-storage-pre", preMoveTime,
			tagFamilyMoveStorageToSearchableTags("svc-storage-pre", "host-storage-pre", "region-storage-pre", false),
			tagFamilyMoveStorageToSearchableInitialWriteSpec())
		gm.Expect(writeStatus).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()))
		beforeElements := queryStorageToSearchableMoveStream(ctx, clients.StreamWriteClient, groupName, begin, end, false, 1, nil)
		expectStorageOnlyMovedTagValue(beforeElements, "svc-storage-pre", "host-storage-pre")
		storageOnlyCriteriaElements := queryStorageToSearchableMoveStream(ctx, clients.StreamWriteClient, groupName, begin, end, false, 1,
			tagFamilyMoveHostCriteria("host-storage-pre"))
		expectStorageOnlyMovedTagValue(storageOnlyCriteriaElements, "svc-storage-pre", "host-storage-pre")

		g.By("Moving host from storage-only to searchable and adding it to the searchable index binding")
		getResp, getStreamErr := clients.StreamRegClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: streamName, Group: groupName},
		})
		gm.Expect(getStreamErr).ShouldNot(gm.HaveOccurred())
		updatedStream := getResp.GetStream()
		expectTagFamilyLayout(updatedStream.GetTagFamilies(), tagFamilyMoveStorageToSearchableInitialLayout())
		updatedStream.TagFamilies = tagFamilyMoveStorageToSearchableUpdatedFamilies()
		updateResp, updateStreamErr := clients.StreamRegClient.Update(ctx, &databasev1.StreamRegistryServiceUpdateRequest{
			Stream: updatedStream,
		})
		gm.Expect(updateStreamErr).ShouldNot(gm.HaveOccurred())
		indexMoveRevision := moveTagFamilyMoveStorageToSearchableIndexes(ctx, clients, groupName, streamName)

		g.By("Continuing writes with the old storage-only layout while the move is applying")
		writeInMoveStatus := writeTagFamilyMoveStream(ctx, clients.StreamWriteClient, groupName, streamName, "stream-storage-in-move", inMoveTime,
			tagFamilyMoveStorageToSearchableTags("svc-storage-in-move", "host-storage-in-move", "region-storage-in-move", false),
			tagFamilyMoveStorageToSearchableInitialWriteSpec())
		gm.Expect(writeInMoveStatus).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()))

		g.By("Waiting for the moved stream tag schema and index binding to be applied")
		targetRevision := max(updateResp.GetModRevision(), indexMoveRevision)
		gm.Expect(clients.AwaitRevision(ctx, targetRevision, 10*time.Second)).Should(gm.Succeed())

		g.By("Writing stream data with the new searchable host layout")
		writePostMoveStatus := writeTagFamilyMoveStream(ctx, clients.StreamWriteClient, groupName, streamName, "stream-storage-post", postMoveTime,
			tagFamilyMoveStorageToSearchableTags("svc-storage-post", "host-storage-post", "region-storage-post", true),
			tagFamilyMoveStorageToSearchableUpdatedWriteSpec())
		gm.Expect(writePostMoveStatus).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()))

		g.By("Verifying the moved stream schema layout")
		afterResp, getAfterErr := clients.StreamRegClient.Get(ctx, &databasev1.StreamRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: streamName, Group: groupName},
		})
		gm.Expect(getAfterErr).ShouldNot(gm.HaveOccurred())
		expectTagFamilyLayout(afterResp.GetStream().GetTagFamilies(), tagFamilyMoveStorageToSearchableUpdatedLayout())

		g.By("Querying stream data after the storage-only tag becomes searchable")
		afterElements := queryStorageToSearchableMoveStream(ctx, clients.StreamWriteClient, groupName, begin, end, true, 3, nil)
		expectSearchableMovedTagValue(afterElements, "svc-storage-pre", "", true)
		expectSearchableMovedTagValue(afterElements, "svc-storage-in-move", "", true)
		expectSearchableMovedTagValue(afterElements, "svc-storage-post", "host-storage-post", false)

		g.By("Querying the moved tag through the new searchable index")
		indexedPostElements := queryStorageToSearchableMoveStream(ctx, clients.StreamWriteClient, groupName, begin, end, true, 1,
			tagFamilyMoveHostCriteria("host-storage-post"))
		expectSearchableMovedTagValue(indexedPostElements, "svc-storage-post", "host-storage-post", false)
	})
	g.It("keeps measure writes and queries working when a tag moves from searchable to storage-only", func() {
		groupName := fmt.Sprintf("tfm-measure-%d", time.Now().UnixNano())
		measureName := "tfm_measure"
		begin := time.Now().Add(-time.Second)
		preMoveTime := begin.Add(100 * time.Millisecond)
		inMoveTime := begin.Add(200 * time.Millisecond)
		postMoveTime := begin.Add(300 * time.Millisecond)
		end := begin.Add(time.Hour)

		g.By("Creating measure group")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: tagFamilyMoveMeasureGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())
		defer func() {
			_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
		}()

		g.By("Creating measure with searchable and storage-only tag families")
		createResp, createMeasureErr := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: tagFamilyMoveMeasureSpec(groupName, measureName),
		})
		gm.Expect(createMeasureErr).ShouldNot(gm.HaveOccurred())
		baseRevision := createResp.GetModRevision()

		g.By("Binding inverted indexes for the searchable measure tags")
		indexRevision := createTagFamilyMoveMeasureIndexes(ctx, clients, groupName, measureName)
		gm.Expect(clients.AwaitRevision(ctx, max(baseRevision, indexRevision), 10*time.Second)).Should(gm.Succeed())

		g.By("Writing and querying measure data before the tag is moved")
		writeStatus := writeTagFamilyMoveMeasure(ctx, clients.MeasureWriteClient, groupName, measureName, preMoveTime,
			tagFamilyMoveTags("measure-svc-pre", "measure-host-pre", "measure-region-pre", false), tagFamilyMoveMeasureInitialWriteSpec(), 1)
		gm.Expect(writeStatus).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()))
		beforeDataPoints := queryTagFamilyMoveMeasure(ctx, clients.MeasureWriteClient, groupName, begin, end, false, 1, nil)
		expectMeasureIndexedMovedTagValue(beforeDataPoints, "measure-svc-pre", "measure-host-pre")
		indexedDataPoints := queryTagFamilyMoveMeasure(ctx, clients.MeasureWriteClient, groupName, begin, end, false, 1, tagFamilyMoveHostCriteria("measure-host-pre"))
		expectMeasureIndexedMovedTagValue(indexedDataPoints, "measure-svc-pre", "measure-host-pre")

		g.By("Moving host from searchable to storage-only and removing it from the measure index binding")
		getResp, getMeasureErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getMeasureErr).ShouldNot(gm.HaveOccurred())
		updatedMeasure := getResp.GetMeasure()
		expectTagFamilyLayout(updatedMeasure.GetTagFamilies(), tagFamilyMoveInitialLayout())
		updatedMeasure.TagFamilies = tagFamilyMoveUpdatedFamilies()
		updateResp, updateMeasureErr := clients.MeasureRegClient.Update(ctx, &databasev1.MeasureRegistryServiceUpdateRequest{
			Measure: updatedMeasure,
		})
		gm.Expect(updateMeasureErr).ShouldNot(gm.HaveOccurred())
		indexMoveRevision := moveTagFamilyMoveMeasureIndexes(ctx, clients, groupName, measureName)

		g.By("Continuing measure writes with the old layout while the move is applying")
		writeInMoveStatus := writeTagFamilyMoveMeasure(ctx, clients.MeasureWriteClient, groupName, measureName, inMoveTime,
			tagFamilyMoveTags("measure-svc-in-move", "measure-host-in-move", "measure-region-in-move", false), tagFamilyMoveMeasureInitialWriteSpec(), 2)
		gm.Expect(writeInMoveStatus).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()))

		g.By("Waiting for the moved measure tag schema and index binding to be applied")
		targetRevision := max(updateResp.GetModRevision(), indexMoveRevision)
		gm.Expect(clients.AwaitRevision(ctx, targetRevision, 10*time.Second)).Should(gm.Succeed())

		g.By("Writing measure data with the new storage-only host layout")
		writePostMoveStatus := writeTagFamilyMoveMeasure(ctx, clients.MeasureWriteClient, groupName, measureName, postMoveTime,
			tagFamilyMoveTags("measure-svc-post", "measure-host-post", "measure-region-post", true), tagFamilyMoveMeasureUpdatedWriteSpec(), 3)
		gm.Expect(writePostMoveStatus).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()))

		g.By("Verifying the moved measure schema layout")
		afterResp, getAfterErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getAfterErr).ShouldNot(gm.HaveOccurred())
		expectTagFamilyLayout(afterResp.GetMeasure().GetTagFamilies(), tagFamilyMoveUpdatedLayout())

		g.By("Querying measure data after the move")
		afterDataPoints := queryTagFamilyMoveMeasure(ctx, clients.MeasureWriteClient, groupName, begin, end, true, 3, nil)
		expectMeasureMovedTagValue(afterDataPoints, "measure-svc-pre", "", true)
		expectMeasureMovedTagValue(afterDataPoints, "measure-svc-in-move", "", true)
		expectMeasureMovedTagValue(afterDataPoints, "measure-svc-post", "measure-host-post", false)
		expectMeasureHostCriteriaError(ctx, clients.MeasureWriteClient, groupName, measureName, "measure-host-post", begin, end,
			tagFamilyMoveMeasureProjection(true))
	})

	g.It("keeps measure writes and queries working when a tag moves from storage-only to searchable", func() {
		groupName := fmt.Sprintf("tfm-measure-storage-%d", time.Now().UnixNano())
		measureName := "tfm_storage_measure"
		begin := time.Now().Add(-time.Second)
		preMoveTime := begin.Add(100 * time.Millisecond)
		inMoveTime := begin.Add(200 * time.Millisecond)
		postMoveTime := begin.Add(300 * time.Millisecond)
		end := begin.Add(time.Hour)

		g.By("Creating measure group")
		_, createGroupErr := clients.GroupClient.Create(ctx, &databasev1.GroupRegistryServiceCreateRequest{
			Group: tagFamilyMoveMeasureGroup(groupName),
		})
		gm.Expect(createGroupErr).ShouldNot(gm.HaveOccurred())
		defer func() {
			_, _ = clients.GroupClient.Delete(ctx, &databasev1.GroupRegistryServiceDeleteRequest{Group: groupName})
		}()

		g.By("Creating measure with host in the storage-only tag family")
		createResp, createMeasureErr := clients.MeasureRegClient.Create(ctx, &databasev1.MeasureRegistryServiceCreateRequest{
			Measure: tagFamilyMoveStorageToSearchableMeasureSpec(groupName, measureName),
		})
		gm.Expect(createMeasureErr).ShouldNot(gm.HaveOccurred())
		baseRevision := createResp.GetModRevision()

		g.By("Binding an inverted index for the searchable measure entity tag")
		indexRevision := createTagFamilyMoveMeasureSvcIndex(ctx, clients, groupName, measureName)
		gm.Expect(clients.AwaitRevision(ctx, max(baseRevision, indexRevision), 10*time.Second)).Should(gm.Succeed())

		g.By("Writing and querying measure data before the storage-only tag is moved")
		writeStatus := writeTagFamilyMoveMeasure(ctx, clients.MeasureWriteClient, groupName, measureName, preMoveTime,
			tagFamilyMoveStorageToSearchableTags("measure-svc-storage-pre", "measure-host-storage-pre", "measure-region-storage-pre", false),
			tagFamilyMoveMeasureStorageToSearchableInitialWriteSpec(), 1)
		gm.Expect(writeStatus).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()))
		beforeDataPoints := queryStorageToSearchableMoveMeasure(ctx, clients.MeasureWriteClient, groupName, begin, end, false, 1, nil)
		expectMeasureStorageOnlyMovedTagValue(beforeDataPoints, "measure-svc-storage-pre", "measure-host-storage-pre")
		expectMeasureHostCriteriaError(ctx, clients.MeasureWriteClient, groupName, measureName, "measure-host-storage-pre", begin, end,
			tagFamilyMoveMeasureStorageToSearchableProjection(false))

		g.By("Moving host from storage-only to searchable and adding it to the measure index binding")
		getResp, getMeasureErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getMeasureErr).ShouldNot(gm.HaveOccurred())
		updatedMeasure := getResp.GetMeasure()
		expectTagFamilyLayout(updatedMeasure.GetTagFamilies(), tagFamilyMoveStorageToSearchableInitialLayout())
		updatedMeasure.TagFamilies = tagFamilyMoveStorageToSearchableUpdatedFamilies()
		updateResp, updateMeasureErr := clients.MeasureRegClient.Update(ctx, &databasev1.MeasureRegistryServiceUpdateRequest{
			Measure: updatedMeasure,
		})
		gm.Expect(updateMeasureErr).ShouldNot(gm.HaveOccurred())
		indexMoveRevision := moveTagFamilyMoveMeasureStorageToSearchableIndexes(ctx, clients, groupName, measureName)

		g.By("Continuing measure writes with the old storage-only layout while the move is applying")
		writeInMoveStatus := writeTagFamilyMoveMeasure(ctx, clients.MeasureWriteClient, groupName, measureName, inMoveTime,
			tagFamilyMoveStorageToSearchableTags("measure-svc-storage-in-move", "measure-host-storage-in-move", "measure-region-storage-in-move", false),
			tagFamilyMoveMeasureStorageToSearchableInitialWriteSpec(), 2)
		gm.Expect(writeInMoveStatus).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()))

		g.By("Waiting for the moved measure tag schema and index binding to be applied")
		targetRevision := max(updateResp.GetModRevision(), indexMoveRevision)
		gm.Expect(clients.AwaitRevision(ctx, targetRevision, 10*time.Second)).Should(gm.Succeed())

		g.By("Writing measure data with the new searchable host layout")
		writePostMoveStatus := writeTagFamilyMoveMeasure(ctx, clients.MeasureWriteClient, groupName, measureName, postMoveTime,
			tagFamilyMoveStorageToSearchableTags("measure-svc-storage-post", "measure-host-storage-post", "measure-region-storage-post", true),
			tagFamilyMoveMeasureStorageToSearchableUpdatedWriteSpec(), 3)
		gm.Expect(writePostMoveStatus).Should(gm.Equal(modelv1.Status_STATUS_SUCCEED.String()))

		g.By("Verifying the moved measure schema layout")
		afterResp, getAfterErr := clients.MeasureRegClient.Get(ctx, &databasev1.MeasureRegistryServiceGetRequest{
			Metadata: &commonv1.Metadata{Name: measureName, Group: groupName},
		})
		gm.Expect(getAfterErr).ShouldNot(gm.HaveOccurred())
		expectTagFamilyLayout(afterResp.GetMeasure().GetTagFamilies(), tagFamilyMoveStorageToSearchableUpdatedLayout())

		g.By("Querying measure data after the storage-only tag becomes searchable")
		afterDataPoints := queryStorageToSearchableMoveMeasure(ctx, clients.MeasureWriteClient, groupName, begin, end, true, 3, nil)
		expectMeasureSearchableMovedTagValue(afterDataPoints, "measure-svc-storage-pre", "", true)
		expectMeasureSearchableMovedTagValue(afterDataPoints, "measure-svc-storage-in-move", "", true)
		expectMeasureSearchableMovedTagValue(afterDataPoints, "measure-svc-storage-post", "measure-host-storage-post", false)

		g.By("Querying the moved measure tag through the new searchable index")
		indexedPostDataPoints := queryStorageToSearchableMoveMeasure(ctx, clients.MeasureWriteClient, groupName, begin, end, true, 1,
			tagFamilyMoveHostCriteria("measure-host-storage-post"))
		expectMeasureSearchableMovedTagValue(indexedPostDataPoints, "measure-svc-storage-post", "measure-host-storage-post", false)
	})
})
