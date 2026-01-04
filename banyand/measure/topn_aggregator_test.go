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

package measure_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"time"

	apiData "github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/pkg/bus"
	"github.com/apache/skywalking-banyandb/pkg/test/flags"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gleak"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/data"
	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

var _ = Describe("Write TopN Aggregation test data", func() {

	var svcs *services
	var deferFn func()
	var goods []gleak.Goroutine
	const groupName = "sw_metric"

	BeforeEach(func() {
		svcs, deferFn = setUp()
		goods = gleak.Goroutines()
		Eventually(func() bool {
			_, ok := svcs.measure.LoadGroup(groupName)
			return ok
		}).WithTimeout(flags.EventuallyTimeout).Should(BeTrue())
	})

	AfterEach(func() {
		deferFn()
		Eventually(gleak.Goroutines, flags.EventuallyTimeout).ShouldNot(gleak.HaveLeaked(goods))
	})

	It("should write and query TopN binary data", func() {

		topNValue := measure.GenerateTopNValue()
		defer measure.ReleaseTopNValue(topNValue)

		valueName := "endpoint_resp_time-service"
		entityTagNames := []string{"tag1", "tag2"}

		t1 := &measure.TopNValue{}
		t1.SetMetadata(valueName, entityTagNames)

		entities1 := [][]*modelv1.TagValue{
			{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc1"}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity1"}}},
			},
			{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc1"}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity2"}}},
			},
			{
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "svc1"}}},
				{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: "entity3"}}},
			},
		}

		for _, e := range entities1 {
			t1.AddValue(1, e)
		}

		buf, err := t1.Marshal(make([]byte, 0, 128))
		Expect(err).ShouldNot(HaveOccurred())
		now := time.Now()
		nano := time.Now().UnixNano()

		entityValues := []*modelv1.TagValue{
			{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: valueName,
					},
				},
			},
			{
				Value: &modelv1.TagValue_Int{
					Int: &modelv1.Int{
						Value: int64(modelv1.Sort_SORT_DESC),
					},
				},
			},
			{
				Value: &modelv1.TagValue_Str{
					Str: &modelv1.Str{
						Value: base64.StdEncoding.EncodeToString([]byte(groupName)),
					},
				},
			},
		}

		iwr1 := &measurev1.InternalWriteRequest{
			Request: &measurev1.WriteRequest{
				MessageId: uint64(now.UnixNano()),
				Metadata:  &commonv1.Metadata{Name: measure.TopNSchemaName, Group: groupName},
				DataPoint: &measurev1.DataPointValue{
					Timestamp: timestamppb.New(now),
					TagFamilies: []*modelv1.TagFamilyForWrite{
						{Tags: entityValues},
					},
					Fields: []*modelv1.FieldValue{
						{
							Value: &modelv1.FieldValue_BinaryData{
								BinaryData: bytes.Clone(buf),
							},
						},
					},
					Version: nano,
				},
			},
			EntityValues: entityValues,
			ShardId:      0,
		}

		fmt.Println(iwr1)

		publisher := svcs.pipeline.NewBatchPublisher(5 * time.Second)
		defer publisher.Close()
		message := bus.NewBatchMessageWithNode(bus.MessageID(nano), "local", iwr1)
		_, err = publisher.Publish(context.TODO(), apiData.TopicMeasureWrite, message)
		Expect(err).ShouldNot(HaveOccurred())

		req := &measurev1.TopNRequest{
			Groups: []string{groupName},
			Name:   valueName,
			TimeRange: &modelv1.TimeRange{
				Begin: timestamppb.New(timestamp.NowMilli().Add(-time.Hour)),
				End:   timestamppb.New(timestamp.NowMilli().Add(time.Hour)),
			},
		}

		feat, err := svcs.pipeline.Publish(context.Background(), data.TopicTopNQuery, bus.NewMessage(bus.MessageID(time.Now().UnixNano()), req))
		Expect(err).ShouldNot(HaveOccurred())

		msg, err := feat.Get()
		Expect(err).ShouldNot(HaveOccurred())
		data := msg.Data()
		topNResp := data.(*measurev1.TopNResponse)
		fmt.Println(topNResp)
	})
})
