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

// Package cases provides some tools to access test data.
package cases

import (
	"time"

	"github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	tracev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/trace/v1"
	"github.com/apache/skywalking-banyandb/pkg/grpchelper"
	casesmeasuredata "github.com/apache/skywalking-banyandb/test/cases/measure/data"
	caseproperty "github.com/apache/skywalking-banyandb/test/cases/property/data"
	casesstreamdata "github.com/apache/skywalking-banyandb/test/cases/stream/data"
	casestrace "github.com/apache/skywalking-banyandb/test/cases/trace/data"
)

// Initialize test data.
func Initialize(addr string, now time.Time) {
	conn, err := grpchelper.Conn(addr, 10*time.Second, grpc.WithTransportCredentials(insecure.NewCredentials()))
	gomega.Expect(err).NotTo(gomega.HaveOccurred())
	defer conn.Close()
	interval := 500 * time.Millisecond
	// stream
	casesstreamdata.Write(conn, "sw", now, interval)
	casesstreamdata.Write(conn, "duplicated", now, 0)
	casesstreamdata.WriteDeduplicationTest(conn, "deduplication_test", now, time.Millisecond)
	casesstreamdata.WriteToGroup(conn, "sw", "updated", "sw_updated", now.Add(time.Minute), interval)
	casesstreamdata.WriteMixed(conn, now.Add(2*time.Minute), interval,
		casesstreamdata.WriteSpec{
			Metadata: &commonv1.Metadata{Name: "sw", Group: "default-spec"},
			DataFile: "sw_schema_order.json",
		},
		casesstreamdata.WriteSpec{
			Spec: []*streamv1.TagFamilySpec{
				{
					Name:     "data",
					TagNames: []string{"data_binary"},
				},
				{
					Name:     "searchable",
					TagNames: []string{"trace_id", "state", "service_id", "service_instance_id", "endpoint_id", "duration", "start_time", "http.method", "status_code", "span_id"},
				},
			},
			DataFile: "sw_spec_order.json",
		},
		casesstreamdata.WriteSpec{
			Metadata: &commonv1.Metadata{Name: "sw", Group: "default-spec2"},
			Spec: []*streamv1.TagFamilySpec{
				{
					Name:     "searchable",
					TagNames: []string{"span_id", "status_code", "http.method", "duration", "state", "endpoint_id", "service_instance_id", "start_time", "service_id", "trace_id"},
				},
				{
					Name:     "data",
					TagNames: []string{"data_binary"},
				},
			},
			DataFile: "sw_spec_order2.json",
		})
	time.Sleep(5 * time.Second)
	// measure
	interval = time.Minute
	casesmeasuredata.Write(conn, "service_traffic", "index_mode", "service_traffic_data_old.json", now.AddDate(0, 0, -2), interval)
	casesmeasuredata.Write(conn, "service_traffic", "index_mode", "service_traffic_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_traffic", "replicated_group", "service_traffic_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_instance_traffic", "sw_metric", "service_instance_traffic_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_cpm_minute", "sw_metric", "service_cpm_minute_data.json", now, interval)
	casesmeasuredata.Write(conn, "instance_clr_cpu_minute", "sw_metric", "instance_clr_cpu_minute_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_instance_cpm_minute", "sw_metric", "service_instance_cpm_minute_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_instance_cpm_minute", "sw_metric", "service_instance_cpm_minute_data1.json", now.Add(10*time.Second), interval)
	casesmeasuredata.Write(conn, "service_instance_cpm_minute", "sw_metric", "service_instance_cpm_minute_data2.json", now.Add(10*time.Minute), interval)
	casesmeasuredata.Write(conn, "service_instance_endpoint_cpm_minute", "sw_metric", "service_instance_endpoint_cpm_minute_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_instance_endpoint_cpm_minute", "sw_metric", "service_instance_endpoint_cpm_minute_data1.json", now.Add(10*time.Second), interval)
	casesmeasuredata.Write(conn, "service_instance_endpoint_cpm_minute", "sw_metric", "service_instance_endpoint_cpm_minute_data2.json", now.Add(10*time.Minute), interval)
	casesmeasuredata.Write(conn, "service_latency_minute", "sw_metric", "service_latency_minute_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_instance_latency_minute", "sw_metric", "service_instance_latency_minute_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_instance_latency_minute", "sw_metric", "service_instance_latency_minute_data1.json", now.Add(1*time.Minute), interval)
	casesmeasuredata.Write(conn, "endpoint_traffic", "sw_metric", "endpoint_traffic.json", now, interval)
	casesmeasuredata.Write(conn, "duplicated", "exception", "duplicated.json", now, 0)
	casesmeasuredata.Write(conn, "service_cpm_minute", "sw_updated", "service_cpm_minute_updated_data.json", now.Add(10*time.Minute), interval)
	casesmeasuredata.Write(conn, "endpoint_resp_time_minute", "sw_metric", "endpoint_resp_time_minute_data.json", now, interval)
	casesmeasuredata.Write(conn, "endpoint_resp_time_minute", "sw_metric", "endpoint_resp_time_minute_data1.json", now.Add(10*time.Second), interval)
	casesmeasuredata.WriteMixed(conn, now.Add(30*time.Minute), interval,
		casesmeasuredata.WriteSpec{
			Metadata: &commonv1.Metadata{Name: "service_cpm_minute", Group: "sw_spec"},
			DataFile: "service_cpm_minute_schema_order.json",
		},
		casesmeasuredata.WriteSpec{
			Spec: &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{
					{
						Name:     "default",
						TagNames: []string{"entity_id", "id"},
					},
				},
				FieldNames: []string{"value", "total"},
			},
			DataFile: "service_cpm_minute_spec_order.json",
		},
		casesmeasuredata.WriteSpec{
			Metadata: &commonv1.Metadata{Name: "service_cpm_minute", Group: "sw_spec2"},
			Spec: &measurev1.DataPointSpec{
				TagFamilySpec: []*measurev1.TagFamilySpec{
					{
						Name:     "default",
						TagNames: []string{"id", "entity_id"},
					},
				},
				FieldNames: []string{"total", "value"},
			},
			DataFile: "service_cpm_minute_spec_order2.json",
		})
	time.Sleep(10 * time.Second)
	// trace
	interval = 500 * time.Millisecond
	casestrace.WriteToGroup(conn, "sw", "test-trace-group", "sw", now, interval)
	casestrace.WriteToGroup(conn, "zipkin", "zipkinTrace", "zipkin", now, interval)
	casestrace.WriteToGroup(conn, "sw", "test-trace-updated", "sw_updated", now.Add(time.Minute), interval)
	time.Sleep(5 * time.Second)
	casestrace.WriteToGroup(conn, "sw", "test-trace-group", "sw_mixed_traces", now.Add(time.Minute), interval)
	casestrace.WriteMixed(conn, now.Add(2*time.Minute), interval,
		casestrace.WriteSpec{
			Metadata: &commonv1.Metadata{Name: "sw", Group: "test-trace-spec"},
			DataFile: "sw_schema_order.json",
		},
		casestrace.WriteSpec{
			Spec: &tracev1.TagSpec{
				TagNames: []string{"trace_id", "state", "service_id", "service_instance_id", "endpoint_id", "duration", "span_id", "timestamp"},
			},
			DataFile: "sw_spec_order.json",
		},
		casestrace.WriteSpec{
			Metadata: &commonv1.Metadata{Name: "sw", Group: "test-trace-spec2"},
			Spec: &tracev1.TagSpec{
				TagNames: []string{"span_id", "duration", "endpoint_id", "service_instance_id", "service_id", "state", "trace_id", "timestamp"},
			},
			DataFile: "sw_spec_order2.json",
		})
	// property
	caseproperty.Write(conn, "sw1")
	caseproperty.Write(conn, "sw2")
}
