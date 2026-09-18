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
	casesstreamdata.SeedAll(conn, now, interval)
	// measure
	interval = time.Minute
	casesmeasuredata.Write(conn, "service_traffic", "index_mode", "service_traffic_data_old.json", now.AddDate(0, 0, -2), interval)
	casesmeasuredata.Write(conn, "service_traffic", "index_mode", "service_traffic_data.json", now, interval)
	// Seed data in a fully expired segment, well past the index_mode group's
	// 7-day TTL (distinct entity ids 901/902). It must never surface in query
	// results: it backs the "index mode excludes data expired beyond TTL" case,
	// which fails without the retention filter that drops fully expired segments.
	casesmeasuredata.Write(conn, "service_traffic", "index_mode", "service_traffic_data_expired.json", now.AddDate(0, 0, -10), interval)
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
	casesmeasuredata.Write(conn, "service_instance_metric_topn_test", "sw_metric", "service_instance_metric_topn_test_data.json", now, interval)
	casesmeasuredata.Write(conn, "service_instance_float_metric", "sw_metric", "service_instance_float_metric_data.json", now, interval)
	// time_bucket_metric is seeded at its own 20s interval (not the shared
	// 1m `interval`) so its 6 points land 20s apart ending at `now`. `now`
	// is guaranteed minute-aligned (see the suite's `now` construction), so
	// a 1-minute time_bucket width deterministically splits them 2/3/1
	// across three buckets regardless of the wall-clock minute `now` lands
	// on — see test/cases/measure/data/input/group_time_bucket*.yaml.
	casesmeasuredata.Write(conn, "time_bucket_metric", "sw_metric", "time_bucket_metric_data.json", now, 20*time.Second)
	// The off-cadence fixture (design §11's "COUNT and COUNT_DISTINCT must be
	// allowed to disagree at the interval width" case): two points for the
	// SAME entity, 34s apart — off the measure's own 20s write cadence, but
	// both landing inside the single 1-minute bucket [now+1m, now+2m).
	// `now+73s` and `now+107s` are both in that bucket regardless of
	// interval, since `now` is minute-aligned; the window is offset a full
	// minute past `now` (not [now, now+1m)) because the base
	// time_bucket_metric_data.json's last point lands exactly AT `now`,
	// which a [now, ...) query would otherwise include as a third, unwanted
	// row. See group_count_distinct_off_cadence.yaml /
	// group_count_off_cadence.yaml.
	casesmeasuredata.Write(conn, "time_bucket_metric", "sw_metric", "time_bucket_metric_off_cadence_data.json", now.Add(107*time.Second), 34*time.Second)
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
	time.Sleep(2 * time.Second)
	// trace
	casestrace.SeedAll(conn, now, 500*time.Millisecond)
	// property
	caseproperty.Write(conn, "sw1")
	caseproperty.Write(conn, "sw2")
}
