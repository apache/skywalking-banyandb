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

package measure

import (
	"time"

	"github.com/apache/skywalking-banyandb/api/data"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/banyand/queue"
)

// RegisterMeasureSeriesSyncHandlerForTest wires the REAL measure series-index
// sync handler (setUpSyncSeriesCallback) of the supplied standalone service onto
// the supplied chunked-sync server, injecting the test-controlled protector and
// memWaitTimeout. It is the production registration from svc_data.go, exposed to
// the external measure_test package so an integration test can drive the receive
// path against a real target tsdb without spinning up the full data service.
func RegisterMeasureSeriesSyncHandlerForTest(svc Service, server queue.Server, pm protector.Memory, memWaitTimeout time.Duration) {
	s := svc.(*standalone)
	server.RegisterChunkedSyncHandler(data.TopicMeasureSeriesSync, setUpSyncSeriesCallback(s.l, s.schemaRepo, pm, memWaitTimeout))
}
