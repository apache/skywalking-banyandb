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
	"embed"
	"encoding/json"
	"time"

	"github.com/golang/protobuf/jsonpb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/measure"
)

var _ = Describe("Write", func() {
	var svcs *services
	var deferFn func()
	var measure measure.Measure

	BeforeEach(func() {
		svcs, deferFn = setUp()
		var err error
		measure, err = svcs.measure.Measure(&commonv1.Metadata{
			Name:  "cpm",
			Group: "default",
		})
		Expect(err).ShouldNot(HaveOccurred())
	})
	AfterEach(func() {
		deferFn()
	})
	It("writes data", func() {
		writeData("write_data.json", measure)
	})
})

//go:embed testdata/*.json
var dataFS embed.FS

func writeData(dataFile string, measure measure.Measure) (baseTime time.Time) {
	var templates []interface{}
	baseTime = time.Now()
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	Expect(err).ShouldNot(HaveOccurred())
	Expect(json.Unmarshal(content, &templates)).ShouldNot(HaveOccurred())
	for i, template := range templates {
		rawDataPointValue, errMarshal := json.Marshal(template)
		Expect(errMarshal).ShouldNot(HaveOccurred())
		dataPointValue := &measurev1.DataPointValue{}
		if dataPointValue.Timestamp == nil {
			dataPointValue.Timestamp = timestamppb.New(baseTime.Add(time.Duration(i) * time.Minute))
		}
		Expect(jsonpb.UnmarshalString(string(rawDataPointValue), dataPointValue)).ShouldNot(HaveOccurred())
		errInner := measure.Write(dataPointValue)
		Expect(errInner).ShouldNot(HaveOccurred())
	}
	return baseTime
}
