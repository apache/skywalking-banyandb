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
	"context"
	"embed"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	testmeasure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	teststream "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

func Test_Measure_Write(t *testing.T) {
	s, deferFunc := setup(t)
	defer deferFunc()
	writeData(t, "write_data.json", s)
}

func setup(t *testing.T) (*measure, func()) {
	require := require.New(t)
	require.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "info",
	}))

	tempDir, deferFunc := test.Space(require)

	var mService metadata.Service
	var etcdRootDir string
	var sa *databasev1.Measure
	var iRules []*databasev1.IndexRule
	var m *measure

	flow := test.NewTestFlow().PushErrorHandler(func() {
		deferFunc()
	}).RunWithoutSideEffect(context.TODO(), func() (err error) {
		mService, err = metadata.NewService(context.TODO())
		return err
	}).Run(context.TODO(), func() error {
		etcdRootDir = teststream.RandomTempDir()
		return mService.FlagSet().Parse([]string{"--metadata-root-path=" + etcdRootDir})
	}, func() {
		if len(etcdRootDir) > 0 {
			_ = os.RemoveAll(etcdRootDir)
		}
	}).Run(context.TODO(), func() error {
		return mService.PreRun()
	}, func() {
		if mService != nil {
			mService.GracefulStop()
		}
	}).RunWithoutSideEffect(context.TODO(), func() error {
		return testmeasure.PreloadSchema(mService.SchemaRegistry())
	}).RunWithoutSideEffect(context.TODO(), func() (err error) {
		sa, err = mService.MeasureRegistry().GetMeasure(context.TODO(), &commonv1.Metadata{
			Name:  "cpm",
			Group: "default",
		})
		if err != nil {
			return
		}

		iRules, err = mService.IndexRules(context.TODO(), sa.Metadata)
		return
	}).Run(context.TODO(), func() (err error) {
		m, err = openMeasure(tempDir, measureSpec{
			schema:     sa,
			indexRules: iRules,
		}, logger.GetLogger("test"))

		return
	}, func() {
		if m != nil {
			_ = m.Close()
		}
	})

	require.NoError(flow.Error())

	return m, flow.Shutdown()
}

//go:embed testdata/*.json
var dataFS embed.FS

func writeData(testing *testing.T, dataFile string, measure *measure) (baseTime time.Time) {
	t := assert.New(testing)
	r := require.New(testing)
	var templates []interface{}
	baseTime = time.Now()
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	r.NoError(err)
	r.NoError(json.Unmarshal(content, &templates))
	now := time.Now()
	for i, template := range templates {
		rawDataPointValue, errMarshal := json.Marshal(template)
		r.NoError(errMarshal)
		dataPointValue := &measurev1.DataPointValue{}
		if dataPointValue.Timestamp == nil {
			dataPointValue.Timestamp = timestamppb.New(now.Add(time.Duration(i) * time.Minute))
		}
		r.NoError(jsonpb.UnmarshalString(string(rawDataPointValue), dataPointValue))
		errInner := measure.Write(dataPointValue)
		t.NoError(errInner)
	}
	return baseTime
}
