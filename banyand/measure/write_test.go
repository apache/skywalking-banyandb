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
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	testmeasure "github.com/apache/skywalking-banyandb/pkg/test/measure"
)

func Test_Measure_Write(t *testing.T) {
	s, deferFunc := setup(t)
	defer deferFunc()
	writeData(t, "write_data.json", s)
}

func setup(t *testing.T) (*measure, func()) {
	req := require.New(t)
	req.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "info",
	}))
	tempDir, deferFunc := test.Space(req)

	mService, err := metadata.NewService(context.TODO())
	req.NoError(err)

	etcdRootDir := testmeasure.RandomTempDir()
	err = mService.FlagSet().Parse([]string{"--metadata-root-path=" + etcdRootDir})
	req.NoError(err)

	err = mService.PreRun()
	req.NoError(err)

	err = testmeasure.PreloadSchema(mService.SchemaRegistry())
	req.NoError(err)

	sa, err := mService.MeasureRegistry().GetMeasure(context.TODO(), &commonv1.Metadata{
		Name:  "cpm",
		Group: "default",
	})
	req.NoError(err)
	iRules, err := mService.IndexRules(context.TODO(), sa.Metadata)
	req.NoError(err)
	sSpec := measureSpec{
		schema:     sa,
		indexRules: iRules,
	}
	s, err := openMeasure(tempDir, sSpec, logger.GetLogger("test"))
	req.NoError(err)
	return s, func() {
		_ = s.Close()
		mService.GracefulStop()
		deferFunc()
		_ = os.RemoveAll(etcdRootDir)
	}
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
