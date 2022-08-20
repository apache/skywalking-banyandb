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

package logical_test

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/discovery"
	"github.com/apache/skywalking-banyandb/banyand/measure"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/queue"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
	testmeasure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	teststream "github.com/apache/skywalking-banyandb/pkg/test/stream"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"
)

//go:embed testdata/*.json
var dataFS embed.FS

func setupQueryData(testing *testing.T, dataFile string, stream stream.Stream) (baseTime time.Time) {
	t := assert.New(testing)
	var templates []interface{}
	baseTime = timestamp.NowMilli()
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	t.NoError(err)
	t.NoError(json.Unmarshal(content, &templates))
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")
	for i, template := range templates {
		rawSearchTagFamily, errMarshal := json.Marshal(template)
		t.NoError(errMarshal)
		searchTagFamily := &modelv1.TagFamilyForWrite{}
		t.NoError(protojson.Unmarshal(rawSearchTagFamily, searchTagFamily))
		e := &streamv1.ElementValue{
			ElementId: strconv.Itoa(i),
			Timestamp: timestamppb.New(baseTime.Add(500 * time.Millisecond * time.Duration(i))),
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: []*modelv1.TagValue{
						{
							Value: &modelv1.TagValue_BinaryData{
								BinaryData: bb,
							},
						},
					},
				},
			},
		}
		e.TagFamilies = append(e.TagFamilies, searchTagFamily)
		errInner := stream.Write(e)
		t.NoError(errInner)
	}
	return baseTime
}

func setup(t *require.Assertions) (stream.Stream, metadata.Service, func()) {
	t.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "warn",
	}))

	tempDir, deferFunc := test.Space(t)
	// Init `Discovery` module
	repo, err := discovery.NewServiceRepo(context.Background())
	t.NoError(err)
	// Init `Queue` module
	pipeline, err := queue.NewQueue(context.TODO(), repo)
	t.NoError(err)

	metadataSvc, err := metadata.NewService(context.TODO())
	t.NoError(err)
	listenClientURL, listenPeerURL, err := test.NewEtcdListenUrls()
	t.NoError(err)

	etcdRootDir := teststream.RandomTempDir()
	err = metadataSvc.FlagSet().Parse([]string{
		"--metadata-root-path=" + etcdRootDir,
		"--etcd-listen-client-url=" + listenClientURL, "--etcd-listen-peer-url=" + listenPeerURL,
	})
	t.NoError(err)

	streamSvc, err := stream.NewService(context.TODO(), metadataSvc, repo, pipeline)
	t.NoError(err)

	// 1 - (MetadataService).PreRun
	err = metadataSvc.PreRun()
	t.NoError(err)

	err = teststream.PreloadSchema(metadataSvc.SchemaRegistry())
	t.NoError(err)

	err = streamSvc.FlagSet().Parse([]string{"--stream-root-path=" + tempDir})
	t.NoError(err)

	// 2 - (StreamService).PreRun
	err = streamSvc.PreRun()
	t.NoError(err)

	s, err := streamSvc.Stream(&commonv1.Metadata{
		Name:  "sw",
		Group: "default",
	})
	t.NoError(err)
	t.NotNil(s)

	return s, metadataSvc, func() {
		_ = s.Close()
		metadataSvc.GracefulStop()
		deferFunc()
		_ = os.RemoveAll(etcdRootDir)
	}
}

func setupMeasure(t *require.Assertions) (measure.Measure, metadata.Service, func()) {
	t.NoError(logger.Init(logger.Logging{
		Env:   "dev",
		Level: "warn",
	}))

	tempDir, deferFunc := test.Space(t)
	// Init `Discovery` module
	repo, err := discovery.NewServiceRepo(context.Background())
	t.NoError(err)
	// Init `Queue` module
	pipeline, err := queue.NewQueue(context.TODO(), repo)
	t.NoError(err)

	metadataSvc, err := metadata.NewService(context.TODO())
	t.NoError(err)
	listenClientURL, listenPeerURL, err := test.NewEtcdListenUrls()
	t.NoError(err)

	etcdRootDir := teststream.RandomTempDir()
	err = metadataSvc.FlagSet().Parse([]string{
		"--metadata-root-path=" + etcdRootDir,
		"--etcd-listen-client-url=" + listenClientURL, "--etcd-listen-peer-url=" + listenPeerURL,
	})
	t.NoError(err)

	measureSvc, err := measure.NewService(context.TODO(), metadataSvc, repo, pipeline)
	t.NoError(err)

	// 1 - (MeasureService).PreRun
	err = metadataSvc.PreRun()
	t.NoError(err)

	err = testmeasure.PreloadSchema(metadataSvc.SchemaRegistry())
	t.NoError(err)

	err = measureSvc.FlagSet().Parse([]string{"--measure-root-path=" + tempDir})
	t.NoError(err)

	// 2 - (MeasureService).PreRun
	err = measureSvc.PreRun()
	t.NoError(err)

	m, err := measureSvc.Measure(&commonv1.Metadata{
		Name:  "service_cpm_minute",
		Group: "sw_metric",
	})
	t.NoError(err)
	t.NotNil(m)

	return m, metadataSvc, func() {
		_ = m.Close()
		metadataSvc.GracefulStop()
		deferFunc()
		_ = os.RemoveAll(etcdRootDir)
	}
}

func setupMeasureQueryData(testing *testing.T, dataFile string, measure measure.Measure) (baseTime time.Time) {
	t := assert.New(testing)
	var templates []interface{}
	baseTime = timestamp.NowMilli()
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	t.NoError(err)
	t.NoError(json.Unmarshal(content, &templates))
	for i, template := range templates {
		rawDataPointValue, errMarshal := json.Marshal(template)
		t.NoError(errMarshal)
		dataPointValue := &measurev1.DataPointValue{}
		t.NoError(protojson.Unmarshal(rawDataPointValue, dataPointValue))
		if dataPointValue.Timestamp == nil {
			dataPointValue.Timestamp = timestamppb.New(baseTime.Add(time.Duration(i) * time.Minute))
		}
		errInner := measure.Write(dataPointValue)
		t.NoError(errInner)
	}
	return baseTime
}
