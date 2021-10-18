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

	"github.com/golang/protobuf/jsonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	"github.com/apache/skywalking-banyandb/banyand/stream"
	"github.com/apache/skywalking-banyandb/pkg/logger"
	"github.com/apache/skywalking-banyandb/pkg/test"
)

//go:embed testdata/*.json
var dataFS embed.FS

func setupQueryData(testing *testing.T, dataFile string, stream stream.Stream) (baseTime time.Time) {
	t := assert.New(testing)
	var templates []interface{}
	baseTime = time.Now()
	content, err := dataFS.ReadFile("testdata/" + dataFile)
	t.NoError(err)
	t.NoError(json.Unmarshal(content, &templates))
	bb, _ := base64.StdEncoding.DecodeString("YWJjMTIzIT8kKiYoKSctPUB+")
	for i, template := range templates {
		rawSearchTagFamily, errMarshal := json.Marshal(template)
		t.NoError(errMarshal)
		searchTagFamily := &modelv1.TagFamilyForWrite{}
		t.NoError(jsonpb.UnmarshalString(string(rawSearchTagFamily), searchTagFamily))
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
		Level: "info",
	}))

	tempDir, deferFunc := test.Space(t)

	metadataSvc, err := metadata.NewService(context.TODO())
	t.NoError(err)

	lc, lp := test.RandomUnixDomainListener()
	etcdRootDir := test.RandomTempDir()
	err = metadataSvc.FlagSet().Parse([]string{"--listener-client-url=" + lc, "--listener-peer-url=" + lp, "--metadata-root-path=" + etcdRootDir})
	t.NoError(err)

	streamSvc, err := stream.NewService(context.TODO(), metadataSvc, nil, nil)
	t.NoError(err)

	// 1 - (MetadataService).PreRun
	err = metadataSvc.PreRun()
	t.NoError(err)

	err = test.PreloadSchema(metadataSvc.SchemaRegistry())
	t.NoError(err)

	err = streamSvc.FlagSet().Parse([]string{"--root-path=" + tempDir})
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
