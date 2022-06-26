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

package sources_test

import (
	"context"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/flow/api"
	streamingApi "github.com/apache/skywalking-banyandb/pkg/flow/streaming/api"
	"github.com/apache/skywalking-banyandb/pkg/flow/streaming/sources"
)

func TestSource_io_reader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	inlet := streamingApi.NewMockInlet(ctrl)

	assert := require.New(t)
	src := sources.FromReader(strings.NewReader(ALPHABET), sources.WithBufferSize(1))
	assert.NoError(src.Setup(context.TODO()))

	in := make(chan interface{})
	inlet.
		EXPECT().
		In().Times(len(ALPHABET) + 1).
		Return(in)

	src.Exec(inlet)

	var result strings.Builder
	for item := range in {
		assert.IsType(api.StreamRecord{}, item)
		result.Write(item.(api.StreamRecord).Data().([]byte))
	}

	assert.Equal(ALPHABET, result.String())

	assert.NoError(src.Teardown(context.TODO()))
}
