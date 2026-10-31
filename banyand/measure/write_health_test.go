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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/api/common"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	"github.com/apache/skywalking-banyandb/banyand/protector"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

type highPressureProtector struct {
	protector.Nop
}

func (highPressureProtector) State() protector.State { return protector.StateHigh }

func TestWriteListenersReportServerBusyUnderMemoryPressure(t *testing.T) {
	l := logger.GetLogger("test")
	standalone := setUpWriteCallback(l, nil, 90, highPressureProtector{})
	liaison := setUpWriteQueueCallback(l, nil, 90, nil, highPressureProtector{})
	for name, ce := range map[string]*common.Error{"standalone": standalone.CheckHealth(), "liaison": liaison.CheckHealth()} {
		require.NotNil(t, ce, name)
		assert.Equal(t, modelv1.Status_STATUS_SERVER_BUSY, ce.Status(), name)
	}
}
