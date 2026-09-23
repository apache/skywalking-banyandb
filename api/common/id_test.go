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

package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apiversion "github.com/apache/skywalking-banyandb/api/proto/banyandb"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	"github.com/apache/skywalking-banyandb/pkg/fileformat"
	"github.com/apache/skywalking-banyandb/pkg/host"
)

func TestToProtoNodeReportsVersionAndTimeZone(t *testing.T) {
	n := Node{NodeID: "10.0.0.5:17912", GrpcAddress: "10.0.0.5:17912"}

	pn := n.ToProtoNode([]databasev1.Role{databasev1.Role_ROLE_DATA})

	require.NotNil(t, pn.Version)
	assert.Equal(t, apiversion.Version, pn.Version.ApiVersion)
	assert.Equal(t, fileformat.CurrentVersion, pn.Version.FileFormatVersion)
	// The gate tests membership in this list, so a node that reports a list its own
	// version is missing from would reject data it can actually read.
	assert.Contains(t, pn.Version.CompatibleFileFormatVersion, fileformat.CurrentVersion)
	// Which name the host resolves, and when it declines to name one at all, is
	// pkg/host's contract and is tested there; here the name only has to reach the
	// wire unchanged.
	assert.Equal(t, host.TimeZoneName(), pn.TzName)
}
