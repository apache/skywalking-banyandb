// Licensed to the Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pub

import (
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/pkg/bytes"
)

var nativeReceiveFlattenPool = bytes.NewBufferPool("native-query-codec-flatten")

// nativeReceiveCodec preserves the registered protobuf wire codec while avoiding
// its 1MiB pooled flatten buffer for small fragmented native responses. The guard
// prepays the exact-length flatten copy before this codec can receive any bytes.
type nativeReceiveCodec struct {
	encoding.CodecV2
	limit int
}

func (c nativeReceiveCodec) Unmarshal(data mem.BufferSlice, value any) error {
	response, ok := value.(*clusterv1.SendResponse)
	if !ok {
		return fmt.Errorf("native receive codec requires *cluster.SendResponse, got %T", value)
	}
	if data.Len() > c.limit {
		return status.Errorf(codes.ResourceExhausted, "native response exceeds reserved message cap: %d > %d", data.Len(), c.limit)
	}
	var payload []byte
	if len(data) == 1 {
		payload = data[0].ReadOnlyData()
	} else {
		buffer := nativeReceiveFlattenPool.GenerateBounded(c.limit)
		defer nativeReceiveFlattenPool.Release(buffer)
		buffer.Buf = bytes.ResizeExact(buffer.Buf, data.Len())
		data.CopyTo(buffer.Buf)
		payload = buffer.Buf
	}
	return proto.Unmarshal(payload, response)
}
