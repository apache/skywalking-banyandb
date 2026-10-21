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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	clusterv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/cluster/v1"
	"github.com/apache/skywalking-banyandb/pkg/pool"
)

func TestNativeReceiveCodecPreservesProtobufAndOwnsDecodedPayload(t *testing.T) {
	registered := encoding.GetCodecV2("proto")
	input := &clusterv1.SendResponse{Body: make([]byte, 64<<10)}
	input.ProtoReflect().SetUnknown([]byte{0x78, 0x07})
	for i := range input.Body {
		input.Body[i] = byte(i)
	}
	wire, err := proto.Marshal(input)
	require.NoError(t, err)
	codec := nativeReceiveCodec{CodecV2: registered, limit: len(wire)}
	require.Equal(t, registered.Name(), codec.Name())
	for _, fragmented := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "fragmented"}[fragmented], func(t *testing.T) {
			data := mem.BufferSlice{mem.SliceBuffer(wire)}
			if fragmented {
				data = mem.BufferSlice{mem.SliceBuffer(wire[:17]), mem.SliceBuffer(wire[17:])}
			}
			var actual, expected clusterv1.SendResponse
			require.NoError(t, registered.Unmarshal(data, &expected))
			require.NoError(t, codec.Unmarshal(data, &actual))
			require.True(t, proto.Equal(&expected, &actual))
			actual.Body[0] ^= 0xff
			require.Equal(t, byte(0), input.Body[0])
			require.NoError(t, codec.Unmarshal(data, &actual))
			require.True(t, proto.Equal(input, &actual), "decoding must not retain a mutable view of receive bytes")
		})
	}
	request := &clusterv1.SendRequest{Body: []byte("request")}
	encoded, err := codec.Marshal(request)
	require.NoError(t, err)
	defer encoded.Free()
	expectedWire, err := proto.Marshal(request)
	require.NoError(t, err)
	require.Equal(t, expectedWire, encoded.Materialize(), "outbound protobuf codec remains unchanged")
}

type unreadNativeBuffer struct {
	mem.Buffer
	length int
}

func (b unreadNativeBuffer) Len() int           { return b.length }
func (unreadNativeBuffer) ReadOnlyData() []byte { panic("must reject before touching payload") }

func TestNativeReceiveCodecRejectsBeforeFlattenAndPreservesMalformedError(t *testing.T) {
	registered := encoding.GetCodecV2("proto")
	codec := nativeReceiveCodec{CodecV2: registered, limit: 1024}
	data := mem.BufferSlice{unreadNativeBuffer{length: 1024}, unreadNativeBuffer{length: 1}}
	err := codec.Unmarshal(data, &clusterv1.SendResponse{})
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.ErrorContains(t, codec.Unmarshal(nil, &clusterv1.SendRequest{}), "requires *cluster.SendResponse")
	malformed := mem.BufferSlice{mem.SliceBuffer([]byte{0xff, 0xff})}
	var actual, expected clusterv1.SendResponse
	expectedErr := registered.Unmarshal(malformed, &expected)
	err = codec.Unmarshal(malformed, &actual)
	require.Error(t, expectedErr)
	require.Equal(t, expectedErr.Error(), err.Error())
}

func TestNativeReceiveCodecFlattenPoolBoundAndRelease(t *testing.T) {
	const name = "native-query-codec-flatten"
	before := pool.AllRefsCount()[name]
	// A previous larger query may leave a cached backing; the next query must
	// discard it before attaching the buffer to its smaller prepaid allowance.
	oversized := nativeReceiveFlattenPool.Generate()
	oversized.Buf = make([]byte, 1<<20)
	nativeReceiveFlattenPool.Release(oversized)
	codec := nativeReceiveCodec{CodecV2: encoding.GetCodecV2("proto"), limit: 1024}
	validWire, err := proto.Marshal(&clusterv1.SendResponse{Body: []byte("x")})
	require.NoError(t, err)
	for _, wire := range [][]byte{validWire, {0xff, 0xff}} {
		fragments := mem.BufferSlice{mem.SliceBuffer(wire[:1]), mem.SliceBuffer(wire[1:])}
		_ = codec.Unmarshal(fragments, &clusterv1.SendResponse{})
		require.Equal(t, before, pool.AllRefsCount()[name], "success and malformed payload must release flatten ownership")
		cached := nativeReceiveFlattenPool.Generate()
		require.LessOrEqual(t, cap(cached.Buf), codec.limit)
		nativeReceiveFlattenPool.Release(cached)
	}
}
