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

package data

// traceQueryResponseCodec dispatches TopicTraceQuery encode/decode by the
// per-process wire mode, mirroring measureQueryResponseCodec: flag-on passes the
// caller-supplied columnar frame ([]byte) through RawFrameCodec; flag-off (and
// any proto.Message the caller still emits, e.g. the tracing path) keeps the
// byte-identical proto body via ProtoCodec.
type traceQueryResponseCodec struct {
	proto *ProtoCodec
	raw   *RawFrameCodec
}

// Marshal encodes the response. In raw wire mode a []byte body (the columnar
// trace frame) is passed through unchanged; everything else (proto.Message) goes
// through the proto path, covering both flag-off and the proto fallback.
func (c *traceQueryResponseCodec) Marshal(v any) ([]byte, error) {
	if TraceWireModeRaw() {
		if b, ok := v.([]byte); ok {
			return c.raw.Marshal(b)
		}
	}
	return c.proto.Marshal(v)
}

// Unmarshal decodes the response. In raw wire mode a magic-prefixed body is
// returned as []byte (the columnar frame, decoded downstream by the liaison);
// otherwise it is proto-unmarshaled.
func (c *traceQueryResponseCodec) Unmarshal(body []byte) (any, error) {
	if TraceWireModeRaw() && len(body) > 0 && body[0] == RawFrameMagicLeadingByte {
		return c.raw.Unmarshal(body)
	}
	return c.proto.Unmarshal(body)
}
