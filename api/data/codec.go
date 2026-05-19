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

import (
	"fmt"
	"sync/atomic"

	"google.golang.org/protobuf/proto"
)

// RawFrameMagicLeadingByte is the mandatory first byte of every raw vec
// columnar frame body. It is a complete varint tag decoding to field number 0,
// which protowire.ConsumeTag rejects with errCodeFieldNumber and proto/decode.go
// converts into a hard errDecode before any unknown-field skip (verified against
// google.golang.org/protobuf@v1.36.11). Emitting this leading byte
// deterministically forces a flag-off node's proto.Unmarshal of a raw frame
// into measurev1.InternalQueryResponse{} to return a non-nil error, collapsing
// the "garbage-but-parsed silently-empty" outcome into the already-loud decode
// outcome (G9f spec Principle 3). The full frame magic/wire-version body is
// produced and consumed by G9f.2/G9f.3; G9f.0 only wires the passthrough plus
// this contract byte.
const RawFrameMagicLeadingByte byte = 0x00

// ResponseCodec encodes and decodes the clusterv1.SendResponse.body for a
// single topic. The ~18 proto topics use a behavior-preserving ProtoCodec;
// TopicInternalMeasureQuery uses a wire-mode-dispatching codec that selects
// RawFrameCodec when the process is flag-on and ProtoCodec when flag-off
// (topic-AND-process-wire-mode selection — one static supplier serves both
// flag-on raw bodies and flag-off proto bodies on the same topic).
type ResponseCodec interface {
	// Marshal encodes a response value into the SendResponse.body bytes.
	Marshal(any) ([]byte, error)
	// Unmarshal decodes the SendResponse.body bytes into a response value.
	Unmarshal([]byte) (any, error)
}

// ProtoCodec is the behavior-preserving codec for every proto-bodied topic. It
// marshals and unmarshals byte-identically to the pre-G9f.0
// proto.Marshal/proto.Unmarshal path: Marshal calls proto.Marshal on the
// supplied proto.Message; Unmarshal allocates a fresh message via the supplier
// and proto.Unmarshal-s into it.
type ProtoCodec struct {
	newMessage func() proto.Message
}

// NewProtoCodec wraps a proto message supplier in a ProtoCodec.
func NewProtoCodec(newMessage func() proto.Message) *ProtoCodec {
	return &ProtoCodec{newMessage: newMessage}
}

// Marshal proto-marshals the value, byte-identical to the pre-refactor path.
func (c *ProtoCodec) Marshal(v any) ([]byte, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("ProtoCodec: expected proto.Message, got %T", v)
	}
	return proto.Marshal(m)
}

// Unmarshal proto-unmarshals into a fresh message from the supplier,
// byte-identical to the pre-refactor path.
func (c *ProtoCodec) Unmarshal(body []byte) (any, error) {
	m := c.newMessage()
	if err := proto.Unmarshal(body, m); err != nil {
		return nil, err
	}
	return m, nil
}

// RawFrameCodec passes the SendResponse.body through as opaque bytes — the
// future vec columnar frame. It applies no proto encoding. A nil/empty body is
// a legitimate empty distributed result and is NOT magic-validated (sub.go
// sends a body-less SendResponse for an empty result and pub.go returns before
// codec dispatch on a nil body — magic-validating those legitimate empties
// would fail loud incorrectly). A non-empty body MUST begin with
// RawFrameMagicLeadingByte; the full magic/wire-version validation lands in
// G9f.3, but the leading-byte contract is enforced here so the G9f.0
// passthrough is the SOLE place the contract is asserted on decode.
type RawFrameCodec struct{}

// NewRawFrameCodec returns the opaque-bytes passthrough codec.
func NewRawFrameCodec() *RawFrameCodec {
	return &RawFrameCodec{}
}

// Marshal returns the raw frame bytes unchanged. A nil/empty value encodes to
// a nil body (a legitimate empty distributed result).
func (c *RawFrameCodec) Marshal(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("RawFrameCodec: expected []byte raw frame, got %T", v)
	}
	return b, nil
}

// Unmarshal returns the raw frame bytes unchanged. A nil/empty body is a valid
// empty result and is NOT magic-validated. A non-empty body MUST begin with
// RawFrameMagicLeadingByte or it fails loud.
func (c *RawFrameCodec) Unmarshal(body []byte) (any, error) {
	if len(body) == 0 {
		return []byte(nil), nil
	}
	if body[0] != RawFrameMagicLeadingByte {
		return nil, fmt.Errorf("RawFrameCodec: invalid raw frame magic: leading byte 0x%02x, want 0x%02x",
			body[0], RawFrameMagicLeadingByte)
	}
	return body, nil
}

// measureWireModeRaw is the per-process wire mode for TopicInternalMeasureQuery.
// It is set once at measure-service startup from --measure-vectorized-enabled
// (a per-process CLI bool with zero cluster propagation — G9f spec Principle 2).
// Default false (flag-off / proto) so any process that never sets it keeps the
// pre-G9f.0 proto behavior.
var measureWireModeRaw atomic.Bool

// SetMeasureWireModeRaw publishes the per-process wire mode for
// TopicInternalMeasureQuery. raw==true selects RawFrameCodec (flag-on,
// vec columnar frame body); raw==false selects ProtoCodec (flag-off, proto
// body). Called at measure-service startup after flags are parsed.
func SetMeasureWireModeRaw(raw bool) {
	measureWireModeRaw.Store(raw)
}

// MeasureWireModeRaw reports whether this process is flag-on (raw vec frame
// body) for TopicInternalMeasureQuery.
func MeasureWireModeRaw() bool {
	return measureWireModeRaw.Load()
}

// measureQueryResponseCodec dispatches TopicInternalMeasureQuery encode/decode
// by the per-process wire mode: flag-on → RawFrameCodec, flag-off → ProtoCodec.
// One static supplier serves both flag-on raw bodies and flag-off proto bodies
// on the same topic, so the codec is selected by topic-AND-process-wire-mode,
// not topic alone (G9f spec G9f.0 topic-AND-mode guard).
type measureQueryResponseCodec struct {
	proto *ProtoCodec
	raw   *RawFrameCodec
}

func (c *measureQueryResponseCodec) Marshal(v any) ([]byte, error) {
	if MeasureWireModeRaw() {
		return c.raw.Marshal(v)
	}
	return c.proto.Marshal(v)
}

func (c *measureQueryResponseCodec) Unmarshal(body []byte) (any, error) {
	if MeasureWireModeRaw() {
		return c.raw.Unmarshal(body)
	}
	return c.proto.Unmarshal(body)
}
