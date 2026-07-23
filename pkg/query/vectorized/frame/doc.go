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

// Package frame defines the shared, engine-agnostic non-proto columnar binary
// frame carried as the SendResponse.body for a vec-native query topic. It is a
// copy-fork of measure's mature frame core (pkg/query/vectorized/measure/frame)
// lifted into a reusable base so the stream and trace engines do not each fork
// the fail-loud magic/version/validity-bitmap/uvarint logic. Per-engine
// bindings supply a Codec whose Magic, WireVersion and role/type wire mappings
// select their own numbering; the byte layout below is identical across engines.
//
// Wire layout:
//
//	off  len       field            description
//	---  --------  ---------------  ---------------------------------------------
//	  0  4         Magic            codec.Magic; first byte 0x00
//	                                (data.RawFrameMagicLeadingByte) forces a
//	                                flag-off proto.Unmarshal of the body to
//	                                return a non-nil error deterministically.
//	  4  1         WireVersion      codec.WireVersion.
//	  5  uvarint   NumRows          number of (active) rows.
//	  ?  uvarint   NumCols          number of column blocks.
//	  ?  ...       Columns          NumCols column blocks, in schema order.
//
// Each column block (header + body):
//
//	off  len       field            description
//	---  --------  ---------------  ---------------------------------------------
//	  0  1         Role             codec.RoleToWire(def.Role).
//	  1  1         Type             codec.TypeToWire(def.Type).
//	  2  uvarint   NameLen          length of the UTF-8 column name.
//	  ?  NameLen   Name             column name bytes.
//	  ?  uvarint   TagFamilyLen     length of the UTF-8 tag family name.
//	                                Empty for non-RoleTag columns.
//	  ?  TFL       TagFamily        tag family name bytes.
//	  ?  ⌈N/8⌉     Validity bitmap  N = NumRows; bit i set ⇒ row i is NULL
//	                                (1 = null). Empty for N=0.
//	  ?  ...       Data             type-specific; see appendColumnData.
//
// Per-type body encoding (N = NumRows; null-row slots are present but the
// validity bitmap is the source of truth for nullness):
//
//   - Int64:      N × 8 bytes little-endian.
//   - Float64:    N × 8 bytes IEEE-754 little-endian.
//   - String:     For each row: uvarint(len) + len UTF-8 bytes.
//   - Bytes:      Same shape as String, opaque bytes.
//   - TagValue:   For each row: uvarint(len) + proto.Marshal(*TagValue) bytes.
//   - FieldValue: For each row: uvarint(len) + proto.Marshal(*FieldValue) bytes.
package frame
