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

package bytes

type ByteBuf struct {
	b []byte
}

func (b *ByteBuf) Len() int {
	return len(b.b)
}

func (b *ByteBuf) Bytes() []byte {
	return b.b
}

func (b *ByteBuf) Append(raw []byte) (int, error) {
	b.b = append(b.b, raw...)
	return len(raw), nil
}

func (b *ByteBuf) AppendByte(ch byte) (int, error) {
	b.b = append(b.b, ch)
	return 1, nil
}

func (b *ByteBuf) AppendString(s string) (int, error) {
	b.b = append(b.b, s...)
	return len(s), nil
}

// AppendUInt64 appends uint64 with BigEndianness
func (b *ByteBuf) AppendUInt64(v uint64) (int, error) {
	b.b = append(b.b, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	return 8, nil
}

// Reset the underlying slice while keeping the capacity
func (b *ByteBuf) Reset() {
	b.b = b.b[:0]
}
