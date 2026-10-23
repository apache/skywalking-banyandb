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

package encoding

import "testing"

func TestBytesBlockDecoderResetBoundedDropsCachedBacking(t *testing.T) {
	decoder := BytesBlockDecoder{data: make([]byte, 1, 16384)}
	decoder.ResetBounded(1024)
	if len(decoder.data) != 0 || cap(decoder.data) != 0 {
		t.Fatal("oversized decoder backing survived")
	}
	decoder.data = make([]byte, 1, 512)
	decoder.ResetBounded(1024)
	if len(decoder.data) != 0 || cap(decoder.data) != 512 {
		t.Fatal("normal decoder backing was not reused")
	}
}
