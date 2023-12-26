// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package stream

type partMetadata struct {
	CompressedSizeBytes   uint64
	UncompressedSizeBytes uint64
	TotalCount            uint64
	BlocksCount           uint64
	MinTimestamp          int64
	MaxTimestamp          int64
	Version               int64
}

func (ph *partMetadata) reset() {
	ph.CompressedSizeBytes = 0
	ph.UncompressedSizeBytes = 0
	ph.TotalCount = 0
	ph.BlocksCount = 0
	ph.MinTimestamp = 0
	ph.MaxTimestamp = 0
	ph.Version = 0
}
