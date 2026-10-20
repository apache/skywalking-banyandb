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

//go:build race

package measure

// raceEnabled reports whether the test binary is built with the race detector.
const raceEnabled = true

// frontierSeries is the series count of the frontier memory-bound tests: the race
// build keeps it large enough for a frontier of several blocks per quarter.
const frontierSeries = 16

// mixedTailEnd is the last timestamp of the long series of mixedFixture. The race build shortens the long series of mixedFixture to just over one block (8,192 rows): still several blocks, a tail part overlapping its last block, and pages cut at 4,096 rows.
const mixedTailEnd = 8400
