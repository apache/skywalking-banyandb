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

package host

// Resolving a zone by name needs a zone database, and the minimal images do not
// ship one -- busybox, alpine and even ubuntu leave out /usr/share/zoneinfo.
// Without it TimeZoneName cannot confirm the zone TZ names and answers "unknown",
// and, worse, the Go runtime cannot load that zone either: it falls back to UTC,
// so a node ends up aligning its segments on a grid the operator did not ask for
// while reporting no time zone at all. The images this project publishes disagree
// among themselves about carrying a database, which is how a data node and a
// lifecycle sidecar of one cluster can read the same segment directory eight
// hours apart.
//
// Embedding it here rather than in the storage engine ties the guarantee to the
// function that needs it instead of to whichever binary happens to link the
// engine, and the engine still gets it through api/common. It costs about 400KiB.
import _ "time/tzdata"
