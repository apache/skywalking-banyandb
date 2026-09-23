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

package storage

// The segment grid is a function of time.Local: parseSegmentTime turns a segment
// directory suffix back into an instant with it. Every binary that touches
// segments therefore has to resolve the same zone, and it has to resolve the one
// the operator asked for.
//
// Resolving a zone by name needs a zone database, and the minimal images do not
// ship one -- busybox, alpine and even ubuntu leave out /usr/share/zoneinfo. With
// no database the Go runtime cannot load the zone TZ names, silently falls back
// to UTC, and the node then aligns its segments on the wrong grid while reporting
// no time zone at all. The images this project publishes disagree among
// themselves about it: the distroless one carries a database, the busybox ones do
// not, so today a data node and a lifecycle sidecar of the same cluster can read
// the same segment directory eight hours apart.
//
// Embedding the database settles that in the binary, so the answer no longer
// depends on which base image a binary happens to ship in. It costs roughly
// 400KiB. It belongs here rather than in a build tag because a tag would leave
// tests and local builds resolving zones differently from the released image,
// and this package is linked into every banyand binary but not into bydbctl.
import _ "time/tzdata"
