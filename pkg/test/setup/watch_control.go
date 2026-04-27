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

package setup

import "errors"

// ErrWatchControlNotImplemented is returned by PauseDataNodeWatch and
// ResumeDataNodeWatch until the data-node watch control hooks land in
// Phase 2 (Step 2.8). Cluster-only specs in §6.12 of the schema-consistency
// plan must skip themselves when this sentinel surfaces.
var ErrWatchControlNotImplemented = errors.New("watch control not implemented: landed in Phase 2")

// PauseDataNodeWatch suspends the named data node's schema watch loop so a
// test can observe behavior while the node is missing schema events. It is a
// distributed-only hook; the standalone harness has no separate watch path.
// Phase 2 will provide a real implementation; until then it returns
// ErrWatchControlNotImplemented and callers should skip.
func PauseDataNodeWatch(_ string) error {
	return ErrWatchControlNotImplemented
}

// ResumeDataNodeWatch resumes a previously paused data-node watch loop. Same
// Phase 2 caveat as PauseDataNodeWatch: it currently returns
// ErrWatchControlNotImplemented so cluster-only specs can self-skip.
func ResumeDataNodeWatch(_ string) error {
	return ErrWatchControlNotImplemented
}
