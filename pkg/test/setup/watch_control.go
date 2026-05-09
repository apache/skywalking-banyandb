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

import (
	"errors"
	"fmt"
	"sync"

	"github.com/apache/skywalking-banyandb/banyand/metadata/schema/property"
)

// ErrWatchControlNotImplemented is the legacy sentinel from the Step 1.0
// stub era. The watch-control helpers below are now wired to a real
// SchemaRegistry handle, but the sentinel is still returned (wrapped) when
// the caller passes a node name that the harness has not registered —
// typically because PauseDataNodeWatch was called before the matching
// setup.DataNode call completed its health check, or for a liaison node
// (no SchemaRegistry roster binding).
var ErrWatchControlNotImplemented = errors.New("watch control not implemented: landed in Phase 2")

var (
	watchControlMu     sync.Mutex
	watchControlByNode = map[string]*property.SchemaRegistry{}
)

// bindNodeWatchControl associates the data node's gRPC address with its
// SchemaRegistry handle so Pause / Resume can route to the right node.
// Called from startDataNode after the health check passes.
func bindNodeWatchControl(nodeAddr string, reg *property.SchemaRegistry) {
	if reg == nil {
		return
	}
	watchControlMu.Lock()
	watchControlByNode[nodeAddr] = reg
	watchControlMu.Unlock()
}

// unbindNodeWatchControl removes the binding for the given node address.
// Called from the data node's close fn so SchemaRegistry references do
// not leak across a setup -> teardown -> setup cycle.
func unbindNodeWatchControl(nodeAddr string) {
	watchControlMu.Lock()
	delete(watchControlByNode, nodeAddr)
	watchControlMu.Unlock()
}

func lookupNodeWatchControl(nodeAddr string) (*property.SchemaRegistry, bool) {
	watchControlMu.Lock()
	defer watchControlMu.Unlock()
	reg, ok := watchControlByNode[nodeAddr]
	return reg, ok
}

// PauseDataNodeWatch suspends the named data node's schema-watch loop so
// a test can observe behavior while the node is missing schema events.
// nodeAddr is the gRPC address returned from setup.DataNodeFromDataDir
// (e.g. "127.0.0.1:31921"); pkg/test/setup binds the address during node
// startup. Returns ErrWatchControlNotImplemented (wrapped) when the
// address is not registered — typically because the node is a liaison or
// because PauseDataNodeWatch was called before the matching
// setup.DataNode invocation completed its health check.
func PauseDataNodeWatch(nodeAddr string) error {
	reg, ok := lookupNodeWatchControl(nodeAddr)
	if !ok {
		return fmt.Errorf("PauseDataNodeWatch: %w (no binding for node %q)",
			ErrWatchControlNotImplemented, nodeAddr)
	}
	reg.PauseNotifications()
	return nil
}

// ResumeDataNodeWatch resumes a previously paused data-node watch loop
// and drains any events that arrived during the pause window. Same
// lookup semantics as PauseDataNodeWatch.
func ResumeDataNodeWatch(nodeAddr string) error {
	reg, ok := lookupNodeWatchControl(nodeAddr)
	if !ok {
		return fmt.Errorf("ResumeDataNodeWatch: %w (no binding for node %q)",
			ErrWatchControlNotImplemented, nodeAddr)
	}
	reg.ResumeNotifications()
	return nil
}
