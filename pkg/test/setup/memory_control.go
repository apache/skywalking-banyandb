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
	"fmt"
	"sync"

	"github.com/apache/skywalking-banyandb/banyand/protector"
)

var (
	memoryControlMu     sync.Mutex
	memoryControlByNode = map[string]int{}
)

// bindNodeMemory associates a liaison's gRPC address with the roster index of its memory protector.
func bindNodeMemory(nodeAddr string, index int) {
	memoryControlMu.Lock()
	memoryControlByNode[nodeAddr] = index
	memoryControlMu.Unlock()
}

func unbindNodeMemory(nodeAddr string) {
	memoryControlMu.Lock()
	delete(memoryControlByNode, nodeAddr)
	memoryControlMu.Unlock()
}

// SetLiaisonMemoryHigh forces the named liaison's memory protector into (or out of) the high-pressure state, so a test
// can make the liaison's trace write listener report STATUS_SERVER_BUSY. The liaison's Write handler and that listener
// share the protector, and a liaison in high pressure also sheds new streams. nodeAddr is the gRPC address returned
// from LiaisonNode or LiaisonNodeWithHTTP.
func SetLiaisonMemoryHigh(nodeAddr string, high bool) error {
	memoryControlMu.Lock()
	index, ok := memoryControlByNode[nodeAddr]
	memoryControlMu.Unlock()
	if !ok {
		return fmt.Errorf("SetLiaisonMemoryHigh: no memory protector bound to node %q", nodeAddr)
	}
	if !protector.ForceHighByIndex(index, high) {
		return fmt.Errorf("SetLiaisonMemoryHigh: protector %d of node %q does not exist", index, nodeAddr)
	}
	return nil
}
