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

package stream

import (
	"fmt"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/pkg/fs"
	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// NodeSelector provides functionality to locate nodes for data distribution.
type NodeSelector interface {
	Locate(group, name string, shardID, replicaID uint32) (string, error)
	fmt.Stringer
}

// newWriteQueue is like newTSTable but does not start the merge loop (or any background loops).
func newWriteQueue(fileSystem fs.FileSystem, rootPath string, p common.Position,
	l *logger.Logger, option option, m any, group string, shardID common.ShardID, getNodes func() []string,
) (*tsTable, error) {
	t, epoch, err := initTSTable(fileSystem, rootPath, p, l, option, m)
	if err != nil {
		return nil, err
	}
	t.getNodes = getNodes
	t.group = group
	t.shardID = shardID
	t.startLoopNoMerge(epoch)
	return t, nil
}
