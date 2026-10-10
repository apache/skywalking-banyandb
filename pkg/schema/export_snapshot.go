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

package schema

import (
	"context"
	"path/filepath"

	"go.uber.org/multierr"

	"github.com/apache/skywalking-banyandb/pkg/logger"
)

// GroupLoader is the part of Repository that SnapshotGroups reads.
type GroupLoader interface {
	LoadGroup(name string) (Group, bool)
	LoadAllGroups() []Group
}

// SnapshotGroups snapshots every group that has an open tsdb into <dstRoot>/<group> through
// take and joins the failures. It is the shared body of the engines'
// export.Backend.TakeExportSnapshot: a group without a tsdb yet has no data and is
// skipped, so is a group deleted while the loop runs (its take fails and the group is
// gone, or without a tsdb, when reloaded); a canceled ctx stops the loop.
func SnapshotGroups(ctx context.Context, groups GroupLoader, l *logger.Logger, dstRoot string,
	take func(dstDir, groupName string) (bool, error),
) error {
	var err error
	for _, g := range groups.LoadAllGroups() {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		groupName := g.GetSchema().GetMetadata().GetName()
		if g.SupplyTSDB() == nil {
			l.Info().Str("group", groupName).Msg("skip export snapshot of a group without tsdb")
			continue
		}
		if _, takeErr := take(filepath.Join(dstRoot, groupName), groupName); takeErr != nil {
			if current, ok := groups.LoadGroup(groupName); !ok || current.SupplyTSDB() == nil {
				l.Info().Str("group", groupName).Msg("skip export snapshot of a group deleted meanwhile")
				continue
			}
			err = multierr.Append(err, takeErr)
		}
	}
	return err
}
