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

import "github.com/apache/skywalking-banyandb/pkg/logger"

// logForGroup returns the logger for what is logged about an event of group.
// An event of the native log group is a native log write wherever it came
// from, so its lines log under the native log service and never reach native
// storage: a failed write would otherwise log a line that is written, and
// fails, again. A liaison or a data node receives the event over the
// network, without the mark the sender's context carried, and a liaison's
// write queue looks up nodes for the group outside any request.
func logForGroup(l *logger.Logger, group string) *logger.Logger {
	if group == logger.NativeLogGroup {
		return logger.GetLogger(logger.NativeWriteModule).Named("stream")
	}
	return l
}
