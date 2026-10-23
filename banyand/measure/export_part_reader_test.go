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

package measure

import (
	"github.com/apache/skywalking-banyandb/banyand/transfer/export"
	"github.com/apache/skywalking-banyandb/banyand/transfer/export/csv"
)

// The export unit reads part metadata, index-mode rows and Top-N merges through these
// services; it finds the last two by type assertion, so a signature drift fails here.
var (
	_ export.PartReader   = (*standalone)(nil)
	_ export.PartReader   = (*dataSVC)(nil)
	_ csv.IndexModeReader = (*standalone)(nil)
	_ csv.IndexModeReader = (*dataSVC)(nil)
	_ csv.TopNMerger      = (*standalone)(nil)
	_ csv.TopNMerger      = (*dataSVC)(nil)
)
