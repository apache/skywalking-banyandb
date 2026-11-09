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

// Package csvfmt defines the CSV export format: the group-level column layout derived
// from schema protos, the cell value encoding with its `\N` and `\B` sentinels, and a
// rolling writer that splits one CSV stream into size-bounded files. A layout's columns
// follow resources in name order, so it does not depend on the order schemas are listed.
//
// The package must not import anything under banyand/ so that the data node, the client
// and the importer share exactly the same pure functions.
package csvfmt
