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

// Package exporter is the client-side orchestration of data export: the connection
// preflight against the liaisons, the dry-run Plan and its report, the export session library
// (create, list, heartbeat, release), and RunExport, which writes the schema, drives one
// Export stream per data node, dealt round robin over the liaisons, verifies and persists
// every file and seals manifest.json. The settings are resolved by pkg/transfer/config. It talks to the
// liaison only and never depends on any banyand/ storage package.
package exporter
