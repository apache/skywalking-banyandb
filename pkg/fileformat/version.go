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

// Package fileformat holds the on-disk segment file format version line. It is a
// leaf package so that both the storage engine and the low-level node types in
// api/common can report the same versions without importing each other.
package fileformat

import (
	"embed"
	"encoding/json"
	"fmt"
	"slices"

	"sigs.k8s.io/yaml"
)

// CurrentVersion is the version string a freshly-written segment must carry.
const CurrentVersion = "1.5.0"

const (
	versionsFile = "versions.yml"
	versionsKey  = "versions"
)

//go:embed versions.yml
var versionFS embed.FS

var compatibleVersions = readCompatibleVersions()

// CompatibleVersions returns every segment file format version this build reads.
// The result is a copy: callers hand it straight to proto messages, and one of
// them appending to it would rewrite the list for the whole process.
func CompatibleVersions() []string {
	return slices.Clone(compatibleVersions)
}

// readCompatibleVersions panics rather than returning an error: versions.yml is
// embedded at build time, so a failure here means the binary itself is built
// wrong and must not start. The messages name the file and the key so that is
// obvious from the crash alone.
func readCompatibleVersions() []string {
	i, err := versionFS.ReadFile(versionsFile)
	if err != nil {
		panic(fmt.Sprintf("fileformat: cannot read embedded %s: %v", versionsFile, err))
	}
	j, err := yaml.YAMLToJSON(i)
	if err != nil {
		panic(fmt.Sprintf("fileformat: embedded %s is not valid YAML: %v", versionsFile, err))
	}
	var versions map[string][]string
	if err := json.Unmarshal(j, &versions); err != nil {
		panic(fmt.Sprintf("fileformat: embedded %s does not decode into map[string][]string: %v", versionsFile, err))
	}
	vv, ok := versions[versionsKey]
	if !ok {
		panic(fmt.Sprintf("fileformat: embedded %s has no %q key", versionsFile, versionsKey))
	}
	return vv
}
