// Licensed to Apache Software Foundation (ASF) under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Apache Software Foundation (ASF) licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Package version provides functionality related to version information.
package version

import (
	"embed"
	"encoding/json"
	"errors"

	"sigs.k8s.io/yaml"
)

const (
	// VersionFilename represents the name of the version file.
	VersionFilename = "version"
	// CurrentVersion represents the current version number.
	CurrentVersion = "0.1"
	// CompatibleVersionsKey represents the key for compatible versions in versions.yml.
	CompatibleVersionsKey      = "versions"
	compatibleVersionsFilename = "versions.yml"
)

// ErrVersionIncompatible is returned when versions are not compatible.
var ErrVersionIncompatible = errors.New("version not compatible")

//go:embed versions.yml
var versionFS embed.FS

// ReadCompatibleVersions reads compatible versions from versions.yml.
func ReadCompatibleVersions() (map[string][]float64, error) {
	i, err := versionFS.ReadFile(compatibleVersionsFilename)
	if err != nil {
		return nil, err
	}
	j, err := yaml.YAMLToJSON(i)
	if err != nil {
		return nil, err
	}
	var compatibleVersions map[string][]float64
	if err := json.Unmarshal(j, &compatibleVersions); err != nil {
		return nil, err
	}
	return compatibleVersions, nil
}
