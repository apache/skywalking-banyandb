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

package storage

import (
	"embed"
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
	"sigs.k8s.io/yaml"
)

const (
	metadataFilename           = "metadata"
	currentVersion             = "1.2.0"
	compatibleVersionsKey      = "versions"
	compatibleVersionsFilename = "versions.yml"
)

var errVersionIncompatible = errors.New("version not compatible")

var compatibleVersions = readCompatibleVersions()

//go:embed versions.yml
var versionFS embed.FS

func checkVersion(version string) error {
	for _, v := range compatibleVersions {
		if v == version {
			return nil
		}
	}
	return errors.WithMessagef(errVersionIncompatible, "incompatible version %s, supported versions: %s", version, strings.Join(compatibleVersions, ", "))
}

func readCompatibleVersions() []string {
	i, err := versionFS.ReadFile(compatibleVersionsFilename)
	if err != nil {
		panic(err)
	}
	j, err := yaml.YAMLToJSON(i)
	if err != nil {
		panic(err)
	}
	var compatibleVersions map[string][]string
	if err := json.Unmarshal(j, &compatibleVersions); err != nil {
		panic(err)
	}
	vv, ok := compatibleVersions[compatibleVersionsKey]
	if !ok {
		panic("versions not found")
	}
	return vv
}
