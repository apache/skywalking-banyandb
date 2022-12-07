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

// Package version can be used to implement embedding versioning details from
// git branches and tags into the binary importing this package.
package version

import (
	"fmt"
	"strings"
)

// build is to be populated at build time using -ldflags -X.
var build string

// Build shows the service's build information.
func Build() string {
	return build
}

// Show the service's version information.
func Show(serviceName string) {
	fmt.Println(serviceName + " " + Parse())
}

// Parse returns the parsed service's version information. (from raw git label).
func Parse() string {
	// versionString syntax:
	//   <release tag>-<commits since release tag>-g<commit hash>-<branch name>
	v := strings.SplitN(build, "-", 4)
	// prefix v on semantic versioning tags omitting it
	// Go module tags should include the 'v'
	if len(v[0]) > 1 && strings.ToLower(v[0])[0] != 'v' {
		v[0] = "v" + v[0]
	}
	switch {
	case len(v) != 4:
		// built without using the make tooling
		return "v0.0.0-unofficial"
	case v[1] != "0":
		// built from a non release commit point
		// In the version string, the commit tag is prefixed with "-g" (which stands for "git").
		// When printing the version string, remove that prefix to just show the real commit hash.
		return fmt.Sprintf("%s-%s (%s, +%s)", v[0], v[3], v[2][1:], v[1])
	case v[3] != "master":
		// specific branch release build
		return fmt.Sprintf("%s-%s", v[0], v[3])
	default:
		// master release build
		return v[0]
	}
}
