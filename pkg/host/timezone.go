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

package host

// TimeZoneName returns the IANA name of the host's local time zone, such as
// "Asia/Shanghai", or an empty string when the real name cannot be determined.
// An empty name means "unknown": callers comparing two hosts must treat it as a
// mismatch rather than as agreement.
//
// There is no portable way to ask for this. The Go runtime resolves the local
// zone but never exposes its name — time.Local.String() reports the literal
// "Local" whenever TZ is unset, which is the normal state inside a container,
// so two hosts in different zones would look identical. Each platform therefore
// resolves the name the same way its own runtime resolves the zone: see
// localZoneName in timezone_unix.go and timezone_windows.go.
func TimeZoneName() string {
	return localZoneName()
}
