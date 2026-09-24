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

//go:build !windows

package host

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

// localtimePath is the symlink the Go runtime itself consults when TZ is unset.
// It is a variable so tests can point it at a fixture.
var localtimePath = "/etc/localtime"

// runtimeZoneName reports the name the Go runtime gave the local zone. It is a
// variable so tests can drive the branch that reads it without depending on the
// zone the test runner happens to be in.
var runtimeZoneName = time.Local.String

// zoneinfoMarker is the last path element shared by every zoneinfo database
// layout: /usr/share/zoneinfo on Linux, /var/db/timezone/zoneinfo on macOS.
const zoneinfoMarker = "zoneinfo"

// utcName is both an IANA zone and the name the Go runtime gives the local zone
// on the branches where it gave up and fell back to UTC, which is why it turns
// up as an answer, as a probe and as a marker of that fallback.
const utcName = "UTC"

// A zone that is UTC now but not always is not UTC. January and July of every
// year in this range catch both daylight saving and the historical offset
// changes that every non-UTC zone carries.
const (
	firstProbeYear = 1970
	lastProbeYear  = 2038
)

// localZoneName reads the zone the way the Go runtime does on this platform:
// TZ when it is set, otherwise the /etc/localtime link.
func localZoneName() string {
	if tz, ok := os.LookupEnv("TZ"); ok {
		// Read TZ the way the Go runtime does: an empty value means UTC, a leading
		// colon is stripped, and an absolute path names a zone file rather than a
		// zone, so the name has to be recovered from the path.
		name := strings.TrimPrefix(tz, ":")
		if name == "" {
			return utcName
		}
		if strings.HasPrefix(name, "/") {
			name = zoneNameFromPath(name)
		}
		return validZoneName(name)
	}
	// EvalSymlinks rather than Readlink: it follows a chain of links, and on a
	// plain file it yields that file's own path, which carries no zone name.
	if target, err := filepath.EvalSymlinks(localtimePath); err == nil {
		if name := validZoneName(zoneNameFromPath(target)); name != "" {
			return name
		}
	}
	// No name anywhere in the path. Container images copy the zone file instead of
	// linking it, so the next thing to read is the file's own contents.
	if name := utcZoneName(localtimePath); name != "" {
		return name
	}
	// Distroless images ship no /etc/localtime at all, so nothing on disk can
	// answer. The runtime resolved a zone anyway, and initLocal names it "UTC"
	// only on the branches where it gave up and used UTC — a zone it loaded by
	// name keeps that name, and one loaded from /etc/localtime is called "Local".
	// So when this reports UTC, UTC is what the process is really running.
	if runtimeZoneName() == utcName {
		return utcName
	}
	return ""
}

// utcZoneName reports "UTC" when the zone file at path is the UTC zone, and an
// empty string otherwise.
//
// A copied zone file carries rules but no name, and rules cannot be reversed
// into an IANA name in general because several zones share them. UTC is the one
// case worth resolving: it is what the stock container images put in
// /etc/localtime, and it is the zone whose offset is zero and whose abbreviation
// is "UTC" at every instant — Africa/Abidjan is also permanently UTC+0 but calls
// itself GMT, so the abbreviation keeps the two apart.
func utcZoneName(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	loc, err := time.LoadLocationFromTZData("", data)
	if err != nil {
		return ""
	}
	for year := firstProbeYear; year <= lastProbeYear; year++ {
		for _, month := range [...]time.Month{time.January, time.July} {
			name, offset := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC).In(loc).Zone()
			if offset != 0 || name != utcName {
				return ""
			}
		}
	}
	return utcName
}

// zoneNameFromPath extracts the part of a zoneinfo file path that follows the
// zoneinfo directory, which is exactly the IANA name.
func zoneNameFromPath(path string) string {
	segments := strings.Split(filepath.Clean(path), "/")
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i] == zoneinfoMarker {
			return strings.Join(segments[i+1:], "/")
		}
	}
	return ""
}

func validZoneName(name string) string {
	if name == "" {
		return ""
	}
	if _, err := time.LoadLocation(name); err != nil {
		return ""
	}
	return name
}
