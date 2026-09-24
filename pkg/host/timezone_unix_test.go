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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTimeZoneNameFromEnv(t *testing.T) {
	tests := []struct {
		name string
		tz   string
		want string
	}{
		{name: "iana name", tz: "Asia/Shanghai", want: "Asia/Shanghai"},
		{name: "leading colon", tz: ":Europe/Paris", want: "Europe/Paris"},
		{name: "utc", tz: "UTC", want: "UTC"},
		{name: "empty means utc", tz: "", want: "UTC"},
		{name: "absolute path names a zone file", tz: "/usr/share/zoneinfo/Asia/Tokyo", want: "Asia/Tokyo"},
		{name: "colon before absolute path", tz: ":/var/db/timezone/zoneinfo/Europe/Paris", want: "Europe/Paris"},
		// A POSIX offset string is not an IANA name, and the runtime silently falls
		// back to UTC for it, so the node must not claim the zone it spells out.
		{name: "posix offset yields no claim", tz: "CST-8", want: ""},
		{name: "unknown name yields no claim", tz: "Mars/Olympus_Mons", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TZ", tt.tz)
			assert.Equal(t, tt.want, localZoneName())
		})
	}
}

func TestTimeZoneNameFromLocaltime(t *testing.T) {
	tests := []struct {
		name     string
		zonePath string
		want     string
		chained  bool
		plain    bool
	}{
		{name: "linux layout", zonePath: "usr/share/zoneinfo/Asia/Shanghai", want: "Asia/Shanghai"},
		{name: "macos layout", zonePath: "var/db/timezone/zoneinfo/Europe/Paris", want: "Europe/Paris"},
		{name: "nested region", zonePath: "usr/share/zoneinfo/America/Argentina/Buenos_Aires", want: "America/Argentina/Buenos_Aires"},
		{name: "chain of symlinks", zonePath: "usr/share/zoneinfo/America/New_York", chained: true, want: "America/New_York"},
		{name: "plain file outside zoneinfo", zonePath: "etc/localtime", plain: true, want: ""},
		// A copy that happens to sit under a zoneinfo tree still names its zone.
		{name: "plain file under zoneinfo", zonePath: "usr/share/zoneinfo/Asia/Shanghai", plain: true, want: "Asia/Shanghai"},
		{name: "link outside zoneinfo", zonePath: "etc/localtime-copy", want: ""},
		{name: "unknown zone", zonePath: "usr/share/zoneinfo/Mars/Olympus_Mons", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withoutTZ(t)
			root := t.TempDir()
			target := filepath.Join(root, tt.zonePath)
			require.NoError(t, os.MkdirAll(filepath.Dir(target), 0o750))
			require.NoError(t, os.WriteFile(target, nil, 0o600))
			if tt.plain {
				withLocaltimePath(t, target)
				assert.Equal(t, tt.want, localZoneName())
				return
			}
			if tt.chained {
				mid := filepath.Join(root, "middle-link")
				require.NoError(t, os.Symlink(target, mid))
				target = mid
			}
			link := filepath.Join(root, "localtime")
			require.NoError(t, os.Symlink(target, link))
			withLocaltimePath(t, link)
			assert.Equal(t, tt.want, localZoneName())
		})
	}
}

// busybox and friends copy the zone data into /etc/localtime instead of linking
// it, so nothing in the path names the zone and the contents are all that is
// left to read. The file every stock image ships is UTC, and saying so is what
// lets a default deployment answer at all.
func TestTimeZoneNameFromLocaltimeContents(t *testing.T) {
	tests := []struct {
		name    string
		fixture string
		want    string
	}{
		{name: "utc zone file", fixture: "utc.tzif", want: "UTC"},
		// Permanently UTC+0 but named GMT, so the offset alone must not decide it.
		{name: "abidjan is not utc", fixture: "abidjan.tzif", want: ""},
		{name: "shanghai", fixture: "shanghai.tzif", want: ""},
		{name: "not a zone file", fixture: "", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withoutTZ(t)
			target := filepath.Join(t.TempDir(), "localtime")
			var data []byte
			if tt.fixture == "" {
				data = []byte("not a zone file")
			} else {
				var err error
				data, err = os.ReadFile(filepath.Join("testdata", tt.fixture))
				require.NoError(t, err)
			}
			require.NoError(t, os.WriteFile(target, data, 0o600))
			withLocaltimePath(t, target)
			assert.Equal(t, tt.want, localZoneName())
		})
	}
}

// The zone file reached through a symlink is read the same way, so an image that
// links /etc/localtime at a path with no zoneinfo element still answers.
func TestTimeZoneNameFromSymlinkedUTCFile(t *testing.T) {
	withoutTZ(t)
	root := t.TempDir()
	target := filepath.Join(root, "utc-copy")
	data, err := os.ReadFile(filepath.Join("testdata", "utc.tzif"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(target, data, 0o600))
	link := filepath.Join(root, "localtime")
	require.NoError(t, os.Symlink(target, link))
	withLocaltimePath(t, link)
	assert.Equal(t, "UTC", localZoneName())
}

// A container without TZ and without /etc/localtime must report no name at all:
// reporting "Local" here would make two nodes in different zones compare equal.
func TestTimeZoneNameUnresolvable(t *testing.T) {
	withoutTZ(t)
	withLocaltimePath(t, filepath.Join(t.TempDir(), "absent"))
	assert.Empty(t, localZoneName())
}

// Distroless images carry no /etc/localtime, so the runtime's own fallback is the
// only thing left that knows the process is on UTC.
func TestTimeZoneNameFromRuntimeFallback(t *testing.T) {
	tests := []struct {
		name        string
		runtimeZone string
		want        string
	}{
		{name: "runtime fell back to utc", runtimeZone: "UTC", want: "UTC"},
		// "Local" means the runtime loaded a zone file it cannot name, which is not
		// a claim this can turn into an IANA name.
		{name: "runtime says local", runtimeZone: "Local", want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withoutTZ(t)
			withRuntimeZone(t, tt.runtimeZone)
			withLocaltimePath(t, filepath.Join(t.TempDir(), "absent"))
			assert.Equal(t, tt.want, localZoneName())
		})
	}
}

func withoutTZ(t *testing.T) {
	t.Helper()
	// t.Setenv registers the restore; Unsetenv afterwards leaves TZ unset for the test.
	t.Setenv("TZ", "")
	require.NoError(t, os.Unsetenv("TZ"))
	// Pin the runtime's own answer so a case that expects "unknown" cannot be
	// rescued by the runner happening to sit in UTC.
	withRuntimeZone(t, "Local")
}

func withRuntimeZone(t *testing.T, name string) {
	t.Helper()
	original := runtimeZoneName
	runtimeZoneName = func() string { return name }
	t.Cleanup(func() { runtimeZoneName = original })
}

func withLocaltimePath(t *testing.T, path string) {
	t.Helper()
	original := localtimePath
	localtimePath = path
	t.Cleanup(func() { localtimePath = original })
}
