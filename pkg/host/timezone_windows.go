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

import (
	"golang.org/x/sys/windows/registry"
)

// tzInfoKey is where Windows records the zone the whole machine runs on. It is a
// variable so tests can point it at a fixture key.
var tzInfoKey = `SYSTEM\CurrentControlSet\Control\TimeZoneInformation`

// localZoneName reads the Windows zone ID from the registry and translates it to
// an IANA name.
//
// It deliberately ignores TZ: the Go runtime's initLocal on Windows calls
// GetTimeZoneInformation and never looks at TZ, so honouring it here would name
// a zone the process is not actually running in. It also does not validate the
// result with time.LoadLocation, because Windows ships no zoneinfo database —
// the CLDR table is what makes a Windows ID mean exactly one IANA zone.
func localZoneName() string {
	k, err := registry.OpenKey(registry.LOCAL_MACHINE, tzInfoKey, registry.QUERY_VALUE)
	if err != nil {
		return ""
	}
	defer func() {
		_ = k.Close()
	}()
	// TimeZoneKeyName is the invariant zone ID. StandardName sits next to it but is
	// localised, so it cannot be looked up in the CLDR table.
	id, _, err := k.GetStringValue("TimeZoneKeyName")
	if err != nil {
		return ""
	}
	return windowsToIANA[id]
}
