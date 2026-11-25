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

package resume

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// fieldDigest is "sha256:<hex>" of the fields, each written as its byte length, a colon and its
// bytes, so that no two field lists share a digest whatever characters the fields hold.
func fieldDigest(fields ...string) string {
	h := sha256.New()
	for _, f := range fields {
		_, _ = fmt.Fprintf(h, "%d:", len(f))
		_, _ = h.Write([]byte(f))
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

// DigestBytes is "sha256:<hex>" of raw bytes, such as a manifest file.
func DigestBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// DigestSet is the fieldDigest of the values sorted and deduplicated: the order they were given in
// does not count.
func DigestSet(values []string) string {
	return fieldDigest(slices.Compact(slices.Sorted(slices.Values(values)))...)
}

// Binding is one recorded binding of a progress file next to the value this invocation has.
type Binding struct {
	Name     string
	Recorded string
	Current  string
	// Hint explains the binding in the refusal, e.g. which options make up a digest.
	Hint string
	// Details say field by field how the binding differs, from the plain values recorded next
	// to its digest (OptionDiffs, SetDiffs, Missing). A binding with details differs even when
	// Recorded and Current are equal, so a check that is not an equality, such as Missing, is a
	// binding too; the details replace the "recorded <digest>, now <digest>" text.
	Details []string
}

// DiffBindings names every binding whose recorded and current values differ, or that has
// details, or returns "" when none does:
//
//	<name> differs: recorded <x>, now <y> (<hint>)
//	<name> differs (<hint>):
//	  <detail>
func DiffBindings(bindings []Binding) string {
	var b strings.Builder
	for _, x := range bindings {
		if x.Recorded == x.Current && len(x.Details) == 0 {
			continue
		}
		hint := ""
		if x.Hint != "" {
			hint = " (" + x.Hint + ")"
		}
		if len(x.Details) == 0 {
			fmt.Fprintf(&b, "  %s differs: recorded %s, now %s%s\n", x.Name, x.Recorded, x.Current, hint)
			continue
		}
		fmt.Fprintf(&b, "  %s differs%s:\n", x.Name, hint)
		for _, d := range x.Details {
			fmt.Fprintf(&b, "    %s\n", d)
		}
	}
	return b.String()
}

// OptionDiffs spells, in name order, every option whose recorded and current values differ:
// "compress: recorded gzip (default), now none"; a name one side lacks has the value "(none)".
// show renders a value of an option (nil: as recorded), and a value equal to the option's entry
// in defaults is marked "(default)".
func OptionDiffs(recorded, current, defaults map[string]string, show func(name, value string) string) []string {
	names := slices.Collect(maps.Keys(recorded))
	for n := range current {
		if _, ok := recorded[n]; !ok {
			names = append(names, n)
		}
	}
	slices.Sort(names)
	var out []string
	for _, n := range names {
		r, rok := recorded[n]
		c, cok := current[n]
		if rok && cok && r == c {
			continue
		}
		out = append(out, fmt.Sprintf("%s: recorded %s, now %s", n, showOption(n, r, rok, defaults, show), showOption(n, c, cok, defaults, show)))
	}
	return out
}

func showOption(name, v string, ok bool, defaults map[string]string, show func(name, value string) string) string {
	if !ok {
		return "(none)"
	}
	text := v
	if show != nil {
		text = show(name, v)
	}
	if text == "" {
		text = `""`
	}
	if d, has := defaults[name]; has && d == v {
		text += " (default)"
	}
	return text
}

// SetDiffs spells, sorted, every value only one of two sets holds: "recorded only: <v>" or
// "now only: <v>". The order and duplicates of the values do not count.
func SetDiffs(recorded, current []string) []string {
	var out []string
	for _, v := range Missing(recorded, current) {
		out = append(out, "recorded only: "+v)
	}
	for _, v := range Missing(current, recorded) {
		out = append(out, "now only: "+v)
	}
	return out
}

// Missing returns, sorted and deduplicated, the values of want that have does not hold.
func Missing(want, have []string) []string {
	var out []string
	for _, v := range slices.Compact(slices.Sorted(slices.Values(want))) {
		if !slices.Contains(have, v) {
			out = append(out, v)
		}
	}
	return out
}
