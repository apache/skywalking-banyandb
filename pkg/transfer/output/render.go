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

// Package output renders what the data export and import commands print: tables, YAML and
// JSON, counts, sizes and times, and the live progress of a transfer on a terminal.
package output

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"sigs.k8s.io/yaml"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	"github.com/apache/skywalking-banyandb/pkg/transfer/exitcode"
)

// ValidateOutputFormat accepts the dry-run renderings table (also ""), yaml and json and
// rejects anything else as a usage error.
func ValidateOutputFormat(format string) error {
	switch format {
	case "", "table", "yaml", "json":
		return nil
	default:
		return exitcode.Exit(exitcode.ExitUsage, "unsupported output format %q (table|yaml|json)", format)
	}
}

// RenderAs writes v as two-space indented json for "json", as yaml for "yaml", and with
// table for any other format; it does not validate the format.
func RenderAs(w io.Writer, format string, v any, table func(io.Writer) error) error {
	switch format {
	case "json":
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	case "yaml":
		out, err := yaml.Marshal(v)
		if err != nil {
			return err
		}
		_, err = w.Write(out)
		return err
	default:
		return table(w)
	}
}

// HumanCount renders a count with one decimal and a K/M/G/T suffix; a value that would
// round up to 1000.0 of a unit carries into the next one (999_999 -> 1.0M).
func HumanCount(n uint64) string { return humanUnits(n, 1000, countSuffixes, "") }

// HumanRate renders a per-second rate as HumanCount renders a count, with one decimal below
// 10, so that a rate below one per second does not read as 0.
func HumanRate(perSecond float64) string {
	if perSecond < 10 {
		return strconv.FormatFloat(max(perSecond, 0), 'f', 1, 64)
	}
	return HumanCount(uint64(perSecond))
}

// HumanBytes renders bytes in IEC units with one decimal (humanize.IBytes rounds to whole
// units from 10 upwards, so 18.3 GiB would print as 18 GiB).
func HumanBytes(n uint64) string { return humanUnits(n, 1024, byteSuffixes, " ") }

// humanUnits divides n by base until it fits one decimal below base and appends that
// unit's suffix; suffixes[0] names the base unit, in which n prints as a plain integer.
func humanUnits(n uint64, base float64, suffixes []string, sep string) string {
	carry := base - 0.05
	v := float64(n)
	for i := 0; ; i++ {
		if i == len(suffixes)-1 || v < carry {
			if i == 0 {
				return strconv.FormatUint(n, 10) + sep + suffixes[i]
			}
			return fmt.Sprintf("%.1f%s%s", v, sep, suffixes[i])
		}
		v /= base
	}
}

// TimeRange renders the bounds as MM-DD, with the year when the bounds fall in different years.
func TimeRange(minTS, maxTS int64) string {
	if minTS == 0 && maxTS == 0 {
		return "-"
	}
	lo, hi := time.Unix(0, minTS).UTC(), time.Unix(0, maxTS).UTC()
	layout := "01-02"
	if lo.Year() != hi.Year() {
		layout = "2006-01-02"
	}
	return lo.Format(layout) + " ~ " + hi.Format(layout)
}

var (
	countSuffixes = []string{"", "K", "M", "G", "T"}
	byteSuffixes  = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
)

// IntervalLabel renders an interval rule as "7d" or "1h"; "" when unset.
func IntervalLabel(r *commonv1.IntervalRule) string {
	switch r.GetUnit() {
	case commonv1.IntervalRule_UNIT_DAY:
		return fmt.Sprintf("%dd", r.GetNum())
	case commonv1.IntervalRule_UNIT_HOUR:
		return fmt.Sprintf("%dh", r.GetNum())
	default:
		return ""
	}
}
