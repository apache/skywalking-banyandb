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

package config

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

// RateKind tells ParseRate which quantity grammar applies.
type RateKind int

const (
	// RateBytes is a bandwidth: the quantity is a size (bare bytes or B/KB/.../PiB).
	RateBytes RateKind = iota
	// RateRows is a row rate: the quantity is a plain decimal integer.
	RateRows
)

func (k RateKind) String() string {
	if k == RateRows {
		return "row"
	}
	return "byte"
}

var ratePeriodSeconds = map[string]float64{"s": 1, "min": 60, "h": 3600}

// ParseRate parses `<quantity>[/<period>]` and normalizes it to units per
// second: "0" is no limit, a bare number is per second, and the period is s, min or h.
// A negative value, an unknown period (including the ambiguous "m"), a byte unit on a row
// rate and a non-zero rate below 1 per second are rejected.
func ParseRate(raw string, kind RateKind) (float64, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return 0, fmt.Errorf("empty %s rate", kind)
	}
	quantity, period, hasPeriod := strings.Cut(s, "/")
	quantity = strings.TrimSpace(quantity)
	period = strings.TrimSpace(period)
	seconds := 1.0
	if hasPeriod {
		var ok bool
		if seconds, ok = ratePeriodSeconds[period]; !ok {
			if period == "m" {
				return 0, fmt.Errorf("rate %q: period \"m\" is ambiguous, use \"min\" for minutes", raw)
			}
			return 0, fmt.Errorf("rate %q: unknown period %q (s|min|h)", raw, period)
		}
	}
	if strings.HasPrefix(quantity, "-") {
		return 0, fmt.Errorf("rate %q must not be negative", raw)
	}
	var amount float64
	switch kind {
	case RateRows:
		n, err := strconv.ParseUint(quantity, 10, 64)
		if err != nil {
			if strings.IndexFunc(quantity, isLetter) >= 0 {
				return 0, fmt.Errorf("rate %q: a row rate takes a plain integer, not a byte unit", raw)
			}
			return 0, fmt.Errorf("rate %q: a row rate takes a plain integer: %w", raw, err)
		}
		amount = float64(n)
	default:
		n, err := ParseSize(quantity)
		if err != nil {
			return 0, fmt.Errorf("rate %q: %w", raw, err)
		}
		amount = float64(n)
	}
	perSecond := amount / seconds
	if perSecond > 0 && perSecond < 1 {
		return 0, fmt.Errorf("rate %q is %g %ss per second, below 1; check the unit", raw, perSecond, kind)
	}
	return perSecond, nil
}

func isLetter(r rune) bool { return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') }

// ParseSize parses a non-negative size: bare bytes or a B/KB/.../PiB unit (pkg/convert). A
// non-zero size below one byte is rejected: it would round to 0, which means no limit.
func ParseSize(raw string) (uint64, error) {
	s := strings.TrimSpace(raw)
	if strings.HasPrefix(s, "-") {
		return 0, fmt.Errorf("size %q must not be negative", raw)
	}
	n, err := convert.ParseSize(s)
	if err != nil {
		return 0, fmt.Errorf("size %q: %w", raw, err)
	}
	// A size beyond int64 converts to a CPU-dependent value, so it is refused before use.
	if n < 0 || n == math.MaxInt64 {
		return 0, fmt.Errorf("size %q is out of range", raw)
	}
	if n == 0 && strings.ContainsAny(s, "123456789") {
		return 0, fmt.Errorf("size %q is below 1 byte; check the unit", raw)
	}
	return uint64(n), nil
}
