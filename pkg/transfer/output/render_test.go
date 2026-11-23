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

package output

import (
	"testing"
	"time"
)

func TestHumanUnits(t *testing.T) {
	for n, want := range map[uint64]string{
		999: "999", 1_000: "1.0K", 999_949: "999.9K", 999_999: "1.0M", 118_200_000: "118.2M", 1_400_000_000: "1.4G", 999_999_999_999: "1.0T",
	} {
		if got := HumanCount(n); got != want {
			t.Fatalf("HumanCount(%d) = %q, want %q", n, got, want)
		}
	}
	for n, want := range map[uint64]string{
		0: "0 B", 1023: "1023 B", 1024: "1.0 KiB", 1536: "1.5 KiB", 19_649_000_000: "18.3 GiB", 1 << 40: "1.0 TiB",
	} {
		if got := HumanBytes(n); got != want {
			t.Fatalf("HumanBytes(%d) = %q, want %q", n, got, want)
		}
	}
	if TimeRange(0, 0) != "-" {
		t.Fatal("an empty range renders as a dash")
	}
	if got := TimeRange(1_700_000_000_000_000_000, 1_700_086_400_000_000_000); got != "11-14 ~ 11-15" {
		t.Fatalf("same year renders MM-DD: %q", got)
	}
	if got := TimeRange(1_700_000_000_000_000_000, 1_704_067_200_000_000_000); got != "2023-11-14 ~ 2024-01-01" {
		t.Fatalf("different years render the year: %q", got)
	}
}

// Durations round to a tenth of a second below a minute and to whole seconds from a minute
// on: 59.95s rounds up into the minute.
func TestFormatDuration_AroundTheMinute(t *testing.T) {
	for _, c := range []struct {
		want string
		d    time.Duration
	}{
		{d: 59*time.Second + 900*time.Millisecond, want: "59.9s"},
		{d: 59*time.Second + 949*time.Millisecond, want: "59.9s"},
		{d: 59*time.Second + 950*time.Millisecond, want: "1m0s"},
		{d: time.Minute, want: "1m0s"},
		{d: time.Minute + 449*time.Millisecond, want: "1m0s"},
		{d: time.Minute + 450*time.Millisecond, want: "1m1s"},
		{d: 89*time.Second + 500*time.Millisecond, want: "1m30s"},
	} {
		if got := FormatElapsed(c.d); got != c.want {
			t.Fatalf("FormatElapsed(%s) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	for _, c := range []struct {
		want string
		d    time.Duration
	}{
		{d: 0, want: "0.0s"},
		{d: 340 * time.Millisecond, want: "0.3s"},
		{d: 59 * time.Second, want: "59.0s"},
		{d: 59*time.Second + 960*time.Millisecond, want: "1m0s"},
		{d: time.Minute, want: "1m0s"},
		{d: 125*time.Second + 600*time.Millisecond, want: "2m6s"},
		{d: 2 * time.Hour, want: "2h0m0s"},
	} {
		if got := FormatElapsed(c.d); got != c.want {
			t.Fatalf("FormatElapsed(%s) = %q, want %q", c.d, got, c.want)
		}
	}
}

func TestHumanRate(t *testing.T) {
	for _, tc := range []struct {
		want string
		rate float64
	}{
		{rate: 0, want: "0.0"},
		{rate: -1, want: "0.0"},
		{rate: 0.53, want: "0.5"},
		{rate: 2.96, want: "3.0"},
		{rate: 9.94, want: "9.9"},
		{rate: 10, want: "10"},
		{rate: 1234, want: "1.2K"},
		{rate: 2_500_000, want: "2.5M"},
	} {
		if got := HumanRate(tc.rate); got != tc.want {
			t.Errorf("HumanRate(%v) = %q, want %q", tc.rate, got, tc.want)
		}
	}
}
