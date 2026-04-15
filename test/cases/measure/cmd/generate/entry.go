package main

import (
	"fmt"
	"strings"
)

// GenerateEntryCode produces g.Entry lines for measure.go.
func GenerateEntryCode(cases []*TestCase) string {
	var lines []string
	for _, tc := range cases {
		if tc.Measure == nil {
			continue
		}
		line := renderEntryLine(tc)
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

func renderEntryLine(tc *TestCase) string {
	name := tc.Name
	want := name
	wantEmpty := "false"
	wantErr := "false"
	disOrder := "false"
	duration := "25 * time.Minute"
	offset := "-20 * time.Minute"

	if tc.WantErr {
		wantErr = "true"
	}
	if tc.WantEmpty {
		wantEmpty = "true"
	}
	if tc.DisOrder {
		disOrder = "true"
	}
	if tc.Duration != "" {
		duration = tc.Duration
	}

	return fmt.Sprintf(`g.Entry("%s", helpers.Args{Input: "%s", Want: "%s", WantEmpty: %s, WantErr: %s, DisOrder: %s, Duration: %s, Offset: %s})`,
		name, name, want, wantEmpty, wantErr, disOrder, duration, offset)
}

// PrintEntryCode prints all entry lines to stdout.
func PrintEntryCode(cases []*TestCase) {
	fmt.Println("\n=== Entry Code for measure.go ===")
	fmt.Println("// Paste the following lines into measureEntries:")
	fmt.Println(GenerateEntryCode(cases))
}
