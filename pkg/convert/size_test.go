package convert_test

import (
	"testing"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

func TestParseSize(t *testing.T) {
	tests := []struct {
		input    string
		expected int64
	}{
		{"0B", 0},
		{"1KB", 1000},
		{"1MB", 1000 * 1000},
		{"1GB", 1000 * 1000 * 1000},
		{"1KiB", 1 << 10},
		{"1MiB", 1 << 20},
		{"1GiB", 1 << 30},
		{"1.5Gi", int64(1.5 * float64(1<<30))},
		{"1024 M", 1024 * 1000 * 1000},
		{"42 Ki", 42 * (1 << 10)},
	}

	for _, test := range tests {
		result, err := convert.ParseSize(test.input)
		if err != nil {
			t.Errorf("Failed to parse size for input %q: %v", test.input, err)
		} else if result != test.expected {
			t.Errorf("Incorrect result for input %q: expected %d, got %d", test.input, test.expected, result)
		}
	}
}
