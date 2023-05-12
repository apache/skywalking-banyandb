package run

import (
	"fmt"
	"math"

	"github.com/apache/skywalking-banyandb/pkg/convert"
)

// Bytes is a custom type to store memory size in bytes.
type Bytes int64

// String returns a string representation of the Bytes value.
func (b Bytes) String() string {
	suffixes := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
	size := float64(b)
	base := 1024.0
	if size < base {
		return fmt.Sprintf("%.0f%s", size, suffixes[0])
	}

	exp := int(math.Log(size) / math.Log(base))
	result := size / math.Pow(base, float64(exp))
	return fmt.Sprintf("%.2f%s", result, suffixes[exp])
}

// Set sets the Bytes value from the input string.
func (b *Bytes) Set(s string) error {
	size, err := convert.ParseSize(s)
	if err != nil {
		return err
	}
	*b = Bytes(size)
	return nil
}

// Type returns the type name of the Bytes custom type.
func (b *Bytes) Type() string {
	return "bytes"
}
