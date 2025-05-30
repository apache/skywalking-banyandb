package encoding

import (
	"math"
)

// Float64ToUint64 converts float64 to uint64 representation
func Float64ToUint64(f float64) uint64 {
	return math.Float64bits(f)
}

// Uint64ToFloat64 converts uint64 representation back to float64
func Uint64ToFloat64(u uint64) float64 {
	return math.Float64frombits(u)
}

// Float64ToBytes converts float64 to byte array
func Float64ToBytes(dst []byte, f float64) []byte {
	u := Float64ToUint64(f)
	return Uint64ToBytes(dst, u)
}

// BytesToFloat64 converts byte array to float64
func BytesToFloat64(b []byte) float64 {
	u := BytesToUint64(b)
	return Uint64ToFloat64(u)
}
