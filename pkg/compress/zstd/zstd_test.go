package zstd_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/apache/skywalking-banyandb/pkg/compress/zstd"
	"github.com/stretchr/testify/require"
)

func randString(n int) []byte {
	var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	rand.Seed(time.Now().UnixNano())

	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}

	return b
}

func TestCompressAndDecompress(t *testing.T) {
	testCases := []struct {
		name string
		data []byte
	}{
		{
			name: "SingleByte",
			data: randString(1),
		},
		{
			name: "NormalSizeBytes",
			data: randString(1000), // 1000 bytes
		},
		{
			name: "SuperBigBytes",
			data: randString(1e6), // 1 million bytes
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			compressionLevel := 3

			// Test Compress
			compressed := zstd.Compress(nil, tc.data, compressionLevel)
			require.NotEmpty(t, compressed, "Compress should return non-empty result")

			// Test Decompress
			decompressed, err := zstd.Decompress(nil, compressed)
			require.NoError(t, err, "Decompress should not return an error")
			require.Equal(t, tc.data, decompressed, "Decompressed data should be equal to the original data")
		})
	}
}
