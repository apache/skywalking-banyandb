package inverted

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/apache/skywalking-banyandb/pkg/index/inverted/internal/nativeice"
)

// This compatibility contract intentionally remains red until the native
// writer emits the documented ICE frequency stream: the retired ICE reader is
// the independent grammar oracle, not the native reader under test.
func TestNativeFrequencyStreamLoadsInLegacyICE(t *testing.T) {
	payload, encodeErr := nativeice.EncodeSegment(nativeice.Generation{Documents: []nativeice.EncodeDocument{
		{Identifier: []byte("id-0"), Fields: []nativeice.EncodeField{{Name: "keyword", Index: true, Terms: []nativeice.EncodeTerm{{Value: []byte("term"), Frequency: 3}}}}},
		{Identifier: []byte("id-1"), Fields: []nativeice.EncodeField{{Name: "keyword", Index: true, Terms: []nativeice.EncodeTerm{{Value: []byte("term"), Frequency: 1}}}}},
	}})
	require.NoError(t, encodeErr)
	loaded, loadErr := loadLegacySegment(newSegmentBytes(payload))
	require.NoError(t, loadErr)
	dictionary, dictionaryErr := loaded.Dictionary("keyword")
	require.NoError(t, dictionaryErr)
	postings, postingsErr := dictionary.PostingsList([]byte("term"), nil, nil)
	require.NoError(t, postingsErr)
	iterator, iteratorErr := postings.Iterator(true, true, false, nil)
	require.NoError(t, iteratorErr)
	frequencies := make([]int, 0, 2)
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				t.Fatalf("legacy ICE iterator panicked on missing frequency stream: %v", recovered)
			}
		}()
		for {
			posting, nextErr := iterator.Next()
			require.NoError(t, nextErr)
			if posting == nil {
				break
			}
			frequencies = append(frequencies, posting.Frequency())
		}
	}()
	require.Equal(t, []int{3, 1}, frequencies)
}
