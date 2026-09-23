package inverted

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

type reviewDocument struct {
	fields []segmentField
}

func (d *reviewDocument) Analyze() {}

func (d *reviewDocument) EachField(visit segmentVisitField) {
	for _, field := range d.fields {
		visit(field)
	}
}

func (d *reviewDocument) Timestamp() int64 { return 0 }

type reviewField struct {
	name      string
	value     []byte
	terms     []reviewTerm
	index     bool
	store     bool
	docValues bool
}

func (f *reviewField) Name() string         { return f.name }
func (f *reviewField) Length() int          { return len(f.terms) }
func (f *reviewField) Value() []byte        { return f.value }
func (f *reviewField) Index() bool          { return f.index }
func (f *reviewField) Store() bool          { return f.store }
func (f *reviewField) IndexDocValues() bool { return f.docValues }
func (f *reviewField) EachTerm(visit segmentVisitTerm) {
	for termIndex := range f.terms {
		visit(&f.terms[termIndex])
	}
}

type reviewTerm struct {
	value     []byte
	frequency int
}

func (t *reviewTerm) Term() []byte                      { return t.value }
func (t *reviewTerm) Frequency() int                    { return t.frequency }
func (t *reviewTerm) EachLocation(segmentVisitLocation) {}

func reviewIDField(id string) *reviewField {
	return &reviewField{
		name: docIDField, value: []byte(id), index: true, store: true,
		terms: []reviewTerm{{value: []byte(id), frequency: 1}},
	}
}

func reviewSegmentDocument(id string, fields ...*reviewField) segmentDocument {
	allFields := make([]segmentField, 0, len(fields)+1)
	allFields = append(allFields, reviewIDField(id))
	for _, field := range fields {
		allFields = append(allFields, field)
	}
	return &reviewDocument{fields: allFields}
}

func reviewPostingFrequency(t *testing.T, value segmentValue, field, term string) int {
	t.Helper()
	dictionary, dictionaryErr := value.Dictionary(field)
	require.NoError(t, dictionaryErr)
	postings, postingsErr := dictionary.PostingsList([]byte(term), nil, nil)
	require.NoError(t, postingsErr)
	iterator, iteratorErr := postings.Iterator(true, true, false, nil)
	require.NoError(t, iteratorErr)
	posting, nextErr := iterator.Next()
	require.NoError(t, nextErr)
	require.NotNil(t, posting)
	frequencies := posting.Frequency()
	require.NoError(t, iterator.Close())
	require.NoError(t, dictionary.Close())
	return frequencies
}

func reviewRoundTrip(t *testing.T, value segmentValue) segmentValue {
	t.Helper()
	var payload bytes.Buffer
	_, writeErr := value.WriteTo(&payload, nil)
	require.NoError(t, writeErr)
	loaded, loadErr := nativeSegmentPluginLoad(newSegmentBytes(payload.Bytes()))
	require.NoError(t, loadErr)
	return loaded
}

func reviewMerge(t *testing.T, value segmentValue) segmentValue {
	t.Helper()
	merger := nativeSegmentPluginMerge([]segmentValue{value}, nil, 0)
	var payload bytes.Buffer
	_, writeErr := merger.WriteTo(&payload, nil)
	require.NoError(t, writeErr)
	merged, loadErr := nativeSegmentPluginLoad(newSegmentBytes(payload.Bytes()))
	require.NoError(t, loadErr)
	return merged
}

func TestNativePluginReviewPreservesTermFrequencyAcrossLifecycle(t *testing.T) {
	document := reviewSegmentDocument("id", &reviewField{
		name: "frequency", value: []byte("raw"), index: true, store: true,
		terms: []reviewTerm{{value: []byte("term"), frequency: 3}},
	})
	built, _, buildErr := nativeSegmentPluginNew([]segmentDocument{document}, nil)
	require.NoError(t, buildErr)
	for _, value := range []segmentValue{built, reviewRoundTrip(t, built), reviewMerge(t, built)} {
		require.Equal(t, 3, reviewPostingFrequency(t, value, "frequency", "term"))
		stats, statsErr := value.CollectionStats("frequency")
		require.NoError(t, statsErr)
		require.Equal(t, uint64(3), stats.SumTotalTermFrequency())
	}
}

func TestNativePluginReviewKeepsEmptyIndexedFieldsAcrossLifecycle(t *testing.T) {
	document := reviewSegmentDocument("id", &reviewField{name: "empty", value: []byte("raw"), index: true})
	built, _, buildErr := nativeSegmentPluginNew([]segmentDocument{document}, nil)
	require.NoError(t, buildErr)
	for _, value := range []segmentValue{built, reviewRoundTrip(t, built), reviewMerge(t, built)} {
		require.Contains(t, value.Fields(), "empty")
		dictionary, dictionaryErr := value.Dictionary("empty")
		require.NoError(t, dictionaryErr)
		iterator := dictionary.Iterator(nil, nil, nil)
		entry, nextErr := iterator.Next()
		require.NoError(t, nextErr)
		require.Nil(t, entry)
		require.NoError(t, iterator.Close())
		require.NoError(t, dictionary.Close())
	}
}

func TestNativePluginReviewMergesRepeatedFieldModalitiesInEitherOrder(t *testing.T) {
	for _, order := range []string{"indexed-first", "stored-first"} {
		t.Run(order, func(t *testing.T) {
			indexed := &reviewField{
				name: "mixed", value: []byte("indexed-value"), index: true,
				terms: []reviewTerm{{value: []byte("needle"), frequency: 1}},
			}
			stored := &reviewField{name: "mixed", value: []byte("stored-value"), store: true}
			fields := []*reviewField{indexed, stored}
			if order == "stored-first" {
				fields = []*reviewField{stored, indexed}
			}
			built, _, buildErr := nativeSegmentPluginNew([]segmentDocument{reviewSegmentDocument("id", fields...)}, nil)
			require.NoError(t, buildErr)
			merged := reviewMerge(t, built)
			require.Equal(t, []uint64{0}, nidx02bDocsMatching(t, merged, "mixed", "needle"))
			var storedValue []byte
			require.NoError(t, merged.VisitStoredFields(0, func(name string, value []byte) bool {
				if name == "mixed" {
					storedValue = append([]byte(nil), value...)
				}
				return true
			}))
			require.Equal(t, []byte("stored-value"), storedValue)
		})
	}
}

type reviewPrefixAutomaton struct{ prefix []byte }

func (a reviewPrefixAutomaton) Start() int               { return 0 }
func (a reviewPrefixAutomaton) IsMatch(state int) bool   { return state >= len(a.prefix) }
func (a reviewPrefixAutomaton) CanMatch(state int) bool  { return state >= 0 }
func (a reviewPrefixAutomaton) WillAlwaysMatch(int) bool { return false }
func (a reviewPrefixAutomaton) Accept(state int, value byte) int {
	if state == len(a.prefix) {
		return state
	}
	if state < 0 || state >= len(a.prefix) || a.prefix[state] != value {
		return -1
	}
	return state + 1
}

func TestNativePluginReviewDictionaryHonorsAutomaton(t *testing.T) {
	built, _, buildErr := nativeSegmentPluginNew([]segmentDocument{
		reviewSegmentDocument("id-0", &reviewField{
			name: "terms", index: true,
			terms: []reviewTerm{{value: []byte("apple"), frequency: 1}, {value: []byte("apply"), frequency: 1}, {value: []byte("banana"), frequency: 1}},
		}),
	}, nil)
	require.NoError(t, buildErr)
	dictionary, dictionaryErr := built.Dictionary("terms")
	require.NoError(t, dictionaryErr)
	iterator := dictionary.Iterator(reviewPrefixAutomaton{prefix: []byte("app")}, nil, nil)
	var terms []string
	for {
		entry, nextErr := iterator.Next()
		require.NoError(t, nextErr)
		if entry == nil {
			break
		}
		terms = append(terms, entry.Term())
	}
	require.NoError(t, iterator.Close())
	require.NoError(t, dictionary.Close())
	require.Equal(t, []string{"apple", "apply"}, terms)
}

type reviewChunkWriter struct {
	bytes.Buffer
	maxWrite int
	closeCh  chan struct{}
	closeOn  int
	calls    int
}

func (w *reviewChunkWriter) Write(payload []byte) (int, error) {
	w.calls++
	if w.closeOn > 0 && w.calls == w.closeOn {
		close(w.closeCh)
	}
	if len(payload) > w.maxWrite {
		return 0, io.ErrShortWrite
	}
	return w.Buffer.Write(payload)
}

func TestNativePluginReviewMergeHonorsBufferSizeAndCancellationBetweenWrites(t *testing.T) {
	built, _, buildErr := nativeSegmentPluginNew([]segmentDocument{
		reviewSegmentDocument("id", &reviewField{name: "stored", value: bytes.Repeat([]byte("value"), 100), store: true}),
	}, nil)
	require.NoError(t, buildErr)

	merger := nativeSegmentPluginMerge([]segmentValue{built}, nil, 7)
	writer := &reviewChunkWriter{maxWrite: 7, closeCh: make(chan struct{})}
	_, writeErr := merger.WriteTo(writer, nil)
	require.NoError(t, writeErr)
	require.Greater(t, writer.calls, 1)

	cancelMerger := nativeSegmentPluginMerge([]segmentValue{built}, nil, 7)
	cancelWriter := &reviewChunkWriter{maxWrite: 7, closeCh: make(chan struct{}), closeOn: 1}
	_, cancelErr := cancelMerger.WriteTo(cancelWriter, cancelWriter.closeCh)
	require.Error(t, cancelErr)
	require.Less(t, cancelWriter.Len(), writer.Len())
}
