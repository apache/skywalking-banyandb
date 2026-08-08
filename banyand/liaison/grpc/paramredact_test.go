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

package grpc

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
)

func strParam(value string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Str{Str: &modelv1.Str{Value: value}}}
}

func intParam(value int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Int{Int: &modelv1.Int{Value: value}}}
}

func strArrayParam(values ...string) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{StrArray: &modelv1.StrArray{Value: values}}}
}

func intArrayParam(values ...int64) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{IntArray: &modelv1.IntArray{Value: values}}}
}

func binaryParam(value []byte) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: value}}
}

func timestampParam(at time.Time) *modelv1.TagValue {
	return &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{Timestamp: timestamppb.New(at)}}
}

func TestParseParamModeAcceptsEveryDocumentedMode(t *testing.T) {
	for _, want := range []paramMode{paramModeNone, paramModeFingerprint, paramModeRaw} {
		got, err := parseParamMode(string(want))
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}
}

func TestParseParamModeRejectsUnknownMode(t *testing.T) {
	_, err := parseParamMode("verbose")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "none|fingerprint|raw",
		"the error must tell the operator what the valid values are")
}

func TestRedactParamsNoneRendersNothing(t *testing.T) {
	assert.Empty(t, redactParams(paramModeNone, []*modelv1.TagValue{strParam("secret"), intParam(7)}),
		"none must not leak even the parameter count")
}

func TestRedactParamsEmptyInputRendersNothing(t *testing.T) {
	assert.Empty(t, redactParams(paramModeFingerprint, nil))
}

// Numeric, timestamp and null parameters are what explain a slow query (window width,
// LIMIT, thresholds) and carry no user-identifying content, so every mode above none
// renders them verbatim.
func TestRedactParamsRendersNonSensitiveTypesVerbatimInEveryMode(t *testing.T) {
	ts := timestamppb.New(time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC))
	params := []*modelv1.TagValue{
		intParam(100000),
		{Value: &modelv1.TagValue_Timestamp{Timestamp: ts}},
		{Value: &modelv1.TagValue_Null{}},
	}
	for _, mode := range []paramMode{paramModeFingerprint, paramModeRaw} {
		got := redactParams(mode, params)
		assert.Contains(t, got, "100000", "mode %s must keep the numeric value", mode)
		assert.Contains(t, got, "2026-08-04T10:00:00Z", "mode %s must keep the timestamp", mode)
		assert.Contains(t, got, "null", "mode %s must keep the null marker", mode)
	}
}

func TestRedactParamsFingerprintKeepsTheLengthAlongsideTheDigest(t *testing.T) {
	got := redactParams(paramModeFingerprint, []*modelv1.TagValue{strParam("checkout-svc")})
	assert.Equal(t, "str(len=12):fp="+fingerprint("checkout-svc"), got)
	assert.NotContains(t, got, "checkout-svc")
}

func TestRedactParamsFingerprintHidesValueButCorrelatesRepeats(t *testing.T) {
	same := redactParams(paramModeFingerprint, []*modelv1.TagValue{strParam("checkout-svc")})
	again := redactParams(paramModeFingerprint, []*modelv1.TagValue{strParam("checkout-svc")})
	other := redactParams(paramModeFingerprint, []*modelv1.TagValue{strParam("payment-svc")})

	assert.NotContains(t, same, "checkout-svc")
	assert.Contains(t, same, "fp=")
	assert.Equal(t, same, again, "the same value must fingerprint identically, that is the whole point")
	assert.NotEqual(t, same, other, "different values must fingerprint differently")
}

func TestRedactParamsRawRendersStringVerbatimAndQuoted(t *testing.T) {
	got := redactParams(paramModeRaw, []*modelv1.TagValue{strParam(`with "quote"`)})
	assert.Equal(t, `"with \"quote\""`, got,
		"raw must quote so an embedded quote cannot forge a log field boundary")
}

// Binary is the one type raw does not render verbatim: it would corrupt the log line
// and carries no diagnostic value beyond its size.
func TestRedactParamsRawStillFingerprintsBinary(t *testing.T) {
	binary := &modelv1.TagValue{Value: &modelv1.TagValue_BinaryData{BinaryData: []byte{0x00, 0x01, 0x02}}}
	got := redactParams(paramModeRaw, []*modelv1.TagValue{binary})
	assert.Contains(t, got, "bytes(len=3)")
	assert.Contains(t, got, "fp=")
}

func TestRedactParamsJoinsEveryParameterInOrder(t *testing.T) {
	got := redactParams(paramModeFingerprint, []*modelv1.TagValue{intParam(1), strParam("ab"), intParam(2)})
	assert.Equal(t, "1, str(len=2):fp="+fingerprint("ab")+", 2", got)
}

// `IN (?)` bound to a str_array is the documented way to express a value set, so an
// array parameter can legitimately hold thousands of elements.
func TestRedactParamsCapsLongArraysAndReportsTheRemainder(t *testing.T) {
	const total = 20
	values := make([]string, 0, total)
	for idx := 0; idx < total; idx++ {
		values = append(values, "svc-"+string(rune('a'+idx)))
	}
	got := redactParams(paramModeFingerprint, []*modelv1.TagValue{strArrayParam(values...)})

	digests := make([]string, 0, maxRenderedArrayElems)
	for _, value := range values[:maxRenderedArrayElems] {
		digests = append(digests, fingerprint(value))
	}
	assert.Equal(t, fmt.Sprintf("str[n=%d]:fp=[%s +%d more]",
		total, strings.Join(digests, " "), total-maxRenderedArrayElems), got,
		"the true element count, the first maxRenderedArrayElems digests and the dropped remainder must all be exact")
}

// The array path shares no code with the scalar one, so it needs its own negative
// assertion. Without this, a regression that rendered elements verbatim would leak user
// data under the DEFAULT mode while every structural assertion still held.
func TestRedactParamsStrArrayHidesElementValuesBelowRaw(t *testing.T) {
	params := []*modelv1.TagValue{strArrayParam("checkout-svc", "payment-svc")}

	fp := redactParams(paramModeFingerprint, params)
	assert.NotContains(t, fp, "checkout-svc", "fingerprint must not render an element verbatim")
	assert.NotContains(t, fp, "payment-svc")
	// Elements must digest exactly as the scalar path does, so the same value stays
	// correlatable whether it arrives alone or inside an IN (?) set.
	assert.Equal(t, "str[n=2]:fp=["+fingerprint("checkout-svc")+" "+fingerprint("payment-svc")+"]", fp)
}

func TestRedactParamsStrArrayRawRendersElementsVerbatim(t *testing.T) {
	assert.Equal(t, `str[n=1]:["checkout-svc"]`,
		redactParams(paramModeRaw, []*modelv1.TagValue{strArrayParam("checkout-svc")}))
}

// Redaction runs inside a defer on the gRPC query path. A panic here would take down the
// liaison, and the trigger would be a client-supplied parameter, so every degenerate proto
// shape must render rather than crash.

func TestRedactParamsNeverPanicsOnMalformedParams(t *testing.T) {
	cases := []struct {
		param *modelv1.TagValue
		name  string
		want  string
	}{
		{name: "nil parameter", param: nil, want: "unrecognized"},
		{name: "no value set", param: &modelv1.TagValue{}, want: "unrecognized"},
		{name: "nil Str message", param: &modelv1.TagValue{Value: &modelv1.TagValue_Str{}}, want: "str(len=0):fp=" + fingerprint("")},
		{name: "nil Int message", param: &modelv1.TagValue{Value: &modelv1.TagValue_Int{}}, want: "0"},
		{name: "nil StrArray message", param: &modelv1.TagValue{Value: &modelv1.TagValue_StrArray{}}, want: "str[n=0]:fp=[]"},
		{name: "nil IntArray message", param: &modelv1.TagValue{Value: &modelv1.TagValue_IntArray{}}, want: "int[n=0]:[]"},
		{name: "nil binary payload", param: binaryParam(nil), want: "bytes(len=0):fp=" + fingerprint("")},
		{name: "nil Timestamp message", param: &modelv1.TagValue{Value: &modelv1.TagValue_Timestamp{}}, want: "1970-01-01T00:00:00Z"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got string
			require.NotPanics(t, func() {
				got = redactParams(paramModeFingerprint, []*modelv1.TagValue{tc.param})
			}, "a malformed parameter must not crash the liaison on the slow-query path")
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestRedactParamsNoneShortCircuitsBeforeTouchingParams(t *testing.T) {
	assert.NotPanics(t, func() {
		assert.Empty(t, redactParams(paramModeNone, []*modelv1.TagValue{nil, {}}))
	}, "none must not inspect parameters at all")
}

// The zero value of paramMode is "", which parseParamMode rejects — but a bydbQLService
// built without Validate still carries it. Only an exact raw match may reveal a value, so
// every other mode string, valid or not, has to redact.
func TestRedactParamsFailsClosedOnAnUnrecognisedMode(t *testing.T) {
	for _, mode := range []paramMode{"", "shape", "RAW", "Raw", "unknown"} {
		got := redactParams(mode, []*modelv1.TagValue{strParam("checkout-svc")})
		assert.NotContains(t, got, "checkout-svc",
			"mode %q must not reveal the value; only an exact %q match may", mode, paramModeRaw)
	}
}

// parseParamMode has to stay strict about the empty string: server.validateParamMode
// resolves "" to the default itself, and that only stays a deliberate decision for as long
// as the parser refuses to make it silently.
func TestParseParamModeRejectsEmptyAndMiscasedValues(t *testing.T) {
	for _, bad := range []string{"", " ", "None", "RAW", "Fingerprint", " none", "none ", "shape"} {
		_, err := parseParamMode(bad)
		assert.Error(t, err, "%q must not be accepted", bad)
	}
}

func TestRedactParamsStrLengthCountsBytesNotRunes(t *testing.T) {
	const multibyte = "日本語" // 3 runes, 9 bytes
	assert.Equal(t, "str(len=9):fp="+fingerprint(multibyte),
		redactParams(paramModeFingerprint, []*modelv1.TagValue{strParam(multibyte)}),
		"len is the byte length; a reader comparing it against a rune count would be misled")
}

func TestRedactParamsRendersEmptyStringsExplicitly(t *testing.T) {
	assert.Equal(t, "str(len=0):fp="+fingerprint(""),
		redactParams(paramModeFingerprint, []*modelv1.TagValue{strParam("")}))
	assert.Equal(t, `""`, redactParams(paramModeRaw, []*modelv1.TagValue{strParam("")}))
}

// Parameter values are client-controlled, so raw mode is the one place a crafted value
// could forge log structure. strconv.Quote is what prevents that; pin it.
func TestRedactParamsRawEscapesControlCharacters(t *testing.T) {
	got := redactParams(paramModeRaw, []*modelv1.TagValue{strParam("a\nb\tc\x00d")})
	assert.Equal(t, `"a\nb\tc\x00d"`, got)
	assert.NotContains(t, got, "\n", "a literal newline would split one log line into two")
	assert.NotContains(t, got, "\t")
	assert.NotContains(t, got, "\x00")
}

// The point of the default mode is that log volume stops depending on client input.
func TestRedactParamsFingerprintKeepsHugeValuesBounded(t *testing.T) {
	huge := strings.Repeat("x", 1<<20)
	got := redactParams(paramModeFingerprint, []*modelv1.TagValue{strParam(huge)})
	assert.NotContains(t, got, huge)
	assert.Less(t, len(got), 64,
		"the default mode must bound the log line regardless of how large the parameter is")
}

func TestRedactParamsArrayCapBoundaries(t *testing.T) {
	build := func(n int) []string {
		values := make([]string, 0, n)
		for idx := 0; idx < n; idx++ {
			values = append(values, fmt.Sprintf("v%d", idx))
		}
		return values
	}
	t.Run("empty array", func(t *testing.T) {
		assert.Equal(t, "str[n=0]:fp=[]",
			redactParams(paramModeFingerprint, []*modelv1.TagValue{strArrayParam()}))
	})
	t.Run("exactly at the cap claims no remainder", func(t *testing.T) {
		got := redactParams(paramModeFingerprint, []*modelv1.TagValue{strArrayParam(build(maxRenderedArrayElems)...)})
		assert.Contains(t, got, fmt.Sprintf("str[n=%d]", maxRenderedArrayElems))
		assert.NotContains(t, got, "more", "a list that fits whole must not claim a remainder")
	})
	t.Run("one past the cap reports exactly one dropped", func(t *testing.T) {
		got := redactParams(paramModeFingerprint, []*modelv1.TagValue{strArrayParam(build(maxRenderedArrayElems + 1)...)})
		assert.Contains(t, got, fmt.Sprintf("str[n=%d]", maxRenderedArrayElems+1))
		assert.Contains(t, got, "+1 more")
	})
}

func TestRedactParamsIntArrayRendersVerbatimAndIdenticallyInEveryMode(t *testing.T) {
	values := make([]int64, 0, 10)
	for idx := 0; idx < 10; idx++ {
		values = append(values, int64(idx))
	}
	param := []*modelv1.TagValue{intArrayParam(values...)}
	const want = "int[n=10]:[0 1 2 3 4 5 6 7 +2 more]"
	assert.Equal(t, want, redactParams(paramModeFingerprint, param))
	assert.Equal(t, want, redactParams(paramModeRaw, param),
		"numeric arrays carry no user-identifying content, so raw has nothing extra to reveal")
}

func TestRedactParamsBinaryRendersIdenticallyInEveryModeAboveNone(t *testing.T) {
	payload := []byte{0xff, 0x00, 0xfe}
	param := []*modelv1.TagValue{binaryParam(payload)}
	got := redactParams(paramModeFingerprint, param)
	assert.Equal(t, "bytes(len=3):fp="+fingerprint(string(payload)), got)
	assert.Equal(t, got, redactParams(paramModeRaw, param), "raw must not unlock binary payloads")
}

func TestRedactParamsRendersExtremeIntegersExactly(t *testing.T) {
	assert.Equal(t, "-9223372036854775808, -1, 0, 9223372036854775807",
		redactParams(paramModeFingerprint, []*modelv1.TagValue{
			intParam(math.MinInt64), intParam(-1), intParam(0), intParam(math.MaxInt64),
		}))
}

func TestRedactParamsTimestampKeepsSubSecondPrecision(t *testing.T) {
	assert.Equal(t, "2026-08-04T10:00:00.123456789Z",
		redactParams(paramModeFingerprint, []*modelv1.TagValue{
			timestampParam(time.Date(2026, 8, 4, 10, 0, 0, 123456789, time.UTC)),
		}),
		"sub-second precision is what separates a 1ms window from a 100ms one")
}

func TestRedactParamsTimestampNormalisesToUTC(t *testing.T) {
	east8 := time.FixedZone("UTC+8", 8*60*60)
	assert.Equal(t, "2026-08-04T10:00:00Z",
		redactParams(paramModeFingerprint, []*modelv1.TagValue{
			timestampParam(time.Date(2026, 8, 4, 18, 0, 0, 0, east8)),
		}),
		"timestamps must stay comparable across nodes running in different zones")
}

// Parameters are joined with ", " while array elements are joined with " ". If those ever
// converge, an operator can no longer tell one array parameter from several scalar ones.
func TestRedactParamsKeepsParameterAndElementSeparatorsDistinct(t *testing.T) {
	assert.Equal(t, `1, str[n=2]:["a" "b"], 2`,
		redactParams(paramModeRaw, []*modelv1.TagValue{intParam(1), strArrayParam("a", "b"), intParam(2)}))
}

func TestFingerprintIsCompactLowercaseHex(t *testing.T) {
	for _, value := range []string{"", "a", "checkout-svc", strings.Repeat("x", 4096)} {
		fp := fingerprint(value)
		assert.NotEmpty(t, fp)
		assert.LessOrEqual(t, len(fp), 16, "a 64-bit digest is at most 16 hex digits")
		for _, char := range fp {
			assert.True(t, (char >= '0' && char <= '9') || (char >= 'a' && char <= 'f'),
				"digest %q must be lowercase hex so it is safe to embed unquoted in a log line", fp)
		}
	}
}

// The digest only earns its place if distinct values stay distinct across the kind of
// low-cardinality identifiers that actually get bound to slow queries.
func TestFingerprintDistinguishesRealisticServiceNames(t *testing.T) {
	seen := make(map[string]string, 1000)
	for idx := 0; idx < 1000; idx++ {
		value := fmt.Sprintf("service-%d.namespace-%d", idx, idx%7)
		fp := fingerprint(value)
		if prev, dup := seen[fp]; dup {
			t.Fatalf("digest collision between %q and %q", prev, value)
		}
		seen[fp] = value
	}
}

// benchSink keeps the compiler from eliminating the benchmarked calls.
var benchSink string

// BenchmarkRedactParams covers every mode against every TagValue variant, plus an array
// long enough to exercise joinCapped past the cap.
//
// Rendering is reached only by queries already over --bydbql-slow-query-threshold, so these
// numbers bound a per-slow-query cost rather than a per-request one. They exist as a
// regression baseline: extending the type switch with a new variant, or swapping the digest
// (a salted HMAC, SHA-256), should be measured against them rather than against an ad-hoc
// benchmark written after the fact.
func BenchmarkRedactParams(b *testing.B) {
	longArray := make([]string, 0, 64)
	for idx := 0; idx < 64; idx++ {
		longArray = append(longArray, fmt.Sprintf("svc-%02d", idx))
	}
	variants := []struct {
		param *modelv1.TagValue
		name  string
	}{
		{name: "null", param: &modelv1.TagValue{Value: &modelv1.TagValue_Null{}}},
		{name: "int", param: intParam(100000)},
		{name: "int_array", param: intArrayParam(1, 2, 3)},
		{name: "timestamp", param: timestampParam(time.Date(2026, 8, 4, 10, 0, 0, 0, time.UTC))},
		{name: "str", param: strParam("checkout-svc")},
		{name: "str_array", param: strArrayParam("checkout-svc", "payment-svc")},
		{name: "str_array_over_cap", param: strArrayParam(longArray...)},
		{name: "binary", param: binaryParam([]byte{0x01, 0x02, 0x03})},
	}
	for _, mode := range []paramMode{paramModeNone, paramModeFingerprint, paramModeRaw} {
		for _, variant := range variants {
			params := []*modelv1.TagValue{variant.param}
			b.Run(string(mode)+"/"+variant.name, func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					benchSink = redactParams(mode, params)
				}
			})
		}
	}
}

// BenchmarkFingerprint isolates the digest, which is the inner loop of the default mode:
// one call per string parameter, and one per element of a string array up to the cap.
func BenchmarkFingerprint(b *testing.B) {
	cases := []struct {
		name  string
		value string
	}{
		{name: "service_name_12B", value: "checkout-svc"},
		{name: "value_1KiB", value: strings.Repeat("x", 1024)},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(tc.value)))
			for i := 0; i < b.N; i++ {
				benchSink = fingerprint(tc.value)
			}
		})
	}
}
