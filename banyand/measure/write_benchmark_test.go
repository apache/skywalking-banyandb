package measure

import (
	"testing"
	"time"

	commonv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/common/v1"
	databasev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/database/v1"
	measurev1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/measure/v1"
	modelv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/model/v1"
	partition "github.com/apache/skywalking-banyandb/pkg/partition"
	"github.com/apache/skywalking-banyandb/pkg/timestamp"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/apache/skywalking-banyandb/api/common"
)

// BenchmarkAppendDataPoints benchmarks the performance of the appendDataPoints function.
// BenchmarkAppendDataPoints-10    	 3995332	       307.8 ns/op	     344 B/op	       9 allocs/op
// BenchmarkAppendDataPoints-10    	 1467444	       823.0 ns/op	    1152 B/op	      21 allocs/op
func BenchmarkAppendDataPoints(b *testing.B) {
	// Prepare a mock schema
	schema := &databasev1.Measure{
		Metadata: &commonv1.Metadata{
			Name:  "benchmark_measure",
			Group: "benchmark_group",
		},
		TagFamilies: []*databasev1.TagFamilySpec{
			{
				Name: "default",
				Tags: []*databasev1.TagSpec{
					{
						Name: "host",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
					{
						Name: "ip",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
				},
			},
			{
				Name: "location",
				Tags: []*databasev1.TagSpec{
					{
						Name: "city",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
					{
						Name: "country",
						Type: databasev1.TagType_TAG_TYPE_STRING,
					},
				},
			},
		},
		Fields: []*databasev1.FieldSpec{
			{
				Name:              "value",
				FieldType:         databasev1.FieldType_FIELD_TYPE_FLOAT,
				EncodingMethod:    databasev1.EncodingMethod_ENCODING_METHOD_GORILLA,
				CompressionMethod: databasev1.CompressionMethod_COMPRESSION_METHOD_ZSTD,
			},
		},
		Entity: &databasev1.Entity{
			TagNames: []string{"host"},
		},
	}

	// Prepare a mock write request
	req := &measurev1.WriteRequest{
		Metadata: schema.Metadata,
		DataPoint: &measurev1.DataPointValue{
			Timestamp: timestamppb.New(time.Now()),
			TagFamilies: []*modelv1.TagFamilyForWrite{
				{
					Tags: []*modelv1.TagValue{
						{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{
									Value: "host-1",
								},
							},
						},
						{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{
									Value: "ip-1",
								},
							},
						},
					},
				},
				{
					Tags: []*modelv1.TagValue{
						{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{
									Value: "city-1",
								},
							},
						},
						{
							Value: &modelv1.TagValue_Str{
								Str: &modelv1.Str{
									Value: "country-1",
								},
							},
						},
					},
				},
			},
			Fields: []*modelv1.FieldValue{
				{
					Value: &modelv1.FieldValue_Float{
						Float: &modelv1.Float{
							Value: 1234.56,
						},
					},
				},
			},
			Version: 1,
		},
		MessageId: 12345,
	}

	// Prepare a mock index rule locator
	locator := partition.IndexRuleLocator{
		EntitySet: map[string]int{
			"host": 1,
		},
		TagFamilyTRule: []map[string]*databasev1.IndexRule{
			{
				"ip": {
					Metadata: &commonv1.Metadata{
						Id: 2,
					},
					Tags: []string{"ip"},
				},
			},
			{
				"city": {
					Metadata: &commonv1.Metadata{
						Id: 3,
					},
					Tags: []string{"city"},
				},
				"country": {
					Metadata: &commonv1.Metadata{
						Id: 4,
					},
					Tags: []string{"country"},
				},
			},
		},
	}

	// Prepare a dataPointsInTable with pre-allocated slices
	dest := &dataPointsInTable{
		timeRange: timestamp.TimeRange{
			Start: time.Now().Add(-time.Hour),
			End:   time.Now(),
		},
		dataPoints: dataPoints{
			seriesIDs:   make([]common.SeriesID, 0, 1000),
			timestamps:  make([]int64, 0, 1000),
			versions:    make([]int64, 0, 1000),
			tagFamilies: make([][]nameValues, 0, 1000),
			fields:      make([]nameValues, 0, 1000),
		},
	}

	ts := time.Now().UnixNano()
	sid := common.SeriesID(1)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset slice lengths to zero
		dest.dataPoints.tagFamilies = dest.dataPoints.tagFamilies[:0]
		dest.dataPoints.timestamps = dest.dataPoints.timestamps[:0]
		dest.dataPoints.versions = dest.dataPoints.versions[:0]
		dest.dataPoints.seriesIDs = dest.dataPoints.seriesIDs[:0]
		dest.dataPoints.fields = dest.dataPoints.fields[:0]

		// Call appendDataPoints
		appendDataPoints(dest, ts, sid, schema, req, locator)
	}
}
