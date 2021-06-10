package clientutil

import (
	flatbuffers "github.com/google/flatbuffers/go"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

func BuildTraceSeries(group, name string) *apiv1.Series {
	builder := flatbuffers.NewBuilder(0)
	g, n := builder.CreateString(group), builder.CreateString(name)
	apiv1.MetadataStart(builder)
	apiv1.MetadataAddGroup(builder, g)
	apiv1.MetadataAddName(builder, n)
	meta := apiv1.MetadataEnd(builder)

	apiv1.SeriesStart(builder)
	apiv1.SeriesAddCatalog(builder, apiv1.CatalogTrace)
	apiv1.SeriesAddSeries(builder, meta)
	seriesOffset := apiv1.SeriesEnd(builder)
	builder.Finish(seriesOffset)
	buf := builder.Bytes[builder.Head():]
	return apiv1.GetRootAsSeries(buf, 0)
}
