package clientutil

import (
	flatbuffers "github.com/google/flatbuffers/go"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

func BuildIndexObject(fieldName string, indexType apiv1.IndexType) apiv1.IndexObject {
	builder := flatbuffers.NewBuilder(0)
	fn := builder.CreateString(fieldName)
	apiv1.IndexObjectStartFieldsVector(builder, 1)
	builder.PrependUOffsetT(fn)
	fields := builder.EndVector(1)

	apiv1.IndexObjectStart(builder)
	apiv1.IndexObjectAddFields(builder, fields)
	apiv1.IndexObjectAddType(builder, indexType)
	indexObject := apiv1.IndexObjectEnd(builder)
	builder.Finish(indexObject)
	buf := builder.Bytes[builder.Head():]
	return *apiv1.GetRootAsIndexObject(buf, 0)
}
