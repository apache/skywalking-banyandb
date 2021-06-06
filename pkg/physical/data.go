package physical

import (
	"fmt"

	"github.com/apache/skywalking-banyandb/api/common"
	"github.com/apache/skywalking-banyandb/api/data"
)

type DataType uint8

const (
	ChunkID DataType = iota
	Trace
	Unknown
)

type Data interface {
	DataType() DataType
}

var _ Data = (DataGroup)(nil)

// DataGroup is a group of Data with the same DataType
type DataGroup []Data

func (dg DataGroup) Append(data Data) (DataGroup, error) {
	if dg.dataType() != Unknown && dg.dataType() != data.DataType() {
		return dg, fmt.Errorf("fail to append data due to different data type, expect %v, actual %v", dg.dataType(), data.DataType())
	}
	return append(dg, data), nil
}

func (dg DataGroup) dataType() DataType {
	if len(dg) == 0 {
		return Unknown
	}
	return dg[len(dg)-1].DataType()
}

func (dg DataGroup) DataType() DataType {
	return dg[len(dg)-1].DataType()
}

var _ Data = (*chunkIDs)(nil)

type chunkIDs struct {
	ids []common.ChunkID
}

func (c *chunkIDs) DataType() DataType {
	return ChunkID
}

var _ Data = (*traces)(nil)

type traces struct {
	traces []*data.Trace
}

func (t *traces) DataType() DataType {
	return Trace
}

func NewTraceData(input ...*data.Trace) Data {
	return &traces{
		traces: input,
	}
}
