package logical_test

import (
	"github.com/apache/skywalking-banyandb/pkg/logical"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

var binaryOpsMap = map[string]apiv1.BinaryOp{
	"=":  apiv1.BinaryOpEQ,
	"!=": apiv1.BinaryOpNE,
	">":  apiv1.BinaryOpGT,
}

type criteriaBuilder struct {
	*flatbuffers.Builder
}

func (b *criteriaBuilder) buildMetaData(group, name string) flatbuffers.UOffsetT {
	g, n := b.CreateString(group), b.CreateString(name)
	apiv1.MetadataStart(b.Builder)
	apiv1.MetadataAddGroup(b.Builder, g)
	apiv1.MetadataAddGroup(b.Builder, n)
	return apiv1.MetadataEnd(b.Builder)
}

func (b *criteriaBuilder) buildTimeStampNanoSeconds(start, end time.Time) flatbuffers.UOffsetT {
	apiv1.RangeQueryStart(b.Builder)
	apiv1.RangeQueryAddBegin(b.Builder, uint64(start.UnixNano()))
	apiv1.RangeQueryAddEnd(b.Builder, uint64(end.UnixNano()))
	return apiv1.RangeQueryEnd(b.Builder)
}

func (b *criteriaBuilder) buildOrderBy(keyName string, sort apiv1.Sort) flatbuffers.UOffsetT {
	k := b.Builder.CreateString(keyName)
	apiv1.QueryOrderStart(b.Builder)
	apiv1.QueryOrderAddKeyName(b.Builder, k)
	apiv1.QueryOrderAddSort(b.Builder, sort)
	return apiv1.QueryOrderEnd(b.Builder)
}

func (b *criteriaBuilder) buildProjection(keyNames ...string) flatbuffers.UOffsetT {
	var keyNamesOffsets []flatbuffers.UOffsetT
	for i := 0; i < len(keyNames); i++ {
		keyNamesOffsets = append(keyNamesOffsets, b.Builder.CreateString(keyNames[i]))
	}
	apiv1.ProjectionStartKeyNamesVector(b.Builder, len(keyNames))
	for i := 0; i < len(keyNamesOffsets); i++ {
		b.Builder.PrependUOffsetT(keyNamesOffsets[i])
	}
	strArr := b.Builder.EndVector(len(keyNames))
	apiv1.ProjectionStart(b.Builder)
	apiv1.ProjectionAddKeyNames(b.Builder, strArr)
	return apiv1.ProjectionEnd(b.Builder)
}

func (b *criteriaBuilder) buildIntPair(key string, values ...int64) flatbuffers.UOffsetT {
	apiv1.IntPairStartValuesVector(b.Builder, len(values))
	for i := 0; i < len(values); i++ {
		b.Builder.PrependInt64(values[i])
	}
	int64Arr := b.Builder.EndVector(len(values))

	keyOffset := b.CreateString(key)
	apiv1.IntPairStart(b.Builder)
	apiv1.IntPairAddKey(b.Builder, keyOffset)
	apiv1.IntPairAddValues(b.Builder, int64Arr)
	return apiv1.IntPairEnd(b.Builder)
}

func (b *criteriaBuilder) buildCondition(key string, value interface{}) flatbuffers.UOffsetT {
	var pairOffset flatbuffers.UOffsetT
	var pairType apiv1.TypedPair
	switch v := value.(type) {
	case int:
		pairOffset = b.buildIntPair(key, int64(v))
		pairType = apiv1.TypedPairIntPair
	case int8:
		pairOffset = b.buildIntPair(key, int64(v))
		pairType = apiv1.TypedPairIntPair
	case int16:
		pairOffset = b.buildIntPair(key, int64(v))
		pairType = apiv1.TypedPairIntPair
	case int32:
		pairOffset = b.buildIntPair(key, int64(v))
		pairType = apiv1.TypedPairIntPair
	case int64:
		pairOffset = b.buildIntPair(key, v)
		pairType = apiv1.TypedPairIntPair
	default:
		panic("not supported values")
	}

	apiv1.PairStart(b.Builder)
	apiv1.PairAddPair(b.Builder, pairOffset)
	apiv1.PairAddPairType(b.Builder, pairType)
	return apiv1.PairEnd(b.Builder)
}

func (b *criteriaBuilder) buildFields(items ...interface{}) flatbuffers.UOffsetT {
	if len(items)%3 != 0 {
		panic("invalid items list")
	}
	l := len(items) / 3
	var pairQueryOffsets []flatbuffers.UOffsetT
	for i := 0; i < l; i++ {
		key, op, values := items[i*3+0], items[i*3+1], items[i*3+2]
		condition := b.buildCondition(key.(string), values)
		apiv1.PairQueryStart(b.Builder)
		// add op
		apiv1.PairQueryAddOp(b.Builder, binaryOpsMap[op.(string)])
		// build condition
		apiv1.PairQueryAddCondition(b.Builder, condition)
		pairQueryOffsets = append(pairQueryOffsets, apiv1.PairQueryEnd(b.Builder))
	}
	apiv1.EntityCriteriaStartFieldsVector(b.Builder, l)
	for i := 0; i < len(pairQueryOffsets); i++ {
		b.PrependUOffsetT(pairQueryOffsets[i])
	}
	return b.EndVector(l)
}

func Test_Compose(t *testing.T) {
	builder := &criteriaBuilder{
		flatbuffers.NewBuilder(1024),
	}
	metadata := builder.buildMetaData("group1", "name1")
	timeRange := builder.buildTimeStampNanoSeconds(time.Now().Add(-5*time.Hour), time.Now())
	orderBy := builder.buildOrderBy("startTime", apiv1.SortDESC)
	projection := builder.buildProjection("traceID", "spanID")
	fields := builder.buildFields("duration", ">", 4000)
	// start
	apiv1.EntityCriteriaStart(builder.Builder)
	// metadata
	apiv1.EntityCriteriaAddMetatdata(builder.Builder, metadata)
	// limit
	apiv1.EntityCriteriaAddLimit(builder.Builder, 20)
	// offset
	apiv1.EntityCriteriaAddOffset(builder.Builder, 0)
	// time range
	apiv1.EntityCriteriaAddTimestampNanoseconds(builder.Builder, timeRange)
	// orderBy
	apiv1.EntityCriteriaAddOrderBy(builder.Builder, orderBy)
	// projection
	apiv1.EntityCriteriaAddProjection(builder.Builder, projection)
	// selection
	apiv1.EntityCriteriaAddFields(builder.Builder, fields)
	// Deserialize
	apiv1.EntityCriteriaEnd(builder.Builder)
	buf := builder.Bytes[builder.Head():]
	criteria := apiv1.GetRootAsEntityCriteria(buf, 0)
	assert.NotNil(t, criteria)
	plan, err := logical.ComposeLogicalPlan(criteria)
	assert.NoError(t, err)
	assert.NotNil(t, plan)
}
