package logical_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/internal/mocks"
	"github.com/apache/skywalking-banyandb/pkg/logical"
	"github.com/apache/skywalking-banyandb/pkg/types"
)

func TestExpr_KeyRef_Stringer(t *testing.T) {
	keyRef := logical.NewKeyRef("duration")
	assert.Equal(t, keyRef.String(), "#duration")
}

func TestExpr_KeyRef_ToField(t *testing.T) {
	keyRef := logical.NewKeyRef("duration")
	plan := &mocks.Plan{}
	schema := &mocks.Schema{}
	plan.On("Schema").Return(schema)
	schema.On("GetFields").Return([]types.Field{
		types.NewField("duration", types.INT64),
		types.NewField("serviceName", types.STRING),
		types.NewField("spanID", types.STRING),
	})
	refField, err := keyRef.ToField(plan)
	assert.NoError(t, err)
	assert.Equal(t, refField, types.NewField("duration", types.INT64))
}

func TestExpr_KeyRef_ToField_Failure(t *testing.T) {
	keyRef := logical.NewKeyRef("traceID")
	plan := &mocks.Plan{}
	schema := &mocks.Schema{}
	plan.On("Schema").Return(schema)
	schema.On("GetFields").Return([]types.Field{
		types.NewField("duration", types.INT64),
		types.NewField("serviceName", types.STRING),
		types.NewField("spanID", types.STRING),
	})
	_, err := keyRef.ToField(plan)
	assert.ErrorIs(t, err, logical.NoSuchField)
}
