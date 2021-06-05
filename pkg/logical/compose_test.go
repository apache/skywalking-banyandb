package logical_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/apache/skywalking-banyandb/pkg/clientutil"
	"github.com/apache/skywalking-banyandb/pkg/logical"
)

func TestTableScan(t *testing.T) {
	tester := assert.New(t)
	builder := clientutil.NewCriteriaBuilder()
	criteria := builder.Build(
		clientutil.AddLimit(0),
		clientutil.AddOffset(0),
		builder.BuildMetaData("skywalking", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
	)
	plan := logical.Compose(criteria)
	tester.NotNil(plan)
	tester.NoError(plan.Validate())
}

func TestIndexScan(t *testing.T) {
	tester := assert.New(t)
	builder := clientutil.NewCriteriaBuilder()
	criteria := builder.Build(
		clientutil.AddLimit(0),
		clientutil.AddOffset(0),
		builder.BuildMetaData("skywalking", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildFields("duration", ">", 500, "duration", "<=", 1000),
	)
	plan := logical.Compose(criteria)
	tester.NotNil(plan)
	tester.NoError(plan.Validate())
}

func TestTraceIDSearch(t *testing.T) {
	tester := assert.New(t)
	builder := clientutil.NewCriteriaBuilder()
	criteria := builder.Build(
		clientutil.AddLimit(0),
		clientutil.AddOffset(0),
		builder.BuildMetaData("skywalking", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildFields("traceID", "=", "aaaaaaaa"),
	)
	plan := logical.Compose(criteria)
	tester.NotNil(plan)
	tester.NoError(plan.Validate())
}
