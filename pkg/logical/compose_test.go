package logical_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
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
		builder.BuildOrderBy("startTime", apiv1.SortDESC),
	)
	plan, err := logical.Compose(criteria)
	tester.NoError(err)
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
		builder.BuildOrderBy("startTime", apiv1.SortDESC),
	)
	plan, err := logical.Compose(criteria)
	tester.NoError(err)
	tester.NotNil(plan)
	tester.NoError(plan.Validate())
	fmt.Println(plan.Plot())
}

func TestMultiIndexesScan(t *testing.T) {
	tester := assert.New(t)
	builder := clientutil.NewCriteriaBuilder()
	criteria := builder.Build(
		clientutil.AddLimit(0),
		clientutil.AddOffset(0),
		builder.BuildMetaData("skywalking", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildFields("duration", ">", 500, "duration", "<=", 1000, "component", "=", "mysql"),
		builder.BuildOrderBy("startTime", apiv1.SortDESC),
	)
	plan, err := logical.Compose(criteria)
	tester.NoError(err)
	tester.NotNil(plan)
	tester.NoError(plan.Validate())
	fmt.Println(plan.Plot())
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
		builder.BuildOrderBy("startTime", apiv1.SortDESC),
	)
	plan, err := logical.Compose(criteria)
	tester.NoError(err)
	tester.NotNil(plan)
	tester.NoError(plan.Validate())
}

func TestTraceIDSearchAndIndexSearch(t *testing.T) {
	tester := assert.New(t)
	builder := clientutil.NewCriteriaBuilder()
	criteria := builder.Build(
		clientutil.AddLimit(0),
		clientutil.AddOffset(0),
		builder.BuildMetaData("skywalking", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildFields("traceID", "=", "aaaaaaaa", "duration", "<=", 1000),
		builder.BuildOrderBy("startTime", apiv1.SortDESC),
	)
	plan, err := logical.Compose(criteria)
	tester.NoError(err)
	tester.NotNil(plan)
	tester.NoError(plan.Validate())
	fmt.Println(plan.Plot())
}

func TestProjection(t *testing.T) {
	tester := assert.New(t)
	builder := clientutil.NewCriteriaBuilder()
	criteria := builder.Build(
		clientutil.AddLimit(0),
		clientutil.AddOffset(0),
		builder.BuildMetaData("skywalking", "trace"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-3*time.Hour), time.Now()),
		builder.BuildFields("traceID", "=", "aaaaaaaa", "duration", "<=", 1000),
		builder.BuildOrderBy("startTime", apiv1.SortDESC),
		builder.BuildProjection("startTime", "traceID"),
	)
	plan, err := logical.Compose(criteria)
	tester.NoError(err)
	tester.NotNil(plan)
	tester.NoError(plan.Validate())
	fmt.Println(plan.Plot())
}
