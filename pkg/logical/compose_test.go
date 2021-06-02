package logical_test

import (
	"fmt"
	"github.com/apache/skywalking-banyandb/pkg/clientutil"
	"github.com/apache/skywalking-banyandb/pkg/logical"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
)

func Test_Compose(t *testing.T) {
	builder := clientutil.NewCriteriaBuilder()
	criteria := builder.Build(
		clientutil.AddLimit(20),
		clientutil.AddOffset(0),
		builder.BuildMetaData("group1", "name1"),
		builder.BuildTimeStampNanoSeconds(time.Now().Add(-5*time.Hour), time.Now()),
		builder.BuildOrderBy("startTime", apiv1.SortDESC),
		builder.BuildProjection("traceID", "spanID"),
		builder.BuildFields(
			"duration", ">=", 4000,
			"duration", "<", 10000,
			"serviceName", "=", "demo",
			"serviceInstanceID", "!=", "pod-xxxxx",
			"keys", "having", []string{"key1", "key2"},
		),
	)
	assert.NotNil(t, criteria)
	plan, err := logical.ComposeLogicalPlan(criteria)
	assert.NoError(t, err)
	assert.NotNil(t, plan)
	fmt.Printf("%s", logical.FormatPlan(plan))
	assert.Equal(t, logical.FormatPlan(plan), "Projection: #spanID, #traceID\n\tSelection: #duration GT 4000\n\t\tScan: Metadata{group=group1, name=name1}; projection=None\n")
}
