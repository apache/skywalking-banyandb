package physical_test

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/pkg/clientutil"
	"github.com/apache/skywalking-banyandb/pkg/logical"
	"github.com/apache/skywalking-banyandb/pkg/physical"
)

func TestTableScanTransform_Run(t *testing.T) {
	tester := assert.New(t)
	ctrl := gomock.NewController(t)
	builder := clientutil.NewCriteriaBuilder()

	sT, eT := time.Now().Add(-3*time.Hour), time.Now()

	criteria := builder.Build(
		clientutil.AddLimit(0),
		clientutil.AddOffset(0),
		builder.BuildMetaData("skywalking", "trace"),
		builder.BuildTimeStampNanoSeconds(sT, eT),
		builder.BuildOrderBy("startTime", apiv1.SortDESC),
	)

	params := logical.NewTableScan(criteria.Metadata(nil), criteria.TimestampNanoseconds(nil), criteria.Projection(nil))
	transform := physical.NewTableScanTransform(params.(*logical.TableScan))
	ec := physical.NewMockExecutionContext(ctrl)
	uniModel := series.NewMockUniModel(ctrl)

	mockErr := errors.New("not found")

	ec.
		EXPECT().
		UniModel().
		Return(uniModel)
	uniModel.
		EXPECT().
		ScanEntity(uint64(sT.UnixNano()), uint64(eT.UnixNano()), []string{}).
		Return(nil, mockErr)

	f := transform.Run(ec)
	tester.NotNil(f)
	// TODO: polish, use something like await here
	time.Sleep(500 * time.Millisecond)
	tester.True(f.IsComplete())
	tester.Error(f.Value().Error())
}
