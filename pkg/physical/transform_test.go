package physical_test

import (
	"errors"
	"time"

	"github.com/golang/mock/gomock"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	apiv1 "github.com/apache/skywalking-banyandb/api/fbs/v1"
	"github.com/apache/skywalking-banyandb/banyand/series"
	"github.com/apache/skywalking-banyandb/pkg/clientutil"
	"github.com/apache/skywalking-banyandb/pkg/logical"
	"github.com/apache/skywalking-banyandb/pkg/physical"
)

var _ = Describe("TableScanTransform", func() {
	It("should return error", func() {
		ctrl := gomock.NewController(GinkgoT())
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
		Expect(f).ShouldNot(BeNil())
		Eventually(func() bool {
			return f.IsComplete()
		}).Should(BeTrue())
		Eventually(func() error {
			return f.Value().Error()
		}).Should(HaveOccurred())
	})
})
