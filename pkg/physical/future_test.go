package physical_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/physical"
)

var _ = Describe("Future", func() {
	It("should run single future", func() {
		f := physical.NewFuture(func() physical.Result {
			return physical.Success(physical.NewChunkIDs(1, 2, 3))
		})

		Eventually(func() bool {
			return f.IsComplete()
		}).Should(BeTrue())
		Eventually(func() physical.Data {
			return f.Value().Success()
		}).Should(BeEquivalentTo(physical.NewChunkIDs(1, 2, 3)))
	})

	It("should return error if panic", func() {
		f := physical.NewFuture(func() physical.Result {
			panic("panic in future")
		})

		Eventually(func() bool {
			return f.IsComplete()
		}).Should(BeTrue())
		Eventually(func() error {
			return f.Value().Error()
		}).Should(HaveOccurred())
	})
})
