package trace_test

import (
	"flag"
	"path"
	"runtime"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/apache/skywalking-banyandb/pkg/test/query"
)

func TestIntegrationLoad(t *testing.T) {
	t.Skip("Skip the stress trace test")
	RegisterFailHandler(Fail)
	RunSpecs(t, "Stress Trace Suite")
}

var _ = Describe("Query", func() {
	const timeout = 30 * time.Minute
	var (
		fs       *flag.FlagSet
		basePath string
	)

	BeforeEach(func() {
		fs = flag.NewFlagSet("", flag.PanicOnError)
		fs.String("base-url", "http://localhost:12800/graphql", "")
		fs.String("service-id", "c2VydmljZV8x.1", "")
		_, basePath, _, _ = runtime.Caller(0)
		basePath = path.Dir(basePath)
	})

	It("TraceListOrderByDuration", func() {
		query.TraceListOrderByDuration(basePath, timeout, fs)
	})
})
