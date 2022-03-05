package query_test

import (
	"fmt"
	"reflect"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	errorsutil "github.com/onsi/gomega/gstruct/errors"
	"github.com/onsi/gomega/types"
	"github.com/pkg/errors"

	streamv1 "github.com/apache/skywalking-banyandb/api/proto/banyandb/stream/v1"
	"github.com/apache/skywalking-banyandb/banyand/metadata"
	testmeasure "github.com/apache/skywalking-banyandb/pkg/test/measure"
	teststream "github.com/apache/skywalking-banyandb/pkg/test/stream"
)

func TestQuery(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Query Suite")
}

// preloadMeasureService is to preload measure
type preloadMeasureService struct {
	metaSvc metadata.Service
}

func (p *preloadMeasureService) Name() string {
	return "preload-measure"
}

func (p *preloadMeasureService) PreRun() error {
	return testmeasure.PreloadSchema(p.metaSvc.SchemaRegistry())
}

// preloadStreamService is to preload measure
type preloadStreamService struct {
	metaSvc metadata.Service
}

func (p *preloadStreamService) Name() string {
	return "preload-measure"
}

func (p *preloadStreamService) PreRun() error {
	return teststream.PreloadSchema(p.metaSvc.SchemaRegistry())
}

var (
	_ types.GomegaMatcher = (*binaryDataChecker)(nil)
)

type binaryDataChecker struct {
	shouldHaveBinaryData bool

	// State.
	failures []error
}

func HaveBinary() types.GomegaMatcher {
	return &binaryDataChecker{
		shouldHaveBinaryData: true,
	}
}

func NotHaveBinary() types.GomegaMatcher {
	return &binaryDataChecker{
		shouldHaveBinaryData: false,
	}
}

func (b *binaryDataChecker) Match(actual interface{}) (success bool, err error) {
	if reflect.TypeOf(actual).Kind() != reflect.Slice {
		return false, fmt.Errorf("%v is type %T, expected slice", actual, actual)
	}

	b.failures = b.matchElements(actual)
	if len(b.failures) > 0 {
		return false, nil
	}
	return true, nil
}

func (b *binaryDataChecker) matchElements(actual interface{}) (errs []error) {
	val := reflect.ValueOf(actual)
itemInSlice:
	for i := 0; i < val.Len(); i++ {
		element := val.Index(i).Interface()
		if streamElem, ok := element.(*streamv1.Element); ok {
			for _, tagFamily := range streamElem.GetTagFamilies() {
				if tagFamily.GetName() == "data" {
					if b.shouldHaveBinaryData {
						continue itemInSlice
					} else {
						errs = append(errs, errors.New("should not contain `data`"))
					}
				}
			}
			if b.shouldHaveBinaryData {
				errs = append(errs, errors.New("expect to contain `data`, but not found"))
			}
		} else {
			errs = append(errs, errors.New("element is not a type of *streamv1.Element"))
		}

	}
	return
}

func (b *binaryDataChecker) FailureMessage(actual interface{}) (message string) {
	failure := errorsutil.AggregateError(b.failures)
	return format.Message(actual, fmt.Sprintf("to match elements: %v", failure))
}

func (b *binaryDataChecker) NegatedFailureMessage(actual interface{}) (message string) {
	return format.Message(actual, "not to match elements")
}
