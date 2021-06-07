package physical_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestPhysical(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Physical Suite")
}
