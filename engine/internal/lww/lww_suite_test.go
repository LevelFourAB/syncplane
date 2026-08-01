package lww_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestLww(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LWW Merge Suite")
}
