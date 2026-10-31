package fractional_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestFractional(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Fractional Suite")
}
