package protoopts_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestProtoopts(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Protoopts Suite")
}
