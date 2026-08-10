package otelstore_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestOtelstore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Otelstore Suite")
}
