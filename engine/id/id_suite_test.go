package id_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestID(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ID Suite")
}
