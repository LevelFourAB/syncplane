package presence_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/presence"
	"github.com/levelfourab/syncplane/engine/presence/presencetest"
)

func TestPresence(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Presence Suite")
}

var _ = presencetest.Conformance("local", func() presence.Bus {
	return presence.NewLocal()
})
