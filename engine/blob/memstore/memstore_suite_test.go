package memstore_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/blob"
	"github.com/levelfourab/syncplane/engine/blob/blobtest"
	"github.com/levelfourab/syncplane/engine/blob/memstore"
)

func TestMemstore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Blob Memstore Suite")
}

var _ = blobtest.Conformance("memory", func(now func() time.Time) blob.Store {
	return memstore.New(memstore.WithClock(now))
})

// One Store handed back twice: a memory Store is shared only within its
// process, by every caller that holds it.
var _ = blobtest.Shared("memory", func(now func() time.Time) (blob.Store, blob.Store, func()) {
	s := memstore.New(memstore.WithClock(now))
	return s, s, func() { Expect(s.Close()).To(Succeed()) }
})
