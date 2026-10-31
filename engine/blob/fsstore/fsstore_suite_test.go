package fsstore_test

import (
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/blob"
	"github.com/levelfourab/syncplane/engine/blob/blobtest"
	"github.com/levelfourab/syncplane/engine/blob/fsstore"
)

func TestFsstore(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Blob Fsstore Suite")
}

func open(dir string, now func() time.Time) *fsstore.Store {
	GinkgoHelper()
	s, err := fsstore.Open(dir, fsstore.WithClock(now))
	Expect(err).NotTo(HaveOccurred())
	return s
}

var _ = blobtest.Conformance("fs", func(now func() time.Time) blob.Store {
	return open(GinkgoT().TempDir(), now)
})

// Two Stores over one directory stand in for two processes that share it.
var _ = blobtest.Shared("fs", func(now func() time.Time) (blob.Store, blob.Store, func()) {
	dir := GinkgoT().TempDir()
	a, b := open(dir, now), open(dir, now)
	return a, b, func() {
		Expect(a.Close()).To(Succeed())
		Expect(b.Close()).To(Succeed())
	}
})
