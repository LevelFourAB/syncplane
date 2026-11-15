package connect_test

import (
	"context"
	"errors"
	"fmt"
	"sync"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	engineconnect "github.com/levelfourab/syncplane/engine/connect"
	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
)

// closingStore is a memStore of one workspace that counts its Close calls.
type closingStore struct {
	*memStore
	closed *int
	mu     *sync.Mutex
}

func (s closingStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	*s.closed++
	return nil
}

// storeOpener opens one closingStore for each workspace, over one memStore.
type storeOpener struct {
	mu     sync.Mutex
	store  *memStore
	opened map[string]int
	closed map[string]*int
	fail   error
}

func (o *storeOpener) open(_ context.Context, ws string) (engineconnect.RecordStore, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.fail != nil {
		return nil, o.fail
	}
	o.opened[ws]++
	if o.closed[ws] == nil {
		o.closed[ws] = new(int)
	}
	return closingStore{memStore: o.store, closed: o.closed[ws], mu: &o.mu}, nil
}

func (o *storeOpener) counts(ws string) (opened, closed int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if c := o.closed[ws]; c != nil {
		closed = *c
	}
	return o.opened[ws], closed
}

var _ = Describe("Record mode, OpenStore", func() {
	var ctx context.Context

	BeforeEach(func() { ctx = context.Background() })

	newOpenFixture := func() (*recFixture, *storeOpener) {
		GinkgoHelper()
		o := &storeOpener{opened: map[string]int{}, closed: map[string]*int{}}
		f := newRecFixture(withRecords(func(r *engineconnect.Records) {
			o.store = r.Types[0].Store.(*memStore)
			r.Types[0].Store = nil
			r.Types[0].OpenStore = o.open
		}))
		return f, o
	}

	It("refuses a type with both or neither of Store and OpenStore", func() {
		f := newRecFixture()
		build := func(t engineconnect.RecordType) error {
			t.Root, t.Codec = &testdatav1.Project{}, f.codec
			_, err := engineconnect.New(engineconnect.Config{Server: f.srv, WriterID: "x", Records: &engineconnect.Records{
				Types: []engineconnect.RecordType{t},
			}})
			return err
		}
		o := &storeOpener{}
		Expect(build(engineconnect.RecordType{})).To(MatchError(ContainSubstring("Store")))
		Expect(build(engineconnect.RecordType{Store: f.store, OpenStore: o.open})).To(MatchError(ContainSubstring("OpenStore")))
	})

	It("opens a store for each workspace on its first pass, reuses it, and closes it on Forget and Close", func() {
		f, o := newOpenFixture()
		f.store.put("ws1", "a.txt", "A\none")
		Expect(f.conn.Reconcile(ctx, "ws1")).To(Succeed())
		Expect(f.conn.Reconcile(ctx, "ws1")).To(Succeed())
		Expect(f.lines(ctx, "ws1", f.key(ctx, "ws1", "a.txt"))).To(Equal("A\none"))
		Expect(o.counts("ws1")).To(Equal(1))

		Expect(f.conn.Reconcile(ctx, "ws2")).To(Succeed())
		f.conn.Forget("ws1")
		opened, closed := o.counts("ws1")
		Expect([]int{opened, closed}).To(Equal([]int{1, 1}))
		_, closed = o.counts("ws2")
		Expect(closed).To(BeZero())

		Expect(f.conn.Close()).To(Succeed())
		opened, closed = o.counts("ws2")
		Expect([]int{opened, closed}).To(Equal([]int{1, 1}))
	})

	It("fails the pass when OpenStore fails, and opens again on the next pass", func() {
		f, o := newOpenFixture()
		o.mu.Lock()
		o.fail = errors.New("no folder")
		o.mu.Unlock()
		Expect(f.conn.Reconcile(ctx, "ws1")).To(MatchError(`connect: open project store for workspace "ws1": no folder`))

		o.mu.Lock()
		o.fail = nil
		o.mu.Unlock()
		f.store.put("ws1", "a.txt", "A")
		Expect(f.conn.Reconcile(ctx, "ws1")).To(Succeed())
		Expect(f.projects(ctx, "ws1")).To(HaveLen(1))
	})

	It("closes the store after a pass that fails with ErrNeedsUserAction, and opens it again on the next pass", func() {
		f, o := newOpenFixture()
		f.store.put("ws1", "a.txt", "A")
		Expect(f.conn.Reconcile(ctx, "ws1")).To(Succeed())

		f.store.fail(fmt.Errorf("token revoked: %w", engineconnect.ErrNeedsUserAction), nil)
		Expect(f.conn.Reconcile(ctx, "ws1")).To(MatchError(engineconnect.ErrNeedsUserAction))
		opened, closed := o.counts("ws1")
		Expect([]int{opened, closed}).To(Equal([]int{1, 1}))

		f.store.fail(nil, nil)
		Expect(f.conn.Reconcile(ctx, "ws1")).To(Succeed())
		opened, closed = o.counts("ws1")
		Expect([]int{opened, closed}).To(Equal([]int{2, 1}))
	})
})
