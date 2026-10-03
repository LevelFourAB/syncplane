package pgpresence

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/presence"
)

// The publish loop drains this queue as fast as the database answers, so a
// fold only happens when the database is slow. These specs drive the queue on
// its own to reach that case without a slow database.
var _ = Describe("publish queue", func() {
	var q *queue

	BeforeEach(func() {
		q = &queue{}
		q.init()
	})

	topic := presence.Topic{WorkspaceID: "ws", Name: "cursors"}

	push := func(session, subject string, rev uint64, line string) {
		q.push(presence.Event{
			Topic: topic,
			Kind:  presence.KindState,
			Participant: presence.Participant{
				SessionID: session,
				Subject:   subject,
				Revision:  rev,
			},
		}, line)
	}

	// drain pops what is queued. pop hands over a queued entry before it looks
	// at the context, so a cancelled one stops it only once the queue is empty.
	drain := func() []string {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var lines []string
		for {
			item, ok := q.pop(ctx)
			if !ok {
				return lines
			}
			lines = append(lines, item.line)
		}
	}

	It("folds a burst from one session into one entry with the newest state", func() {
		push("s1", "u1", 1, "v1")
		push("s1", "u1", 2, "v2")
		push("s1", "u1", 3, "v3")
		Expect(drain()).To(Equal([]string{"v3"}))
	})

	It("keeps the higher revision when a lower one is queued behind it", func() {
		push("s1", "u1", 5, "v5")
		push("s1", "u1", 4, "v4")
		Expect(drain()).To(Equal([]string{"v5"}))
	})

	It("does not fold state from two subjects that name one session", func() {
		push("s1", "u1", 5, "mine")
		push("s1", "u2", 9, "spoofed")
		Expect(drain()).To(Equal([]string{"mine", "spoofed"}))
	})
})
