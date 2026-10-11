package pgpresence

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/presence"
)

// A node that predates the issuer field sends events without it. A record
// belongs to its issuer and subject together, so such an event must not be
// read as one of any issuer, the empty issuer included.
var _ = Describe("the notification wire format", func() {
	topic := presence.Topic{WorkspaceID: "ws", Name: "cursors"}
	prefix := "node-old" + fieldSep + routingPrefix(topic) + fieldSep

	It("carries the issuer, also when it is empty", func() {
		for _, issuer := range []string{"https://id.example.com", ""} {
			line, err := encodeBody(presence.Event{
				Topic: topic,
				Kind:  presence.KindState,
				Participant: presence.Participant{
					SessionID: "s1", Issuer: issuer, Subject: "u1", Revision: 1,
				},
			})
			Expect(err).NotTo(HaveOccurred())
			_, ev, ok, err := decode(prefix + line)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeTrue())
			Expect(ev.Participant.Issuer).To(Equal(issuer))
			Expect(ev.Participant.Subject).To(Equal("u1"))
		}
	})

	It("drops a state or a leave from a node that sends no issuer", func() {
		for _, raw := range []string{
			`{"k":0,"sid":"s1","sub":"u1","rev":9,"st":"c3Bvb2Y="}`,
			`{"k":1,"sid":"s1","sub":"u1"}`,
		} {
			_, _, ok, err := decode(prefix + raw)
			Expect(err).NotTo(HaveOccurred())
			Expect(ok).To(BeFalse(), raw)
		}
	})

	It("delivers a broadcast from a node that sends no issuer", func() {
		_, ev, ok, err := decode(prefix + `{"k":2,"sid":"s1","sub":"u1","pl":"aGk="}`)
		Expect(err).NotTo(HaveOccurred())
		Expect(ok).To(BeTrue())
		Expect(ev.Kind).To(Equal(presence.KindMessage))
		Expect(string(ev.Payload)).To(Equal("hi"))
	})
})
