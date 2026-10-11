package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"

	"connectrpc.com/connect"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/gen/syncplane/v1/syncplanev1connect"
	"github.com/levelfourab/syncplane/engine/server"
)

// issuerHeader names the issuer of a request's caller to the authenticator of
// the room specs below.
const issuerHeader = "X-Test-Issuer"

// The room gate admits a caller to a room of a workspace only where the
// workspace gate and the access model admit it, on every path into the room.
var _ = Describe("The room gate in a schema that uses the access model", func() {
	var (
		srv      *server.Server
		presence syncplanev1connect.PresenceServiceClient
		ctx      context.Context
	)

	// caller sets the issuer and subject of a request.
	caller := func(issuer, subject string, h http.Header) {
		h.Set(issuerHeader, issuer)
		h.Set(userHeader, subject)
	}

	BeforeEach(func() {
		ctx = GinkgoT().Context()
		// Given a deployment that accepts two issuers, whose callers' subjects
		// name the issuer as WithCallerAccess advises, and in which only
		// issuer "a"'s user u1 holds a grant
		srv = mustNewServer(newTestStore(), accessEntities(),
			server.WithAuthenticator(server.AuthenticatorFunc(func(_ context.Context, h http.Header) (server.Principal, error) {
				return server.Principal{Issuer: h.Get(issuerHeader), Subject: h.Get(userHeader)}, nil
			})),
			server.WithCallerAccess(func(_ context.Context, p server.Principal, _ string) (server.CallerAccess, error) {
				if p.Subject == "" {
					return server.CallerAccess{}, nil
				}
				return server.CallerAccess{Subjects: []string{p.Issuer + "|" + p.Subject}}, nil
			}),
		)
		Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			return tx.Put(&accessv1.OrgMember{Id: "om-a-u1", UserId: "a|u1", Role: "org_member"})
		})).To(Succeed())
		ts := httptest.NewServer(srv.HTTPHandler())
		DeferCleanup(ts.Close)
		presence = syncplanev1connect.NewPresenceServiceClient(ts.Client(), ts.URL)
	})

	topic := &syncplanev1.Topic{WorkspaceId: ws, Name: "cursors"}

	// watch opens a Watch stream as the caller and returns its first frame
	// that is not a heartbeat. The stream stays open until the spec ends.
	watch := func(issuer, subject, session string) *syncplanev1.WatchResponse {
		GinkgoHelper()
		streamCtx, cancel := context.WithCancel(ctx)
		DeferCleanup(cancel)
		req := connect.NewRequest(&syncplanev1.WatchRequest{SessionId: session, Topics: []*syncplanev1.Topic{topic}})
		caller(issuer, subject, req.Header())
		stream, err := presence.Watch(streamCtx, req)
		Expect(err).NotTo(HaveOccurred())
		for stream.Receive() {
			if !stream.Msg().GetHeartbeat() {
				return stream.Msg()
			}
		}
		Fail("stream closed early: " + stream.Err().Error())
		return nil
	}

	It("refuses a Watch from a caller that holds no permission in the workspace", func() {
		// When issuer "b"'s user u1, who holds no grant, watches the room
		msg := watch("b", "u1", "s-b")

		// Then the room is dropped and no snapshot arrives
		Expect(msg.GetSnapshot()).To(BeNil())
		Expect(msg.GetDropped()).NotTo(BeNil())
	})

	It("drops the room of a caller with no grant yet as NO_ACCESS, which a later grant lifts", func() {
		// When issuer "b"'s user u1, who holds no grant, watches the room
		msg := watch("b", "u1", "s-b")

		// Then the room is dropped as NO_ACCESS, not as a permanent revocation
		Expect(msg.GetDropped().GetReason()).To(Equal(syncplanev1.TopicDropped_REASON_NO_ACCESS))

		// And when a grant names the caller, the same watch is admitted
		Expect(srv.Mutate(server.Trusted(ctx), ws, func(tx *server.EntityTx) error {
			return tx.Put(&accessv1.OrgMember{Id: "om-b-u1", UserId: "b|u1", Role: "org_member"})
		})).To(Succeed())
		Expect(watch("b", "u1", "s-b2").GetSnapshot()).NotTo(BeNil())
	})

	It("refuses a Publish into a room that only a caller of another issuer with the same subject joined", func() {
		publish := func(issuer, subject, session string) error {
			req := connect.NewRequest(&syncplanev1.PublishRequest{
				SessionId: session,
				Items: []*syncplanev1.PublishItem{{
					Topic: topic,
					Body:  &syncplanev1.PublishItem_Message{Message: []byte("from " + issuer)},
				}},
			})
			caller(issuer, subject, req.Header())
			_, err := presence.Publish(ctx, req)
			return err
		}

		// Given issuer "b"'s user u1, who holds no grant in the workspace,
		// cannot publish to the room under a session of its own
		Expect(connect.CodeOf(publish("b", "u1", "s-b"))).To(Equal(connect.CodePermissionDenied))

		// And issuer "a"'s user u1 watches the room with session s-a
		Expect(watch("a", "u1", "s-a").GetSnapshot()).NotTo(BeNil())

		// When issuer "b"'s user u1 publishes to the room under session s-a
		err := publish("b", "u1", "s-a")

		// Then the publish is refused as well
		Expect(connect.CodeOf(err)).To(Equal(connect.CodePermissionDenied),
			"a caller with no grant published into the workspace's room: %v", err)
	})
})
