package server

import (
	"encoding/hex"
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanecursorv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/cursor/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
)

// The canonical encoding is the input of hashes that cursors and clients
// hold across deploys. These specs pin it byte for byte, and pin the hashes
// built on it, so a change to either shows here before it resets clients.
var _ = Describe("canonical encoding", func() {
	encode := func(m protoreflect.ProtoMessage) string {
		return hex.EncodeToString(appendCanonical(nil, m.ProtoReflect()))
	}

	It("encodes strings by field number, with their lengths", func() {
		Expect(encode(&syncplanecursorv1.AnchorRef{Entity: "task", Key: "t1"})).
			To(Equal("01" + "04" + "7461736b" + "02" + "02" + "7431"))
	})

	It("encodes a bool, a list of messages, and each message with its length", func() {
		pa := &syncplanecursorv1.PermissionAnchors{
			Permission: "view",
			Root:       true,
			Rows:       []*syncplanecursorv1.ClosureSet_Entry{{Entity: "project", Keys: []string{"p1", "p2"}}},
		}
		Expect(encode(pa)).To(Equal("01" + "04" + "76696577" +
			"02" + "01" +
			"03" + "01" + "11" + ("01" + "07" + "70726f6a656374" + "02" + "02" + "02" + "7031" + "02" + "7032")))
	})

	It("encodes a map in the order of its keys' encodings, whatever order it was built in", func() {
		st := &syncplanecursorv1.WithinState{
			Subjects: map[string]string{"owner_id": "ann", "b": "x"},
			Matched:  []string{"m1"},
		}
		Expect(encode(st)).To(Equal("01" + "02" +
			"01" + "62" + "01" + "78" +
			"08" + "6f776e65725f6964" + "03" + "616e6e" +
			"02" + "01" + "02" + "6d31"))
	})

	It("leaves out fields that are not set", func() {
		Expect(encode(&syncplanecursorv1.EntityReadRule{Entity: "a"})).To(Equal("01" + "01" + "61"))
		Expect(encode(&syncplanecursorv1.ReadersRule{})).To(BeEmpty())
	})

	It("encodes a signed integer as its zigzag varint, an unsigned one as its varint, and a double as its bits", func() {
		Expect(encode(&syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_IntValue{IntValue: -1}})).To(Equal("03" + "01"))
		Expect(encode(&syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_IntValue{IntValue: 0}})).To(Equal("03" + "00"))
		Expect(encode(&syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_UintValue{UintValue: 300}})).To(Equal("04" + "ac02"))
		Expect(encode(&syncplanev1.QueryValue{Kind: &syncplanev1.QueryValue_DoubleValue{DoubleValue: 1.5}})).
			To(Equal("02" + "3ff8000000000000"))
	})

	Describe("the hashes built on it", func() {
		rules := []*syncplanecursorv1.EntityReadRule{
			{Entity: "note", Open: true},
			{
				Entity: "task",
				Read: &syncplanecursorv1.ReadersRule{
					Permissions: []string{"view_tasks"},
					Checked: []*syncplanecursorv1.CheckedRule{{
						Ops:      []string{"read"},
						Requires: []string{"view_own"},
						CallerIs: "owner_id",
						When:     query.Eq("archived", false),
						Within:   []*syncplanecursorv1.WithinRule{{Entity: "project", When: query.Eq("private", false)}},
					}},
				},
				Parents: []*syncplanecursorv1.ParentCase{{Path: "project", Entity: "project"}},
				Hides: []*syncplanecursorv1.HidingGuard{{
					Units: []string{"salary"},
					Read:  &syncplanecursorv1.ReadersRule{Permissions: []string{"view_pay"}},
				}},
			},
		}
		access := &syncplanecursorv1.ReadAccess{
			Subjects: []string{"user:ann"},
			Permissions: []*syncplanecursorv1.PermissionAnchors{
				{Permission: "view_pay", Root: true},
				{Permission: "view_tasks", Rows: []*syncplanecursorv1.ClosureSet_Entry{{Entity: "project", Keys: []string{"p1", "p2"}}}},
			},
			Rules:          rules,
			MaxAccessDepth: 32,
		}
		summary := &syncplanev1.CallerAccess{
			Subjects:        []string{"user:ann"},
			RootPermissions: []string{"view_pay"},
			AnchorGroups: []*syncplanev1.AnchorGroup{{
				Permissions: []string{"view_tasks"},
				Anchors:     []*syncplanev1.AnchorRows{{Entity: "project", Keys: []string{"p1", "p2"}}},
			}},
		}

		It("keeps the read-policy fingerprint", func() {
			Expect(fmt.Sprintf("%016x", readPolicyOf(rules, 32))).To(Equal("d811599a3bc1323d"))
		})

		It("moves the read-policy fingerprint with the depth limit", func() {
			Expect(readPolicyOf(rules, 31)).NotTo(Equal(readPolicyOf(rules, 32)))
		})

		It("keeps the access hash, which leaves out the rules and the depth limit", func() {
			Expect(fmt.Sprintf("%016x", readAccessHash(access))).To(Equal("6708479c2cf8b98f"))
			bare := &syncplanecursorv1.ReadAccess{Subjects: access.GetSubjects(), Permissions: access.GetPermissions()}
			Expect(readAccessHash(bare)).To(Equal(readAccessHash(access)))
		})

		It("keeps the CallerAccess version", func() {
			version, _ := callerAccessHash(summary)
			Expect(fmt.Sprintf("%016x", version)).To(Equal("750166ff8ea1b7b9"))
		})
	})
})
