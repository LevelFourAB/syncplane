package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	accessv1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/access/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/storage"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

// The fingerprint is what a cursor carries to notice that the read rules its
// rows were chosen by changed under it, so it has to move with every such
// change and stay put for every other one. What roles grant moves the anchors
// a cursor records instead.
var _ = Describe("access model fingerprint", func() {
	str := protoreflect.StringKind
	specs := func() []DynamicEntity {
		return []DynamicEntity{
			{
				Name:      "member",
				KeyFields: []string{"id"},
				Fields: []DynamicField{
					{Name: "id", Number: 1, Kind: str},
					{Name: "project", Number: 2, Ref: "project", AccessParent: true},
					{Name: "user_id", Number: 3, Kind: str},
					{Name: "role", Number: 4, Kind: str},
				},
				Grant: &syncplanev1.GrantOptions{Subject: "user_id", Role: "role", Roles: []*syncplanev1.Role{
					{Name: "reader", Grants: []string{"view"}},
					{Name: "writer", Grants: []string{"view", "edit"}},
				}},
				Access: &syncplanev1.AccessOptions{Read: "view", Write: "edit"},
			},
			{
				Name:      "project",
				KeyFields: []string{"id"},
				Fields:    []DynamicField{{Name: "id", Number: 1, Kind: str}},
				Access:    &syncplanev1.AccessOptions{Read: "view", Write: "edit"},
			},
		}
	}
	compile := func(specs []DynamicEntity) *accessModel {
		GinkgoHelper()
		fd, err := BuildDynamicFile(specs...)
		Expect(err).NotTo(HaveOccurred())
		c := &catalog{entities: map[string]*entityInfo{}, byType: map[protoreflect.FullName]*entityInfo{}}
		for i := 0; i < fd.Messages().Len(); i++ {
			info, err := buildEntityInfo(dynamicpb.NewMessageType(fd.Messages().Get(i)))
			Expect(err).NotTo(HaveOccurred())
			c.entities[info.name] = info
			c.byType[info.msgType.Descriptor().FullName()] = info
		}
		m, err := compileAccessModel(c, nil, 32)
		Expect(err).NotTo(HaveOccurred())
		Expect(m).NotTo(BeNil())
		return m
	}

	It("is stable for the same model", func() {
		Expect(compile(specs()).fingerprint).To(Equal(compile(specs()).fingerprint))
		Expect(compile(specs()).fingerprint).NotTo(BeZero())
	})

	It("ignores the order roles and grants are declared in", func() {
		s := specs()
		roles := s[0].Grant.Roles
		roles[0], roles[1] = roles[1], roles[0]
		roles[0].Grants = []string{"edit", "view"}
		Expect(compile(s).fingerprint).To(Equal(compile(specs()).fingerprint))
	})

	It("ignores a field outside the model", func() {
		s := specs()
		s[1].Fields = append(s[1].Fields, DynamicField{Name: "title", Number: 2, Kind: str})
		Expect(compile(s).fingerprint).To(Equal(compile(specs()).fingerprint))
	})

	DescribeTable("stays put when only what a caller holds changes",
		func(change func([]DynamicEntity)) {
			s := specs()
			change(s)
			Expect(compile(s).fingerprint).To(Equal(compile(specs()).fingerprint))
		},
		Entry("a role grants another permission", func(s []DynamicEntity) {
			s[0].Grant.Roles[0].Grants = []string{"view", "edit"}
		}),
		Entry("a role is renamed", func(s []DynamicEntity) {
			s[0].Grant.Roles[0].Name = "viewer"
		}),
		Entry("the grant subject moves to another field", func(s []DynamicEntity) {
			s[0].Fields = append(s[0].Fields, DynamicField{Name: "email", Number: 5, Kind: str})
			s[0].Grant.Subject = "email"
		}),
		Entry("an entity writes by another permission", func(s []DynamicEntity) {
			s[1].Access.Write = "view"
		}),
	)

	DescribeTable("moves when a read rule changes",
		func(change func([]DynamicEntity)) {
			s := specs()
			change(s)
			Expect(compile(s).fingerprint).NotTo(Equal(compile(specs()).fingerprint))
		},
		Entry("an entity swaps its permissions", func(s []DynamicEntity) {
			s[1].Access = &syncplanev1.AccessOptions{Read: "edit", Write: "view"}
		}),
		Entry("the access parent is dropped", func(s []DynamicEntity) {
			s[0].Fields[1].AccessParent = false
		}),
		Entry("the access parent moves to another field", func(s []DynamicEntity) {
			s[0].Fields[1].AccessParent = false
			s[0].Fields = append(s[0].Fields, DynamicField{Name: "home", Number: 5, Ref: "project", AccessParent: true})
		}),
	)
})

// The conformance vectors are shared with the TS client
// (client/test/unit/access-vectors.test.ts): the engine's decision and the
// client's prediction must agree wherever both can decide.
const accessVectorsPath = "../../testdata/access/can.json"

type accessVector struct {
	Name       string                      `json:"name"`
	Access     map[string]accessVectorPerm `json:"access"`
	Permission string                      `json:"permission"`
	Entity     string                      `json:"entity"`
	Key        string                      `json:"key"`
	Chain      []json.RawMessage           `json:"chain"`
	Want       string                      `json:"want"`
}

type accessVectorPerm struct {
	Root    bool                `json:"root"`
	Anchors map[string][]string `json:"anchors"`
}

var _ = Describe("access conformance vectors", func() {
	raw, err := os.ReadFile(accessVectorsPath)
	if err != nil {
		panic(err)
	}
	var file struct {
		Cases []accessVector `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		panic(err)
	}
	for _, c := range file.Cases {
		if c.Access == nil || slices.ContainsFunc(c.Chain, func(r json.RawMessage) bool { return string(r) == `"unknown"` }) {
			continue // only a client can lack these inputs
		}
		It(c.Name, func() {
			set := &accessSet{perms: map[string]*permAnchors{}}
			for perm, p := range c.Access {
				a := &permAnchors{root: p.Root}
				for entity, keys := range p.Anchors {
					for _, k := range keys {
						if a.rows == nil {
							a.rows = map[anchorRow]struct{}{}
						}
						a.rows[anchorRow{entity: entity, key: k}] = struct{}{}
					}
				}
				set.perms[perm] = a
			}
			// The chain is the whole of what the walker reads: every edge is
			// an override, and the last row's edge is unset.
			chains := newAccessChainsFrom(nil, func(string, string) (storage.Row, error) {
				Fail("the walk read a row the chain does not name")
				return storage.Row{}, nil
			}, 32, nil)
			chains.overrides = map[anchorRow]chainEdge{}
			at := anchorRow{entity: c.Entity, key: c.Key}
			for _, raw := range c.Chain {
				var p struct{ Entity, Key string }
				Expect(json.Unmarshal(raw, &p)).To(Succeed())
				next := anchorRow{entity: p.Entity, key: p.Key}
				chains.overrides[at] = chainEdge{parent: next, ok: true}
				at = next
			}
			chains.overrides[at] = chainEdge{}
			got, err := chains.holds(set.perms[c.Permission], &entityInfo{name: c.Entity}, c.Key, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(c.Want == "yes"))
		})
	}
})

// A Subscribe stream keeps one emitter and binds the caller's access again on
// every drain, so no verdict the emitter memoizes from the access it held may
// outlive a new binding.
var _ = Describe("rowEmitter access binding", func() {
	It("judges text delta safety under the access bound now", func() {
		ctx := context.Background()
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		srv, err := New(store, WithEntities(
			&accessv1.OrgMember{}, &accessv1.Project{}, &accessv1.ProjectMember{},
			&accessv1.Task{}, &accessv1.Label{}, &accessv1.Note{},
		))
		Expect(err).NotTo(HaveOccurred())
		e, err := newRowEmitter(ctx, srv, "ws1", nil)
		Expect(err).NotTo(HaveOccurred())
		e.caller = &accessCaller{model: srv.catalogPtr().access}

		e.useAccess(&accessSet{perms: map[string]*permAnchors{"view_tasks": {root: true}}}, nil)
		safe, err := e.deltaSafe("task")
		Expect(err).NotTo(HaveOccurred())
		Expect(safe).To(BeTrue(), "every task is visible to a caller who reads tasks at the root")

		// Below the root the model filters tasks, so a task the caller sees for
		// the first time can arrive in a forward page and must ship whole.
		e.useAccess(&accessSet{perms: map[string]*permAnchors{"view_tasks": {
			rows: map[anchorRow]struct{}{{entity: "project", key: "p1"}: {}},
		}}}, nil)
		safe, err = e.deltaSafe("task")
		Expect(err).NotTo(HaveOccurred())
		Expect(safe).To(BeFalse())
	})
})
