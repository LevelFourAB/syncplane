package server

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
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
				Access: rwAccess("view", "edit"),
			},
			{
				Name:      "project",
				KeyFields: []string{"id"},
				Fields:    []DynamicField{{Name: "id", Number: 1, Kind: str}},
				Access:    rwAccess("view", "edit"),
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
		m, err := compileAccessModel(c, nil, 32, false)
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
			s[1].Access = rwAccess("view", "view")
		}),
		Entry("an entity splits its write rule by operation", func(s []DynamicEntity) {
			s[1].Access.Allow = append(s[1].Access.Allow[:1],
				&syncplanev1.AccessRule{Create: true, Requires: []string{"edit"}},
				&syncplanev1.AccessRule{Update: true, Delete: true, Requires: []string{"view"}})
		}),
		Entry("an entity declares its rules in another order", func(s []DynamicEntity) {
			allow := s[1].Access.Allow
			allow[0], allow[1] = allow[1], allow[0]
		}),
	)

	DescribeTable("moves when a read rule changes",
		func(change func([]DynamicEntity)) {
			s := specs()
			change(s)
			Expect(compile(s).fingerprint).NotTo(Equal(compile(specs()).fingerprint))
		},
		Entry("an entity swaps its permissions", func(s []DynamicEntity) {
			s[1].Access = rwAccess("edit", "view")
		}),
		Entry("an entity reads by a second permission", func(s []DynamicEntity) {
			s[1].Access.Allow = append(s[1].Access.Allow,
				&syncplanev1.AccessRule{Read: true, Requires: []string{"edit"}})
		}),
		Entry("an entity is read by every admitted caller", func(s []DynamicEntity) {
			s[1].Access.Allow[0].Requires, s[1].Access.Allow[0].Admitted = nil, &syncplanev1.AdmittedCaller{}
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

// rwAccess returns access options with one rule that admits reads by read and
// one that admits every write by write.
func rwAccess(read, write string) *syncplanev1.AccessOptions {
	return &syncplanev1.AccessOptions{Allow: []*syncplanev1.AccessRule{
		{Read: true, Requires: []string{read}},
		{Write: true, Requires: []string{write}},
	}}
}

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
// every drain. A text delta verdict reads nothing of that access, so it stays
// true across bindings: what keeps delta state safe for a caller below the
// root lives on the write paths and in the resync (see recordReadGaps).
var _ = Describe("rowEmitter text delta verdict", func() {
	var srv *Server

	BeforeEach(func() {
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "test.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		srv, err = New(store, WithEntities(
			&accessv1.OrgMember{}, &accessv1.Project{}, &accessv1.ProjectMember{},
			&accessv1.Task{}, &accessv1.Label{}, &accessv1.Note{},
		))
		Expect(err).NotTo(HaveOccurred())
	})

	newEmitter := func() *rowEmitter {
		GinkgoHelper()
		e, err := newRowEmitter(context.Background(), srv, "ws1", nil)
		Expect(err).NotTo(HaveOccurred())
		e.caller = &accessCaller{model: srv.catalogPtr().access}
		return e
	}

	belowRoot := &accessSet{perms: map[string]*permAnchors{"view_tasks": {
		rows: map[anchorRow]struct{}{{entity: "project", key: "p1"}: {}},
	}}}

	It("fails until the access is bound", func() {
		_, err := newEmitter().deltaSafe("task")
		Expect(err).To(MatchError(errAccessUnbound))
	})

	It("admits a caller who reads at the root and one who reads below it", func() {
		e := newEmitter()
		e.useAccess(&accessSet{perms: map[string]*permAnchors{"view_tasks": {root: true}}}, nil)
		Expect(e.deltaSafe("task")).To(BeTrue())

		e = newEmitter()
		e.useAccess(belowRoot, nil)
		Expect(e.deltaSafe("task")).To(BeTrue())
	})

	It("declines an entity the application's ReadFilter filters", func() {
		e := newEmitter()
		e.filterSource = func(string) (RowFilter, error) {
			return func(string, proto.Message) bool { return true }, nil
		}
		e.useAccess(belowRoot, nil)
		Expect(e.deltaSafe("task")).To(BeFalse(),
			"the filter reads the row, and nothing records what it read at the cursor")
	})
})

// A move compares where each way of reading the rows below it holds, so
// loose must keep apart every rule that admits other rows than the plain
// permissions do.
var _ = Describe("readers.loose", func() {
	// owned stands for a rule that checks the row: loose drops the check, so
	// which check it is does not matter here.
	owned := func(requires ...string) accessRule {
		return accessRule{ops: opRead, requires: requires, whenSpec: "owned"}
	}
	rule := func(requires ...string) accessRule {
		return accessRule{ops: opRead, requires: requires}
	}

	It("keeps a rule with a check on the row apart from the plain permissions", func() {
		r := readers{perms: []string{"view_all"}, checked: []accessRule{owned("view_own")}}
		Expect(r.loose()).To(Equal(readers{perms: []string{"view_all"}, checked: []accessRule{rule("view_own")}}))
	})

	It("keeps an admitted rule with a check on the row instead of reading every row", func() {
		r := readers{perms: []string{"view_all"}, checked: []accessRule{owned()}}
		Expect(r.loose()).To(Equal(readers{perms: []string{"view_all"}, checked: []accessRule{rule()}}))
	})

	It("drops a rule whose permission is one of the plain permissions", func() {
		r := readers{perms: []string{"view_all"}, checked: []accessRule{owned("view_all"), rule("view_all", "use")}}
		Expect(r.loose()).To(Equal(readers{perms: []string{"view_all"}}))
	})

	It("keeps a rule that needs several permissions beside a checked rule that needs one of them", func() {
		r := readers{checked: []accessRule{owned("view_own"), rule("use", "view_own")}}
		Expect(r.loose()).To(Equal(readers{checked: []accessRule{rule("use", "view_own"), rule("view_own")}}))
	})

	It("keeps two rules with the same permissions once", func() {
		r := readers{checked: []accessRule{owned("view_own"), {ops: opRead, requires: []string{"view_own"}, whenSpec: "open"}}}
		Expect(r.loose()).To(Equal(readers{checked: []accessRule{rule("view_own")}}))
	})
})
