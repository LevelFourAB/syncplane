package server

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
)

// The fingerprint is what a cursor will carry to notice that the rules
// deciding visibility changed under it, so it has to move with every such
// change and stay put for every other one.
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
		m, err := compileAccessModel(c)
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

	DescribeTable("moves when the model changes",
		func(change func([]DynamicEntity)) {
			s := specs()
			change(s)
			Expect(compile(s).fingerprint).NotTo(Equal(compile(specs()).fingerprint))
		},
		Entry("a role grants another permission", func(s []DynamicEntity) {
			s[0].Grant.Roles[0].Grants = []string{"view", "edit"}
		}),
		Entry("a role is renamed", func(s []DynamicEntity) {
			s[0].Grant.Roles[0].Name = "viewer"
		}),
		Entry("an entity swaps its permissions", func(s []DynamicEntity) {
			s[1].Access = &syncplanev1.AccessOptions{Read: "edit", Write: "view"}
		}),
		Entry("the grant subject moves to another field", func(s []DynamicEntity) {
			s[0].Fields = append(s[0].Fields, DynamicField{Name: "email", Number: 5, Kind: str})
			s[0].Grant.Subject = "email"
		}),
		Entry("the access parent is dropped", func(s []DynamicEntity) {
			s[0].Fields[1].AccessParent = false
		}),
	)
})
