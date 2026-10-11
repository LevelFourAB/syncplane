package server_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/reflect/protoreflect"

	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/server"
)

// A registration error names where the access option says what it refuses,
// in the path a reader follows in the declaration: the guard, then the rule
// of that guard.
var _ = Describe("Access model registration errors", func() {
	var srv *server.Server

	BeforeEach(func() {
		srv = newBareServer()
	})

	register := func(specs []server.DynamicEntity) error {
		GinkgoHelper()
		_, err := srv.RegisterDescriptorSet(descriptorSetBytes(specs...))
		return err
	}

	// guarded is the org schema with a status field on task that guard[0]
	// restricts with rules, which change sets up.
	guarded := func(rules ...*syncplanev1.AccessRule) []server.DynamicEntity {
		specs := orgSpecs()
		task := specNamed(specs, "task")
		task.Fields = append(task.Fields, server.DynamicField{Name: "status", Number: 3, Kind: protoreflect.StringKind})
		task.Access.Guard = []*syncplanev1.FieldGuard{{Fields: []string{"status"}, Allow: rules}}
		return specs
	}

	DescribeTable("names the guard and its rule",
		func(rules []*syncplanev1.AccessRule, want string) {
			// Given a guard whose second rule breaks a rule of the model
			specs := guarded(rules...)

			// When it registers
			err := register(specs)

			// Then the error names the guard's rule, not a rule of allow
			Expect(err).To(MatchError(ContainSubstring(want)))
		},
		Entry("a rule without an operation",
			[]*syncplanev1.AccessRule{admittedRule("read"), {Requires: []string{"edit_tasks"}}},
			"access guard[0] allow[1] names no operation"),
		Entry("a rule without a source",
			[]*syncplanev1.AccessRule{admittedRule("read"), opsRule("write")},
			"access guard[0] allow[1] names no source"),
		Entry("a within entry that names an entity outside the model",
			[]*syncplanev1.AccessRule{admittedRule("read"), func() *syncplanev1.AccessRule {
				r := permRule("edit_tasks", "write")
				r.Within = []*syncplanev1.AccessWithin{{Entity: "nothing", CallerIs: "id"}}
				return r
			}()},
			"access guard[0] allow[1] within[0]"),
	)
})
