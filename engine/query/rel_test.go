package query_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/query"
)

var issueDesc = (&testdatav1.Issue{}).ProtoReflect().Descriptor()

// rewriteFor runs the two-phase execution the way an engine does: extract the
// rel nodes, select the matching targets from `targets` with each node's inner
// condition, and rewrite the predicate row-local.
func rewriteFor(cond *syncplanev1.Condition, targets map[string][]proto.Message) *query.Compiled {
	GinkgoHelper()
	rels, err := query.ExtractRels(issueDesc, cond)
	Expect(err).NotTo(HaveOccurred())
	replacements := make(map[*syncplanev1.RelCond]*syncplanev1.Condition, len(rels))
	for _, node := range rels {
		inner, err := query.Compile(node.Target, node.Cond.GetCond())
		Expect(err).NotTo(HaveOccurred())
		var matched [][]*syncplanev1.QueryValue
		for _, msg := range targets[string(node.Target.Name())] {
			if inner.Eval(msg.ProtoReflect()) {
				matched = append(matched, query.TargetKeyValues(node, msg.ProtoReflect()))
			}
		}
		replacements[node.Cond] = query.RelReplacement(node, matched, "ws")
	}
	compiled, err := query.Compile(issueDesc, query.RewriteRels(cond, replacements))
	Expect(err).NotTo(HaveOccurred())
	return compiled
}

var _ = Describe("Rel", func() {
	users := []proto.Message{
		&testdatav1.User{Id: "u1", Name: "Maya"},
		&testdatav1.User{Id: "u2", Name: "Noor"},
	}
	pairs := []proto.Message{
		&testdatav1.Pair{Ns: "a", Id: "p1", Label: "keep"},
		&testdatav1.Pair{Ns: "b", Id: "p1", Label: "other"},
	}
	issues := []proto.Message{
		&testdatav1.Issue{Id: "i2", State: "open"},
		&testdatav1.Issue{Id: "i3", State: "done"},
	}
	targets := map[string][]proto.Message{"User": users, "Pair": pairs, "Issue": issues}

	It("matches a to-one ref whose target satisfies the condition", func() {
		compiled := rewriteFor(query.Rel("assignee", query.Eq("name", "Maya")), targets)
		Expect(compiled.Eval((&testdatav1.Issue{Assignee: &testdatav1.User{Id: "u1"}}).ProtoReflect())).To(BeTrue())
		Expect(compiled.Eval((&testdatav1.Issue{Assignee: &testdatav1.User{Id: "u2"}}).ProtoReflect())).To(BeFalse())
	})

	It("treats an unset to-one ref and an orphaned stub as no match", func() {
		compiled := rewriteFor(query.Rel("assignee", query.Eq("name", "Maya")), targets)
		Expect(compiled.Eval((&testdatav1.Issue{}).ProtoReflect())).To(BeFalse())
		Expect(compiled.Eval((&testdatav1.Issue{Assignee: &testdatav1.User{Id: "ghost"}}).ProtoReflect())).To(BeFalse())
	})

	It("matches a repeated ref when SOME target satisfies the condition", func() {
		compiled := rewriteFor(query.Rel("blocked_by", query.Eq("state", "open")), targets)
		row := &testdatav1.Issue{BlockedBy: []*testdatav1.Issue{{Id: "i3"}, {Id: "i2"}}}
		Expect(compiled.Eval(row.ProtoReflect())).To(BeTrue())
		only := &testdatav1.Issue{BlockedBy: []*testdatav1.Issue{{Id: "i3"}}}
		Expect(compiled.Eval(only.ProtoReflect())).To(BeFalse())
		Expect(compiled.Eval((&testdatav1.Issue{}).ProtoReflect())).To(BeFalse())
	})

	It("compares composite keys as whole tuples", func() {
		compiled := rewriteFor(query.Rel("pair", query.Eq("label", "keep")), targets)
		Expect(compiled.Eval((&testdatav1.Issue{Pair: &testdatav1.Pair{Ns: "a", Id: "p1"}}).ProtoReflect())).To(BeTrue())
		// Matching id under the wrong ns is a different row.
		Expect(compiled.Eval((&testdatav1.Issue{Pair: &testdatav1.Pair{Ns: "b", Id: "p1"}}).ProtoReflect())).To(BeFalse())
	})

	It("matches nothing when no target satisfies the condition", func() {
		compiled := rewriteFor(query.Rel("assignee", query.Eq("name", "Nobody")), targets)
		Expect(compiled.Eval((&testdatav1.Issue{Assignee: &testdatav1.User{Id: "u1"}}).ProtoReflect())).To(BeFalse())
	})

	It("composes under not/and/or", func() {
		cond := query.And(
			query.Not(query.Rel("assignee", query.Eq("name", "Noor"))),
			query.Eq("state", "open"),
		)
		compiled := rewriteFor(cond, targets)
		Expect(compiled.Eval((&testdatav1.Issue{State: "open", Assignee: &testdatav1.User{Id: "u1"}}).ProtoReflect())).To(BeTrue())
		Expect(compiled.Eval((&testdatav1.Issue{State: "open", Assignee: &testdatav1.User{Id: "u2"}}).ProtoReflect())).To(BeFalse())
		// not(rel) also matches a row with no ref at all.
		Expect(compiled.Eval((&testdatav1.Issue{State: "open"}).ProtoReflect())).To(BeTrue())
	})

	Describe("validation", func() {
		It("rejects a path that is not a reference field", func() {
			_, err := query.ExtractRels(issueDesc, query.Rel("title", query.Eq("name", "x")))
			Expect(err).To(MatchError(ContainSubstring("matches needs a reference field")))
		})

		It("rejects a path into the entity field of a reference message", func() {
			// assignee.user is the stub inside a workspace-qualified reference.
			// Resolving it as a plain reference would drop the workspace test.
			taskDesc := (&testdatav1.Task{}).ProtoReflect().Descriptor()
			_, err := query.ExtractRels(taskDesc, query.Rel("assignee.user", query.Eq("name", "x")))
			Expect(err).To(MatchError(ContainSubstring(`matches names the reference field itself`)))
			Expect(err).To(MatchError(ContainSubstring(`"assignee"`)))
		})

		It("rejects a missing inner condition", func() {
			cond := &syncplanev1.Condition{Kind: &syncplanev1.Condition_Rel{Rel: &syncplanev1.RelCond{
				Path: []string{"assignee"},
			}}}
			_, err := query.ExtractRels(issueDesc, cond)
			Expect(err).To(MatchError(ContainSubstring("needs a condition over the target entity")))
		})

		It("rejects an inner condition that does not compile against the target", func() {
			_, err := query.ExtractRels(issueDesc, query.Rel("assignee", query.Eq("title", "x")))
			Expect(err).To(MatchError(ContainSubstring(`has no field "title"`)))
		})

		It("rejects a rel nested inside a rel", func() {
			nested := query.Rel("assignee", query.Rel("assignee", query.Eq("name", "x")))
			_, err := query.ExtractRels(issueDesc, nested)
			Expect(err).To(MatchError(ContainSubstring("one hop")))
		})

		It("rejects a rel nested inside any()", func() {
			cond := query.Any("blocked_by", query.Rel("assignee", query.Eq("name", "x")))
			_, err := query.Compile(issueDesc, cond)
			Expect(err).To(MatchError(ContainSubstring("cannot nest inside any()")))
		})

		It("is rejected by Compile when left unresolved", func() {
			_, err := query.Compile(issueDesc, query.Rel("assignee", query.Eq("name", "x")))
			Expect(err).To(MatchError(ContainSubstring("matches is not supported here")))
		})
	})
})
