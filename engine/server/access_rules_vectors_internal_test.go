package server

import (
	"encoding/json"
	"os"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The rule vectors are shared with the TS client
// (client/test/unit/access-rules-vectors.test.ts): the client predicts with
// the decision the server makes here.
const accessRulesVectorsPath = "../../testdata/access/rules.json"

type rulesVectorRule struct {
	Read       bool     `json:"read"`
	Create     bool     `json:"create"`
	Update     bool     `json:"update"`
	Delete     bool     `json:"delete"`
	Write      bool     `json:"write"`
	Permission string   `json:"permission"`
	Admitted   bool     `json:"admitted"`
	Subject    string   `json:"subject"`
	Fields     []string `json:"fields"`
}

// rulesVectorRow is one side of an operation. A nil value is an input only
// the server knows.
type rulesVectorRow struct {
	Held   map[string]*bool   `json:"held"`
	Fields map[string]*string `json:"fields"`
}

func (r *rulesVectorRow) known() bool {
	if r == nil {
		return true
	}
	for _, v := range r.Held {
		if v == nil {
			return false
		}
	}
	for _, v := range r.Fields {
		if v == nil {
			return false
		}
	}
	return true
}

type rulesVectorCase struct {
	Name     string            `json:"name"`
	Op       string            `json:"op"`
	Rules    []rulesVectorRule `json:"rules"`
	Subjects []string          `json:"subjects"`
	Row      *rulesVectorRow   `json:"row"`
	Final    *rulesVectorRow   `json:"final"`
	// Changed is nil for an update of fields no fields list covers.
	Changed *[]string `json:"changed"`
	Want    string    `json:"want"`
}

var _ = Describe("access rule conformance vectors", func() {
	It("decides every case the server can decide the way the vectors say", func() {
		raw, err := os.ReadFile(accessRulesVectorsPath)
		Expect(err).NotTo(HaveOccurred())
		var file struct {
			Cases []rulesVectorCase `json:"cases"`
		}
		Expect(json.Unmarshal(raw, &file)).To(Succeed())
		Expect(file.Cases).NotTo(BeEmpty())

		decided := 0
		for _, tc := range file.Cases {
			if tc.Want == "unknown" || !tc.Row.known() || !tc.Final.known() {
				continue
			}
			decided++
			subjects := slices.Sorted(slices.Values(tc.Subjects))
			rules := make([]accessRule, len(tc.Rules))
			for i, r := range tc.Rules {
				rules[i] = rulesVectorRuleOf(r)
			}
			// admits mirrors txAccess.admits, with the chain walk answered by
			// the vector.
			admits := func(rule accessRule, row *rulesVectorRow) bool {
				if rule.subject != nil {
					v := row.Fields[string(rule.subject.Name())]
					if v == nil || !subjectMatches(subjects, *v) {
						return false
					}
				}
				if rule.admitted() {
					return true
				}
				held := row.Held[rule.permission]
				return held != nil && *held
			}

			var got bool
			switch op := rulesVectorOp(tc.Op); op {
			case opUpdate:
				var updating []accessRule
				for _, rule := range rules {
					if rule.names(opUpdate) && admits(rule, tc.Row) {
						updating = append(updating, rule)
					}
				}
				// No list names "*", so it stands for a field no list covers.
				changed := []string{"*"}
				if tc.Changed != nil {
					changed = slices.Sorted(slices.Values(*tc.Changed))
				}
				got, err = admitsUpdate(updating, func() []string { return changed },
					func(rule accessRule) (bool, error) { return admits(rule, tc.Final), nil })
				Expect(err).NotTo(HaveOccurred())
			default:
				got = slices.ContainsFunc(rules, func(rule accessRule) bool {
					return rule.names(op) && admits(rule, tc.Row)
				})
			}
			want := map[string]bool{"yes": true, "no": false}[tc.Want]
			Expect(got).To(Equal(want), tc.Name)
		}
		Expect(decided).To(BeNumerically(">", 0))
	})
})

func rulesVectorRuleOf(r rulesVectorRule) accessRule {
	var rule accessRule
	for _, flag := range []struct {
		set bool
		op  accessOp
	}{{r.Read, opRead}, {r.Create, opCreate}, {r.Update, opUpdate}, {r.Delete, opDelete}, {r.Write, opWrite}} {
		if flag.set {
			rule.ops |= flag.op
		}
	}
	if !r.Admitted {
		rule.permission = r.Permission
	}
	if r.Subject != "" {
		rule.subject = vectorKeyField(r.Subject)
	}
	if r.Fields != nil {
		rule.fields = slices.Sorted(slices.Values(r.Fields))
	}
	return rule
}

func rulesVectorOp(op string) accessOp {
	switch op {
	case "read":
		return opRead
	case "create":
		return opCreate
	case "update":
		return opUpdate
	case "delete":
		return opDelete
	}
	Fail("unknown op " + op)
	return 0
}
