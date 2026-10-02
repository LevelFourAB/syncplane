package server

import (
	"encoding/json"
	"os"
	"slices"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/query"
)

// The rule vectors are shared with the TS client
// (client/test/unit/access-rules-vectors.test.ts): the client predicts with
// the decision the server makes here.
const accessRulesVectorsPath = "../../testdata/access/rules.json"

type rulesVectorRule struct {
	Read     bool     `json:"read"`
	Create   bool     `json:"create"`
	Update   bool     `json:"update"`
	Delete   bool     `json:"delete"`
	Write    bool     `json:"write"`
	Requires []string `json:"requires"`
	Admitted bool     `json:"admitted"`
	CallerIs string   `json:"callerIs"`
	Fields   []string `json:"fields"`
	// When names the rule's condition, which each row's Matches answers.
	When string `json:"when"`
	// Within names the rule's within entries, which each row's Within
	// answers.
	Within []string `json:"within"`
}

// rulesVectorRow is one side of an operation. A nil value is an input only
// the server knows.
type rulesVectorRow struct {
	Held    map[string]*bool   `json:"held"`
	Fields  map[string]*string `json:"fields"`
	Matches map[string]*bool   `json:"matches"`
	Within  map[string]*bool   `json:"within"`
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
	for _, v := range r.Matches {
		if v == nil {
			return false
		}
	}
	for _, v := range r.Within {
		if v == nil {
			return false
		}
	}
	return true
}

// rulesVectorGuard is one field guard of the entity.
type rulesVectorGuard struct {
	Units []string          `json:"units"`
	Rules []rulesVectorRule `json:"rules"`
}

type rulesVectorCase struct {
	Name   string             `json:"name"`
	Op     string             `json:"op"`
	Rules  []rulesVectorRule  `json:"rules"`
	Guards []rulesVectorGuard `json:"guards"`
	// ReadFields are the units a read reads, nil for a read of the row.
	ReadFields []string        `json:"readFields"`
	Subjects   []string        `json:"subjects"`
	Row        *rulesVectorRow `json:"row"`
	Final      *rulesVectorRow `json:"final"`
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
			// admits mirrors txAccess.admits, with the chain walks and the
			// condition answered by the vector.
			admits := func(rule accessRule, row *rulesVectorRow) bool {
				if rule.callerIs != nil {
					v := row.Fields[string(rule.callerIs.Name())]
					if v == nil || !subjectMatches(subjects, *v) {
						return false
					}
				}
				if rule.when != nil {
					v := row.Matches[rule.whenSpec]
					if v == nil || !*v {
						return false
					}
				}
				for _, w := range rule.within {
					if v := row.Within[w.entity]; v == nil || !*v {
						return false
					}
				}
				for _, p := range rule.requires {
					if held := row.Held[p]; held == nil || !*held {
						return false
					}
				}
				return true
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
			// A guard that restricts a unit the operation reads or changes needs
			// a rule of its own, as guardAdmits and the guard read filter decide.
			units := tc.ReadFields
			if tc.Changed != nil {
				units = *tc.Changed
			}
			for _, g := range tc.Guards {
				if !got || !slices.ContainsFunc(g.Units, func(u string) bool { return slices.Contains(units, u) }) {
					continue
				}
				op := rulesVectorOp(tc.Op)
				got = slices.ContainsFunc(g.Rules, func(r rulesVectorRule) bool {
					rule := rulesVectorRuleOf(r)
					return rule.names(op) && admits(rule, tc.Row) && (op != opUpdate || admits(rule, tc.Final))
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
		rule.requires = slices.Sorted(slices.Values(r.Requires))
	}
	if r.CallerIs != "" {
		rule.callerIs = vectorKeyField(r.CallerIs)
	}
	if r.Fields != nil {
		rule.fields = slices.Sorted(slices.Values(r.Fields))
	}
	for _, name := range r.Within {
		// The entity stands for the entry's name, which each row answers.
		rule.within = append(rule.within, withinEntry{entity: name})
	}
	if r.When != "" {
		// The vector names the condition, so the spec carries the name and
		// the compiled condition only marks that the rule sets one.
		rule.when, rule.whenSpec = &query.Compiled{}, r.When
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
