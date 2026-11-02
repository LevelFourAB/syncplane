package rebase_test

import (
	"fmt"
	"math/rand/v2"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"

	testdatav1 "github.com/levelfourab/syncplane/engine/gen/syncplane/testdata/v1"
	syncplanev1 "github.com/levelfourab/syncplane/engine/gen/syncplane/v1"
	"github.com/levelfourab/syncplane/engine/text"
	"github.com/levelfourab/syncplane/engine/upstream/internal/rebase"
)

// gen makes one random write to one row, built the way a writer that sees cur
// builds it: text documents extend the documents cur holds, under peer. With
// fragments set, a PATCH may carry a document it names outright as delta state
// against cur, which only the engine that holds cur can apply.
type gen struct {
	prototype proto.Message
	entity    string
	op        func(rng *rand.Rand, key string, cur proto.Message, peer string, fragments bool) *syncplanev1.CrudMutation
}

// The conformance that matters: for a replica (local) and an origin that share
// a starting state, then take different writes — the local ones forwarded to
// the origin in the form UpstreamValue builds — replaying the local writes on
// the origin's confirmed row gives exactly the row the origin ends up with.
// Writes the origin refuses must leave the replay where the origin left the row.
var _ = Describe("Replay against an origin", func() {
	gens := []gen{
		{prototype: &testdatav1.Widget{}, entity: "widget", op: widgetOp},
		{prototype: &testdatav1.KeyedTextEntity{}, entity: "keyed_text", op: keyedOp},
	}
	keys := []string{"k1", "k2"}

	for _, g := range gens {
		It(fmt.Sprintf("converges with the origin for %s", g.entity), func() {
			model := mustModel(g.prototype)
			var refusedUpstream, appliedUpstream int
			for iter := 0; iter < 60; iter++ {
				seed := fmt.Sprintf("seed %d iteration %d", GinkgoRandomSeed(), iter)
				rng := rand.New(rand.NewPCG(uint64(GinkgoRandomSeed()), uint64(iter)))
				origin, local := newNode(), newNode()
				decoded := func(n *node, key string) proto.Message {
					r := n.row(g.prototype, key)
					if !r.Present {
						return nil
					}
					msg := g.prototype.ProtoReflect().New().Interface()
					Expect(proto.Unmarshal(r.Value, msg)).To(Succeed())
					return msg
				}

				// Most rows exist before the replicas part ways, so that most
				// PATCHes have a row to land on.
				for _, key := range keys {
					if rng.IntN(4) == 0 {
						continue
					}
					c := g.op(rng, key, nil, "setup", false)
					for c.GetOp() != syncplanev1.CrudOp_CRUD_OP_PUT {
						c = g.op(rng, key, nil, "setup", false)
					}
					Expect(origin.push("setup", c)).To(Equal(local.push("setup", c)), seed)
				}
				for i := 0; i < rng.IntN(4); i++ {
					key := keys[rng.IntN(len(keys))]
					c := g.op(rng, key, decoded(origin, key), "setup", false)
					Expect(origin.push("setup", c)).To(Equal(local.push("setup", c)), seed)
				}
				shared := map[string]rebase.Row{}
				for _, key := range keys {
					shared[key] = origin.row(g.prototype, key)
				}

				for i := 0; i < rng.IntN(5); i++ {
					key := keys[rng.IntN(len(keys))]
					origin.push("remote", g.op(rng, key, decoded(origin, key), "remote", true))
				}
				if rng.IntN(2) == 0 {
					// A concurrent delete is what turns forwarded PATCHes into
					// refusals.
					key := keys[rng.IntN(len(keys))]
					origin.push("remote", crud(syncplanev1.CrudOp_CRUD_OP_DELETE, g.entity, key, nil))
				}

				type pendingOp struct {
					key  string
					crud *syncplanev1.CrudMutation
					op   rebase.Op
				}
				var pending []pendingOp
				for i := 0; i < 1+rng.IntN(6); i++ {
					key := keys[rng.IntN(len(keys))]
					c := g.op(rng, key, decoded(local, key), "local", true)
					if !local.push("local", c) {
						continue // never queued: the replica refused it
					}
					op := opOf(c)
					after := local.row(g.prototype, key)
					var err error
					op.Text, err = model.CaptureText(op, after.Value)
					Expect(err).NotTo(HaveOccurred(), seed)
					pending = append(pending, pendingOp{key: key, crud: c, op: op})
				}

				// The replica's own rows are its confirmed state plus its
				// pending writes.
				for _, key := range keys {
					var ops []rebase.Op
					for _, p := range pending {
						if p.key == key {
							ops = append(ops, p.op)
						}
					}
					got, err := model.Replay(shared[key], ops)
					Expect(err).NotTo(HaveOccurred(), seed)
					want := local.row(g.prototype, key)
					Expect(model.Equal(got, want)).To(BeTrue(), "%s: replica row %s\nreplayed:\n%s\nreplica:\n%s",
						seed, key, describe(g.prototype, got), describe(g.prototype, want))
				}

				confirmed := map[string]rebase.Row{}
				for _, key := range keys {
					confirmed[key] = origin.row(g.prototype, key)
				}
				for _, p := range pending {
					value, err := model.UpstreamValue(p.op)
					Expect(err).NotTo(HaveOccurred(), seed)
					up := proto.Clone(p.crud).(*syncplanev1.CrudMutation)
					up.Value = value
					if origin.push("link", up) {
						appliedUpstream++
					} else {
						refusedUpstream++
					}
				}

				for _, key := range keys {
					var ops []rebase.Op
					for _, p := range pending {
						if p.key == key {
							ops = append(ops, p.op)
						}
					}
					got, err := model.Replay(confirmed[key], ops)
					Expect(err).NotTo(HaveOccurred(), seed)
					want := origin.row(g.prototype, key)
					Expect(model.Equal(got, want)).To(BeTrue(), "%s: origin row %s\nreplayed:\n%s\norigin:\n%s",
						seed, key, describe(g.prototype, got), describe(g.prototype, want))
				}
			}
			// Both outcomes have to occur, or the property says nothing about
			// one of them.
			Expect(appliedUpstream).To(BeNumerically(">", 0), "refused %d", refusedUpstream)
			Expect(refusedUpstream).To(BeNumerically(">", 0), "applied %d", appliedUpstream)
		})
	}
})

func pick[T any](rng *rand.Rand, xs ...T) T { return xs[rng.IntN(len(xs))] }

func subset[T any](rng *rand.Rand, xs ...T) []T {
	var out []T
	for _, x := range xs {
		if rng.IntN(2) == 0 {
			out = append(out, x)
		}
	}
	return out
}

// editDoc extends base (nil for none) with a random edit by peer: an insert,
// or a delete when there is text to delete.
func editDoc(rng *rand.Rand, base []byte, peer string) []byte {
	d, err := text.Decode(base, peer)
	Expect(err).NotTo(HaveOccurred())
	if d.Len() > 2 && rng.IntN(3) == 0 {
		Expect(d.Delete(rng.IntN(d.Len()-1), 1)).To(Succeed())
	} else {
		Expect(d.Insert(rng.IntN(d.Len()+1), pick(rng, "a", "bc", "def", "x y"))).To(Succeed())
	}
	return d.Encode()
}

// asFragment turns full, an extension of base, into the fragment that carries
// what full holds beyond base.
func asFragment(full, base []byte) []byte {
	next, err := text.Decode(full, "")
	Expect(err).NotTo(HaveOccurred())
	var b *text.Doc
	if len(base) > 0 {
		b, err = text.Decode(base, "")
		Expect(err).NotTo(HaveOccurred())
	}
	frag, err := text.EncodeFragment(next, b)
	Expect(err).NotTo(HaveOccurred())
	return frag
}

func widgetOp(rng *rand.Rand, key string, cur proto.Message, peer string, fragments bool) *syncplanev1.CrudMutation {
	var curBody []byte
	if cur != nil {
		curBody = cur.(*testdatav1.Widget).GetBody()
	}
	w := &testdatav1.Widget{Id: key, Label: pick(rng, "", "red", "blue"), Count: int32(rng.IntN(4))}
	if rng.IntN(2) == 0 {
		w.Settings = &testdatav1.Settings{Color: pick(rng, "", "teal"), Size: int32(rng.IntN(3))}
	}
	for _, k := range subset(rng, "a", "b", "c") {
		if w.Attrs == nil {
			w.Attrs = map[string]string{}
		}
		w.Attrs[k] = pick(rng, "1", "2")
	}
	for _, k := range subset(rng, int64(1), int64(2)) {
		if w.ById == nil {
			w.ById = map[int64]string{}
		}
		w.ById[k] = pick(rng, "x", "y")
	}
	for _, uid := range subset(rng, "u1", "u2", "u3") {
		w.Members = append(w.Members, &testdatav1.Member{Uid: uid, Name: pick(rng, "ann", "bo"), Role: int32(rng.IntN(3))})
	}
	// Labels on reference stubs are stripped by every write path.
	for _, id := range subset(rng, "p1", "p2") {
		w.Refs = append(w.Refs, &testdatav1.Pair{Ns: "n", Id: id, Label: pick(rng, "", "stub label")})
	}
	w.Tags = subset(rng, "x", "y", "z")
	switch rng.IntN(4) {
	case 1:
		w.Choice = &testdatav1.Widget_ChoiceText{ChoiceText: pick(rng, "t1", "t2")}
	case 2:
		w.Choice = &testdatav1.Widget_ChoiceNumber{ChoiceNumber: int32(rng.IntN(5))}
	case 3:
		w.Choice = &testdatav1.Widget_ChoiceSettings{ChoiceSettings: &testdatav1.Settings{Color: "c"}}
	}
	if rng.IntN(2) == 0 {
		w.Maybe = proto.String(pick(rng, "", "m"))
	}
	if rng.IntN(3) == 0 {
		w.Doc = &testdatav1.Blob{Data: []byte{byte(rng.IntN(4))}, Note: "n"}
	}
	if rng.IntN(4) != 0 {
		w.Body = editDoc(rng, curBody, peer)
	}

	switch r := rng.IntN(10); {
	case r == 0:
		return crud(syncplanev1.CrudOp_CRUD_OP_DELETE, "widget", key, nil)
	case r < 4:
		return crud(syncplanev1.CrudOp_CRUD_OP_PUT, "widget", key, w)
	}
	mask := subset(rng, "label", "count", "settings.color", "settings", "attrs.a", "attrs.c", "attrs",
		"by_id.2", "members.u1", "members.u2.name", "members", "refs.n/p1", "refs", "tags.x", "tags",
		"choice", "body", "maybe", "doc")
	if len(mask) == 0 {
		mask = []string{"body"}
	}
	if fragments && len(w.Body) > 0 && rng.IntN(2) == 0 {
		for _, p := range mask {
			if p == "body" {
				w.Body = asFragment(w.Body, curBody)
			}
		}
	}
	return crud(syncplanev1.CrudOp_CRUD_OP_PATCH, "widget", key, w, mask...)
}

func keyedOp(rng *rand.Rand, key string, cur proto.Message, peer string, fragments bool) *syncplanev1.CrudMutation {
	var c *testdatav1.KeyedTextEntity
	if cur != nil {
		c = cur.(*testdatav1.KeyedTextEntity)
	}
	noteDocs := func(id string) (body, deep []byte) {
		for _, n := range c.GetNotes() {
			if n.GetId() == id {
				return n.GetBody(), n.GetDeep().GetBody()
			}
		}
		return nil, nil
	}
	e := &testdatav1.KeyedTextEntity{Id: key, Title: pick(rng, "", "t1", "t2")}
	for _, id := range subset(rng, "n1", "n2", "n3") {
		body, deep := noteDocs(id)
		n := &testdatav1.KeyedNote{Id: id, Label: pick(rng, "", "l")}
		if rng.IntN(4) != 0 {
			n.Body = editDoc(rng, body, peer)
		}
		if rng.IntN(2) == 0 {
			n.Deep = &testdatav1.TextHolder{Body: editDoc(rng, deep, peer)}
		}
		e.Notes = append(e.Notes, n)
	}
	for _, k := range subset(rng, "s1", "s2") {
		if e.Sections == nil {
			e.Sections = map[string]*testdatav1.TextHolder{}
		}
		e.Sections[k] = &testdatav1.TextHolder{Body: editDoc(rng, c.GetSections()[k].GetBody(), peer)}
	}

	switch r := rng.IntN(10); {
	case r == 0:
		return crud(syncplanev1.CrudOp_CRUD_OP_DELETE, "keyed_text", key, nil)
	case r < 4:
		return crud(syncplanev1.CrudOp_CRUD_OP_PUT, "keyed_text", key, e)
	}
	mask := subset(rng, "title", "notes.n1", "notes.n2.body", "notes.n1.label", "notes.n3.deep.body",
		"notes.n1.deep", "notes", "sections.s1", "sections.s2.body", "sections")
	if len(mask) == 0 {
		mask = []string{"notes.n2.body"}
	}
	if fragments && rng.IntN(2) == 0 {
		// Delta state only where the mask names the document outright and
		// nothing above it: an ancestor path replaces the subtree whole, and
		// the engine refuses a fragment there.
		has := func(p string) bool {
			for _, m := range mask {
				if m == p {
					return true
				}
			}
			return false
		}
		if has("notes.n2.body") && !has("notes") {
			for _, n := range e.Notes {
				if n.Id == "n2" && len(n.Body) > 0 {
					body, _ := noteDocs("n2")
					n.Body = asFragment(n.Body, body)
				}
			}
		}
		if has("sections.s2.body") && !has("sections") {
			if s := e.Sections["s2"]; s != nil {
				s.Body = asFragment(s.Body, c.GetSections()["s2"].GetBody())
			}
		}
	}
	return crud(syncplanev1.CrudOp_CRUD_OP_PATCH, "keyed_text", key, e, mask...)
}
