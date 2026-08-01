package admin

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/storage/sqlitestore"
)

var _ = Describe("admin HTTP", func() {
	const (
		appID = "demo"
		ws    = "w1"
	)
	var (
		ctx    context.Context
		mgr    *server.Manager
		ts     *httptest.Server
		client *http.Client
	)

	project := server.DynamicEntity{
		Name: "project", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "name", Number: 2, Kind: protoreflect.StringKind},
		},
	}
	issue := server.DynamicEntity{
		Name: "issue", KeyFields: []string{"id"},
		Fields: []server.DynamicField{
			{Name: "id", Number: 1, Kind: protoreflect.StringKind},
			{Name: "title", Number: 2, Kind: protoreflect.StringKind},
			{Name: "settings", Number: 3, Message: []server.DynamicField{
				{Name: "color", Number: 1, Kind: protoreflect.StringKind},
			}},
			{Name: "project", Number: 4, Ref: "project"},
			{Name: "done", Number: 5, Kind: protoreflect.BoolKind},
			{Name: "checklist", Number: 6, Repeated: true, KeyField: []string{"id"}, Message: []server.DynamicField{
				{Name: "id", Number: 1, Kind: protoreflect.StringKind},
				{Name: "text", Number: 2, Kind: protoreflect.StringKind},
				{Name: "done", Number: 3, Kind: protoreflect.BoolKind},
			}},
		},
	}

	BeforeEach(func() {
		ctx = context.Background()
		store, err := sqlitestore.Open(filepath.Join(GinkgoT().TempDir(), "admin.db"))
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(store.Close()).To(Succeed()) })
		mgr, err = server.NewManager(ctx, store)
		Expect(err).NotTo(HaveOccurred())
		DeferCleanup(func() { Expect(mgr.Close()).To(Succeed()) })
		_, err = mgr.Register(ctx, appID, descriptorSetBytes(project, issue))
		Expect(err).NotTo(HaveOccurred())

		ts = httptest.NewServer(Handler(mgr))
		DeferCleanup(ts.Close)
		client = ts.Client()
		client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	})

	get := func(path string) (int, string) {
		GinkgoHelper()
		resp, err := client.Get(ts.URL + path)
		Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body)
	}
	submit := func(method, path string, vals url.Values) *http.Response {
		GinkgoHelper()
		req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(vals.Encode()))
		Expect(err).NotTo(HaveOccurred())
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("HX-Request", "true")
		resp, err := client.Do(req)
		Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()
		return resp
	}
	// readIssue loads a stored issue row through the read path.
	readIssue := func(key string) (protoreflect.Message, bool) {
		GinkgoHelper()
		meta, ok := mgr.App(appID).Entity("issue")
		Expect(ok).To(BeTrue())
		msg := dynamicpb.NewMessage(meta.Descriptor)
		var found bool
		Expect(mgr.App(appID).View(ctx, ws, func(tx *server.ViewTx) error {
			f, _, err := tx.Get(msg, key)
			found = f
			return err
		})).To(Succeed())
		return msg, found
	}
	fieldStr := func(m protoreflect.Message, name string) string {
		fd := m.Descriptor().Fields().ByName(protoreflect.Name(name))
		return m.Get(fd).String()
	}

	entityPath := "/apps/" + appID + "/ws/" + ws + "/e/issue"

	It("lists the app and its entities on the index", func() {
		status, body := get("/")
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(ContainSubstring(appID))
		Expect(body).To(ContainSubstring("issue"))
		Expect(body).To(ContainSubstring("project"))
	})

	It("renders a row table fragment listing stored rows", func() {
		Expect(mgr.App(appID).Mutate(ctx, ws, func(tx *server.EntityTx) error {
			meta, _ := mgr.App(appID).Entity("issue")
			m := dynamicpb.NewMessage(meta.Descriptor)
			m.Set(meta.Descriptor.Fields().ByName("id"), protoreflect.ValueOfString("seed"))
			m.Set(meta.Descriptor.Fields().ByName("title"), protoreflect.ValueOfString("Seeded"))
			return tx.Put(m)
		})).To(Succeed())

		status, body := get(entityPath)
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(ContainSubstring("seed"))
		Expect(body).To(ContainSubstring("Seeded"))
	})

	It("prefills a generated id on the new-item form", func() {
		status, body := get(entityPath + "/new")
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(MatchRegexp(`[0-9A-HJKMNP-TV-Z]{26}`))
	})

	It("mints an id when the create form leaves the key blank", func() {
		resp := submit(http.MethodPost, entityPath+"/rows", url.Values{
			"title": {"No id supplied"},
		})
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		// Exactly one issue exists, keyed by a generated 26-char Crockford id.
		meta, _ := mgr.App(appID).Entity("issue")
		var keys []string
		Expect(mgr.App(appID).View(ctx, ws, func(tx *server.ViewTx) error {
			_, _, err := tx.ScanPage(dynamicpb.NewMessage(meta.Descriptor), "", 100,
				func(m proto.Message, _ uint64) error {
					keys = append(keys, fieldStr(m.ProtoReflect(), "id"))
					return nil
				})
			return err
		})).To(Succeed())
		Expect(keys).To(HaveLen(1))
		Expect(keys[0]).To(MatchRegexp(`^[0-9A-HJKMNP-TV-Z]{26}$`))

		msg, found := readIssue(keys[0])
		Expect(found).To(BeTrue())
		Expect(fieldStr(msg, "title")).To(Equal("No id supplied"))
	})

	It("round-trips create -> list -> edit (PATCH) -> delete", func() {
		// Create.
		resp := submit(http.MethodPost, entityPath+"/rows", url.Values{
			"id":             {"i1"},
			"title":          {"First"},
			"settings.color": {"red"},
			"project.id":     {"p1"},
		})
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		Expect(resp.Header.Get("HX-Redirect")).To(Equal(entityPath))

		msg, found := readIssue("i1")
		Expect(found).To(BeTrue())
		Expect(fieldStr(msg, "title")).To(Equal("First"))
		settings := msg.Get(msg.Descriptor().Fields().ByName("settings")).Message()
		Expect(settings.Get(settings.Descriptor().Fields().ByName("color")).String()).To(Equal("red"))

		// List shows it.
		_, body := get(entityPath)
		Expect(body).To(ContainSubstring("i1"))
		Expect(body).To(ContainSubstring("First"))

		// Edit form carries the version.
		status, editBody := get(entityPath + "/rows/i1")
		Expect(status).To(Equal(http.StatusOK))
		Expect(editBody).To(ContainSubstring(`name="_version"`))

		// Edit: change only the title; project + settings must survive (PATCH).
		var version uint64
		Expect(mgr.App(appID).View(ctx, ws, func(tx *server.ViewTx) error {
			m := dynamicpb.NewMessage(msg.Descriptor())
			_, v, err := tx.Get(m, "i1")
			version = v
			return err
		})).To(Succeed())

		resp = submit(http.MethodPut, entityPath+"/rows/i1", url.Values{
			"_version":       {strconv.FormatUint(version, 10)},
			"id":             {"i1"},
			"title":          {"Updated"},
			"settings.color": {"red"},
			"project.id":     {"p1"},
		})
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		msg, _ = readIssue("i1")
		Expect(fieldStr(msg, "title")).To(Equal("Updated"))
		proj := msg.Get(msg.Descriptor().Fields().ByName("project")).Message()
		Expect(proj.Get(proj.Descriptor().Fields().ByName("id")).String()).To(Equal("p1"), "the untouched reference survives the PATCH")

		// A stale edit conflicts.
		resp = submit(http.MethodPut, entityPath+"/rows/i1", url.Values{
			"_version": {strconv.FormatUint(version, 10)}, // now stale
			"id":       {"i1"},
			"title":    {"Conflicting"},
		})
		Expect(resp.StatusCode).To(Equal(http.StatusConflict))

		// Delete.
		resp = submit(http.MethodDelete, entityPath+"/rows/i1", url.Values{})
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		_, found = readIssue("i1")
		Expect(found).To(BeFalse())
	})

	// seedProject writes a project row with a display name.
	seedProject := func(id, name string) {
		GinkgoHelper()
		Expect(mgr.App(appID).Mutate(ctx, ws, func(tx *server.EntityTx) error {
			meta, _ := mgr.App(appID).Entity("project")
			m := dynamicpb.NewMessage(meta.Descriptor)
			m.Set(meta.Descriptor.Fields().ByName("id"), protoreflect.ValueOfString(id))
			m.Set(meta.Descriptor.Fields().ByName("name"), protoreflect.ValueOfString(name))
			return tx.Put(m)
		})).To(Succeed())
	}

	It("serves a reference picker of labelled target rows, filtered by ?q", func() {
		seedProject("p1", "Platform")
		seedProject("p2", "Mobile")
		pickerPath := "/apps/" + appID + "/ws/" + ws + "/e/project/picker"

		status, body := get(pickerPath)
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(ContainSubstring("Platform"))
		Expect(body).To(ContainSubstring("Mobile"))
		Expect(body).To(ContainSubstring("data-ref-result"))
		// The selectable row carries the target key fields as JSON.
		Expect(body).To(ContainSubstring(`&#34;id&#34;:&#34;p1&#34;`))

		// ?__q narrows by label substring (case-insensitive).
		_, filtered := get(pickerPath + "?__q=mob")
		Expect(filtered).To(ContainSubstring("Mobile"))
		Expect(filtered).NotTo(ContainSubstring("Platform"))
	})

	// seedIssue writes an issue row with a title and a done flag.
	seedIssue := func(id, title string, done bool) {
		GinkgoHelper()
		Expect(mgr.App(appID).Mutate(ctx, ws, func(tx *server.EntityTx) error {
			meta, _ := mgr.App(appID).Entity("issue")
			m := dynamicpb.NewMessage(meta.Descriptor)
			m.Set(meta.Descriptor.Fields().ByName("id"), protoreflect.ValueOfString(id))
			m.Set(meta.Descriptor.Fields().ByName("title"), protoreflect.ValueOfString(title))
			m.Set(meta.Descriptor.Fields().ByName("done"), protoreflect.ValueOfBool(done))
			return tx.Put(m)
		})).To(Succeed())
	}

	It("narrows the browse table to the rows a filter matches", func() {
		seedIssue("i1", "Alpha", false)
		seedIssue("i2", "Beta", true)
		seedIssue("i3", "Alpine", true)

		q := url.Values{"f[0].path": {"title"}, "f[0].op": {"starts_with"}, "f[0].v": {"Alp"}}
		status, body := get(entityPath + "?" + q.Encode())
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(ContainSubstring("Alpha"))
		Expect(body).To(ContainSubstring("Alpine"))
		Expect(body).NotTo(ContainSubstring("Beta"))
		// The header counts the matches, not the entity.
		Expect(body).To(ContainSubstring("2 matching"))
		// The bar comes back holding the filter that produced the page.
		Expect(body).To(ContainSubstring(`value="starts_with" selected`))

		// A second condition under "match all" narrows further.
		q.Set("f[1].path", "done")
		q.Set("f[1].op", "eq")
		q.Set("f[1].v", "true")
		_, both := get(entityPath + "?" + q.Encode())
		Expect(both).To(ContainSubstring("Alpine"))
		Expect(both).NotTo(ContainSubstring("Alpha"))

		// The same two conditions under "match any" widen it again.
		q.Set("match", "any")
		_, either := get(entityPath + "?" + q.Encode())
		Expect(either).To(ContainSubstring("Alpha"))
		Expect(either).To(ContainSubstring("Beta"))
	})

	It("filters by a chosen reference and labels the chip with the target's name", func() {
		seedProject("p1", "Platform")
		seedProject("p2", "Mobile")
		for _, tc := range []struct{ id, title, project string }{
			{"i1", "Alpha", "p1"},
			{"i2", "Beta", "p2"},
		} {
			resp := submit(http.MethodPost, entityPath+"/rows", url.Values{
				"id": {tc.id}, "title": {tc.title}, "project.id": {tc.project},
			})
			Expect(resp.StatusCode).To(Equal(http.StatusOK))
		}

		q := url.Values{"f[0].path": {"project"}, "f[0].op": {"eq"}, "f[0].v": {"p1"}}
		status, body := get(entityPath + "?" + q.Encode())
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(ContainSubstring("Alpha"))
		Expect(body).NotTo(ContainSubstring("Beta"))
		// The operand comes back as a picker chip holding the target key, and
		// the chip reads as the project's display name rather than "p1".
		Expect(body).To(ContainSubstring(`type="hidden" name="f[0].v" value="p1"`))
		Expect(body).To(MatchRegexp(`data-ref-label[^>]*>Platform<`))
		Expect(body).To(ContainSubstring(`/e/project/picker`))
	})

	It("filters by a condition group scoped to one list element", func() {
		// i1 has one item that is both about deploys and done; i2 has the two
		// halves split across separate items, which the group must not match.
		resp := submit(http.MethodPost, entityPath+"/rows", url.Values{
			"id": {"i1"}, "title": {"Alpha"},
			"checklist[0].id": {"c1"}, "checklist[0].text": {"deploy it"}, "checklist[0].done": {"true"},
		})
		Expect(resp.StatusCode).To(Equal(http.StatusOK))
		resp = submit(http.MethodPost, entityPath+"/rows", url.Values{
			"id": {"i2"}, "title": {"Beta"},
			"checklist[0].id": {"c1"}, "checklist[0].text": {"deploy it"}, "checklist[0].done": {"false"},
			"checklist[1].id": {"c2"}, "checklist[1].text": {"write it up"}, "checklist[1].done": {"true"},
		})
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		q := url.Values{
			"f[0].path":      {"checklist"},
			"f[0].op":        {"any"},
			"f[0].s[0].path": {"text"},
			"f[0].s[0].op":   {"contains"},
			"f[0].s[0].v":    {"deploy"},
			"f[0].s[1].path": {"done"},
			"f[0].s[1].op":   {"eq"},
			"f[0].s[1].v":    {"true"},
		}
		status, body := get(entityPath + "?" + q.Encode())
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(ContainSubstring("Alpha"))
		Expect(body).NotTo(ContainSubstring("Beta"), "the two halves match different items")
		Expect(body).To(ContainSubstring("1 matching"))
	})

	It("re-renders a condition group with a sub-row against the element", func() {
		q := url.Values{"f[0].path": {"checklist"}, "f[0].op": {"any"}}
		status, body := get(entityPath + "/filter?" + q.Encode())
		Expect(status).To(Equal(http.StatusOK))
		// The group is its own repeat scope, seeded with one blank sub-row and
		// carrying the template its "+ Condition" clones.
		Expect(body).To(ContainSubstring(`data-repeat="f[0].s"`))
		Expect(body).To(ContainSubstring(`name="f[0].s[0].path"`))
		Expect(body).To(ContainSubstring(`data-repeat-tpl="f[0].s"`))
		Expect(body).To(ContainSubstring(`name="f[0].s[__i__].path"`))

		// The sub-row's field select names the ELEMENT's fields; the group
		// head's own select, just above it, still names the row's.
		subSelect := body[strings.Index(body, `name="f[0].s[0].path"`):]
		subSelect = subSelect[:strings.Index(subSelect, "</select>")]
		Expect(subSelect).To(ContainSubstring(`value="text"`))
		Expect(subSelect).NotTo(ContainSubstring(`value="title"`))
	})

	It("reports a condition it cannot read instead of showing unfiltered rows", func() {
		seedIssue("i1", "Alpha", false)
		q := url.Values{"f[0].path": {"nope"}, "f[0].op": {"eq"}, "f[0].v": {"x"}}
		status, body := get(entityPath + "?" + q.Encode())
		Expect(status).To(Equal(http.StatusOK))
		Expect(body).To(ContainSubstring("Fix the highlighted condition"))
		Expect(body).To(ContainSubstring("cannot be filtered"))
		Expect(body).NotTo(ContainSubstring("Alpha"))
	})

	It("re-renders one condition row against the field it names", func() {
		q := url.Values{"f[2].path": {"done"}}
		status, body := get(entityPath + "/filter?" + q.Encode())
		Expect(status).To(Equal(http.StatusOK))
		// The row keeps its index, so the reload lands back where it came from.
		Expect(body).To(ContainSubstring(`name="f[2].path"`))
		Expect(body).To(ContainSubstring(`name="f[2].op"`))
		// A bool takes a yes/no operand, and only "is" applies to it.
		Expect(body).To(ContainSubstring(`name="f[2].v"`))
		Expect(body).To(ContainSubstring(">yes</option>"))
		Expect(body).NotTo(ContainSubstring(`value="contains"`))

		// With no field chosen the row is just the field select.
		_, blank := get(entityPath + "/filter")
		Expect(blank).To(ContainSubstring(`name="f[__i__].path"`))
		Expect(blank).NotTo(ContainSubstring(".op"))
	})

	It("renders a selected reference as a labelled chip and a bool as a No/Yes toggle", func() {
		seedProject("p1", "Platform")
		resp := submit(http.MethodPost, entityPath+"/rows", url.Values{
			"id": {"i1"}, "title": {"First"}, "project.id": {"p1"}, "done": {"true"},
		})
		Expect(resp.StatusCode).To(Equal(http.StatusOK))

		status, body := get(entityPath + "/rows/i1")
		Expect(status).To(Equal(http.StatusOK))
		// Reference chip shows the resolved target label plus a hidden key input,
		// not a bare editable id field.
		Expect(body).To(ContainSubstring(`data-ref-name="project"`))
		Expect(body).To(ContainSubstring("Platform"))
		Expect(body).To(ContainSubstring(`type="hidden" name="project.id" value="p1"`))
		// Boolean renders as an explicit No/Yes choice with the stored value set.
		Expect(body).To(ContainSubstring(">No</label>"))
		Expect(body).To(ContainSubstring(">Yes</label>"))
		Expect(body).To(MatchRegexp(`name="done" value="true" checked`))
	})
})
