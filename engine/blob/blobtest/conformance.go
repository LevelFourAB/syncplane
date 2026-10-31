// Package blobtest provides a shared Ginkgo behavior suite every blob.Store
// implementation must pass. An implementation registers it from its own test
// suite:
//
//	var _ = blobtest.Conformance("fs", func(now func() time.Time) blob.Store {
//		s, err := fsstore.Open(GinkgoT().TempDir(), fsstore.WithClock(now))
//		Expect(err).NotTo(HaveOccurred())
//		return s
//	})
//
// A Store that several processes or handles can share also registers Shared,
// which runs the race specs across two handles on the same backing store.
//
// The suite stores blobs of up to three chunks, so one run writes several
// hundred MiB through the Store.
package blobtest

import (
	"bytes"
	"context"
	"io"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/levelfourab/syncplane/engine/blob"
)

// Conformance registers the blob.Store behavior specs. open is called once
// per spec and must return a fresh, empty Store that reads the time only from
// now; the suite moves now forward to check CommittedAt and StartedAt. The
// suite closes the Store after the spec.
func Conformance(name string, open func(now func() time.Time) blob.Store) bool {
	return Describe("blob.Store conformance: "+name, func() {
		var (
			s   blob.Store
			clk *clock
			h   = helpers{ctx: context.Background()}
			ctx = context.Background()
		)

		BeforeEach(func() {
			clk = newClock()
			s = open(clk.Now)
			DeferCleanup(func() { Expect(s.Close()).To(Succeed()) })
		})

		It("starts empty", func() {
			Expect(h.workspaces(s)).To(BeEmpty())
			Expect(h.list(s, "ws")).To(BeEmpty())
			Expect(h.listStaged(s, "ws")).To(BeEmpty())
			h.expectGone(s, "ws", fix("small").digest())
			h.expectNotStaged(s, "ws", fix("small").digest())
		})

		Describe("Begin and Staged", func() {
			It("stages an upload with no chunks at the current time", func() {
				small := fix("small")
				h.begin(s, "ws", small)

				u, have := h.staged(s, "ws", small.digest())
				expectManifest(u.Manifest, small.m)
				Expect(u.StartedAt).To(BeTemporally("==", clk.Now()))
				Expect(have).To(BeEmpty())
				h.expectGone(s, "ws", small.digest())
			})

			It("keeps the first upload and its chunks when the digest is staged again", func() {
				three := fix("three")
				h.begin(s, "ws", three)
				started := clk.Now()
				h.put(s, "ws", three, 1)

				clk.Advance(time.Hour)
				h.begin(s, "ws", three)

				u, have := h.staged(s, "ws", three.digest())
				Expect(u.StartedAt).To(BeTemporally("==", started))
				expectManifest(u.Manifest, three.m)
				Expect(have).To(Equal([]int{1}))
			})

			It("does nothing for a committed blob", func() {
				small := fix("small")
				h.commit(s, "ws", small)
				committed := h.stat(s, "ws", small.digest()).CommittedAt

				clk.Advance(time.Hour)
				h.begin(s, "ws", small)

				h.expectNotStaged(s, "ws", small.digest())
				Expect(h.stat(s, "ws", small.digest()).CommittedAt).To(BeTemporally("==", committed))
				Expect(h.read(s, "ws", small.digest())).To(Equal(small.data))
			})

			It("lists the stored chunks in ascending order", func() {
				three := fix("three")
				h.begin(s, "ws", three)
				h.put(s, "ws", three, 2)
				h.put(s, "ws", three, 0)

				_, have := h.staged(s, "ws", three.digest())
				Expect(have).To(Equal([]int{0, 2}))

				h.put(s, "ws", three, 1)
				_, have = h.staged(s, "ws", three.digest())
				Expect(have).To(Equal([]int{0, 1, 2}))
			})

			It("returns ErrNotFound for a digest that is not staged", func() {
				h.begin(s, "ws", fix("small"))
				_, _, err := s.Staged(ctx, "ws", fix("other").digest())
				Expect(err).To(MatchError(blob.ErrNotFound))
			})
		})

		Describe("PutChunk", func() {
			It("returns ErrNotFound when the digest is not staged", func() {
				small := fix("small")
				err := s.PutChunk(ctx, "ws", small.digest(), 0, bytes.NewReader(small.chunk(0)))
				Expect(err).To(MatchError(blob.ErrNotFound))
				h.expectNotStaged(s, "ws", small.digest())
			})

			It("returns ErrNotFound for a committed blob that is not staged", func() {
				small := fix("small")
				h.commit(s, "ws", small)
				err := s.PutChunk(ctx, "ws", small.digest(), 0, bytes.NewReader(small.chunk(0)))
				Expect(err).To(MatchError(blob.ErrNotFound))
			})

			It("stores nothing and wraps ErrInvalid when the bytes do not match the hash", func() {
				two := fix("two")
				h.begin(s, "ws", two)
				bad := bytes.Clone(two.chunk(1))
				bad[len(bad)-1] ^= 0xff

				err := s.PutChunk(ctx, "ws", two.digest(), 1, bytes.NewReader(bad))
				Expect(err).To(MatchError(blob.ErrInvalid))
				err = s.PutChunk(ctx, "ws", two.digest(), 1, bytes.NewReader(two.chunk(0)[:len(bad)]))
				Expect(err).To(MatchError(blob.ErrInvalid), "another chunk's bytes")

				_, have := h.staged(s, "ws", two.digest())
				Expect(have).To(BeEmpty())
			})

			It("stores nothing and wraps ErrInvalid when the reader ends before the chunk does", func() {
				two := fix("two")
				h.begin(s, "ws", two)

				err := s.PutChunk(ctx, "ws", two.digest(), 0, bytes.NewReader(two.chunk(0)[:blob.ChunkSize-1]))
				Expect(err).To(MatchError(blob.ErrInvalid))
				err = s.PutChunk(ctx, "ws", two.digest(), 1, bytes.NewReader(nil))
				Expect(err).To(MatchError(blob.ErrInvalid), "empty reader")

				_, have := h.staged(s, "ws", two.digest())
				Expect(have).To(BeEmpty())
			})

			It("returns the reader's error and stores nothing", func() {
				small := fix("small")
				h.begin(s, "ws", small)
				boom := errBoom
				r := io.MultiReader(bytes.NewReader(small.chunk(0)[:10]), failingReader{boom})

				Expect(s.PutChunk(ctx, "ws", small.digest(), 0, r)).To(MatchError(boom))
				_, have := h.staged(s, "ws", small.digest())
				Expect(have).To(BeEmpty())
			})

			It("wraps ErrInvalid for an index outside the blob", func() {
				two := fix("two")
				h.begin(s, "ws", two)
				for _, i := range []int{-1, 2, 1 << 20} {
					err := s.PutChunk(ctx, "ws", two.digest(), i, bytes.NewReader(two.chunk(1)))
					Expect(err).To(MatchError(blob.ErrInvalid), "index %d", i)
				}
				_, have := h.staged(s, "ws", two.digest())
				Expect(have).To(BeEmpty())
			})

			It("reads exactly the chunk's length from the reader", func() {
				small := fix("small")
				h.begin(s, "ws", small)
				r := bytes.NewReader(append(bytes.Clone(small.chunk(0)), "rest"...))

				Expect(s.PutChunk(ctx, "ws", small.digest(), 0, r)).To(Succeed())
				rest, err := io.ReadAll(r)
				Expect(err).NotTo(HaveOccurred())
				Expect(string(rest)).To(Equal("rest"))
			})

			It("replaces a stored chunk with the same bytes", func() {
				two := fix("two")
				h.begin(s, "ws", two)
				h.put(s, "ws", two, 1)
				h.put(s, "ws", two, 1)

				_, have := h.staged(s, "ws", two.digest())
				Expect(have).To(Equal([]int{1}))

				h.put(s, "ws", two, 0)
				Expect(s.Complete(ctx, "ws", two.digest())).To(Succeed())
				Expect(h.read(s, "ws", two.digest())).To(Equal(two.data))
			})

			It("keeps a stored chunk when a later put of it fails", func() {
				small := fix("small")
				h.begin(s, "ws", small)
				h.put(s, "ws", small, 0)

				bad := bytes.Clone(small.chunk(0))
				bad[0] ^= 1
				Expect(s.PutChunk(ctx, "ws", small.digest(), 0, bytes.NewReader(bad))).To(MatchError(blob.ErrInvalid))

				_, have := h.staged(s, "ws", small.digest())
				Expect(have).To(Equal([]int{0}))
				Expect(s.Complete(ctx, "ws", small.digest())).To(Succeed())
				Expect(h.read(s, "ws", small.digest())).To(Equal(small.data))
			})
		})

		Describe("Complete", func() {
			It("returns ErrNotFound when the digest is not staged", func() {
				Expect(s.Complete(ctx, "ws", fix("small").digest())).To(MatchError(blob.ErrNotFound))
				h.expectGone(s, "ws", fix("small").digest())
			})

			It("returns ErrIncomplete and keeps the upload when a chunk is missing", func() {
				three := fix("three")
				h.begin(s, "ws", three)
				h.put(s, "ws", three, 0)
				h.put(s, "ws", three, 2)

				Expect(s.Complete(ctx, "ws", three.digest())).To(MatchError(blob.ErrIncomplete))
				h.expectGone(s, "ws", three.digest())
				_, have := h.staged(s, "ws", three.digest())
				Expect(have).To(Equal([]int{0, 2}))

				h.put(s, "ws", three, 1)
				Expect(s.Complete(ctx, "ws", three.digest())).To(Succeed())
				Expect(h.read(s, "ws", three.digest())).To(Equal(three.data))
			})

			It("returns ErrIncomplete for an upload with no chunks", func() {
				small := fix("small")
				h.begin(s, "ws", small)
				Expect(s.Complete(ctx, "ws", small.digest())).To(MatchError(blob.ErrIncomplete))
			})

			It("commits the blob at the current time and removes the staged state", func() {
				two := fix("two")
				h.begin(s, "ws", two)
				h.putAll(s, "ws", two)
				clk.Advance(time.Minute)

				Expect(s.Complete(ctx, "ws", two.digest())).To(Succeed())

				info := h.stat(s, "ws", two.digest())
				expectManifest(info.Manifest, two.m)
				Expect(info.CommittedAt).To(BeTemporally("==", clk.Now()))
				Expect(info.UnreferencedSince).To(BeZero())
				Expect(h.read(s, "ws", two.digest())).To(Equal(two.data))
				h.expectNotStaged(s, "ws", two.digest())
				Expect(s.PutChunk(ctx, "ws", two.digest(), 0, bytes.NewReader(two.chunk(0)))).
					To(MatchError(blob.ErrNotFound))
			})

			It("returns nil and changes nothing for a committed blob", func() {
				small := fix("small")
				h.commit(s, "ws", small)
				committed := h.stat(s, "ws", small.digest()).CommittedAt
				mark := committed.Add(time.Second)
				Expect(s.SetUnreferencedSince(ctx, "ws", small.digest(), mark)).To(Succeed())

				clk.Advance(time.Hour)
				Expect(s.Complete(ctx, "ws", small.digest())).To(Succeed())

				info := h.stat(s, "ws", small.digest())
				Expect(info.CommittedAt).To(BeTemporally("==", committed))
				Expect(info.UnreferencedSince).To(BeTemporally("==", mark))
				h.expectNotStaged(s, "ws", small.digest())
			})

			It("commits an empty blob with no chunks", func() {
				empty := fix("empty")
				h.begin(s, "ws", empty)
				_, have := h.staged(s, "ws", empty.digest())
				Expect(have).To(BeEmpty())

				Expect(s.Complete(ctx, "ws", empty.digest())).To(Succeed())

				info := h.stat(s, "ws", empty.digest())
				Expect(info.Manifest.Size).To(BeZero())
				Expect(info.Manifest.ChunkHashes).To(BeEmpty())
				Expect(h.read(s, "ws", empty.digest())).To(BeEmpty())
				Expect(h.list(s, "ws")).To(ConsistOf(empty.digest()))
			})
		})

		Describe("Open", func() {
			It("returns ErrNotFound for a staged blob", func() {
				small := fix("small")
				h.begin(s, "ws", small)
				h.putAll(s, "ws", small)
				_, err := s.Open(ctx, "ws", small.digest())
				Expect(err).To(MatchError(blob.ErrNotFound))
			})

			It("reads any range with Seek", func() {
				three := fix("three")
				h.commit(s, "ws", three)
				r, err := s.Open(ctx, "ws", three.digest())
				Expect(err).NotTo(HaveOccurred())
				defer func() { Expect(r.Close()).To(Succeed()) }()

				readAt := func(whence int, offset int64, n int) []byte {
					GinkgoHelper()
					_, err := r.Seek(offset, whence)
					Expect(err).NotTo(HaveOccurred())
					p := make([]byte, n)
					_, err = io.ReadFull(r, p)
					Expect(err).NotTo(HaveOccurred())
					return p
				}

				for i := range three.m.ChunkHashes {
					p := readAt(io.SeekStart, int64(i)*blob.ChunkSize, blob.ChunkLen(three.m.Size, i))
					Expect(three.m.CheckChunk(i, p)).To(Succeed(), "chunk %d", i)
				}
				Expect(readAt(io.SeekStart, blob.ChunkSize-5, 10)).
					To(Equal(three.data[blob.ChunkSize-5 : blob.ChunkSize+5]))
				Expect(readAt(io.SeekEnd, -7, 7)).To(Equal(three.data[len(three.data)-7:]))
				Expect(readAt(io.SeekStart, 0, 3)).To(Equal(three.data[:3]))
				Expect(readAt(io.SeekCurrent, 100, 4)).To(Equal(three.data[103:107]))

				end, err := r.Seek(0, io.SeekEnd)
				Expect(err).NotTo(HaveOccurred())
				Expect(end).To(Equal(three.m.Size))
				n, err := r.Read(make([]byte, 1))
				Expect(n).To(BeZero())
				Expect(err).To(Equal(io.EOF))
			})

			It("gives each reader its own position", func() {
				small := fix("small")
				h.commit(s, "ws", small)
				a, err := s.Open(ctx, "ws", small.digest())
				Expect(err).NotTo(HaveOccurred())
				defer a.Close()
				b, err := s.Open(ctx, "ws", small.digest())
				Expect(err).NotTo(HaveOccurred())
				defer b.Close()

				_, err = a.Seek(500, io.SeekStart)
				Expect(err).NotTo(HaveOccurred())
				all, err := io.ReadAll(b)
				Expect(err).NotTo(HaveOccurred())
				Expect(all).To(Equal(small.data))
				rest, err := io.ReadAll(a)
				Expect(err).NotTo(HaveOccurred())
				Expect(rest).To(Equal(small.data[500:]))
			})
		})

		Describe("SetUnreferencedSince", func() {
			It("records a time and removes it again with the zero time", func() {
				small := fix("small")
				h.commit(s, "ws", small)
				mark := time.Date(2026, 3, 4, 5, 6, 7, 891011121, time.FixedZone("x", 3600))

				Expect(s.SetUnreferencedSince(ctx, "ws", small.digest(), mark)).To(Succeed())
				Expect(h.stat(s, "ws", small.digest()).UnreferencedSince).To(BeTemporally("==", mark))

				later := mark.Add(time.Hour)
				Expect(s.SetUnreferencedSince(ctx, "ws", small.digest(), later)).To(Succeed())
				Expect(h.stat(s, "ws", small.digest()).UnreferencedSince).To(BeTemporally("==", later))

				Expect(s.SetUnreferencedSince(ctx, "ws", small.digest(), time.Time{})).To(Succeed())
				info := h.stat(s, "ws", small.digest())
				Expect(info.UnreferencedSince.IsZero()).To(BeTrue())
				expectManifest(info.Manifest, small.m)
				Expect(h.read(s, "ws", small.digest())).To(Equal(small.data))
			})

			It("returns ErrNotFound for a blob that is not committed", func() {
				small := fix("small")
				Expect(s.SetUnreferencedSince(ctx, "ws", small.digest(), clk.Now())).To(MatchError(blob.ErrNotFound))
				h.begin(s, "ws", small)
				h.putAll(s, "ws", small)
				Expect(s.SetUnreferencedSince(ctx, "ws", small.digest(), clk.Now())).To(MatchError(blob.ErrNotFound))
				h.expectGone(s, "ws", small.digest())
			})
		})

		Describe("Delete", func() {
			It("removes a committed blob", func() {
				small, other := fix("small"), fix("other")
				h.commit(s, "ws", small)
				h.commit(s, "ws", other)

				Expect(s.Delete(ctx, "ws", small.digest())).To(Succeed())

				h.expectGone(s, "ws", small.digest())
				Expect(s.SetUnreferencedSince(ctx, "ws", small.digest(), clk.Now())).To(MatchError(blob.ErrNotFound))
				Expect(h.list(s, "ws")).To(ConsistOf(other.digest()))
				Expect(h.read(s, "ws", other.digest())).To(Equal(other.data))
			})

			It("is not an error for a blob that is not committed", func() {
				Expect(s.Delete(ctx, "ws", fix("small").digest())).To(Succeed())
			})

			It("does not change a staged upload of the digest", func() {
				two := fix("two")
				h.begin(s, "ws", two)
				h.put(s, "ws", two, 1)

				Expect(s.Delete(ctx, "ws", two.digest())).To(Succeed())

				_, have := h.staged(s, "ws", two.digest())
				Expect(have).To(Equal([]int{1}))
			})

			It("lets the digest be uploaded and committed again", func() {
				small := fix("small")
				h.commit(s, "ws", small)
				Expect(s.Delete(ctx, "ws", small.digest())).To(Succeed())
				clk.Advance(time.Hour)

				h.commit(s, "ws", small)
				info := h.stat(s, "ws", small.digest())
				Expect(info.CommittedAt).To(BeTemporally("==", clk.Now()))
				Expect(info.UnreferencedSince).To(BeZero())
				Expect(h.read(s, "ws", small.digest())).To(Equal(small.data))
			})
		})

		Describe("Abort", func() {
			It("removes a staged upload and its chunks", func() {
				two := fix("two")
				h.begin(s, "ws", two)
				h.put(s, "ws", two, 0)

				Expect(s.Abort(ctx, "ws", two.digest())).To(Succeed())

				h.expectNotStaged(s, "ws", two.digest())
				Expect(s.PutChunk(ctx, "ws", two.digest(), 1, bytes.NewReader(two.chunk(1)))).
					To(MatchError(blob.ErrNotFound))
				Expect(s.Complete(ctx, "ws", two.digest())).To(MatchError(blob.ErrNotFound))

				clk.Advance(time.Hour)
				h.begin(s, "ws", two)
				u, have := h.staged(s, "ws", two.digest())
				Expect(have).To(BeEmpty(), "a new upload starts without the aborted chunks")
				Expect(u.StartedAt).To(BeTemporally("==", clk.Now()))
			})

			It("is not an error for an upload that is not staged", func() {
				Expect(s.Abort(ctx, "ws", fix("small").digest())).To(Succeed())
			})

			It("does not change a committed blob", func() {
				small := fix("small")
				h.commit(s, "ws", small)
				Expect(s.Abort(ctx, "ws", small.digest())).To(Succeed())
				Expect(h.read(s, "ws", small.digest())).To(Equal(small.data))
			})
		})

		Describe("listing", func() {
			It("lists committed blobs and staged uploads apart", func() {
				small, other, two := fix("small"), fix("other"), fix("two")
				h.commit(s, "ws", small)
				h.commit(s, "ws", other)
				h.begin(s, "ws", two)
				h.commit(s, "elsewhere", two)

				Expect(h.list(s, "ws")).To(ConsistOf(small.digest(), other.digest()))
				Expect(h.list(s, "elsewhere")).To(ConsistOf(two.digest()))
				Expect(h.list(s, "none")).To(BeEmpty())

				uploads := collect(s.ListStaged(ctx, "ws"))
				Expect(uploads).To(HaveLen(1))
				expectManifest(uploads[0].Manifest, two.m)
				Expect(uploads[0].StartedAt).To(BeTemporally("==", clk.Now()))
				Expect(h.listStaged(s, "elsewhere")).To(BeEmpty())
			})

			It("lists every workspace with a committed blob or a staged upload", func() {
				h.commit(s, "committed", fix("small"))
				h.begin(s, "staged", fix("small"))
				h.commit(s, "both", fix("small"))
				h.begin(s, "both", fix("other"))
				h.begin(s, "aborted", fix("small"))
				Expect(s.Abort(ctx, "aborted", fix("small").digest())).To(Succeed())
				h.commit(s, "deleted", fix("small"))
				Expect(s.Delete(ctx, "deleted", fix("small").digest())).To(Succeed())
				h.commit(s, "emptied", fix("small"))
				Expect(s.DeleteWorkspace(ctx, "emptied")).To(Succeed())

				Expect(h.workspaces(s)).To(ConsistOf("committed", "staged", "both"))
			})

			It("stops when the caller stops iterating", func() {
				for _, f := range []string{"small", "other"} {
					h.commit(s, "ws", fix(f))
					h.begin(s, "ws", fix("two"))
					h.begin(s, "ws", fix("three"))
					h.commit(s, "ws2", fix(f))
				}
				count := 0
				for range s.List(ctx, "ws") {
					count++
					break
				}
				for range s.ListStaged(ctx, "ws") {
					count++
					break
				}
				for range s.ListWorkspaces(ctx) {
					count++
					break
				}
				Expect(count).To(Equal(3))
			})
		})

		Describe("DeleteWorkspace", func() {
			It("removes every committed blob and staged upload of the workspace only", func() {
				small, other, two := fix("small"), fix("other"), fix("two")
				h.commit(s, "ws", small)
				h.begin(s, "ws", two)
				h.put(s, "ws", two, 1)
				h.commit(s, "keep", small)
				h.begin(s, "keep", other)

				Expect(s.DeleteWorkspace(ctx, "ws")).To(Succeed())

				h.expectGone(s, "ws", small.digest())
				h.expectNotStaged(s, "ws", two.digest())
				Expect(h.list(s, "ws")).To(BeEmpty())
				Expect(h.listStaged(s, "ws")).To(BeEmpty())
				Expect(h.workspaces(s)).To(ConsistOf("keep"))
				Expect(h.read(s, "keep", small.digest())).To(Equal(small.data))
				Expect(h.listStaged(s, "keep")).To(ConsistOf(other.digest()))
			})

			It("is not an error for a workspace that holds nothing", func() {
				Expect(s.DeleteWorkspace(ctx, "none")).To(Succeed())
				Expect(h.workspaces(s)).To(BeEmpty())
			})

			It("leaves the workspace usable", func() {
				small := fix("small")
				h.commit(s, "ws", small)
				Expect(s.DeleteWorkspace(ctx, "ws")).To(Succeed())

				h.commit(s, "ws", small)
				Expect(h.read(s, "ws", small.digest())).To(Equal(small.data))
				Expect(h.workspaces(s)).To(ConsistOf("ws"))
			})
		})

		Describe("workspaces", func() {
			It("keeps the same digest in two workspaces as two blobs", func() {
				small := fix("small")
				h.commit(s, "a", small)

				h.expectGone(s, "b", small.digest())
				h.begin(s, "b", small)
				_, have := h.staged(s, "b", small.digest())
				Expect(have).To(BeEmpty(), "Begin in b must stage although a has the blob")
				Expect(s.Complete(ctx, "b", small.digest())).To(MatchError(blob.ErrIncomplete))
				h.put(s, "b", small, 0)
				Expect(s.Complete(ctx, "b", small.digest())).To(Succeed())

				mark := clk.Now().Add(time.Minute)
				Expect(s.SetUnreferencedSince(ctx, "a", small.digest(), mark)).To(Succeed())
				Expect(h.stat(s, "b", small.digest()).UnreferencedSince).To(BeZero())

				Expect(s.Delete(ctx, "a", small.digest())).To(Succeed())
				h.expectGone(s, "a", small.digest())
				Expect(h.read(s, "b", small.digest())).To(Equal(small.data))
			})

			It("keeps a staged upload in its own workspace", func() {
				small := fix("small")
				h.begin(s, "a", small)
				h.put(s, "a", small, 0)

				h.expectNotStaged(s, "b", small.digest())
				Expect(s.PutChunk(ctx, "b", small.digest(), 0, bytes.NewReader(small.chunk(0)))).
					To(MatchError(blob.ErrNotFound))
				Expect(s.Complete(ctx, "b", small.digest())).To(MatchError(blob.ErrNotFound))
				Expect(s.Abort(ctx, "b", small.digest())).To(Succeed())

				_, have := h.staged(s, "a", small.digest())
				Expect(have).To(Equal([]int{0}))
			})

			It("keeps workspace ids with awkward characters apart", func() {
				ids := awkwardIDs()
				small, other := fix("small"), fix("other")
				for _, ws := range ids {
					h.commit(s, ws, small)
					h.begin(s, ws, other)
				}
				Expect(sorted(h.workspaces(s))).To(Equal(sorted(ids)))
				for _, ws := range ids {
					Expect(h.list(s, ws)).To(ConsistOf(small.digest()), "workspace %q", ws)
					Expect(h.listStaged(s, ws)).To(ConsistOf(other.digest()), "workspace %q", ws)
				}

				// Removing one workspace must leave each id that a careless
				// encoding would map onto it.
				for _, ws := range []string{"Case", "..", "a/b", "ü", ""} {
					Expect(s.DeleteWorkspace(ctx, ws)).To(Succeed())
				}
				for _, ws := range ids {
					switch ws {
					case "Case", "..", "a/b", "ü", "":
						h.expectGone(s, ws, small.digest())
						h.expectNotStaged(s, ws, other.digest())
					default:
						Expect(h.read(s, ws, small.digest())).To(Equal(small.data), "workspace %q", ws)
						Expect(h.listStaged(s, ws)).To(ConsistOf(other.digest()), "workspace %q", ws)
					}
				}
				Expect(h.workspaces(s)).To(HaveLen(len(ids) - 5))
			})
		})

		Describe("malformed digests", func() {
			It("finds nothing and changes nothing", func() {
				small := fix("small")
				h.commit(s, "ws", small)
				h.begin(s, "ws", fix("two"))
				bad := []blob.Digest{
					"", "sp1:", "sp1:..", "../../x", "sp1:../" + small.digest()[7:],
					upperDigest(small.digest()), upperDigest(fix("two").digest()), small.digest() + "/x",
				}
				for _, d := range bad {
					_, err := s.Stat(ctx, "ws", d)
					Expect(err).To(MatchError(blob.ErrNotFound), "Stat %q", d)
					_, err = s.Open(ctx, "ws", d)
					Expect(err).To(MatchError(blob.ErrNotFound), "Open %q", d)
					_, _, err = s.Staged(ctx, "ws", d)
					Expect(err).To(MatchError(blob.ErrNotFound), "Staged %q", d)
					Expect(s.PutChunk(ctx, "ws", d, 0, bytes.NewReader(small.data))).
						To(MatchError(blob.ErrNotFound), "PutChunk %q", d)
					Expect(s.Complete(ctx, "ws", d)).To(MatchError(blob.ErrNotFound), "Complete %q", d)
					Expect(s.SetUnreferencedSince(ctx, "ws", d, clk.Now())).
						To(MatchError(blob.ErrNotFound), "SetUnreferencedSince %q", d)
					Expect(s.Delete(ctx, "ws", d)).To(Succeed(), "Delete %q", d)
					Expect(s.Abort(ctx, "ws", d)).To(Succeed(), "Abort %q", d)
				}
				Expect(h.read(s, "ws", small.digest())).To(Equal(small.data))
				Expect(h.listStaged(s, "ws")).To(ConsistOf(fix("two").digest()))
				Expect(h.list(s, "ws")).To(ConsistOf(small.digest()))
			})
		})

		raceSpecs(func() (blob.Store, blob.Store) { return s, s })
	})
}

// Shared registers the race specs across two handles on one backing store,
// such as two processes that open the same directory. open is called once
// per spec and returns two handles on a fresh, empty store and a func that
// closes both.
func Shared(name string, open func(now func() time.Time) (a, b blob.Store, closeBoth func())) bool {
	return Describe("blob.Store shared handles: "+name, func() {
		var a, b blob.Store
		h := helpers{ctx: context.Background()}

		BeforeEach(func() {
			var closeBoth func()
			a, b, closeBoth = open(newClock().Now)
			DeferCleanup(closeBoth)
		})

		It("sees every change through the other handle", func() {
			small, two := fix("small"), fix("two")
			h.begin(a, "ws", two)
			h.put(b, "ws", two, 1)
			_, have := h.staged(a, "ws", two.digest())
			Expect(have).To(Equal([]int{1}))
			h.put(a, "ws", two, 0)
			Expect(b.Complete(context.Background(), "ws", two.digest())).To(Succeed())
			Expect(h.read(a, "ws", two.digest())).To(Equal(two.data))

			h.commit(a, "ws", small)
			Expect(h.list(b, "ws")).To(ConsistOf(small.digest(), two.digest()))
			Expect(b.Delete(context.Background(), "ws", small.digest())).To(Succeed())
			h.expectGone(a, "ws", small.digest())
			Expect(b.DeleteWorkspace(context.Background(), "ws")).To(Succeed())
			Expect(h.workspaces(a)).To(BeEmpty())
		})

		raceSpecs(func() (blob.Store, blob.Store) { return a, b })
	})
}

// awkwardIDs are workspace ids that a Store which maps ids onto names, such
// as file paths or object keys, must keep apart and must not escape with.
func awkwardIDs() []string {
	return []string{
		"", " ", ".", "..", "...", "../escape", "a/b", "a\\b", "/abs", "C:", "a:b",
		"case", "Case", "CASE",
		"ü", "ü", // NFC and NFD of the same letter
		"日本", "emoji \U0001f600", "nul\u0000byte", "new\nline", "tab\tid",
		"trailing.", "trailing ", "con", "aux.txt", "%2e%2e", "~user", "-dash", "*?<>|\"",
		strings.Repeat("x", 300), strings.Repeat("é", 200),
	}
}
