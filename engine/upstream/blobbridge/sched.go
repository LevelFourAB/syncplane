package blobbridge

import (
	"container/heap"
	"context"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/levelfourab/syncplane/engine/blob"
)

// jobKind names what a job does. Lower kinds run first among due jobs.
type jobKind uint8

const (
	// kindFetch fetches one blob from the origin into the local store.
	kindFetch jobKind = iota
	// kindCheck finds which blobs of a set of digests in one workspace must
	// go to the origin.
	kindCheck
	// kindUpload uploads one blob to the origin.
	kindUpload
	kinds
)

type jobKey struct {
	kind   jobKind
	ws     string
	digest blob.Digest // empty for kindCheck
}

type job struct {
	key jobKey
	due time.Time
	// index is the job's place in its heap, -1 while it is not queued.
	index   int
	running bool
	// again is set when a trigger arrives while the job runs: it then runs
	// once more without waiting.
	again    bool
	failures int

	// digests are the digests a check job has still to check.
	digests map[blob.Digest]struct{}
	// wantUntil is when a fetch job stops retrying.
	wantUntil time.Time
	// settled closes when the run of a fetch job in progress, or the next
	// one, ends. staged closes when that run has staged the blob in the
	// local store, or when it ends.
	settled, staged chan struct{}
}

// outcome is how a run ended.
type outcome uint8

const (
	// done: nothing is left to do, unless a trigger arrived meanwhile.
	done outcome = iota
	// retry: failed, or the origin does not have the blob yet; run again
	// after a backoff.
	retry
)

// scheduler holds the jobs of one Run: each job once, queued by due time,
// with backoff between failed runs. It is safe for concurrent use.
type scheduler struct {
	minBackoff, maxBackoff time.Duration

	mu    sync.Mutex
	jobs  map[jobKey]*job
	queue [kinds]jobHeap
	// changed closes when a job becomes due earlier than before, and is then
	// replaced, so every idle worker looks again.
	changed chan struct{}
	// finished closes when a run ends, and is then replaced.
	finished chan struct{}
	// load counts the digests waiting in check jobs and the queued uploads.
	load int
}

func newScheduler(minBackoff, maxBackoff time.Duration) *scheduler {
	return &scheduler{
		minBackoff: minBackoff,
		maxBackoff: maxBackoff,
		jobs:       map[jobKey]*job{},
		changed:    make(chan struct{}),
		finished:   make(chan struct{}),
	}
}

// check queues the digests for a check in the workspace.
func (s *scheduler) check(ws string, ds []blob.Digest) {
	if len(ds) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobLocked(jobKey{kind: kindCheck, ws: ws})
	s.addDigestsLocked(j, ds)
	s.triggerLocked(j)
}

func (s *scheduler) addDigestsLocked(j *job, ds []blob.Digest) {
	if j.digests == nil {
		j.digests = map[blob.Digest]struct{}{}
	}
	for _, d := range ds {
		if _, ok := j.digests[d]; !ok {
			j.digests[d] = struct{}{}
			s.load++
		}
	}
}

// upload queues an upload of the blob.
func (s *scheduler) upload(ws string, d blob.Digest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := jobKey{kind: kindUpload, ws: ws, digest: d}
	if _, ok := s.jobs[key]; !ok {
		s.load++
	}
	s.triggerLocked(s.jobLocked(key))
}

// want queues a fetch of the blob and keeps it wanted for patience. It
// returns the settled and staged channels of the fetch run in progress or
// due (see job), and whether such a run exists: a fetch that waits out a
// backoff has none.
func (s *scheduler) want(ws string, d blob.Digest, patience time.Duration) (settled, staged <-chan struct{}, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j := s.jobLocked(jobKey{kind: kindFetch, ws: ws, digest: d})
	j.wantUntil = time.Now().Add(patience)
	if j.settled == nil {
		j.settled, j.staged = make(chan struct{}), make(chan struct{})
	}
	s.triggerLocked(j)
	return j.settled, j.staged, j.running || !j.due.After(time.Now())
}

// markStaged reports that the run of fetch job j has staged its blob.
func (s *scheduler) markStaged(j *job) {
	s.mu.Lock()
	defer s.mu.Unlock()
	closeOnce(j.staged)
}

// closeOnce closes ch unless it is nil or closed.
func closeOnce(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// waitLoad waits until the load is below n, or ctx ends.
func (s *scheduler) waitLoad(ctx context.Context, n int) {
	for {
		s.mu.Lock()
		load, finished := s.load, s.finished
		s.mu.Unlock()
		if load < n {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-finished:
		}
	}
}

// jobLocked returns the job of key, made due now when it is new.
func (s *scheduler) jobLocked(key jobKey) *job {
	j, ok := s.jobs[key]
	if !ok {
		j = &job{key: key, due: time.Now(), index: -1}
		s.jobs[key] = j
	}
	return j
}

// triggerLocked makes j run soon: at once when it is idle, after the run in
// progress when it runs. A job that waits out a backoff keeps its time, so
// triggers do not hammer an origin that fails.
func (s *scheduler) triggerLocked(j *job) {
	switch {
	case j.running:
		j.again = true
	case j.index < 0:
		s.pushLocked(j)
	case j.failures == 0 && j.due.After(time.Now()):
		j.due = time.Now()
		heap.Fix(&s.queue[j.key.kind], j.index)
		s.signalLocked()
	}
}

func (s *scheduler) pushLocked(j *job) {
	heap.Push(&s.queue[j.key.kind], j)
	s.signalLocked()
}

func (s *scheduler) signalLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// next waits for a due job, marks it running and returns it. It returns nil
// when ctx ends.
func (s *scheduler) next(ctx context.Context) *job {
	for {
		s.mu.Lock()
		now := time.Now()
		var earliest time.Time
		for k := range s.queue {
			q := &s.queue[k]
			if q.Len() == 0 {
				continue
			}
			if top := (*q)[0]; !top.due.After(now) {
				heap.Pop(q)
				top.running = true
				s.mu.Unlock()
				return top
			} else if earliest.IsZero() || top.due.Before(earliest) {
				earliest = top.due
			}
		}
		changed := s.changed
		s.mu.Unlock()

		var (
			timer *time.Timer
			fire  <-chan time.Time
		)
		if !earliest.IsZero() {
			timer = time.NewTimer(time.Until(earliest))
			fire = timer.C
		}
		select {
		case <-ctx.Done():
		case <-changed:
		case <-fire:
		}
		if timer != nil {
			timer.Stop()
		}
		if ctx.Err() != nil {
			return nil
		}
	}
}

// finish records how a run of j ended and queues it again or forgets it.
func (s *scheduler) finish(j *job, o outcome) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j.running = false
	if j.settled != nil {
		close(j.settled)
		closeOnce(j.staged)
		j.settled, j.staged = make(chan struct{}), make(chan struct{})
	}
	close(s.finished)
	s.finished = make(chan struct{})
	again := j.again
	j.again = false
	now := time.Now()
	switch {
	case o == retry && j.key.kind == kindFetch && now.After(j.wantUntil):
		s.forgetLocked(j)
	case o == retry:
		j.failures++
		j.due = now.Add(s.backoff(j.failures))
		s.pushLocked(j)
	case again || len(j.digests) > 0:
		j.failures = 0
		j.due = now
		s.pushLocked(j)
	default:
		s.forgetLocked(j)
	}
}

func (s *scheduler) forgetLocked(j *job) {
	delete(s.jobs, j.key)
	if j.key.kind == kindUpload {
		s.load--
	}
	if j.settled != nil {
		close(j.settled)
		closeOnce(j.staged)
		j.settled, j.staged = nil, nil
	}
}

// takeDigests removes up to n digests from check job j and returns them.
func (s *scheduler) takeDigests(j *job, n int) []blob.Digest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]blob.Digest, 0, min(n, len(j.digests)))
	for d := range j.digests {
		if len(out) == n {
			break
		}
		out = append(out, d)
		delete(j.digests, d)
	}
	s.load -= len(out)
	return out
}

// returnDigests puts digests back into check job j, for a run that failed.
func (s *scheduler) returnDigests(j *job, ds []blob.Digest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.addDigestsLocked(j, ds)
}

// backoff is the wait after the nth failure in a row: minBackoff doubled
// for each failure after the first, at most maxBackoff, with the upper half
// jittered so that retries of many jobs spread out.
func (s *scheduler) backoff(n int) time.Duration {
	d := s.minBackoff
	for i := 1; i < n && d < s.maxBackoff; i++ {
		d *= 2
	}
	d = min(d, s.maxBackoff)
	half := d / 2
	return half + rand.N(half+1)
}

// jobHeap orders jobs by due time.
type jobHeap []*job

func (h jobHeap) Len() int           { return len(h) }
func (h jobHeap) Less(i, k int) bool { return h[i].due.Before(h[k].due) }
func (h jobHeap) Swap(i, k int) {
	h[i], h[k] = h[k], h[i]
	h[i].index = i
	h[k].index = k
}
func (h *jobHeap) Push(x any) {
	j := x.(*job)
	j.index = len(*h)
	*h = append(*h, j)
}
func (h *jobHeap) Pop() any {
	old := *h
	j := old[len(old)-1]
	old[len(old)-1] = nil
	j.index = -1
	*h = old[:len(old)-1]
	return j
}
