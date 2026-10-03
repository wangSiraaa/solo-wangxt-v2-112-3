package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"incbackup/internal/repo"
)

// scrubBatchChunks bounds one streaming batch: fewer chunks per transaction
// keeps the resume window small and progress updates frequent.
const scrubBatchChunks = 64

// ErrScrubAlreadyRunning is returned when a start is requested while a
// scrub worker is active in this process.
var ErrScrubAlreadyRunning = errors.New("a scrub is already running")

// ErrSnapshotSuspect wraps an early, locatable refusal to restore a snapshot
// whose chunks the latest scrub proved corrupt or missing on disk.
var ErrSnapshotSuspect = errors.New("snapshot marked suspect by integrity scrub")

// SuspectDetail names one bad chunk and every snapshot/path it affects.
type SuspectDetail struct {
	ChunkDigest    string             `json:"chunk_digest"`
	Result         string             `json:"result"` // bad | missing
	DeclaredLength int64              `json:"declared_length"`
	ObservedLength int64              `json:"observed_length"`
	BlobPath       string             `json:"blob_path"`
	Detail         string             `json:"detail"`
	Affected       []repo.AffectedRef `json:"affected"`
}

// SuspectError is returned by Restore before touching disk when the latest
// scrub found the snapshot corrupt. It carries the exact chunks and the
// full reverse-lookup of affected snapshots/file paths.
type SuspectError struct {
	SnapshotID int64
	ScrubID    int64
	Details    []SuspectDetail
}

func (e *SuspectError) Error() string {
	return fmt.Sprintf("snapshot %d marked suspect by scrub %d: %d corrupt/missing chunk(s)",
		e.SnapshotID, e.ScrubID, len(e.Details))
}

// Unwrap lets errors.Is(err, ErrSnapshotSuspect) match.
func (e *SuspectError) Unwrap() error { return ErrSnapshotSuspect }

// ScrubProgress is the live view of one scrub run.
type ScrubProgress struct {
	Scrub repo.ScrubInfo
	// SnapshotCount is the number of in-scope snapshot rows (verdict rows).
	SnapshotCount int
	Snapshots     []repo.SnapshotVerdict
}

// ScrubHooks are test knobs for the scrub worker.
type ScrubHooks struct {
	// AfterChunk, when non-nil, runs after each chunk has been streamed but
	// before the batch commits. Tests use it to interrupt at a stable
	// mid-scrub point and to slow the run down.
	AfterChunk func(scrubID int64, digest []byte, res repo.ScrubChunkResult)
}

// activeScrub tracks the in-process worker. A crashed process leaves a
// "running" row in SQLite with no activeScrub entry, which resume picks up.
type activeScrub struct {
	id     int64
	cancel chan struct{}
	done   chan struct{}
}

// StartScrub starts a repository-wide integrity scrub in the background.
// When force is false and an unfinished scrub exists (paused, or running
// left by a killed process), it resumes that job from its stable cursor;
// otherwise it freezes a new scrub at the current snapshot watermark.
// It is an error (ErrScrubAlreadyRunning) if a worker is already active.
func (e *Engine) StartScrub(force bool, hooks ScrubHooks) (repo.ScrubInfo, error) {
	e.scrubMu.Lock()
	defer e.scrubMu.Unlock()
	if e.scrub != nil {
		return repo.ScrubInfo{}, fmt.Errorf("%w: scrub %d", ErrScrubAlreadyRunning, e.scrub.id)
	}

	sc, err := e.beginScrubLocked(force)
	if err != nil {
		return repo.ScrubInfo{}, err
	}
	act := &activeScrub{id: sc.ID, cancel: make(chan struct{}), done: make(chan struct{})}
	e.scrub = act
	go e.runScrub(sc.ID, hooks, act)
	return sc, nil
}

// beginScrubLocked chooses resume vs. fresh and marks the row running.
func (e *Engine) beginScrubLocked(force bool) (repo.ScrubInfo, error) {
	if !force {
		if prev, ok, err := e.Manifest.LatestUnfinishedScrub(); err != nil {
			return repo.ScrubInfo{}, err
		} else if ok {
			if err := e.Manifest.MarkScrubRunning(prev.ID); err != nil {
				return repo.ScrubInfo{}, err
			}
			return e.Manifest.GetScrub(prev.ID)
		}
	}
	return e.Manifest.CreateScrub()
}

// CancelScrub asks the active worker to stop after the current batch
// commits (a stable, resumable point). It returns immediately; the caller
// can poll progress until status becomes "paused".
func (e *Engine) CancelScrub() (int64, bool) {
	e.scrubMu.Lock()
	defer e.scrubMu.Unlock()
	if e.scrub == nil {
		return 0, false
	}
	select {
	case <-e.scrub.cancel:
	default:
		close(e.scrub.cancel)
	}
	return e.scrub.id, true
}

// ScrubProgress returns the live progress of one scrub, including per
// snapshot verdicts (derived from committed tables — no worker-held state).
func (e *Engine) ScrubProgress(id int64) (ScrubProgress, error) {
	sc, err := e.Manifest.GetScrub(id)
	if err != nil {
		return ScrubProgress{}, err
	}
	snaps, err := e.Manifest.ScrubSnapshots(id)
	if err != nil {
		return ScrubProgress{}, err
	}
	return ScrubProgress{Scrub: sc, SnapshotCount: len(snaps), Snapshots: snaps}, nil
}

// LatestScrubProgress returns progress of the most recent scrub.
func (e *Engine) LatestScrubProgress() (ScrubProgress, error) {
	sc, ok, err := e.Manifest.LatestScrub()
	if err != nil {
		return ScrubProgress{}, err
	}
	if !ok {
		return ScrubProgress{}, repo.ErrNotFound
	}
	return e.ScrubProgress(sc.ID)
}

// LatestScrubRow returns the most recent unfinished scrub row (paused, or a
// running row orphaned by a killed process), or nil when there is nothing
// to resume. Used at daemon startup.
func (e *Engine) LatestScrubRow() (*repo.ScrubInfo, error) {
	sc, ok, err := e.Manifest.LatestUnfinishedScrub()
	if err != nil || !ok {
		return nil, err
	}
	return &sc, nil
}

// runScrub is the worker: claim a batch in stable digest order, stream each
// blob verifying SHA-256 and length, then commit the batch atomically. The
// only interruption points are between batches (and after streaming each
// chunk via hooks), both resumable from done=0 rows.
func (e *Engine) runScrub(id int64, hooks ScrubHooks, act *activeScrub) {
	defer close(act.done)
	for {
		select {
		case <-act.cancel:
			_ = e.Manifest.MarkScrubPaused(id, "cancelled")
			e.clearActive(act)
			return
		default:
		}

		digests, err := e.Manifest.ClaimPendingChunks(id, scrubBatchChunks)
		if err != nil {
			_ = e.Manifest.MarkScrubPaused(id, err.Error())
			e.clearActive(act)
			return
		}
		if len(digests) == 0 {
			if err := e.Manifest.MarkScrubDone(id); err != nil {
				_ = e.Manifest.MarkScrubPaused(id, err.Error())
			}
			e.clearActive(act)
			return
		}

		results := make([]repo.ScrubChunkResult, 0, len(digests))
		cancelled := false
		for _, d := range digests {
			r := e.probeChunk(d)
			results = append(results, r)
			if hooks.AfterChunk != nil {
				hooks.AfterChunk(id, d, r)
			}
			select {
			case <-act.cancel:
				// Stop mid-batch: the chunks already streamed are real
				// verdicts, so commit them as a partial batch — itself a
				// stable cursor — and leave the rest done=0 for resume.
				cancelled = true
			default:
			}
			if cancelled {
				break
			}
		}
		if err := e.Manifest.SaveScrubBatch(id, results); err != nil {
			_ = e.Manifest.MarkScrubPaused(id, err.Error())
			e.clearActive(act)
			return
		}
		if cancelled {
			_ = e.Manifest.MarkScrubPaused(id, "cancelled")
			e.clearActive(act)
			return
		}
	}
}

func (e *Engine) clearActive(act *activeScrub) {
	e.scrubMu.Lock()
	if e.scrub == act {
		e.scrub = nil
	}
	e.scrubMu.Unlock()
}

// probeChunk streams one blob and compares its SHA-256 with the digest key
// and its length with the catalog. Three verdicts:
//   - ok: catalog row present, blob streams, length and digest match;
//   - missing: no catalog row or no blob file;
//   - bad: blob present but length or digest disagrees (tamper/silent rot).
func (e *Engine) probeChunk(digest []byte) repo.ScrubChunkResult {
	r := repo.ScrubChunkResult{Digest: digest, Result: "ok", ObservedLength: -1}

	declared, found, err := e.Manifest.ChunkLength(digest)
	if err != nil {
		r.Result = "missing"
		r.DeclaredLength = -1
		r.Detail = "catalog query failed: " + err.Error()
		return r
	}
	r.DeclaredLength = declared
	if !found {
		r.Result = "missing"
		r.Detail = "chunk missing from catalog (commit interrupted)"
		return r
	}

	rc, err := e.Store.Open(digest)
	if err != nil {
		r.Result = "missing"
		r.Detail = "blob cannot be opened: " + err.Error()
		return r
	}
	h := sha256.New()
	n, copyErr := io.Copy(h, rc)
	closeErr := rc.Close()
	r.ObservedLength = n
	got := h.Sum(nil)

	if copyErr != nil {
		// A digest mismatch surfaced while streaming is rot, not a missing blob.
		if repo.IsBadDigest(copyErr) {
			r.Result = "bad"
			r.ObservedDigest = got
			r.Detail = copyErr.Error()
			return r
		}
		r.Result = "missing"
		r.Detail = "blob read failed: " + copyErr.Error()
		return r
	}
	if closeErr != nil {
		r.Result = "missing"
		r.Detail = "blob close failed: " + closeErr.Error()
		return r
	}
	if n != declared {
		r.Result = "bad"
		r.ObservedDigest = got
		r.Detail = fmt.Sprintf("length mismatch: observed %d bytes, catalog says %d", n, declared)
		return r
	}
	if !equalBytes(got, digest) {
		r.Result = "bad"
		r.ObservedDigest = got
		r.Detail = fmt.Sprintf("digest mismatch: observed %x, catalog says %x", got, digest)
		return r
	}
	r.ObservedDigest = got
	return r
}

// ScrubWaitFor blocks until no worker is active in this process (test
// helper) or the timeout elapses.
func (e *Engine) ScrubWaitFor(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		e.scrubMu.Lock()
		act := e.scrub
		e.scrubMu.Unlock()
		if act == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("scrub still running after timeout")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// GuardSuspectSnapshot checks the latest scrub verdict for snapshotID. If
// the snapshot is suspect, it returns a *SuspectError locating every bad
// chunk, its on-disk path and every affected snapshot/file. Called by
// Restore before any disk work, so known-corrupt snapshots fail fast with
// a locatable error instead of failing mid-restore.
func (e *Engine) GuardSuspectSnapshot(snapshotID int64) error {
	views, err := e.Manifest.IntegrityViews()
	if err != nil {
		return err
	}
	view, ok := views[snapshotID]
	if !ok || view.Status != repo.ScrubSnapSuspect {
		return nil
	}
	bad, err := e.Manifest.ScrubBadChunks(view.ScrubID)
	if err != nil {
		return err
	}
	details := make([]SuspectDetail, 0, len(bad))
	for _, bc := range bad {
		// Only name chunks this snapshot actually references.
		refsSnapshot := false
		for _, r := range bc.Refs {
			if r.SnapshotID == snapshotID {
				refsSnapshot = true
				break
			}
		}
		if !refsSnapshot {
			continue
		}
		d := SuspectDetail{
			ChunkDigest:    hex.EncodeToString(bc.Digest),
			Result:         bc.Result,
			DeclaredLength: bc.DeclaredLength,
			ObservedLength: bc.ObservedLength,
			Detail:         bc.Detail,
			Affected:       bc.Refs,
		}
		if p, perr := e.Store.Path(bc.Digest); perr == nil {
			d.BlobPath = p
		}
		details = append(details, d)
	}
	if len(details) == 0 {
		// Verdict says suspect but the chunk report has no matching row
		// (e.g. newer scrub superseded): treat as guarded anyway.
		return &SuspectError{SnapshotID: snapshotID, ScrubID: view.ScrubID}
	}
	return &SuspectError{SnapshotID: snapshotID, ScrubID: view.ScrubID, Details: details}
}
