package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"incbackup/internal/repo"
)

// ErrScrubRunning is returned when a patrol is asked to start while one is
// already executing in this process. The existing run must be observed (or
// interrupted/resumed) instead of spawning a duplicate report.
var ErrScrubRunning = errors.New("integrity patrol already running")

// scrubBatchChunks bounds how many chunk verdicts accumulate before a
// transactional checkpoint; it also bounds the work lost on interrupt.
const scrubBatchChunks = 16

// ScrubSuspectError refuses a restore whose chunks the latest patrol already
// proved corrupt or missing. It is raised before any block is streamed and
// names the exact chunks and file paths, so maintenance never discovers rot
// by half-way-through restore failure.
type ScrubSuspectError struct {
	SnapshotID int64
	RunID      int64
	Bad        []repo.ScrubBadChunk
}

func (e *ScrubSuspectError) Error() string {
	first := ""
	if len(e.Bad) > 0 {
		bc := e.Bad[0]
		paths := "(unknown paths)"
		if len(bc.RelPaths) > 0 {
			paths = fmt.Sprintf("%q", bc.RelPaths[0])
			if len(bc.RelPaths) > 1 {
				paths = fmt.Sprintf("%q and %d more", bc.RelPaths[0], len(bc.RelPaths)-1)
			}
		}
		first = fmt.Sprintf("; e.g. chunk %x in %s: %s", bc.Digest, paths, bc.Result)
	}
	return fmt.Sprintf("snapshot %d is suspect per integrity patrol run %d (%d bad chunk(s)), restore refused%s",
		e.SnapshotID, e.RunID, len(e.Bad), first)
}

// ScrubOptions tunes a patrol run. Throttle is idle time inserted after each
// chunk so large repositories can patrol without saturating disk IO. Gate,
// when non-nil, is awaited once before the first chunk: callers use it as a
// deterministic "first chunk is slow" barrier (to interrupt mid-cursor or to
// commit a snapshot while the patrol is active) instead of racing timers.
type ScrubOptions struct {
	Throttle time.Duration
	Gate     <-chan struct{}
}

// StartScrub starts a repository-wide integrity patrol. If an earlier run was
// interrupted (status 'running' in SQLite), it is resumed from its stable
// cursor instead of creating a duplicate run. The patrol runs in the
// background; returned progress reflects the starting state.
func (e *Engine) StartScrub(opts ScrubOptions) (*ScrubProgress, bool, error) {
	e.scrubMu.Lock()
	if e.scrubCancel != nil {
		e.scrubMu.Unlock()
		return nil, false, ErrScrubRunning
	}
	run, resumed, err := e.acquireScrubRunLocked()
	if err != nil {
		e.scrubMu.Unlock()
		return nil, false, err
	}
	e.spawnScrubLocked(run, opts)
	runID := run.ID
	e.scrubMu.Unlock()

	prog, err := e.scrubProgress(runID)
	return prog, resumed, err
}

// ResumeInterruptedScrub continues the newest interrupted run, if any. Called
// at daemon startup; returns nil progress when there is nothing to resume.
func (e *Engine) ResumeInterruptedScrub() (*ScrubProgress, bool, error) {
	e.scrubMu.Lock()
	if e.scrubCancel != nil {
		e.scrubMu.Unlock()
		return nil, false, ErrScrubRunning
	}
	run, err := e.Manifest.LatestRunningScrub()
	if errors.Is(err, repo.ErrNotFound) {
		e.scrubMu.Unlock()
		return nil, false, nil
	}
	if err != nil {
		e.scrubMu.Unlock()
		return nil, false, err
	}
	e.spawnScrubLocked(run, ScrubOptions{})
	runID := run.ID
	e.scrubMu.Unlock()

	prog, err := e.scrubProgress(runID)
	return prog, true, err
}

// acquireScrubRunLocked returns the run to execute: an interrupted one to
// resume if present, otherwise a freshly begun run. Caller holds scrubMu.
func (e *Engine) acquireScrubRunLocked() (run repo.ScrubRun, resumed bool, err error) {
	latest, err := e.Manifest.LatestScrubRun()
	if err == nil && latest.Status == repo.ScrubStatusRunning {
		return latest, true, nil
	}
	if err != nil && !errors.Is(err, repo.ErrNotFound) {
		return repo.ScrubRun{}, false, err
	}
	run, err = e.Manifest.BeginScrubRun()
	if err != nil {
		return repo.ScrubRun{}, false, err
	}
	return run, false, nil
}

// spawnScrubLocked starts the patrol goroutine. Caller holds scrubMu.
func (e *Engine) spawnScrubLocked(run repo.ScrubRun, opts ScrubOptions) {
	ctx, cancel := context.WithCancel(context.Background())
	e.scrubCancel = cancel
	e.scrubRunID = run.ID
	go e.runScrub(ctx, run.ID, opts)
}

// InterruptScrub cancels a running patrol in this process. Verdicts already
// checkpointed stay in SQLite, so a later StartScrub resumes from the cursor.
func (e *Engine) InterruptScrub() bool {
	e.scrubMu.Lock()
	cancel := e.scrubCancel
	e.scrubMu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

// IsScrubRunning reports whether a patrol goroutine is active in this process.
func (e *Engine) IsScrubRunning() bool {
	e.scrubMu.Lock()
	defer e.scrubMu.Unlock()
	return e.scrubCancel != nil
}

// WaitUntilScrubIdle blocks until no patrol goroutine is active or the timeout
// elapses. It exists mainly to make interrupt tests deterministic.
func (e *Engine) WaitUntilScrubIdle(d time.Duration) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		e.scrubMu.Lock()
		idle := e.scrubCancel == nil
		e.scrubMu.Unlock()
		if idle {
			return nil
		}
		time.Sleep(2 * time.Millisecond)
	}
	return errors.New("scrub still running after timeout")
}

// runScrub is the resumable streaming patrol loop. Each batch is checkpointed
// together with the cursor in one transaction; on context cancellation the
// current batch is persisted and the run stays 'running' for later resume.
func (e *Engine) runScrub(ctx context.Context, runID int64, opts ScrubOptions) {
	defer func() {
		e.scrubMu.Lock()
		e.scrubCancel = nil
		e.scrubRunID = 0
		e.scrubMu.Unlock()
	}()
	if opts.Gate != nil {
		select {
		case <-opts.Gate:
		case <-ctx.Done():
			return
		}
	}
	var scanned int64
	for {
		if ctx.Err() != nil {
			return
		}
		run, err := e.Manifest.GetScrubRun(runID)
		if err != nil || run.Status == repo.ScrubStatusDone {
			return
		}
		items, err := e.Manifest.NextScrubChunks(runID, run.CursorDigest, scrubBatchChunks)
		if err != nil {
			return
		}
		if len(items) == 0 {
			if err := e.Manifest.FinishScrubRun(runID); err != nil {
				// Retry next time a patrol starts; never leave false verdicts.
				return
			}
			return
		}
		results := make([]repo.ScrubChunkResult, 0, len(items))
		interrupted := false
		for _, it := range items {
			// Throttle before touching disk: an interrupt arriving in this
			// window is observed before this chunk's verdict is produced, so
			// a freshly launched throttled patrol can always be stopped
			// mid-cursor.
			if opts.Throttle > 0 {
				select {
				case <-time.After(opts.Throttle):
				case <-ctx.Done():
					interrupted = true
				}
				if interrupted {
					break
				}
			}
			if ctx.Err() != nil {
				interrupted = true
				break
			}
			results = append(results, e.checkScrubChunk(it))
			scanned++
			if e.Fail.ScrubAfterChunk != nil {
				if hookErr := e.Fail.ScrubAfterChunk(runID, scanned); hookErr != nil {
					// Test/ops hook: checkpoint progress and stop as if killed.
					_ = e.Manifest.SaveScrubChunkResults(runID, results)
					e.InterruptScrub()
					return
				}
			}
		}
		if len(results) > 0 {
			if err := e.Manifest.SaveScrubChunkResults(runID, results); err != nil {
				return
			}
		}
		if interrupted {
			return
		}
	}
}

// checkScrubChunk streams one blob and classifies it. Every failure mode gets
// a distinct, locatable result; a missing catalog row (commit-interrupt
// failpoint) is reported without touching disk.
func (e *Engine) checkScrubChunk(c repo.ScrubChunk) repo.ScrubChunkResult {
	r := repo.ScrubChunkResult{
		Digest:         c.Digest,
		DeclaredLength: c.DeclaredLength,
		ActualLength:   -1,
	}
	if c.DeclaredLength < 0 {
		r.Result = repo.ChunkResultMissingCatalog
		r.Message = "chunk row missing from catalog (commit interrupted)"
		return r
	}
	p, err := e.Store.Path(c.Digest)
	if err != nil {
		r.Result = repo.ChunkResultReadError
		r.Message = err.Error()
		return r
	}
	fi, err := os.Lstat(p)
	if errors.Is(err, os.ErrNotExist) {
		r.Result = repo.ChunkResultMissingBlob
		r.Message = "chunk blob absent from content store: " + p
		return r
	}
	if err != nil {
		r.Result = repo.ChunkResultReadError
		r.Message = err.Error()
		return r
	}
	if !fi.Mode().IsRegular() {
		r.Result = repo.ChunkResultReadError
		r.Message = "chunk path is not a regular file: " + p
		return r
	}
	r.ActualLength = fi.Size()
	if r.ActualLength != c.DeclaredLength {
		r.Result = repo.ChunkResultLengthMismatch
		r.Message = fmt.Sprintf("catalog declares %d bytes, blob is %d bytes", c.DeclaredLength, r.ActualLength)
		return r
	}
	rc, err := e.Store.Open(c.Digest) // digest verified while streaming
	if err != nil {
		r.Result = repo.ChunkResultReadError
		r.Message = err.Error()
		return r
	}
	if _, err := io.Copy(io.Discard, rc); err != nil {
		if repo.IsBadDigest(err) {
			r.Result = repo.ChunkResultDigestMismatch
		} else {
			r.Result = repo.ChunkResultReadError
		}
		r.Message = err.Error()
		_ = rc.Close()
		return r
	}
	if err := rc.Close(); err != nil {
		r.Result = repo.ChunkResultReadError
		r.Message = err.Error()
		return r
	}
	r.Result = repo.ChunkResultOK
	return r
}

// checkScrubGate refuses restore of a snapshot already proven bad by the
// latest patrol. Only runs that covered the snapshot can veto it: a verdict
// older than the snapshot cannot say anything about chunks (re)created at
// the snapshot's commit time. Streaming verification inside Restore remains
// the defense-in-depth for chunks no covering patrol has checked.
func (e *Engine) checkScrubGate(snapshotID int64) error {
	run, err := e.Manifest.LatestScrubRun()
	if errors.Is(err, repo.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	state, err := e.Manifest.GetScrubSnapshot(run.ID, snapshotID)
	if errors.Is(err, repo.ErrNotFound) {
		// Snapshot postdates the run entirely (no row, even uncovered):
		// nothing in this run covers it.
		return nil
	}
	if err != nil {
		return err
	}
	if !state.Covered {
		return nil // uncovered (or not yet finalized): this run cannot veto it
	}
	bad, err := e.Manifest.ScrubBadChunksForSnapshot(run.ID, snapshotID)
	if err != nil {
		return err
	}
	if len(bad) > 0 {
		return &ScrubSuspectError{SnapshotID: snapshotID, RunID: run.ID, Bad: bad}
	}
	return nil
}

// SnapshotScrubView is one snapshot's row in a patrol report.
type SnapshotScrubView struct {
	SnapshotID      int64  `json:"snapshot_id"`
	CommitStatus    string `json:"commit_status"`
	Covered         bool   `json:"covered"`
	Integrity       string `json:"integrity"`
	Note            string `json:"note,omitempty"`
	BadChunkCount   int64  `json:"bad_chunk_count"`
	ReferencedTotal int64  `json:"chunks_referenced"`
	ScannedChunks   int64  `json:"chunks_scanned"`
}

// ScrubProgress is a full progress snapshot of one patrol run.
type ScrubProgress struct {
	RunID         int64               `json:"run_id"`
	Status        string              `json:"status"`
	HighWatermark int64               `json:"high_watermark"`
	TotalChunks   int64               `json:"chunks_total"`
	CleanChunks   int64               `json:"chunks_clean"`
	BadChunks     int64               `json:"chunks_bad"`
	ScannedChunks int64               `json:"chunks_scanned"`
	StartedAt     time.Time           `json:"started_at"`
	FinishedAt    *time.Time          `json:"finished_at,omitempty"`
	Running       bool                `json:"running"`
	Snapshots     []SnapshotScrubView `json:"snapshots"`
	Summary       map[string]int      `json:"summary"`
}

// LatestScrubProgress returns the progress of the most recent patrol run.
func (e *Engine) LatestScrubProgress() (*ScrubProgress, error) {
	run, err := e.Manifest.LatestScrubRun()
	if err != nil {
		return nil, err
	}
	return e.scrubProgress(run.ID)
}

// ScrubProgressOf returns the progress of an arbitrary run id.
func (e *Engine) ScrubProgressOf(runID int64) (*ScrubProgress, error) {
	if _, err := e.Manifest.GetScrubRun(runID); err != nil {
		return nil, err
	}
	return e.scrubProgress(runID)
}

func (e *Engine) scrubProgress(runID int64) (*ScrubProgress, error) {
	run, err := e.Manifest.GetScrubRun(runID)
	if err != nil {
		return nil, err
	}
	states, err := e.Manifest.ScrubSnapshotStates(runID)
	if err != nil {
		return nil, err
	}
	views := make([]SnapshotScrubView, 0, len(states))
	summary := map[string]int{}
	for _, st := range states {
		integrity := e.liveIntegrity(run.Status, st)
		si, err := e.Manifest.GetSnapshot(st.SnapshotID)
		commitStatus := ""
		if err == nil {
			commitStatus = si.Status
		}
		views = append(views, SnapshotScrubView{
			SnapshotID:      st.SnapshotID,
			CommitStatus:    commitStatus,
			Covered:         st.Covered,
			Integrity:       integrity,
			Note:            st.Note,
			BadChunkCount:   st.BadChunks,
			ReferencedTotal: st.ReferencedTotal,
			ScannedChunks:   st.ScannedChunks,
		})
		summary[integrity]++
	}
	e.scrubMu.Lock()
	running := e.scrubCancel != nil && e.scrubRunID == runID
	e.scrubMu.Unlock()
	return &ScrubProgress{
		RunID:         run.ID,
		Status:        run.Status,
		HighWatermark: run.HighWatermark,
		TotalChunks:   run.TotalChunks,
		CleanChunks:   run.CleanChunks,
		BadChunks:     run.BadChunks,
		ScannedChunks: run.CleanChunks + run.BadChunks,
		StartedAt:     run.StartedAt,
		FinishedAt:    run.FinishedAt,
		Running:       running,
		Snapshots:     views,
		Summary:       summary,
	}, nil
}

// liveIntegrity derives the verdict of one snapshot row as it must appear
// *right now*. Persisted rows are final after run completion; while a run is
// ongoing a covered snapshot stays 'unscanned' until every one of its chunks
// has a verdict — and even then it is never reported clean/suspect early in
// the API, because only finalize persists the decision.
func (e *Engine) liveIntegrity(runStatus string, st repo.ScrubSnapshotState) string {
	if !st.Covered {
		return repo.IntegrityUncovered
	}
	if runStatus == repo.ScrubStatusDone {
		return st.Integrity
	}
	switch st.Integrity {
	case repo.IntegrityExcluded:
		// Pending/failed at start; its historical status/errors stay the
		// verdict even if chunk verdicts accumulate.
		return repo.IntegrityExcluded
	default:
		return repo.IntegrityUnscanned
	}
}

// SnapshotScrubDetail is the per-snapshot patrol view with bad-chunk detail.
type SnapshotScrubDetail struct {
	SnapshotScrubView
	BadChunks []ScrubBadChunkView `json:"bad_chunks"`
}

// ScrubBadChunkView names a corrupt/missing chunk and where it is referenced.
type ScrubBadChunkView struct {
	Digest   string   `json:"chunk_digest"`
	Length   int64    `json:"declared_length"`
	Result   string   `json:"result"`
	RelPaths []string `json:"rel_paths"`
	BlobPath string   `json:"expected_blob_path,omitempty"`
}

// ScrubSnapshotDetail returns the patrol verdict and bad-chunk details of one
// snapshot within a run.
func (e *Engine) ScrubSnapshotDetail(runID, snapshotID int64) (*SnapshotScrubDetail, error) {
	if _, err := e.Manifest.GetSnapshot(snapshotID); err != nil {
		return nil, err
	}
	prog, err := e.scrubProgress(runID)
	if err != nil {
		return nil, err
	}
	var view *SnapshotScrubView
	for i := range prog.Snapshots {
		if prog.Snapshots[i].SnapshotID == snapshotID {
			view = &prog.Snapshots[i]
			break
		}
	}
	if view == nil {
		// Snapshot exists but has no row in this run — can only be an
		// uncovered snapshot created after the run started (rows are seeded
		// at finalize; report it explicitly as uncovered).
		run, err := e.Manifest.GetScrubRun(runID)
		if err != nil {
			return nil, err
		}
		if snapshotID > run.HighWatermark {
			return &SnapshotScrubDetail{SnapshotScrubView: SnapshotScrubView{
				SnapshotID: snapshotID,
				Covered:    false,
				Integrity:  repo.IntegrityUncovered,
				Note:       "committed after this patrol started; not covered by this run",
			}}, nil
		}
		return nil, repo.ErrNotFound
	}
	detail := &SnapshotScrubDetail{SnapshotScrubView: *view}
	bad, err := e.Manifest.ScrubBadChunksForSnapshot(runID, snapshotID)
	if err != nil {
		return nil, err
	}
	for _, mc := range bad {
		bv := ScrubBadChunkView{
			Digest:   fmt.Sprintf("%x", mc.Digest),
			Length:   mc.Length,
			Result:   mc.Result,
			RelPaths: append([]string(nil), mc.RelPaths...),
		}
		if p, err := e.Store.Path(mc.Digest); err == nil {
			bv.BlobPath = p
		}
		detail.BadChunks = append(detail.BadChunks, bv)
	}
	return detail, nil
}
