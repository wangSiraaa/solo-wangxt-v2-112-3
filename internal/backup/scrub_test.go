package backup_test

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

// chunkDigestRow is one digest with its catalog length.
type chunkDigestRow struct {
	digest []byte
	length int64
}

// allChunks lists every catalog chunk of a snapshot (via direct SQL through
// the engine's manifest DB), sorted by digest.
func allChunks(t *testing.T, e *backup.Engine, snapID int64) []chunkDigestRow {
	t.Helper()
	rows, err := e.Manifest.DB().Query(`SELECT DISTINCT c.digest, c.length
		FROM entry_chunks ec JOIN chunks c ON c.digest = ec.chunk_digest
		WHERE ec.snapshot_id = ? ORDER BY c.digest`, snapID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []chunkDigestRow
	for rows.Next() {
		var r chunkDigestRow
		if err := rows.Scan(&r.digest, &r.length); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	return out
}

// sharedChunks returns digests referenced by both snapshots.
func sharedChunks(t *testing.T, e *backup.Engine, a, b int64) []chunkDigestRow {
	t.Helper()
	ca := map[string]chunkDigestRow{}
	for _, c := range allChunks(t, e, a) {
		ca[fmt.Sprintf("%x", c.digest)] = c
	}
	var shared []chunkDigestRow
	for _, c := range allChunks(t, e, b) {
		if _, ok := ca[fmt.Sprintf("%x", c.digest)]; ok {
			shared = append(shared, c)
		}
	}
	return shared
}

// singletonChunks returns chunks referenced only by snap among all snapshots.
func singletonChunks(t *testing.T, e *backup.Engine, snap int64) []chunkDigestRow {
	t.Helper()
	var out []chunkDigestRow
	for _, c := range allChunks(t, e, snap) {
		var n int
		if err := e.Manifest.DB().QueryRow(`SELECT count(DISTINCT snapshot_id)
			FROM entry_chunks WHERE chunk_digest = ?`, c.digest).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 1 {
			out = append(out, c)
		}
	}
	return out
}

// corruptBlob flips every byte of the on-disk blob in place, preserving its
// length (chunks are stored 0444) so the failure surfaces as a streaming
// digest mismatch rather than a length mismatch.
func corruptBlob(t *testing.T, e *backup.Engine, digest []byte) {
	t.Helper()
	p, err := e.Store.Path(digest)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	for i := range data {
		data[i] ^= 0xff
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// waitScrubDone polls progress until the run finalizes.
func waitScrubDone(t *testing.T, e *backup.Engine, runID int64) *backup.ScrubProgress {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last *backup.ScrubProgress
	for time.Now().Before(deadline) {
		prog, err := e.ScrubProgressOf(runID)
		if err != nil {
			t.Fatal(err)
		}
		last = prog
		if prog.Status == repo.ScrubStatusDone && !prog.Running {
			return prog
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("scrub %d never finished; last=%+v", runID, last)
	return nil
}

// waitForScrubRunning blocks until the run is executing in-process.
func waitForScrubRunning(t *testing.T, e *backup.Engine, runID int64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if e.IsScrubRunning() {
			return
		}
		prog, err := e.ScrubProgressOf(runID)
		if err != nil {
			t.Fatal(err)
		}
		if prog.Running || prog.ScannedChunks > 0 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("scrub %d never started", runID)
}

func timeout5s() time.Duration { return 5 * time.Second }

func integrityOf(prog *backup.ScrubProgress, snapID int64) (backup.SnapshotScrubView, bool) {
	for _, v := range prog.Snapshots {
		if v.SnapshotID == snapID {
			return v, true
		}
	}
	return backup.SnapshotScrubView{}, false
}

// makeTwoSnapshots creates a big shared file, snapshots it twice (the middle
// edit reuses all-but-one chunk), then returns both snapshot ids and a chunk
// shared by both.
func makeTwoSnapshots(t *testing.T, e *backup.Engine, dir string) (int64, int64, []byte) {
	t.Helper()
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	big := strings.Repeat("0123456789ABCDEF\n", 12000)
	must(t, os.WriteFile(filepath.Join(src, "big.bin"), []byte(big), 0o644))
	r1, err := e.CreateSnapshot(src, "v1", true)
	if err != nil {
		t.Fatalf("snapshot 1: %v", err)
	}
	buf := []byte(big)
	copy(buf[90*1024:], []byte("PATCHED IN THE MIDDLE"))
	must(t, os.WriteFile(filepath.Join(src, "big.bin"), buf, 0o644))
	r2, err := e.CreateSnapshot(src, "v2", true)
	if err != nil {
		t.Fatalf("snapshot 2: %v", err)
	}
	shared := sharedChunks(t, e, r1.SnapshotID, r2.SnapshotID)
	if len(shared) == 0 {
		t.Fatal("test setup: no shared chunks between snapshots")
	}
	return r1.SnapshotID, r2.SnapshotID, shared[0].digest
}

// Acceptance ①: corrupting a chunk shared by two snapshots flags BOTH as
// suspect, lists both snapshots + file paths via the manifest reverse-lookup,
// and refuses restore for both before any byte is streamed.
func TestScrubSharedChunkCorruptionFlagsBothSnapshots(t *testing.T) {
	e, dir := openEngine(t)
	id1, id2, shared := makeTwoSnapshots(t, e, dir)

	// Baseline patrol on healthy storage: both clean.
	prog, resumed, err := e.StartScrub(backup.ScrubOptions{})
	if err != nil || resumed {
		t.Fatalf("start: %v resumed=%v", err, resumed)
	}
	prog = waitScrubDone(t, e, prog.RunID)
	for _, id := range []int64{id1, id2} {
		v, ok := integrityOf(prog, id)
		if !ok || v.Integrity != repo.IntegrityClean {
			t.Fatalf("snapshot %d expected clean, got %+v", id, v)
		}
	}

	// Silent rot of the shared blob after the clean patrol.
	corruptBlob(t, e, shared)

	// A second patrol must catch it for BOTH snapshots.
	prog, resumed, err = e.StartScrub(backup.ScrubOptions{})
	if err != nil || resumed {
		t.Fatalf("second start: %v resumed=%v", err, resumed)
	}
	prog = waitScrubDone(t, e, prog.RunID)
	for _, id := range []int64{id1, id2} {
		v, ok := integrityOf(prog, id)
		if !ok || v.Integrity != repo.IntegritySuspect || v.BadChunkCount == 0 {
			t.Fatalf("snapshot %d expected suspect with bad chunks, got %+v", id, v)
		}
	}

	// Reverse-lookup through the manifest: both snapshots and the path.
	result, refs, err := e.Manifest.ScrubAffected(prog.RunID, shared)
	if err != nil {
		t.Fatal(err)
	}
	if result != repo.ChunkResultDigestMismatch {
		t.Fatalf("result=%s want digest_mismatch", result)
	}
	gotIDs := map[int64]bool{}
	var paths []string
	for _, ref := range refs {
		gotIDs[ref.SnapshotID] = true
		paths = append(paths, ref.RelPath)
	}
	if !gotIDs[id1] || !gotIDs[id2] {
		t.Fatalf("affected snapshots %v want %d and %d", gotIDs, id1, id2)
	}
	for _, p := range paths {
		if p != "big.bin" {
			t.Fatalf("unexpected affected path %q", p)
		}
	}

	// Restore is refused early with a locatable error naming the chunk.
	for _, id := range []int64{id1, id2} {
		_, err := e.Restore(id, filepath.Join(dir, fmt.Sprintf("out-%d", id)))
		var sus *backup.ScrubSuspectError
		if !errors.As(err, &sus) {
			t.Fatalf("snapshot %d restore err=%v, want ScrubSuspectError", id, err)
		}
		if sus.RunID != prog.RunID || len(sus.Bad) == 0 {
			t.Fatalf("suspect error detail wrong: %+v", sus)
		}
		found := false
		for _, mc := range sus.Bad {
			if eqBytes(mc.Digest, shared) {
				for _, rel := range mc.RelPaths {
					if rel == "big.bin" {
						found = true
					}
				}
			}
		}
		if !found {
			t.Fatalf("suspect error does not name shared chunk in big.bin: %+v", sus.Bad)
		}
	}
}

// Acceptance ②: corrupting a chunk referenced by only ONE snapshot marks that
// snapshot suspect while the other stays clean and restorable.
func TestScrubUniqueChunkCorruptionOnlyAffectsOwner(t *testing.T) {
	e, dir := openEngine(t)
	id1, id2, _ := makeTwoSnapshots(t, e, dir)

	// First clean patrol so the later run is the one that detects rot.
	prog, _, err := e.StartScrub(backup.ScrubOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waitScrubDone(t, e, prog.RunID)

	uniq := singletonChunks(t, e, id2)
	if len(uniq) == 0 {
		t.Fatal("test setup: second snapshot has no singleton chunk")
	}
	corruptBlob(t, e, uniq[0].digest)

	prog, _, err = e.StartScrub(backup.ScrubOptions{})
	if err != nil {
		t.Fatal(err)
	}
	prog = waitScrubDone(t, e, prog.RunID)

	v2, _ := integrityOf(prog, id2)
	if v2.Integrity != repo.IntegritySuspect {
		t.Fatalf("owner snapshot: %+v", v2)
	}
	v1, _ := integrityOf(prog, id1)
	if v1.Integrity != repo.IntegrityClean {
		t.Fatalf("unrelated snapshot should stay clean, got %+v", v1)
	}

	// The unrelated snapshot still restores; the owner is rejected.
	if _, err := e.Restore(id1, filepath.Join(dir, "out-ok")); err != nil {
		t.Fatalf("clean snapshot restore should work: %v", err)
	}
	if _, err := e.Restore(id2, filepath.Join(dir, "out-bad")); err == nil {
		t.Fatal("suspect snapshot must not restore")
	} else {
		var sus *backup.ScrubSuspectError
		if !errors.As(err, &sus) {
			t.Fatalf("want ScrubSuspectError, got %v", err)
		}
	}
}

// Acceptance ③: interrupting a patrol mid-run and restarting it yields exactly
// the same verdicts and chunk reports as one uninterrupted patrol: no
// duplicate chunk rows, and unscanned data is never called clean.
func TestScrubInterruptAndResumeIsEquivalent(t *testing.T) {
	e, dir := openEngine(t)
	id1, id2, shared := makeTwoSnapshots(t, e, dir)

	// Interrupt after the 3rd chunk, checkpoint included.
	stop := errors.New("test interrupt")
	e.Fail.ScrubAfterChunk = func(runID int64, scanned int64) error {
		if scanned == 3 {
			return stop
		}
		return nil
	}
	prog, resumed, err := e.StartScrub(backup.ScrubOptions{})
	if err != nil || resumed {
		t.Fatalf("start: %v resumed=%v", err, resumed)
	}
	runID := prog.RunID
	must(t, e.WaitUntilScrubIdle(timeout5s()))

	// Run stays 'running' on disk with a stable cursor; covered snapshots are
	// not clean yet.
	mid, err := e.Manifest.GetScrubRun(runID)
	if err != nil {
		t.Fatal(err)
	}
	if mid.Status != repo.ScrubStatusRunning {
		t.Fatalf("interrupted run status=%s", mid.Status)
	}
	if mid.CursorDigest == nil {
		t.Fatal("cursor must be persisted after checkpoint")
	}
	p, err := e.ScrubProgressOf(runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{id1, id2} {
		v, ok := integrityOf(p, id)
		if !ok {
			t.Fatalf("snapshot %d missing from progress", id)
		}
		if v.Integrity != repo.IntegrityUnscanned {
			t.Fatalf("partially scanned snapshot must present unscanned, got %q", v.Integrity)
		}
	}

	// Restart (same process): must resume the same run, not create a new one.
	e.Fail.ScrubAfterChunk = nil
	prog2, resumed, err := e.StartScrub(backup.ScrubOptions{})
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if !resumed || prog2.RunID != runID {
		t.Fatalf("expected resume of run %d, got run %d (resumed=%v)", runID, prog2.RunID, resumed)
	}
	finished := waitScrubDone(t, e, runID)

	// No duplicate chunk verdict rows.
	var nRows, nDistinct int
	if err := e.Manifest.DB().QueryRow(`SELECT count(*), count(DISTINCT chunk_digest)
		FROM scrub_chunks WHERE run_id = ?`, runID).Scan(&nRows, &nDistinct); err != nil {
		t.Fatal(err)
	}
	if nRows != nDistinct || int64(nRows) != finished.TotalChunks {
		t.Fatalf("chunk rows=%d distinct=%d total=%d", nRows, nDistinct, finished.TotalChunks)
	}
	for _, id := range []int64{id1, id2} {
		v, _ := integrityOf(finished, id)
		if v.Integrity != repo.IntegrityClean {
			t.Fatalf("snapshot %d: %+v", id, v)
		}
	}

	// Now corrupt the shared blob and run a fresh, uninterrupted reference
	// patrol; verdict equality with the resumed run must hold except for the
	// newly corrupted chunk: both must agree on clean-vs-suspect per
	// snapshot, and corrupting before the *reference* run means the resumed
	// run stays historically clean (it finished before the rot) — verify the
	// fresh run catches it and both runs' chunk sets match exactly.
	corruptBlob(t, e, shared)
	fresh, _, err := e.StartScrub(backup.ScrubOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fresh = waitScrubDone(t, e, fresh.RunID)
	if fresh.RunID == runID {
		t.Fatal("corruption patrol must be a new run")
	}
	if fresh.TotalChunks != finished.TotalChunks {
		t.Fatalf("work set drift: resumed=%d fresh=%d", finished.TotalChunks, fresh.TotalChunks)
	}
	v1, _ := integrityOf(fresh, id1)
	v2, _ := integrityOf(fresh, id2)
	if v1.Integrity != repo.IntegritySuspect || v2.Integrity != repo.IntegritySuspect {
		t.Fatalf("fresh patrol verdicts: %q %q", v1.Integrity, v2.Integrity)
	}
	// Chunk-by-chunk agreement: exactly one chunk differs between the two
	// runs, and it is the corrupted shared blob.
	differ := chunkResultDiff(t, e, runID, fresh.RunID)
	if len(differ) != 1 || !eqBytes(differ[0], shared) {
		t.Fatalf("resumed vs fresh runs differ on %x, want only %x", differ, shared)
	}
}

func chunkResultDiff(t *testing.T, e *backup.Engine, runA, runB int64) [][]byte {
	t.Helper()
	rows, err := e.Manifest.DB().Query(`SELECT a.chunk_digest FROM scrub_chunks a
		JOIN scrub_chunks b ON b.run_id = ? AND b.chunk_digest = a.chunk_digest
		WHERE a.run_id = ? AND a.result != b.result`, runB, runA)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var d []byte
		if err := rows.Scan(&d); err != nil {
			t.Fatal(err)
		}
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return fmt.Sprintf("%x", out[i]) < fmt.Sprintf("%x", out[j]) })
	return out
}

func eqBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Acceptance ④: snapshots committed while a patrol runs are recorded
// uncovered (not clean), and pre-existing pending/failed snapshots are kept
// excluded with their original failure records intact.
func TestScrubWatermarkAndPendingFailedAreNeverClean(t *testing.T) {
	e, dir := openEngine(t)

	// 1. one healthy committed snapshot
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	must(t, os.WriteFile(filepath.Join(src, "a.txt"), []byte("first\n"), 0o644))
	r1, err := e.CreateSnapshot(src, "before", true)
	if err != nil {
		t.Fatal(err)
	}

	// 2. a pending snapshot (crash-before-commit failpoint) and a failed one
	pending, err := e.CreateSnapshot(src, "pending", false)
	if err != nil {
		t.Fatal(err)
	}
	if pending.Status != repo.StatusPending {
		t.Fatalf("want pending, got %s", pending.Status)
	}
	must(t, os.WriteFile(filepath.Join(src, "grow.log"), []byte("x"), 0o644))
	e.Fail.LoseChunkCount = 1
	failed, err := e.CreateSnapshot(src, "failed", true)
	e.Fail.LoseChunkCount = 0
	if !errors.As(err, new(*backup.ErrRejected)) {
		t.Fatalf("want rejected, got status=%v err=%v", failed, err)
	}
	failedErrsBefore, err := e.Manifest.ListErrors(failed.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	if len(failedErrsBefore) == 0 {
		t.Fatal("failed snapshot should carry failure records")
	}

	// 3. start a patrol and make it block on a hook before the first chunk
	// of the first batch is checkpointed: this is the critical window in
	// which a new snapshot must be classified uncovered.
	proceed := make(chan struct{})
	e.Fail.ScrubAfterChunk = func(runID int64, scanned int64) error {
		<-proceed // hold the patrol mid-batch
		return nil
	}
	prog, _, err := e.StartScrub(backup.ScrubOptions{})
	if err != nil {
		t.Fatal(err)
	}
	runID := prog.RunID
	waitForScrubRunning(t, e, runID)

	// 4. while the patrol is stuck, commit a brand-new snapshot.
	must(t, os.WriteFile(filepath.Join(src, "new.txt"), []byte("born during scrub\n"), 0o644))
	during, err := e.CreateSnapshot(src, "during", true)
	if err != nil {
		t.Fatalf("snapshot during scrub: %v", err)
	}

	// 5. let the patrol finish
	close(proceed)
	finished := waitScrubDone(t, e, runID)

	v1, _ := integrityOf(finished, r1.SnapshotID)
	if v1.Integrity != repo.IntegrityClean {
		t.Fatalf("committed-before snapshot: %+v", v1)
	}
	vp, _ := integrityOf(finished, pending.SnapshotID)
	if vp.Integrity != repo.IntegrityExcluded || !vp.Covered {
		t.Fatalf("pending snapshot must be excluded, got %+v", vp)
	}
	vf, _ := integrityOf(finished, failed.SnapshotID)
	if vf.Integrity != repo.IntegrityExcluded || !vf.Covered {
		t.Fatalf("failed snapshot must be excluded, got %+v", vf)
	}
	vd, _ := integrityOf(finished, during.SnapshotID)
	if vd.Integrity != repo.IntegrityUncovered || vd.Covered {
		t.Fatalf("during-run snapshot must be uncovered, got %+v", vd)
	}

	// Historical facts untouched.
	pinfo, _ := e.Manifest.GetSnapshot(pending.SnapshotID)
	finfo, _ := e.Manifest.GetSnapshot(failed.SnapshotID)
	dinfo, _ := e.Manifest.GetSnapshot(during.SnapshotID)
	if pinfo.Status != repo.StatusPending {
		t.Fatalf("pending status rewritten: %s", pinfo.Status)
	}
	if finfo.Status != repo.StatusFailed {
		t.Fatalf("failed status rewritten: %s", finfo.Status)
	}
	if dinfo.Status != repo.StatusCommitted {
		t.Fatalf("during snapshot should be committed: %s", dinfo.Status)
	}
	failedErrsAfter, _ := e.Manifest.ListErrors(failed.SnapshotID)
	if len(failedErrsAfter) != len(failedErrsBefore) {
		t.Fatalf("failure records changed: before=%d after=%d", len(failedErrsBefore), len(failedErrsAfter))
	}

	// The during-run snapshot is not blocked by the (older) run gate —
	// uncovered is not suspect — and can itself be covered by a later run.
	if _, err := e.Restore(during.SnapshotID, filepath.Join(dir, "during-out")); err != nil {
		t.Fatalf("uncovered-but-committed snapshot should restore: %v", err)
	}
	next, _, err := e.StartScrub(backup.ScrubOptions{})
	if err != nil {
		t.Fatal(err)
	}
	next = waitScrubDone(t, e, next.RunID)
	vd2, ok := integrityOf(next, during.SnapshotID)
	if !ok || !vd2.Covered || vd2.Integrity != repo.IntegrityClean {
		t.Fatalf("during snapshot in later run: %+v", vd2)
	}
	// The old pending/failed rows stay excluded in every run while pending/failed.
	for _, st := range []struct {
		id int64
	}{{pending.SnapshotID}, {failed.SnapshotID}} {
		v, ok := integrityOf(next, st.id)
		if !ok || v.Integrity != repo.IntegrityExcluded {
			t.Fatalf("snapshot %d in new run = %+v", st.id, v)
		}
	}
}

// TestScrubDoesNotStartDuplicate ensures a second start while running returns
// the sentinel error instead of generating another report.
func TestScrubDoesNotStartDuplicate(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	must(t, os.WriteFile(filepath.Join(src, "a"), []byte("x\n"), 0o644))
	if _, err := e.CreateSnapshot(src, "m", true); err != nil {
		t.Fatal(err)
	}
	proceed := make(chan struct{})
	e.Fail.ScrubAfterChunk = func(runID int64, scanned int64) error {
		<-proceed
		return nil
	}
	first, _, err := e.StartScrub(backup.ScrubOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := e.StartScrub(backup.ScrubOptions{}); !errors.Is(err, backup.ErrScrubRunning) {
		t.Fatalf("want ErrScrubRunning, got %v", err)
	}
	close(proceed)
	waitScrubDone(t, e, first.RunID)
}

// TestScrubEmptyRepoRunsCleanly covers the no-snapshot / empty-snapshot edges.
func TestScrubEmptyRepoRunsCleanly(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	must(t, os.MkdirAll(filepath.Join(src, "emptydir"), 0o755))
	must(t, os.WriteFile(filepath.Join(src, "EMPTY"), nil, 0o600))
	r, err := e.CreateSnapshot(src, "empty files", true)
	if err != nil {
		t.Fatal(err)
	}
	prog, _, err := e.StartScrub(backup.ScrubOptions{})
	if err != nil {
		t.Fatal(err)
	}
	prog = waitScrubDone(t, e, prog.RunID)
	v, _ := integrityOf(prog, r.SnapshotID)
	if v.Integrity != repo.IntegrityClean {
		t.Fatalf("zero-chunk snapshot must be clean: %+v", v)
	}
}

// TestScrubAcrossProcessRestart reopens the manifest like a daemon restart:
// an interrupted run resumes from its cursor with no state lost.
func TestScrubAcrossProcessRestart(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	must(t, os.WriteFile(filepath.Join(src, "big.bin"),
		[]byte(strings.Repeat("0123456789ABCDEF\n", 12000)), 0o644))

	open := func() *backup.Engine {
		m, err := repo.OpenManifest(filepath.Join(dir, "manifest.sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { m.Close() })
		s, err := repo.NewContentStore(filepath.Join(dir, "chunks"))
		if err != nil {
			t.Fatal(err)
		}
		e, err := backup.NewEngine(m, s)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := open()
	r, err := e.CreateSnapshot(src, "v", true)
	if err != nil {
		t.Fatal(err)
	}

	stop := errors.New("simulated kill")
	e.Fail.ScrubAfterChunk = func(runID int64, scanned int64) error {
		if scanned >= 2 {
			return stop
		}
		return nil
	}
	prog, _, err := e.StartScrub(backup.ScrubOptions{})
	if err != nil {
		t.Fatal(err)
	}
	must(t, e.WaitUntilScrubIdle(timeout5s()))
	runID := prog.RunID
	// emulate process death: close everything (the OS process would vanish;
	// here we just release the DB connection).
	if err := e.Manifest.Close(); err != nil {
		t.Fatal(err)
	}

	e2 := open()
	resumed, wasResumed, err := e2.ResumeInterruptedScrub()
	if err != nil || !wasResumed || resumed.RunID != runID {
		t.Fatalf("resume: %+v resumed=%v err=%v", resumed, wasResumed, err)
	}
	finished := waitScrubDone(t, e2, runID)
	v, _ := integrityOf(finished, r.SnapshotID)
	if v.Integrity != repo.IntegrityClean {
		t.Fatalf("after restart resume: %+v", v)
	}
	var nUnscanned int
	if err := e2.Manifest.DB().QueryRow(`SELECT count(*) FROM scrub_work w
		LEFT JOIN scrub_chunks sc ON sc.run_id = w.run_id AND sc.chunk_digest = w.chunk_digest
		WHERE w.run_id = ? AND sc.chunk_digest IS NULL`, runID).Scan(&nUnscanned); err != nil && err != sql.ErrNoRows {
		t.Fatal(err)
	}
	if nUnscanned != 0 {
		t.Fatalf("%d chunks left unscanned after resume", nUnscanned)
	}
}
