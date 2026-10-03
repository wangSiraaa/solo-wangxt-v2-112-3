package backup_test

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"incbackup/internal/backup"
	"incbackup/internal/repo"

	_ "modernc.org/sqlite"
)

func sha256Sum(b []byte) []byte { s := sha256.Sum256(b); return s[:] }

// sharedChunks opens a read-only second connection to the manifest and finds
// digests referenced by both snapshot ids.
func sharedChunks(t *testing.T, dir string, a, b int64) [][]byte {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "manifest.sqlite")+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT chunk_digest FROM entry_chunks
		WHERE snapshot_id IN (?, ?) GROUP BY chunk_digest
		HAVING count(DISTINCT snapshot_id) = 2`, a, b)
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
	return out
}

// corruptBlob overwrites a content-addressed blob in place (chunks are 0444).
func corruptBlob(t *testing.T, e *backup.Engine, digest []byte, payload string) {
	t.Helper()
	p, err := e.Store.Path(digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(payload), 0o644); err != nil {
		t.Fatal(err)
	}
}

func makeTwoSnapshotsSharingBlob(t *testing.T, e *backup.Engine, src string) (int64, int64) {
	t.Helper()
	must(t, os.MkdirAll(filepath.Join(src, "d"), 0o755))
	// A >128KiB file guarantees several chunks; both snapshots share all of
	// them when the tree is unchanged between snapshots.
	content := strings.Repeat("shared-block-content-line-0123456789\n", 4000)
	writeTree(t, src, map[string]string{"d/shared.log": content}, nil)
	r1, err := e.CreateSnapshot(src, "s1", true)
	if err != nil {
		t.Fatal(err)
	}
	writeTree(t, src, map[string]string{"uniq1.txt": "only in snapshot one\n"}, nil)
	r2, err := e.CreateSnapshot(src, "s2", true)
	if err != nil {
		t.Fatal(err)
	}
	return r1.SnapshotID, r2.SnapshotID
}

// runScrubAndWait starts a scrub (fresh or resumed) and waits for the worker
// to finish.
func runScrubAndWait(t *testing.T, e *backup.Engine, force bool, hooks backup.ScrubHooks) {
	t.Helper()
	if _, err := e.StartScrub(force, hooks); err != nil {
		t.Fatalf("start scrub: %v", err)
	}
	if err := e.ScrubWaitFor(30 * time.Second); err != nil {
		t.Fatal(err)
	}
}

func latestProg(t *testing.T, e *backup.Engine) backup.ScrubProgress {
	t.Helper()
	prog, err := e.LatestScrubProgress()
	if err != nil {
		t.Fatal(err)
	}
	return prog
}

// Acceptance ①: corrupt a chunk shared by two snapshots; scrub marks BOTH
// suspect, names all affected snapshots/paths, and restore of either is
// rejected up front with a locatable error.
func TestScrubSharedBlobCorruptsBothSnapshotsAndBlocksRestore(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	id1, id2 := makeTwoSnapshotsSharingBlob(t, e, src)

	shared := sharedChunks(t, dir, id1, id2)
	if len(shared) == 0 {
		t.Fatal("test setup: expected shared chunks between snapshots")
	}
	corruptBlob(t, e, shared[0], "THIS-BLOB-WAS-TAMPERED-WITH-!!")

	runScrubAndWait(t, e, true, backup.ScrubHooks{})

	prog := latestProg(t, e)
	if prog.Scrub.Status != repo.ScrubDone {
		t.Fatalf("scrub status=%s", prog.Scrub.Status)
	}
	if prog.Scrub.BadChunks < 1 {
		t.Fatalf("bad_chunks=%d", prog.Scrub.BadChunks)
	}
	bySnap := map[int64]repo.SnapshotVerdict{}
	for _, v := range prog.Snapshots {
		bySnap[v.SnapshotID] = v
	}
	for _, id := range []int64{id1, id2} {
		v := bySnap[id]
		if v.Status != repo.ScrubSnapSuspect || v.BadChunks < 1 {
			t.Fatalf("snapshot %d verdict=%+v, want suspect", id, v)
		}
	}
	// committed history is untouched
	for _, id := range []int64{id1, id2} {
		si, err := e.Manifest.GetSnapshot(id)
		if err != nil || si.Status != repo.StatusCommitted {
			t.Fatalf("snapshot %d status=%q (history must remain committed)", id, si.Status)
		}
	}

	// Reverse lookup: one bad digest must enumerate both snapshots and paths.
	bad, err := e.Manifest.ScrubBadChunks(prog.Scrub.ID)
	if err != nil {
		t.Fatal(err)
	}
	var found *repo.BadChunk
	for i := range bad {
		if hex.EncodeToString(bad[i].Digest) == hex.EncodeToString(shared[0]) {
			found = &bad[i]
		}
	}
	if found == nil {
		t.Fatal("corrupted shared chunk missing from bad list")
	}
	owners := map[int64][]string{}
	for _, ref := range found.Refs {
		owners[ref.SnapshotID] = append(owners[ref.SnapshotID], ref.RelPath)
	}
	if _, ok := owners[id1]; !ok {
		t.Fatalf("reverse lookup missing snapshot %d: %+v", id1, found.Refs)
	}
	if _, ok := owners[id2]; !ok {
		t.Fatalf("reverse lookup missing snapshot %d: %+v", id2, found.Refs)
	}

	// Restore of either is refused early with a locatable SuspectError.
	for _, id := range []int64{id1, id2} {
		_, err := e.Restore(id, filepath.Join(dir, fmt.Sprintf("out-%d", id)))
		var se *backup.SuspectError
		if !errors.As(err, &se) {
			t.Fatalf("snapshot %d restore err=%v, want SuspectError", id, err)
		}
		if len(se.Details) == 0 || se.Details[0].BlobPath == "" {
			t.Fatalf("snapshot %d suspect error not locatable: %+v", id, se.Details)
		}
		// Detail must reveal the shared sibling too.
		sibling := map[int64]bool{}
		for _, af := range se.Details[0].Affected {
			sibling[af.SnapshotID] = true
		}
		if !sibling[id1] || !sibling[id2] {
			t.Fatalf("suspect error affected refs incomplete: %+v", se.Details[0].Affected)
		}
	}
}

// Acceptance ②: corrupting a chunk unique to one snapshot only marks that
// snapshot suspect; a sibling snapshot with different unique content stays
// clean and restores fine.
func TestScrubUniqueChunkOnlyAffectsOwningSnapshot(t *testing.T) {
	e, dir := openEngine(t)
	srcA := filepath.Join(dir, "srcA")
	srcB := filepath.Join(dir, "srcB")
	must(t, os.MkdirAll(srcA, 0o755))
	must(t, os.MkdirAll(srcB, 0o755))
	writeTree(t, srcA, map[string]string{"a.log": strings.Repeat("AAAA-aaa-line\n", 3000)}, nil)
	writeTree(t, srcB, map[string]string{"b.log": strings.Repeat("BBBB-bbb-line\n", 3000)}, nil)
	ra, err := e.CreateSnapshot(srcA, "a", true)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := e.CreateSnapshot(srcB, "b", true)
	if err != nil {
		t.Fatal(err)
	}

	// Find a chunk referenced only by snapshot a (snapshots a and b contain
	// disjoint content, so all of a's chunks are singleton-owned by a).
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "manifest.sqlite")+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	var unique []byte
	if err := db.QueryRow(`SELECT chunk_digest FROM entry_chunks
		WHERE snapshot_id = ?
		AND chunk_digest NOT IN (
			SELECT chunk_digest FROM entry_chunks WHERE snapshot_id <> ?)
		LIMIT 1`, ra.SnapshotID, ra.SnapshotID).Scan(&unique); err != nil {
		t.Fatal(err)
	}
	db.Close()
	corruptBlob(t, e, unique, "ROT-ONLY-OWNED-BY-A!!!")

	runScrubAndWait(t, e, true, backup.ScrubHooks{})
	prog := latestProg(t, e)
	bySnap := map[int64]repo.SnapshotVerdict{}
	for _, v := range prog.Snapshots {
		bySnap[v.SnapshotID] = v
	}
	if bySnap[ra.SnapshotID].Status != repo.ScrubSnapSuspect {
		t.Fatalf("owner verdict=%v want suspect", bySnap[ra.SnapshotID])
	}
	if bySnap[rb.SnapshotID].Status != repo.ScrubSnapClean {
		t.Fatalf("unaffected snapshot verdict=%v want clean", bySnap[rb.SnapshotID])
	}

	// Owner restore rejected...
	if _, err := e.Restore(ra.SnapshotID, filepath.Join(dir, "outA")); !errors.As(err, new(*backup.SuspectError)) {
		t.Fatalf("owner restore err=%v want SuspectError", err)
	}
	// ...sibling still restores successfully.
	rr, err := e.Restore(rb.SnapshotID, filepath.Join(dir, "outB"))
	if err != nil {
		t.Fatalf("sibling restore should succeed: %v", err)
	}
	if rr.Files != 1 {
		t.Fatalf("restored files=%d", rr.Files)
	}
}

// groundTruthVerdict independently hashes every catalog blob on disk; it is
// the reference the resumed scrub must reproduce without trusting the
// engine's own probe code.
type groundVerdict struct {
	result string // ok | bad | missing
	length int64
}

func groundTruthVerdicts(t *testing.T, dir string) map[string]groundVerdict {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, "manifest.sqlite")+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT digest, length FROM chunks`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	want := map[string]groundVerdict{}
	for rows.Next() {
		var dg []byte
		var declared int64
		if err := rows.Scan(&dg, &declared); err != nil {
			t.Fatal(err)
		}
		key := hex.EncodeToString(dg)
		p := filepath.Join(dir, "chunks", key[:2], key[2:])
		data, rerr := os.ReadFile(p)
		if rerr != nil {
			want[key] = groundVerdict{"missing", -1}
			continue
		}
		sum := sha256Sum(data)
		if int64(len(data)) != declared || hex.EncodeToString(sum) != key {
			want[key] = groundVerdict{"bad", int64(len(data))}
			continue
		}
		want[key] = groundVerdict{"ok", int64(len(data))}
	}
	return want
}

// Acceptance ③: a scrub interrupted mid-way (simulating a killed process)
// and then resumed against the same on-disk repository yields exactly the
// same per-chunk and per-snapshot verdicts as a complete uninterrupted run:
// no chunk is reported twice, no un-scanned chunk is labelled clean, and the
// resumed run keeps the original scrub id.
func TestScrubInterruptAndResumeEqualsFullRun(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	id1, id2 := makeTwoSnapshotsSharingBlob(t, e, src)
	writeTree(t, src, map[string]string{"uniq2.txt": strings.Repeat("z\n", 20000)}, nil)
	r3, err := e.CreateSnapshot(src, "s3", true)
	if err != nil {
		t.Fatal(err)
	}
	shared := sharedChunks(t, dir, id1, id2)
	if len(shared) == 0 {
		t.Fatal("no shared chunks")
	}
	corruptBlob(t, e, shared[0], "TAMPERED-RESUME-CHECK!")
	truth := groundTruthVerdicts(t, dir)
	if truth[hex.EncodeToString(shared[0])].result != "bad" {
		t.Fatal("ground truth must see the tampered blob as bad")
	}

	// Interrupt after the second streamed chunk lands — deliberately inside
	// the first batch so the cursor itself, not just a batch boundary, is
	// exercised.
	var seen int64
	stopped := make(chan struct{})
	sc, err := e.StartScrub(true, backup.ScrubHooks{AfterChunk: func(int64, []byte, repo.ScrubChunkResult) {
		if atomic.AddInt64(&seen, 1) == 2 {
			e.CancelScrub()
			close(stopped)
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-stopped
	if err := e.ScrubWaitFor(30 * time.Second); err != nil {
		t.Fatal(err)
	}
	mid, err := e.Manifest.GetScrub(sc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if mid.Status != repo.ScrubPaused {
		t.Fatalf("interrupted scrub status=%s want paused", mid.Status)
	}
	if mid.ScannedChunks != 2 {
		t.Fatalf("scanned after interrupt=%d want 2", mid.ScannedChunks)
	}
	if mid.ScannedChunks >= mid.TotalChunks {
		t.Fatalf("interrupt did not land mid-scan: %d/%d", mid.ScannedChunks, mid.TotalChunks)
	}
	// Only the two already-streamed chunks may carry a verdict; everything
	// else must still be unscanned — never pre-labelled clean.
	part, err := e.Manifest.ScrubChunkResults(sc.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(part) != 2 {
		t.Fatalf("partial verdicts=%d want 2 (no duplicate/early reports)", len(part))
	}
	midSnaps, err := e.Manifest.ScrubSnapshots(sc.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range midSnaps {
		if v.Status == repo.ScrubSnapClean {
			t.Fatalf("snapshot %d marked clean while scrub incomplete", v.SnapshotID)
		}
	}

	// Simulate a process restart: drop all in-memory state and reopen.
	e.Manifest.Close()
	m2, err := repo.OpenManifest(filepath.Join(dir, "manifest.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m2.Close() })
	s2, err := repo.NewContentStore(filepath.Join(dir, "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	e2, err := backup.NewEngine(m2, s2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e2.StartScrub(false, backup.ScrubHooks{}); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if err := e2.ScrubWaitFor(30 * time.Second); err != nil {
		t.Fatal(err)
	}
	gotProg := latestProg(t, e2)
	if gotProg.Scrub.ID != sc.ID {
		t.Fatalf("resume created a new scrub: %d vs %d", gotProg.Scrub.ID, sc.ID)
	}
	if gotProg.Scrub.Status != repo.ScrubDone {
		t.Fatalf("resumed scrub status=%s", gotProg.Scrub.Status)
	}
	if gotProg.Scrub.ScannedChunks != gotProg.Scrub.TotalChunks {
		t.Fatalf("scanned=%d total=%d", gotProg.Scrub.ScannedChunks, gotProg.Scrub.TotalChunks)
	}
	if gotProg.Scrub.BadChunks != 1 {
		t.Fatalf("bad_chunks=%d want exactly 1", gotProg.Scrub.BadChunks)
	}

	gotResults, err := m2.ScrubChunkResults(sc.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Report table must have exactly one row per queued chunk — resume
	// re-probing the streamed chunks upserts, never duplicates.
	if int64(len(gotResults)) != gotProg.Scrub.TotalChunks {
		t.Fatalf("report rows=%d total chunks=%d (duplicate or missing report)",
			len(gotResults), gotProg.Scrub.TotalChunks)
	}
	for key, tv := range truth {
		gr, ok := gotResults[key]
		if !ok {
			t.Fatalf("resumed run missing chunk %s", key)
		}
		if gr.Result != tv.result {
			t.Fatalf("chunk %s verdict=%s, ground truth=%s", key, gr.Result, tv.result)
		}
	}

	// Every snapshot referencing the tampered shared chunk (s1, s2 and s3,
	// which all include d/shared.log) is suspect; the verdict is identical
	// to what a single uninterrupted run would produce.
	bySnap := map[int64]repo.SnapshotVerdict{}
	for _, v := range gotProg.Snapshots {
		bySnap[v.SnapshotID] = v
	}
	for _, id := range []int64{id1, id2, r3.SnapshotID} {
		if bySnap[id].Status != repo.ScrubSnapSuspect {
			t.Fatalf("snapshot %d verdict=%v want suspect", id, bySnap[id].Status)
		}
	}
}

// Acceptance ④: snapshots created while a scrub runs (and pre-existing
// pending/failed snapshots) are never labelled clean and do not lose their
// original failure records.
func TestScrubDoesNotFalselyMarkNewPendingOrFailed(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	writeTree(t, src, map[string]string{"base.log": strings.Repeat("b\n", 40000)}, nil)
	rb, err := e.CreateSnapshot(src, "base", true)
	if err != nil {
		t.Fatal(err)
	}

	// A pre-existing failed snapshot (lose a singleton chunk).
	writeTree(t, src, map[string]string{"willfail.log": strings.Repeat("F\n", 40000)}, nil)
	e.Fail.LoseChunkCount = 1
	rf, err := e.CreateSnapshot(src, "failed-before-scrub", true)
	e.Fail.LoseChunkCount = 0
	if !errors.As(err, new(*backup.ErrRejected)) {
		t.Fatalf("expected rejection, got %v", err)
	}
	failedID := rf.SnapshotID
	errsBefore, err := e.Manifest.ListErrors(failedID)
	if err != nil {
		t.Fatal(err)
	}
	if len(errsBefore) == 0 {
		t.Fatal("failed snapshot must have recorded errors")
	}

	// A pre-existing pending snapshot (finish=false, blobs present).
	rp, err := e.CreateSnapshot(src, "pending-before-scrub", false)
	if err != nil {
		t.Fatal(err)
	}
	pendingID := rp.SnapshotID

	// Start a slow scrub; create a new committed snapshot while it runs.
	var sawNew atomic.Bool
	sc, err := e.StartScrub(true, backup.ScrubHooks{AfterChunk: func(scrubID int64, _ []byte, _ repo.ScrubChunkResult) {
		if !sawNew.Swap(true) {
			writeTree(t, src, map[string]string{"during.log": strings.Repeat("D\n", 40000)}, nil)
			if _, cerr := e.CreateSnapshot(src, "committed-during-scrub", true); cerr != nil {
				t.Errorf("create during scrub: %v", cerr)
			}
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.ScrubWaitFor(30 * time.Second); err != nil {
		t.Fatal(err)
	}

	prog := latestProg(t, e)
	if prog.Scrub.Status != repo.ScrubDone {
		t.Fatalf("scrub status=%s", prog.Scrub.Status)
	}
	if prog.Scrub.HighSnapshotID < pendingID {
		t.Fatalf("watermark %d below pre-existing snapshot %d", prog.Scrub.HighSnapshotID, pendingID)
	}
	bySnap := map[int64]repo.SnapshotVerdict{}
	for _, v := range prog.Snapshots {
		bySnap[v.SnapshotID] = v
	}
	// Base snapshot in scope -> clean.
	if bySnap[rb.SnapshotID].Status != repo.ScrubSnapClean {
		t.Fatalf("base verdict=%v want clean", bySnap[rb.SnapshotID])
	}
	// Pre-existing failed/pending: excluded, never clean; history preserved.
	if bySnap[failedID].Status != repo.ScrubSnapExcluded {
		t.Fatalf("failed snapshot verdict=%v want excluded", bySnap[failedID])
	}
	if bySnap[pendingID].Status != repo.ScrubSnapExcluded {
		t.Fatalf("pending snapshot verdict=%v want excluded", bySnap[pendingID])
	}
	errsAfter, err := e.Manifest.ListErrors(failedID)
	if err != nil {
		t.Fatal(err)
	}
	if len(errsAfter) != len(errsBefore) {
		t.Fatalf("failure records changed: %d -> %d", len(errsBefore), len(errsAfter))
	}
	fsi, _ := e.Manifest.GetSnapshot(failedID)
	if fsi.Status != repo.StatusFailed {
		t.Fatalf("failed snapshot status=%q history rewritten", fsi.Status)
	}
	psi, _ := e.Manifest.GetSnapshot(pendingID)
	if psi.Status != repo.StatusPending {
		t.Fatalf("pending snapshot status=%q history rewritten", psi.Status)
	}

	// Snapshot created after the watermark: explicitly uncovered.
	later, err := e.Manifest.ListSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	var duringID int64
	for _, s := range later {
		if s.Message == "committed-during-scrub" {
			duringID = s.ID
		}
	}
	if duringID == 0 {
		t.Fatal("during-scrub snapshot not found")
	}
	if duringID <= sc.HighSnapshotID {
		t.Fatalf("during snapshot %d <= watermark %d", duringID, sc.HighSnapshotID)
	}
	if v := bySnap[duringID]; v.Status != repo.ScrubSnapUncovered {
		t.Fatalf("during-scrub snapshot verdict=%v want uncovered", v)
	}

	// Latest-integrity view agrees and labels the during snapshot uncovered.
	views, err := e.Manifest.IntegrityViews()
	if err != nil {
		t.Fatal(err)
	}
	if views[duringID].Status != repo.ScrubSnapUncovered {
		t.Fatalf("integrity view during=%v", views[duringID])
	}
}

// Edge cases: a scrub over an empty repository completes with zero chunks;
// after a scrub is done, starting again creates a fresh scrub with a new
// watermark rather than re-running or mutating the finished one.
func TestScrubEmptyRepoAndNewAfterDone(t *testing.T) {
	e, dir := openEngine(t)
	runScrubAndWait(t, e, true, backup.ScrubHooks{})
	p1 := latestProg(t, e)
	if p1.Scrub.Status != repo.ScrubDone || p1.Scrub.TotalChunks != 0 {
		t.Fatalf("empty scrub: %+v", p1.Scrub)
	}

	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	writeTree(t, src, map[string]string{"f": "hello-scrub\n"}, nil)
	r, err := e.CreateSnapshot(src, "s", true)
	if err != nil {
		t.Fatal(err)
	}
	// force=false: previous scrub is done, so this starts fresh.
	runScrubAndWait(t, e, false, backup.ScrubHooks{})
	p2 := latestProg(t, e)
	if p2.Scrub.ID == p1.Scrub.ID {
		t.Fatal("expected a new scrub id after previous one done")
	}
	if p2.Scrub.HighSnapshotID != r.SnapshotID {
		t.Fatalf("watermark=%d want %d", p2.Scrub.HighSnapshotID, r.SnapshotID)
	}
	if p2.Snapshots[0].Status != repo.ScrubSnapClean {
		t.Fatalf("verdict=%v", p2.Snapshots)
	}
	all, err := e.Manifest.ListScrubs()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("scrubs retained=%d want 2", len(all))
	}
	// force=true while everything is done also starts a new run.
	runScrubAndWait(t, e, true, backup.ScrubHooks{})
	p3 := latestProg(t, e)
	if p3.Scrub.ID <= p2.Scrub.ID {
		t.Fatal("force should start a new scrub")
	}
}

// Starting a second scrub while one runs is rejected, not duplicated.
func TestScrubDoubleStartRejected(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	writeTree(t, src, map[string]string{"a": strings.Repeat("a\n", 50000)}, nil)
	if _, err := e.CreateSnapshot(src, "s", true); err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	started := make(chan struct{})
	if _, err := e.StartScrub(true, backup.ScrubHooks{AfterChunk: func(int64, []byte, repo.ScrubChunkResult) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
	}}); err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := e.StartScrub(true, backup.ScrubHooks{}); !errors.Is(err, backup.ErrScrubAlreadyRunning) {
		t.Fatalf("double start err=%v want ErrScrubAlreadyRunning", err)
	}
	close(release)
	if err := e.ScrubWaitFor(30 * time.Second); err != nil {
		t.Fatal(err)
	}
}
