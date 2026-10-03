package repo

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Patrol ("scrub") run statuses.
const (
	ScrubStatusRunning = "running"
	ScrubStatusDone    = "done"
)

// Per-snapshot integrity verdicts. These are deliberately separate from
// snapshots.status (pending/committed/failed), which records the historical
// commit fact and is never rewritten by a patrol.
const (
	IntegrityClean     = "clean"
	IntegritySuspect   = "suspect"
	IntegrityUnscanned = "unscanned"
	IntegrityExcluded  = "excluded"
	IntegrityUncovered = "uncovered"
)

// Per-chunk streaming check results.
const (
	ChunkResultOK             = "ok"
	ChunkResultMissingCatalog = "missing_catalog"
	ChunkResultMissingBlob    = "missing_blob"
	ChunkResultLengthMismatch = "length_mismatch"
	ChunkResultDigestMismatch = "digest_mismatch"
	ChunkResultReadError      = "read_error"
)

// ScrubRun is one patrol run row.
type ScrubRun struct {
	ID            int64
	HighWatermark int64
	Status        string
	CursorDigest  []byte
	TotalChunks   int64
	CleanChunks   int64
	BadChunks     int64
	StartedAt     time.Time
	FinishedAt    *time.Time
}

func scanScrubRun(row interface{ Scan(...any) error }) (ScrubRun, error) {
	var r ScrubRun
	var cursor []byte
	var started, finished sql.NullString
	if err := row.Scan(&r.ID, &r.HighWatermark, &r.Status, &cursor,
		&r.TotalChunks, &r.CleanChunks, &r.BadChunks, &started, &finished); err != nil {
		return r, err
	}
	r.CursorDigest = cursor
	r.StartedAt, _ = time.Parse(time.RFC3339Nano, started.String)
	if finished.Valid {
		t, err := time.Parse(time.RFC3339Nano, finished.String)
		if err == nil {
			r.FinishedAt = &t
		}
	}
	return r, nil
}

const scrubRunCols = `id, high_watermark, status, cursor_digest,
	total_chunks, clean_chunks, bad_chunks, started_at, finished_at`

// LatestScrubRun returns the most recently started patrol run.
func (m *Manifest) LatestScrubRun() (ScrubRun, error) {
	row := m.db.QueryRow(`SELECT ` + scrubRunCols + ` FROM scrub_runs ORDER BY id DESC LIMIT 1`)
	r, err := scanScrubRun(row)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

// LatestRunningScrub returns the newest run left in 'running' state (e.g. by a
// process kill) so it can be resumed from its stable cursor.
func (m *Manifest) LatestRunningScrub() (ScrubRun, error) {
	row := m.db.QueryRow(`SELECT `+scrubRunCols+
		` FROM scrub_runs WHERE status = ? ORDER BY id DESC LIMIT 1`, ScrubStatusRunning)
	r, err := scanScrubRun(row)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

// GetScrubRun fetches one run by id.
func (m *Manifest) GetScrubRun(id int64) (ScrubRun, error) {
	row := m.db.QueryRow(`SELECT `+scrubRunCols+` FROM scrub_runs WHERE id = ?`, id)
	r, err := scanScrubRun(row)
	if errors.Is(err, sql.ErrNoRows) {
		return r, ErrNotFound
	}
	return r, err
}

// BeginScrubRun starts a patrol in one atomic transaction: it records the
// high-watermark (max snapshot id), freezes the chunk work set referenced by
// the in-scope snapshots, and creates one verdict row per existing snapshot.
// Snapshots that are not committed at start are recorded as 'excluded' — the
// patrol neither blesses them nor touches their status/errors. Nothing
// committed later can enlarge this run: such snapshots are filled in as
// 'uncovered' at finalize time.
func (m *Manifest) BeginScrubRun() (ScrubRun, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return ScrubRun{}, err
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	var watermark sql.NullInt64
	if err := tx.QueryRow(`SELECT max(id) FROM snapshots`).Scan(&watermark); err != nil {
		return ScrubRun{}, err
	}
	var hw int64
	if watermark.Valid {
		hw = watermark.Int64
	}

	res, err := tx.Exec(`INSERT INTO scrub_runs (high_watermark, status, started_at)
		VALUES (?, ?, ?)`, hw, ScrubStatusRunning, now)
	if err != nil {
		return ScrubRun{}, err
	}
	runID, _ := res.LastInsertId()

	if hw > 0 {
		// One verdict row per snapshot existing at start. Only committed
		// snapshots can earn a clean/suspect verdict.
		if _, err := tx.Exec(`INSERT INTO scrub_snapshots
			(run_id, snapshot_id, covered, integrity, note, updated_at)
			SELECT ?, id, 1,
				CASE WHEN status = ? THEN ? ELSE ? END,
				CASE WHEN status = ? THEN 'not committed when the patrol started; commit status and errors stay authoritative'
				     WHEN status = ? THEN 'not committed when the patrol started; commit status and errors stay authoritative'
				     ELSE '' END,
				?
			FROM snapshots WHERE id <= ?`,
			runID, StatusCommitted, IntegrityUnscanned, IntegrityExcluded,
			StatusPending, StatusFailed, now, hw); err != nil {
			return ScrubRun{}, fmt.Errorf("seed scrub snapshots: %w", err)
		}

		// Freeze the work set: distinct chunks referenced by any in-scope
		// snapshot, with their declared catalog lengths at this instant.
		// LEFT JOIN keeps references whose chunks row is gone (commit-interrupt
		// failpoint) so the patrol reports them instead of silently skipping.
		if _, err := tx.Exec(`INSERT INTO scrub_work (run_id, chunk_digest, declared_length)
			SELECT DISTINCT ?, ec.chunk_digest, COALESCE(c.length, -1)
			FROM entry_chunks ec
			LEFT JOIN chunks c ON c.digest = ec.chunk_digest
			WHERE ec.snapshot_id <= ?`, runID, hw); err != nil {
			return ScrubRun{}, fmt.Errorf("freeze scrub work set: %w", err)
		}
	}

	if _, err := tx.Exec(`UPDATE scrub_runs SET total_chunks =
		(SELECT count(*) FROM scrub_work WHERE run_id = ?) WHERE id = ?`, runID, runID); err != nil {
		return ScrubRun{}, err
	}

	if err := tx.Commit(); err != nil {
		return ScrubRun{}, err
	}
	return m.GetScrubRun(runID)
}

// ScrubChunk is one frozen work item with the declared catalog length (-1 when
// the catalog row itself is missing).
type ScrubChunk struct {
	Digest         []byte
	DeclaredLength int64
}

// NextScrubChunks returns up to limit not-yet-scanned work items after the
// stable cursor, in digest order. Chunks already processed after an interrupt
// are skipped via the NOT EXISTS anti-join, so resume never re-reports.
func (m *Manifest) NextScrubChunks(runID int64, cursor []byte, limit int) ([]ScrubChunk, error) {
	if cursor == nil {
		cursor = []byte{}
	}
	rows, err := m.db.Query(`SELECT w.chunk_digest, w.declared_length
		FROM scrub_work w
		WHERE w.run_id = ? AND w.chunk_digest > ?
		  AND NOT EXISTS (
			SELECT 1 FROM scrub_chunks sc WHERE sc.run_id = w.run_id AND sc.chunk_digest = w.chunk_digest)
		ORDER BY w.chunk_digest LIMIT ?`, runID, cursor, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ScrubChunk
	for rows.Next() {
		var c ScrubChunk
		if err := rows.Scan(&c.Digest, &c.DeclaredLength); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ScrubChunkResult is the streaming verdict of one chunk.
type ScrubChunkResult struct {
	Digest         []byte
	DeclaredLength int64
	Result         string
	ActualLength   int64
	Message        string
}

// SaveScrubChunkResults persists one batch of chunk verdicts and advances the
// stable cursor atomically. INSERT OR IGNORE makes a re-driven batch after
// crash/restart a no-op for already recorded chunks.
func (m *Manifest) SaveScrubChunkResults(runID int64, results []ScrubChunkResult) error {
	if len(results) == 0 {
		return nil
	}
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	var maxDigest []byte
	for _, r := range results {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO scrub_chunks
			(run_id, chunk_digest, declared_length, result, actual_length, message, scanned_at)
			VALUES (?,?,?,?,?,?,?)`,
			runID, r.Digest, r.DeclaredLength, r.Result, r.ActualLength, r.Message, now); err != nil {
			return err
		}
		if bytesCmp(r.Digest, maxDigest) > 0 {
			maxDigest = append([]byte(nil), r.Digest...)
		}
	}
	if maxDigest != nil {
		if _, err := tx.Exec(`UPDATE scrub_runs SET cursor_digest = ?
			WHERE id = ? AND (cursor_digest IS NULL OR ? > cursor_digest)`,
			maxDigest, runID, maxDigest); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`UPDATE scrub_runs SET
		clean_chunks = (SELECT count(*) FROM scrub_chunks WHERE run_id = ? AND result = ?),
		bad_chunks   = (SELECT count(*) FROM scrub_chunks WHERE run_id = ? AND result != ?)
		WHERE id = ?`, runID, ChunkResultOK, runID, ChunkResultOK, runID); err != nil {
		return err
	}
	return tx.Commit()
}

// FinishScrubRun closes a patrol run. Every snapshot created after the
// high-watermark is recorded as 'uncovered' (not pretended checked); in-scope
// committed snapshots then receive their derived clean/suspect verdict. The
// operation is idempotent: a duplicated finalize after a crash is a no-op.
func (m *Manifest) FinishScrubRun(runID int64) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var status string
	if err := tx.QueryRow(`SELECT status FROM scrub_runs WHERE id = ?`, runID).Scan(&status); err != nil {
		return err
	}
	if status == ScrubStatusDone {
		return tx.Rollback() // already finalized
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	// Snapshots that did not exist when the run started: explicitly
	// uncovered, regardless of what their chunks look like. A newer snapshot
	// reuses chunks scanned earlier in the run, which the frozen cursor makes
	// indistinguishable, so only the watermark boundary is sound.
	if _, err := tx.Exec(`INSERT INTO scrub_snapshots
		(run_id, snapshot_id, covered, integrity, note, bad_chunk_count, updated_at)
		SELECT ?, s.id, 0, ?,
			'committed after this patrol started; not covered by run ' || ?,
			0, ?
		FROM snapshots s
		WHERE s.id > (SELECT high_watermark FROM scrub_runs WHERE id = ?)
		ON CONFLICT(run_id, snapshot_id) DO NOTHING`,
		runID, IntegrityUncovered, runID, now, runID); err != nil {
		return fmt.Errorf("mark uncovered: %w", err)
	}

	// Suspect: any referenced chunk with a non-ok verdict.
	if _, err := tx.Exec(`UPDATE scrub_snapshots SET integrity = ?, updated_at = ?,
		bad_chunk_count = (
			SELECT count(DISTINCT sc.chunk_digest)
			FROM entry_chunks ec
			JOIN scrub_chunks sc ON sc.run_id = ? AND sc.chunk_digest = ec.chunk_digest
			WHERE ec.snapshot_id = scrub_snapshots.snapshot_id AND sc.result != ?)
		WHERE run_id = ? AND covered = 1 AND integrity = ?
		  AND EXISTS (
			SELECT 1 FROM entry_chunks ec
			JOIN scrub_chunks sc ON sc.run_id = ? AND sc.chunk_digest = ec.chunk_digest
			WHERE ec.snapshot_id = scrub_snapshots.snapshot_id AND sc.result != ?)`,
		IntegritySuspect, now,
		runID, ChunkResultOK, runID, IntegrityUnscanned,
		runID, ChunkResultOK); err != nil {
		return fmt.Errorf("mark suspect: %w", err)
	}

	// Clean: a committed, covered snapshot whose every referenced chunk is
	// in the frozen work set and has an ok verdict. Zero-chunk (empty)
	// snapshots qualify: the NOT EXISTS finds no violating reference.
	if _, err := tx.Exec(`UPDATE scrub_snapshots SET integrity = ?, updated_at = ?,
		bad_chunk_count = 0
		WHERE run_id = ? AND covered = 1 AND integrity = ?
		  AND NOT EXISTS (
			SELECT 1 FROM entry_chunks ec
			JOIN scrub_work w ON w.run_id = ? AND w.chunk_digest = ec.chunk_digest
			WHERE ec.snapshot_id = scrub_snapshots.snapshot_id
			  AND NOT EXISTS (
				SELECT 1 FROM scrub_chunks sc
				WHERE sc.run_id = w.run_id AND sc.chunk_digest = w.chunk_digest
				  AND sc.result = ?))`,
		IntegrityClean, now,
		runID, IntegrityUnscanned, runID, ChunkResultOK); err != nil {
		return fmt.Errorf("mark clean: %w", err)
	}

	// Anything still unscanned at this point means the work set and chunk
	// verdicts disagree; fail loudly rather than label it.
	var left int64
	if err := tx.QueryRow(`SELECT count(*) FROM scrub_runs r
		JOIN scrub_work w ON w.run_id = r.id
		LEFT JOIN scrub_chunks sc ON sc.run_id = w.run_id AND sc.chunk_digest = w.chunk_digest
		WHERE r.id = ? AND sc.chunk_digest IS NULL`, runID).Scan(&left); err != nil {
		return err
	}
	if left != 0 {
		return fmt.Errorf("cannot finish scrub %d: %d chunks without verdicts", runID, left)
	}

	if _, err := tx.Exec(`UPDATE scrub_runs SET status = ?, finished_at = ? WHERE id = ?`,
		ScrubStatusDone, now, runID); err != nil {
		return err
	}
	return tx.Commit()
}

// ScrubSnapshotState is one snapshot's verdict row plus live counters.
type ScrubSnapshotState struct {
	SnapshotID      int64
	Covered         bool
	Integrity       string
	Note            string
	BadChunkCount   int64
	ReferencedTotal int64
	ScannedChunks   int64
	BadChunks       int64
}

// ScrubSnapshotStates returns per-snapshot rows for a run. While a run is in
// progress, counters are derived live from scrub_chunks so a covered snapshot
// reports unscanned until the run finalizes — a partially scanned snapshot
// must never look clean.
func (m *Manifest) ScrubSnapshotStates(runID int64) ([]ScrubSnapshotState, error) {
	rows, err := m.db.Query(`
		SELECT ss.snapshot_id, ss.covered, ss.integrity, ss.note, ss.bad_chunk_count,
			(SELECT count(DISTINCT ec.chunk_digest) FROM entry_chunks ec
			 WHERE ec.snapshot_id = ss.snapshot_id),
			(SELECT count(DISTINCT ec.chunk_digest) FROM entry_chunks ec
			 JOIN scrub_chunks sc ON sc.run_id = ss.run_id AND sc.chunk_digest = ec.chunk_digest
			 WHERE ec.snapshot_id = ss.snapshot_id),
			(SELECT count(DISTINCT ec.chunk_digest) FROM entry_chunks ec
			 JOIN scrub_chunks sc ON sc.run_id = ss.run_id AND sc.chunk_digest = ec.chunk_digest
			 WHERE ec.snapshot_id = ss.snapshot_id AND sc.result != ?)
		FROM scrub_snapshots ss
		WHERE ss.run_id = ?
		ORDER BY ss.snapshot_id`, ChunkResultOK, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ScrubSnapshotState
	for rows.Next() {
		var st ScrubSnapshotState
		var covered int64
		if err := rows.Scan(&st.SnapshotID, &covered, &st.Integrity, &st.Note, &st.BadChunkCount,
			&st.ReferencedTotal, &st.ScannedChunks, &st.BadChunks); err != nil {
			return nil, err
		}
		st.Covered = covered != 0
		out = append(out, st)
	}
	return out, rows.Err()
}

// GetScrubSnapshot returns one snapshot's verdict row.
func (m *Manifest) GetScrubSnapshot(runID, snapshotID int64) (ScrubSnapshotState, error) {
	states, err := m.ScrubSnapshotStates(runID)
	if err != nil {
		return ScrubSnapshotState{}, err
	}
	for _, st := range states {
		if st.SnapshotID == snapshotID {
			return st, nil
		}
	}
	return ScrubSnapshotState{}, ErrNotFound
}

// AffectedRef is one (snapshot, file path) reference to a bad chunk.
type AffectedRef struct {
	SnapshotID int64
	RelPath    string
	Result     string
	Message    string
}

// ScrubAffected returns all snapshot/file references to the given chunk in the
// run, regardless of verdict. It is the manifest reverse-lookup that turns one
// corrupted shared blob into the complete list of affected snapshots.
func (m *Manifest) ScrubAffected(runID int64, digest []byte) (result string, refs []AffectedRef, err error) {
	row := m.db.QueryRow(`SELECT result FROM scrub_chunks WHERE run_id = ? AND chunk_digest = ?`,
		runID, digest)
	if err := row.Scan(&result); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, ErrNotFound
		}
		return "", nil, err
	}
	rows, err := m.db.Query(`SELECT ec.snapshot_id, ec.rel_path
		FROM entry_chunks ec
		WHERE ec.chunk_digest = ?
		ORDER BY ec.snapshot_id, ec.rel_path, ec.seq`, digest)
	if err != nil {
		return "", nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ref AffectedRef
		if err := rows.Scan(&ref.SnapshotID, &ref.RelPath); err != nil {
			return "", nil, err
		}
		ref.Result = result
		refs = append(refs, ref)
	}
	return result, refs, rows.Err()
}

// ScrubBadChunk is a bad chunk verdict together with one snapshot's
// references to it: a chunk can appear in more than one file of a snapshot,
// so RelPaths is a list, never silently collapsed to an empty string.
type ScrubBadChunk struct {
	Digest   []byte
	Length   int64
	Result   string
	Message  string
	RelPaths []string
}

// ScrubBadChunksForSnapshot lists every non-ok chunk verdict affecting one
// snapshot in the run, with all referencing file paths. Used by the restore
// pre-gate so a known-suspect snapshot is refused with a locatable error
// before any byte is streamed.
func (m *Manifest) ScrubBadChunksForSnapshot(runID, snapshotID int64) ([]ScrubBadChunk, error) {
	rows, err := m.db.Query(`SELECT sc.chunk_digest, sc.declared_length, sc.result, sc.message,
		ec.rel_path
		FROM scrub_chunks sc
		JOIN entry_chunks ec ON ec.chunk_digest = sc.chunk_digest
		WHERE sc.run_id = ? AND sc.result != ? AND ec.snapshot_id = ?
		ORDER BY sc.chunk_digest, ec.rel_path`, runID, ChunkResultOK, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ScrubBadChunk
	index := map[string]int{}
	for rows.Next() {
		var c ScrubBadChunk
		var rel string
		if err := rows.Scan(&c.Digest, &c.Length, &c.Result, &c.Message, &rel); err != nil {
			return nil, err
		}
		key := string(c.Digest)
		if i, ok := index[key]; ok {
			out[i].RelPaths = appendUnique(out[i].RelPaths, rel)
			continue
		}
		c.RelPaths = []string{rel}
		index[key] = len(out)
		out = append(out, c)
	}
	return out, rows.Err()
}

func appendUnique(xs []string, s string) []string {
	for _, x := range xs {
		if x == s {
			return xs
		}
	}
	return append(xs, s)
}

// bytesCmp compares two byte slices lexicographically (nil/empty = smallest).
func bytesCmp(a, b []byte) int {
	la, lb := len(a), len(b)
	n := la
	if lb < n {
		n = lb
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	if la == lb {
		return 0
	}
	if la < lb {
		return -1
	}
	return 1
}
