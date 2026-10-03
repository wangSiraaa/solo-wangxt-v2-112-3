package repo

import (
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Scrub status values (independent of snapshots.status).
const (
	ScrubRunning = "running"
	ScrubPaused  = "paused"
	ScrubDone    = "done"
)

// Per-snapshot scrub verdict values. These deliberately do not touch
// snapshots.status: "committed" still means the commit-time verification
// succeeded; clean/suspect/unscanned is the later integrity story.
const (
	ScrubSnapUnscanned = "unscanned"
	ScrubSnapClean     = "clean"
	ScrubSnapSuspect   = "suspect"
	ScrubSnapExcluded  = "excluded" // pending/failed at the watermark
	ScrubSnapUncovered = "uncovered"
	ScrubSnapNever     = "never_scrubbed"
)

// ScrubInfo is one integrity-scrub job.
type ScrubInfo struct {
	ID             int64
	Status         string
	HighSnapshotID int64
	TotalChunks    int64
	ScannedChunks  int64
	BadChunks      int64
	BytesScanned   int64
	StartedAt      time.Time
	PausedAt       *time.Time
	FinishedAt     *time.Time
	LastError      string
}

func scanScrub(row interface{ Scan(...any) error }) (ScrubInfo, error) {
	var sc ScrubInfo
	var started, paused, finished sql.NullString
	if err := row.Scan(&sc.ID, &sc.Status, &sc.HighSnapshotID,
		&sc.TotalChunks, &sc.ScannedChunks, &sc.BadChunks, &sc.BytesScanned,
		&started, &paused, &finished, &sc.LastError); err != nil {
		return sc, err
	}
	sc.StartedAt, _ = time.Parse(time.RFC3339Nano, started.String)
	if paused.Valid {
		t, _ := time.Parse(time.RFC3339Nano, paused.String)
		sc.PausedAt = &t
	}
	if finished.Valid {
		t, _ := time.Parse(time.RFC3339Nano, finished.String)
		sc.FinishedAt = &t
	}
	return sc, nil
}

const scrubCols = `id, status, high_snapshot_id, total_chunks, scanned_chunks,
	bad_chunks, bytes_scanned, started_at, paused_at, finished_at, last_error`

// MaxSnapshotID returns the highest snapshot id at scrub start (the
// watermark), or 0 on an empty repository.
func (m *Manifest) MaxSnapshotID() (int64, error) {
	var max sql.NullInt64
	if err := m.db.QueryRow(`SELECT max(id) FROM snapshots`).Scan(&max); err != nil {
		return 0, err
	}
	return max.Int64, nil
}

// CreateScrub freezes a new scrub job: it records the watermark, snapshots
// every existing snapshot row with its initial verdict, and enqueues every
// distinct chunk referenced by an in-scope (committed-at-start) snapshot.
// The whole freeze is one transaction, so a snapshot committing concurrently
// either fully predates the freeze (id <= watermark and committed) or is
// treated as later and stays uncovered.
func (m *Manifest) CreateScrub() (ScrubInfo, error) {
	tx, err := m.db.Begin()
	if err != nil {
		return ScrubInfo{}, fmt.Errorf("create scrub: %w", err)
	}
	defer tx.Rollback()

	var high sql.NullInt64
	if err := tx.QueryRow(`SELECT max(id) FROM snapshots`).Scan(&high); err != nil {
		return ScrubInfo{}, err
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := tx.Exec(`INSERT INTO scrubs (status, high_snapshot_id, started_at)
		VALUES (?, ?, ?)`, ScrubRunning, high.Int64, now)
	if err != nil {
		return ScrubInfo{}, err
	}
	id, _ := res.LastInsertId()

	// Freeze per-snapshot rows. committed-at-start snapshots begin unscanned;
	// pending/failed ones are excluded from clean/suspect verdicts but keep
	// their own history untouched.
	if _, err := tx.Exec(`INSERT INTO scrub_snapshots (scrub_id, snapshot_id, status)
		SELECT ?, id, CASE WHEN status = ? THEN ? ELSE ? END
		FROM snapshots WHERE id <= ?`,
		id, StatusCommitted, ScrubSnapUnscanned, ScrubSnapExcluded, high.Int64); err != nil {
		return ScrubInfo{}, err
	}

	// Freeze the chunk queue from references of in-scope committed snapshots.
	// No JOIN on chunks is intentional: a committed snapshot cannot normally
	// reference a missing catalog row, but if one exists the worker records
	// it as "missing" instead of silently skipping it.
	if _, err := tx.Exec(`INSERT OR IGNORE INTO scrub_queue (scrub_id, chunk_digest)
		SELECT ?, ec.chunk_digest
		FROM entry_chunks ec
		JOIN snapshots s ON s.id = ec.snapshot_id AND s.status = ?
		WHERE ec.snapshot_id <= ?
		GROUP BY ec.chunk_digest`, id, StatusCommitted, high.Int64); err != nil {
		return ScrubInfo{}, err
	}
	if _, err := tx.Exec(`UPDATE scrubs SET total_chunks =
		(SELECT count(*) FROM scrub_queue WHERE scrub_id = ?) WHERE id = ?`, id, id); err != nil {
		return ScrubInfo{}, err
	}
	if err := tx.Commit(); err != nil {
		return ScrubInfo{}, fmt.Errorf("create scrub: %w", err)
	}
	return m.GetScrub(id)
}

// LatestUnfinishedScrub returns the most recent non-done scrub (a "paused"
// or "running" job, the latter left behind by a hard process kill), if any.
func (m *Manifest) LatestUnfinishedScrub() (ScrubInfo, bool, error) {
	row := m.db.QueryRow(`SELECT `+scrubCols+` FROM scrubs
		WHERE status != ? ORDER BY id DESC LIMIT 1`, ScrubDone)
	sc, err := scanScrub(row)
	if errors.Is(err, sql.ErrNoRows) {
		return ScrubInfo{}, false, nil
	}
	if err != nil {
		return ScrubInfo{}, false, err
	}
	return sc, true, nil
}

// LatestScrub returns the most recent scrub of any status.
func (m *Manifest) LatestScrub() (ScrubInfo, bool, error) {
	row := m.db.QueryRow(`SELECT ` + scrubCols + ` FROM scrubs ORDER BY id DESC LIMIT 1`)
	sc, err := scanScrub(row)
	if errors.Is(err, sql.ErrNoRows) {
		return ScrubInfo{}, false, nil
	}
	if err != nil {
		return ScrubInfo{}, false, err
	}
	return sc, true, nil
}

// GetScrub fetches one scrub job.
func (m *Manifest) GetScrub(id int64) (ScrubInfo, error) {
	row := m.db.QueryRow(`SELECT `+scrubCols+` FROM scrubs WHERE id = ?`, id)
	sc, err := scanScrub(row)
	if errors.Is(err, sql.ErrNoRows) {
		return sc, ErrNotFound
	}
	return sc, err
}

// ListScrubs returns every scrub newest first.
func (m *Manifest) ListScrubs() ([]ScrubInfo, error) {
	rows, err := m.db.Query(`SELECT ` + scrubCols + ` FROM scrubs ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ScrubInfo
	for rows.Next() {
		sc, err := scanScrub(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sc)
	}
	return out, rows.Err()
}

// ClaimPendingChunks returns up to limit not-yet-done chunk digests in stable
// digest order. Digest ordering is the cursor definition: after commit the
// cursor is implicit in done=1 rows and resume reads the next ones.
func (m *Manifest) ClaimPendingChunks(scrubID int64, limit int) ([][]byte, error) {
	rows, err := m.db.Query(`SELECT chunk_digest FROM scrub_queue
		WHERE scrub_id = ? AND done = 0 ORDER BY chunk_digest LIMIT ?`, scrubID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]byte
	for rows.Next() {
		var d []byte
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		out = append(out, append([]byte(nil), d...))
	}
	return out, rows.Err()
}

// ChunkLength returns the catalog-declared length of a chunk; found=false
// when the catalog row itself is missing.
func (m *Manifest) ChunkLength(digest []byte) (length int64, found bool, err error) {
	var l int64
	err = m.db.QueryRow(`SELECT length FROM chunks WHERE digest = ?`, digest).Scan(&l)
	if errors.Is(err, sql.ErrNoRows) {
		return -1, false, nil
	}
	if err != nil {
		return -1, false, err
	}
	return l, true, nil
}

// ScrubChunkResult is the streaming verdict of one chunk.
type ScrubChunkResult struct {
	Digest         []byte
	Result         string // ok | bad | missing
	DeclaredLength int64
	ObservedLength int64
	ObservedDigest []byte // nil unless the digest could actually be computed
	Detail         string
}

// SaveScrubBatch atomically persists one scanned batch: it upserts the
// per-chunk verdicts (idempotent on resume — a re-run batch never creates a
// duplicate report), marks the queue rows done, advances the counters, and
// recomputes every in-scope snapshot verdict. Everything in one transaction
// is what makes each visible progress point stable: interrupting anywhere
// between batches leaves a resumable, never half-labelled state.
func (m *Manifest) SaveScrubBatch(scrubID int64, results []ScrubChunkResult) error {
	tx, err := m.db.Begin()
	if err != nil {
		return fmt.Errorf("save scrub batch: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	var newDone, badDelta int64
	var bytesDelta int64
	for _, r := range results {
		var wasDone int
		if err := tx.QueryRow(`SELECT done FROM scrub_queue
			WHERE scrub_id = ? AND chunk_digest = ?`, scrubID, r.Digest).Scan(&wasDone); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO scrub_chunks
			(scrub_id, chunk_digest, result, declared_length, observed_length,
			 observed_digest, detail, scanned_at)
			VALUES (?,?,?,?,?,?,?,?)
			ON CONFLICT(scrub_id, chunk_digest) DO UPDATE SET
				result=excluded.result, declared_length=excluded.declared_length,
				observed_length=excluded.observed_length,
				observed_digest=excluded.observed_digest,
				detail=excluded.detail, scanned_at=excluded.scanned_at`,
			scrubID, r.Digest, r.Result, r.DeclaredLength, r.ObservedLength,
			r.ObservedDigest, r.Detail, now); err != nil {
			return err
		}
		if wasDone == 1 {
			continue // batch redone after a crash between streaming and commit
		}
		if _, err := tx.Exec(`UPDATE scrub_queue SET done = 1
			WHERE scrub_id = ? AND chunk_digest = ?`, scrubID, r.Digest); err != nil {
			return err
		}
		newDone++
		if r.Result != "ok" {
			badDelta++
		}
		if r.ObservedLength > 0 {
			bytesDelta += r.ObservedLength
		}
	}
	if newDone > 0 {
		if _, err := tx.Exec(`UPDATE scrubs
			SET scanned_chunks = scanned_chunks + ?,
			    bad_chunks = bad_chunks + ?,
			    bytes_scanned = bytes_scanned + ?
			WHERE id = ?`, newDone, badDelta, bytesDelta, scrubID); err != nil {
			return err
		}
	}
	if err := refreshSnapshotVerdicts(tx, scrubID); err != nil {
		return err
	}
	return tx.Commit()
}

// refreshSnapshotVerdicts recomputes clean/suspect/unscanned for every
// in-scope snapshot. A snapshot is suspect as soon as one referenced chunk
// has a non-ok verdict; clean only when every referenced chunk is done and
// ok; otherwise still unscanned — unscanned data can never look clean.
func refreshSnapshotVerdicts(tx *sql.Tx, scrubID int64) error {
	_, err := tx.Exec(`
		UPDATE scrub_snapshots SET
			bad_chunks = (
				SELECT count(DISTINCT ec.chunk_digest)
				FROM entry_chunks ec
				JOIN scrub_chunks sc
					ON sc.scrub_id = scrub_snapshots.scrub_id
					AND sc.chunk_digest = ec.chunk_digest
				WHERE ec.snapshot_id = scrub_snapshots.snapshot_id
					AND sc.result != 'ok'
			),
			status = CASE
				WHEN scrub_snapshots.status = 'excluded' THEN 'excluded'
				WHEN EXISTS (
					SELECT 1 FROM entry_chunks ec
					JOIN scrub_chunks sc
						ON sc.scrub_id = scrub_snapshots.scrub_id
						AND sc.chunk_digest = ec.chunk_digest
					WHERE ec.snapshot_id = scrub_snapshots.snapshot_id
						AND sc.result != 'ok'
				) THEN 'suspect'
				WHEN 0 = (
					SELECT count(*) FROM entry_chunks ec
					LEFT JOIN scrub_queue q
						ON q.scrub_id = scrub_snapshots.scrub_id
						AND q.chunk_digest = ec.chunk_digest
					WHERE ec.snapshot_id = scrub_snapshots.snapshot_id
						AND (q.done IS NULL OR q.done = 0)
				)
				AND (
					SELECT count(DISTINCT ec.chunk_digest) FROM entry_chunks ec
					JOIN scrub_chunks sc
						ON sc.scrub_id = scrub_snapshots.scrub_id
						AND sc.chunk_digest = ec.chunk_digest
					WHERE ec.snapshot_id = scrub_snapshots.snapshot_id
						AND sc.result = 'ok'
				) = (
					SELECT count(DISTINCT chunk_digest) FROM entry_chunks
					WHERE snapshot_id = scrub_snapshots.snapshot_id
				)
				THEN 'clean'
				ELSE 'unscanned'
			END
		WHERE scrub_id = ?`, scrubID)
	return err
}

// MarkScrubPaused records a graceful/error stop at a stable batch boundary.
func (m *Manifest) MarkScrubPaused(scrubID int64, cause string) error {
	_, err := m.db.Exec(`UPDATE scrubs SET status = ?, paused_at = ?, last_error = ?
		WHERE id = ? AND status = ?`,
		ScrubPaused, time.Now().UTC().Format(time.RFC3339Nano), cause, scrubID, ScrubRunning)
	return err
}

// MarkScrubDone finalizes a fully scanned scrub.
func (m *Manifest) MarkScrubDone(scrubID int64) error {
	_, err := m.db.Exec(`UPDATE scrubs SET status = ?, finished_at = ?,
		scanned_chunks = (SELECT count(*) FROM scrub_queue WHERE scrub_id = ? AND done = 1),
		bad_chunks = (SELECT count(*) FROM scrub_chunks WHERE scrub_id = ? AND result != 'ok')
		WHERE id = ?`,
		ScrubDone, time.Now().UTC().Format(time.RFC3339Nano), scrubID, scrubID, scrubID)
	return err
}

// MarkScrubRunning flips a paused (or crashed) job back to running on resume.
func (m *Manifest) MarkScrubRunning(scrubID int64) error {
	_, err := m.db.Exec(`UPDATE scrubs SET status = ? WHERE id = ? AND status != ?`,
		ScrubRunning, scrubID, ScrubDone)
	return err
}

// SnapshotVerdict is one row of scrub_snapshots.
type SnapshotVerdict struct {
	SnapshotID int64
	Status     string
	BadChunks  int64
}

// ScrubSnapshots returns per-snapshot verdicts of one scrub. Snapshots
// beyond the watermark are appended as "uncovered", so the caller always
// sees the whole repository relative to this scrub.
func (m *Manifest) ScrubSnapshots(scrubID int64) ([]SnapshotVerdict, error) {
	sc, err := m.GetScrub(scrubID)
	if err != nil {
		return nil, err
	}
	rows, err := m.db.Query(`SELECT snapshot_id, status, bad_chunks
		FROM scrub_snapshots WHERE scrub_id = ? ORDER BY snapshot_id`, scrubID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]SnapshotVerdict, 0)
	for rows.Next() {
		var v SnapshotVerdict
		if err := rows.Scan(&v.SnapshotID, &v.Status, &v.BadChunks); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	// Snapshots that did not exist at the watermark (committed, pending or
	// failed afterwards) were never frozen: present them as uncovered rather
	// than pretending the scrub checked them.
	later, err := m.db.Query(`SELECT id FROM snapshots WHERE id > ? ORDER BY id`, sc.HighSnapshotID)
	if err != nil {
		return nil, err
	}
	defer later.Close()
	for later.Next() {
		var id int64
		if err := later.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, SnapshotVerdict{SnapshotID: id, Status: ScrubSnapUncovered})
	}
	return out, later.Err()
}

// IntegrityView is a snapshot's verdict against the latest scrub.
type IntegrityView struct {
	SnapshotID int64
	ScrubID    int64 // 0 when no scrub has ever run
	Status     string
	BadChunks  int64
}

// IntegrityViews returns every snapshot's verdict against the most recent
// scrub in one pass. With no scrub at all, snapshots are "never_scrubbed".
func (m *Manifest) IntegrityViews() (map[int64]IntegrityView, error) {
	latest, has, err := m.LatestScrub()
	if err != nil {
		return nil, err
	}
	ids, err := m.allSnapshotIDs()
	if err != nil {
		return nil, err
	}
	views := make(map[int64]IntegrityView, len(ids))
	if !has {
		for _, id := range ids {
			views[id] = IntegrityView{SnapshotID: id, Status: ScrubSnapNever}
		}
		return views, nil
	}
	rows, err := m.db.Query(`SELECT snapshot_id, status, bad_chunks
		FROM scrub_snapshots WHERE scrub_id = ?`, latest.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var v IntegrityView
		if err := rows.Scan(&v.SnapshotID, &v.Status, &v.BadChunks); err != nil {
			return nil, err
		}
		v.ScrubID = latest.ID
		views[v.SnapshotID] = v
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, ok := views[id]; !ok {
			// Committed/pending/failed after the watermark: not covered.
			views[id] = IntegrityView{SnapshotID: id, ScrubID: latest.ID,
				Status: ScrubSnapUncovered}
		}
	}
	return views, nil
}

func (m *Manifest) allSnapshotIDs() ([]int64, error) {
	rows, err := m.db.Query(`SELECT id FROM snapshots ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// AffectedRef is one (snapshot, file path) reference to a bad chunk.
type AffectedRef struct {
	SnapshotID int64  `json:"snapshot_id"`
	RelPath    string `json:"rel_path"`
}

// BadChunk is a non-ok chunk of one scrub with every manifest reference to
// it across the whole repository — the required reverse lookup for a shared
// blob: one bad digest lists all snapshots and file paths that use it,
// including snapshots outside this scrub's watermark.
type BadChunk struct {
	Digest         []byte
	Result         string
	DeclaredLength int64
	ObservedLength int64
	ObservedDigest []byte
	Detail         string
	Refs           []AffectedRef
}

// ScrubBadChunks returns all bad/missing chunks of a scrub and every
// snapshot/path referencing them.
func (m *Manifest) ScrubBadChunks(scrubID int64) ([]BadChunk, error) {
	rows, err := m.db.Query(`SELECT sc.chunk_digest, sc.result, sc.declared_length,
		sc.observed_length, sc.observed_digest, sc.detail
		FROM scrub_chunks sc WHERE sc.scrub_id = ? AND sc.result != 'ok'
		ORDER BY sc.chunk_digest`, scrubID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []BadChunk
	for rows.Next() {
		var bc BadChunk
		var obs []byte // NULL for missing/unreadable
		if err := rows.Scan(&bc.Digest, &bc.Result, &bc.DeclaredLength,
			&bc.ObservedLength, &obs, &bc.Detail); err != nil {
			return nil, err
		}
		bc.ObservedDigest = obs
		out = append(out, bc)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	for i := range out {
		refs, err := m.ReferencesOfDigest(out[i].Digest)
		if err != nil {
			return nil, err
		}
		out[i].Refs = refs
	}
	return out, nil
}

// ReferencesOfDigest lists every snapshot and file path referencing a chunk.
func (m *Manifest) ReferencesOfDigest(digest []byte) ([]AffectedRef, error) {
	rows, err := m.db.Query(`SELECT DISTINCT snapshot_id, rel_path
		FROM entry_chunks WHERE chunk_digest = ?
		ORDER BY snapshot_id, rel_path`, digest)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AffectedRef
	for rows.Next() {
		var r AffectedRef
		if err := rows.Scan(&r.SnapshotID, &r.RelPath); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ScrubChunkResults returns the full per-chunk verdict table of a scrub
// (used to compare one-shot and resumed runs). Keyed by hex digest.
func (m *Manifest) ScrubChunkResults(scrubID int64) (map[string]ScrubChunkResult, error) {
	rows, err := m.db.Query(`SELECT chunk_digest, result, declared_length,
		observed_length, observed_digest, detail
		FROM scrub_chunks WHERE scrub_id = ? ORDER BY chunk_digest`, scrubID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]ScrubChunkResult{}
	for rows.Next() {
		var r ScrubChunkResult
		var obs []byte
		if err := rows.Scan(&r.Digest, &r.Result, &r.DeclaredLength,
			&r.ObservedLength, &obs, &r.Detail); err != nil {
			return nil, err
		}
		r.ObservedDigest = obs
		out[fmt.Sprintf("%x", r.Digest)] = r
	}
	return out, rows.Err()
}
