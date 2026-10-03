package repo

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Snapshot status values. A snapshot is only "committed" after every
// referenced chunk has been proven present in the content store. Pending and
// failed snapshots are kept on purpose: they are what maintenance inspects to
// find out exactly which chunk is missing.
const (
	StatusPending   = "pending"   // scan done, not yet verified/finalized
	StatusCommitted = "committed" // all chunks verified live, usable for restore
	StatusFailed    = "failed"    // verification or commit failed; see snapshot_errors
)

// Manifest is the SQLite-backed backup catalog.
type Manifest struct {
	db *sql.DB
}

// OpenManifest opens or creates the manifest at path.
func OpenManifest(path string) (*Manifest, error) {
	// _txlock=immediate makes write transactions take a RESERVED lock up
	// front, avoiding SQLITE_BUSY under concurrent snapshots/restores.
	dsn := "file:" + path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_txlock=immediate"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // avoid lock churn; all operations are short
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("open manifest: %w", err)
	}
	m := &Manifest{db: db}
	if err := m.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return m, nil
}

// Close releases the database handle.
func (m *Manifest) Close() error { return m.db.Close() }

// DB exposes the handle for package-internal repositories.
func (m *Manifest) DB() *sql.DB { return m.db }

const schemaSQL = `
CREATE TABLE IF NOT EXISTS snapshots (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	root_path   TEXT    NOT NULL,
	status      TEXT    NOT NULL,
	polynomial  INTEGER NOT NULL,
	file_count  INTEGER NOT NULL DEFAULT 0,
	dir_count   INTEGER NOT NULL DEFAULT 0,
	bytes_total INTEGER NOT NULL DEFAULT 0,
	chunks_new  INTEGER NOT NULL DEFAULT 0,
	chunks_ref  INTEGER NOT NULL DEFAULT 0,
	created_at  TEXT    NOT NULL,
	committed_at TEXT,
	message     TEXT    NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS entries (
	snapshot_id     INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
	rel_path        TEXT    NOT NULL,
	kind            TEXT    NOT NULL,            -- 'file' | 'dir' | 'symlink'
	mode            INTEGER NOT NULL,            -- permission bits (os mode without type)
	uid             INTEGER NOT NULL DEFAULT -1,
	gid             INTEGER NOT NULL DEFAULT -1,
	mod_time_ns     INTEGER NOT NULL,
	size            INTEGER NOT NULL DEFAULT 0,
	file_digest     BLOB,                        -- whole-file SHA-256, files only
	link_target     TEXT    NOT NULL DEFAULT '', -- symlinks only
	entry_order     INTEGER NOT NULL,
	PRIMARY KEY (snapshot_id, rel_path)
);
CREATE INDEX IF NOT EXISTS idx_entries_snap ON entries(snapshot_id, entry_order);

-- Chunks are global and content-addressed: the same digest is one row,
-- referenced by many entries across many snapshots.
CREATE TABLE IF NOT EXISTS chunks (
	digest      BLOB PRIMARY KEY,
	length      INTEGER NOT NULL,
	created_at  TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS entry_chunks (
	snapshot_id  INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
	rel_path     TEXT    NOT NULL,
	chunk_digest BLOB    NOT NULL REFERENCES chunks(digest),
	seq          INTEGER NOT NULL,
	PRIMARY KEY (snapshot_id, rel_path, seq),
	FOREIGN KEY (snapshot_id, rel_path) REFERENCES entries(snapshot_id, rel_path) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_ec_digest ON entry_chunks(chunk_digest);

CREATE TABLE IF NOT EXISTS snapshot_errors (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
	stage       TEXT    NOT NULL,   -- scan | verify | commit
	rel_path    TEXT    NOT NULL DEFAULT '',
	chunk_digest BLOB,
	message     TEXT    NOT NULL,
	created_at  TEXT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_err_snap ON snapshot_errors(snapshot_id);

-- Repository-wide settings, notably the chunking polynomial. Content-defined
-- boundaries must be identical across snapshots (and restarts) for chunks of
-- unchanged byte ranges to hash the same.
CREATE TABLE IF NOT EXISTS meta (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);

-- Integrity patrol ("scrub") runs. A run freezes a high-watermark snapshot id
-- at start: only snapshots that existed then are in scope, and their verdict
-- is stored separately from snapshots.status. The historical commit fact
-- (pending/committed/failed) is never rewritten by a patrol.
CREATE TABLE IF NOT EXISTS scrub_runs (
	id             INTEGER PRIMARY KEY AUTOINCREMENT,
	high_watermark INTEGER NOT NULL,            -- max(snapshots.id) at start
	status         TEXT    NOT NULL,            -- 'running' | 'done'
	cursor_digest  BLOB,                        -- last processed chunk (stable digest order)
	total_chunks   INTEGER NOT NULL DEFAULT 0,  -- frozen size of scrub_work
	clean_chunks   INTEGER NOT NULL DEFAULT 0,
	bad_chunks     INTEGER NOT NULL DEFAULT 0,
	started_at     TEXT    NOT NULL,
	finished_at    TEXT
);

-- Frozen work set of one run: distinct chunks referenced by any snapshot at or
-- below the high-watermark. Materialized at start so snapshots committing
-- later can neither enlarge the run nor get retroactively marked checked.
CREATE TABLE IF NOT EXISTS scrub_work (
	run_id          INTEGER NOT NULL REFERENCES scrub_runs(id) ON DELETE CASCADE,
	chunk_digest    BLOB    NOT NULL,
	declared_length INTEGER NOT NULL,           -- chunks.length at start, -1 when the catalog row is gone
	PRIMARY KEY (run_id, chunk_digest)
);

-- Per-chunk streaming verdict. Exactly one row per (run, chunk); resume uses
-- INSERT OR IGNORE so an interrupted run never produces a duplicate report.
CREATE TABLE IF NOT EXISTS scrub_chunks (
	run_id          INTEGER NOT NULL REFERENCES scrub_runs(id) ON DELETE CASCADE,
	chunk_digest    BLOB    NOT NULL,
	declared_length INTEGER NOT NULL,
	result          TEXT    NOT NULL,           -- ok | missing_catalog | missing_blob | length_mismatch | digest_mismatch | read_error
	actual_length   INTEGER NOT NULL DEFAULT -1,
	message         TEXT    NOT NULL DEFAULT '',
	scanned_at      TEXT    NOT NULL,
	PRIMARY KEY (run_id, chunk_digest)
);
CREATE INDEX IF NOT EXISTS idx_scrub_chunks_digest ON scrub_chunks(chunk_digest);

-- Per-snapshot patrol verdict, independent of snapshots.status:
--   clean     - committed at start, every referenced chunk verified
--   suspect   - committed at start, at least one referenced chunk is bad
--   unscanned - in scope, run not finished yet (never persisted as clean early)
--   excluded  - pending/failed at start: commit status and errors stay authoritative
--   uncovered - snapshot id above the watermark: this run does not cover it
CREATE TABLE IF NOT EXISTS scrub_snapshots (
	run_id           INTEGER NOT NULL REFERENCES scrub_runs(id) ON DELETE CASCADE,
	snapshot_id      INTEGER NOT NULL,
	covered          INTEGER NOT NULL,          -- 1 = existed at start, 0 = appeared afterwards
	integrity        TEXT    NOT NULL,
	note             TEXT    NOT NULL DEFAULT '',
	bad_chunk_count  INTEGER NOT NULL DEFAULT 0,
	updated_at       TEXT    NOT NULL,
	PRIMARY KEY (run_id, snapshot_id)
);
CREATE INDEX IF NOT EXISTS idx_scrub_snap_snapshot ON scrub_snapshots(snapshot_id);
`

func (m *Manifest) migrate() error {
	_, err := m.db.Exec(schemaSQL)
	if err != nil {
		return fmt.Errorf("migrate manifest: %w", err)
	}
	return nil
}

// SnapshotInfo is the catalog view of one snapshot.
type SnapshotInfo struct {
	ID          int64
	RootPath    string
	Status      string
	Polynomial  uint64
	FileCount   int64
	DirCount    int64
	BytesTotal  int64
	ChunksNew   int64
	ChunksRef   int64
	CreatedAt   time.Time
	CommittedAt *time.Time
	Message     string
}

func scanSnapshot(row interface {
	Scan(...any) error
}) (SnapshotInfo, error) {
	var s SnapshotInfo
	var created, committed sql.NullString
	var poly int64
	if err := row.Scan(&s.ID, &s.RootPath, &s.Status, &poly, &s.FileCount,
		&s.DirCount, &s.BytesTotal, &s.ChunksNew, &s.ChunksRef,
		&created, &committed, &s.Message); err != nil {
		return s, err
	}
	s.Polynomial = uint64(poly)
	s.CreatedAt, _ = time.Parse(time.RFC3339Nano, created.String)
	if committed.Valid {
		t, err := time.Parse(time.RFC3339Nano, committed.String)
		if err == nil {
			s.CommittedAt = &t
		}
	}
	return s, nil
}

const snapshotCols = `id, root_path, status, polynomial, file_count, dir_count,
	bytes_total, chunks_new, chunks_ref, created_at, committed_at, message`

// ListSnapshots returns all snapshots, newest first.
func (m *Manifest) ListSnapshots() ([]SnapshotInfo, error) {
	rows, err := m.db.Query(`SELECT ` + snapshotCols + ` FROM snapshots ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SnapshotInfo
	for rows.Next() {
		s, err := scanSnapshot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// GetSnapshot fetches one snapshot.
func (m *Manifest) GetSnapshot(id int64) (SnapshotInfo, error) {
	row := m.db.QueryRow(`SELECT `+snapshotCols+` FROM snapshots WHERE id = ?`, id)
	s, err := scanSnapshot(row)
	if errors.Is(err, sql.ErrNoRows) {
		return s, ErrNotFound
	}
	return s, err
}

// ErrNotFound marks a missing snapshot.
var ErrNotFound = errors.New("snapshot not found")

// SnapshotError is a recorded failure detail for a (possibly failed) snapshot.
type SnapshotError struct {
	ID          int64
	Stage       string
	RelPath     string
	ChunkDigest []byte
	Message     string
	CreatedAt   time.Time
}

// ListErrors returns every recorded error for a snapshot, oldest first.
func (m *Manifest) ListErrors(id int64) ([]SnapshotError, error) {
	rows, err := m.db.Query(`SELECT id, stage, rel_path, chunk_digest, message, created_at
		FROM snapshot_errors WHERE snapshot_id = ? ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SnapshotError
	for rows.Next() {
		var e SnapshotError
		var created string
		if err := rows.Scan(&e.ID, &e.Stage, &e.RelPath, &e.ChunkDigest, &e.Message, &created); err != nil {
			return nil, err
		}
		e.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		out = append(out, e)
	}
	return out, rows.Err()
}
