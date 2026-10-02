package repo

import (
	"database/sql"
	"fmt"
	"time"
)

// BeginSnapshot inserts a pending snapshot row.
func (m *Manifest) BeginSnapshot(root string, polynomial uint64, message string) (int64, error) {
	res, err := m.db.Exec(`INSERT INTO snapshots
		(root_path, status, polynomial, created_at, message)
		VALUES (?, ?, ?, ?, ?)`,
		root, StatusPending, int64(polynomial), time.Now().UTC().Format(time.RFC3339Nano), message)
	if err != nil {
		return 0, fmt.Errorf("begin snapshot: %w", err)
	}
	return res.LastInsertId()
}

// SaveSnapshotContents writes all scanned entries, their chunk references and
// chunk rows in one transaction. Missing-chunk failpoints call this with a
// chunk row deliberately absent, which later verification must detect.
func (m *Manifest) SaveSnapshotContents(id int64, entries []Entry, newChunks, refChunks, bytes, files, dirs int64) error {
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	for order, e := range entries {
		if _, err := tx.Exec(`INSERT INTO entries
			(snapshot_id, rel_path, kind, mode, uid, gid, mod_time_ns, size, file_digest, link_target, entry_order)
			VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
			id, e.RelPath, e.Kind, int64(e.Mode), e.UID, e.GID,
			e.ModTime.UnixNano(), e.Size, e.FileDigest, e.LinkTarget, order); err != nil {
			return fmt.Errorf("save entry %q: %w", e.RelPath, err)
		}
		for _, c := range e.Chunks {
			if _, err := tx.Exec(`INSERT OR IGNORE INTO chunks (digest, length, created_at)
				VALUES (?,?,?)`, c.Digest, c.Length, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				return fmt.Errorf("save chunk: %w", err)
			}
			if _, err := tx.Exec(`INSERT INTO entry_chunks
				(snapshot_id, rel_path, chunk_digest, seq) VALUES (?,?,?,?)`,
				id, e.RelPath, c.Digest, c.Seq); err != nil {
				return fmt.Errorf("link chunk: %w", err)
			}
		}
	}
	if _, err := tx.Exec(`UPDATE snapshots SET
		file_count = ?, dir_count = ?, bytes_total = ?, chunks_new = ?, chunks_ref = ?
		WHERE id = ?`, files, dirs, bytes, newChunks, refChunks, id); err != nil {
		return err
	}
	return tx.Commit()
}

// AddError records one failure detail against a snapshot.
func (m *Manifest) AddError(id int64, stage, rel string, digest []byte, msg string) error {
	_, err := m.db.Exec(`INSERT INTO snapshot_errors
		(snapshot_id, stage, rel_path, chunk_digest, message, created_at)
		VALUES (?,?,?,?,?,?)`,
		id, stage, rel, digest, msg, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

// MarkCommitted atomically flips pending -> committed.
func (m *Manifest) MarkCommitted(id int64) error {
	res, err := m.db.Exec(`UPDATE snapshots SET status = ?, committed_at = ?
		WHERE id = ? AND status = ?`,
		StatusCommitted, time.Now().UTC().Format(time.RFC3339Nano), id, StatusPending)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("snapshot %d is not pending", id)
	}
	return nil
}

// MarkFailed flips any snapshot to failed.
func (m *Manifest) MarkFailed(id int64) error {
	_, err := m.db.Exec(`UPDATE snapshots SET status = ? WHERE id = ?`, StatusFailed, id)
	return err
}

// PendingSnapshots returns still-pending snapshots (e.g. process killed
// between scan and commit).
func (m *Manifest) PendingSnapshots() ([]SnapshotInfo, error) {
	rows, err := m.db.Query(`SELECT `+snapshotCols+` FROM snapshots
		WHERE status = ? ORDER BY id`, StatusPending)
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

// StoredEntry is a manifest entry with its ordered chunk digests.
type StoredEntry struct {
	Entry
	ChunkDigests [][]byte
}

// EntriesOf returns all entries of a snapshot in stored (depth-first) order.
func (m *Manifest) EntriesOf(id int64) ([]StoredEntry, error) {
	rows, err := m.db.Query(`SELECT rel_path, kind, mode, uid, gid, mod_time_ns,
		size, file_digest, link_target
		FROM entries WHERE snapshot_id = ? ORDER BY entry_order`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StoredEntry
	for rows.Next() {
		var se StoredEntry
		var ns int64
		var digest sql.NullString
		if err := rows.Scan(&se.RelPath, &se.Kind, &se.Mode, &se.UID, &se.GID,
			&ns, &se.Size, &digest, &se.LinkTarget); err != nil {
			return nil, err
		}
		se.Mode = uint32(se.Mode)
		se.ModTime = time.Unix(0, ns).UTC()
		if digest.Valid && len(digest.String) > 0 {
			se.FileDigest = []byte(digest.String)
		}
		out = append(out, se)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range out {
		crows, err := m.db.Query(`SELECT chunk_digest FROM entry_chunks
			WHERE snapshot_id = ? AND rel_path = ? ORDER BY seq`, id, out[i].RelPath)
		if err != nil {
			return nil, err
		}
		for crows.Next() {
			var d []byte
			if err := crows.Scan(&d); err != nil {
				crows.Close()
				return nil, err
			}
			out[i].ChunkDigests = append(out[i].ChunkDigests, d)
		}
		crows.Close()
	}
	return out, nil
}

// ReferencedChunks returns the distinct chunks of a snapshot with lengths.
func (m *Manifest) ReferencedChunks(id int64) ([]ChunkRef, error) {
	rows, err := m.db.Query(`SELECT DISTINCT c.digest, c.length
		FROM entry_chunks ec JOIN chunks c ON c.digest = ec.chunk_digest
		WHERE ec.snapshot_id = ? ORDER BY c.digest`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChunkRef
	for rows.Next() {
		var c ChunkRef
		var d []byte
		if err := rows.Scan(&d, &c.Length); err != nil {
			return nil, err
		}
		c.Digest = d
		out = append(out, c)
	}
	return out, rows.Err()
}

// MissingChunk is a reference that cannot currently be satisfied from the
// content store: either the chunk row is missing from the catalog, or the
// blob file is absent / has the wrong length.
type MissingChunk struct {
	RelPath string
	Digest  []byte
	Length  int64 // -1 when the chunk row itself is missing
	Reason  string
}

// FindMissingChunks lists every unsatisfied chunk reference of a snapshot.
// It checks both the chunks table and the live blob store, so it catches
// "committed manifest, deleted blob" storage rot too.
func (m *Manifest) FindMissingChunks(id int64, has func(digest []byte, length int64) (bool, error)) ([]MissingChunk, error) {
	// 1. references with no catalog row at all
	rows, err := m.db.Query(`SELECT ec.rel_path, ec.chunk_digest
		FROM entry_chunks ec
		LEFT JOIN chunks c ON c.digest = ec.chunk_digest
		WHERE ec.snapshot_id = ? AND c.digest IS NULL
		ORDER BY ec.rel_path, ec.seq`, id)
	if err != nil {
		return nil, err
	}
	var out []MissingChunk
	for rows.Next() {
		var mc MissingChunk
		var d []byte
		if err := rows.Scan(&mc.RelPath, &d); err != nil {
			rows.Close()
			return nil, err
		}
		mc.Digest = d
		mc.Length = -1
		mc.Reason = "chunk missing from catalog (commit interrupted)"
		out = append(out, mc)
	}
	rows.Close()

	// 2. references whose blob is absent or wrong-sized on disk
	r2, err := m.db.Query(`SELECT ec.rel_path, c.digest, c.length
		FROM entry_chunks ec JOIN chunks c ON c.digest = ec.chunk_digest
		WHERE ec.snapshot_id = ?
		ORDER BY ec.rel_path, ec.seq`, id)
	if err != nil {
		return nil, err
	}
	defer r2.Close()
	seen := map[string]bool{}
	for r2.Next() {
		var rel string
		var d []byte
		var length int64
		if err := r2.Scan(&rel, &d, &length); err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%x@%s", d, rel)
		if seen[key] {
			continue
		}
		seen[key] = true
		ok, err := has(d, length)
		if err != nil {
			return nil, err
		}
		if !ok {
			out = append(out, MissingChunk{
				RelPath: rel,
				Digest:  append([]byte(nil), d...),
				Length:  length,
				Reason:  "chunk blob missing or length mismatch in content store",
			})
		}
	}
	return out, r2.Err()
}

// SingletonChunks returns up to n chunks referenced only by this snapshot
// across the whole repository. The commit-interrupt failpoint removes such
// chunks, simulating loss of a brand-new blob without damaging earlier
// snapshots that share the content-addressed store.
func (m *Manifest) SingletonChunks(id int64, n int) ([]ChunkRef, error) {
	rows, err := m.db.Query(`SELECT c.digest, c.length
		FROM entry_chunks ec JOIN chunks c ON c.digest = ec.chunk_digest
		WHERE c.digest IN (
			SELECT chunk_digest FROM entry_chunks WHERE snapshot_id = ?
		)
		GROUP BY c.digest
		HAVING count(DISTINCT ec.snapshot_id) = 1
		ORDER BY c.digest DESC LIMIT ?`, id, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChunkRef
	for rows.Next() {
		var c ChunkRef
		var d []byte
		if err := rows.Scan(&d, &c.Length); err != nil {
			return nil, err
		}
		c.Digest = d
		out = append(out, c)
	}
	return out, rows.Err()
}

// DeleteChunkRow removes a chunk catalog row without touching blobs. Used by
// the commit-interrupt failpoint to simulate a lost finalize.
func (m *Manifest) DeleteChunkRow(digest []byte) error {
	_, err := m.db.Exec(`DELETE FROM chunks WHERE digest = ?`, digest)
	return err
}

// ChunkRowExists reports whether a chunk row exists.
func (m *Manifest) ChunkRowExists(digest []byte) (bool, error) {
	var n int
	err := m.db.QueryRow(`SELECT count(*) FROM chunks WHERE digest = ?`, digest).Scan(&n)
	return n > 0, err
}
