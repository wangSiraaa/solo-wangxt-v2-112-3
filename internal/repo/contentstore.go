// Package repo implements the content-addressed chunk store and the SQLite
// manifest that together make up a backup repository.
package repo

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// ContentStore saves chunks in a sharded directory layout keyed by SHA-256:
//
//	<root>/ab/cdef...0123
//
// Writes go through a temp file in the same directory and are atomically
// renamed into place, so a crash can never leave a partially written chunk
// under its final name.
type ContentStore struct {
	root string
}

// NewContentStore opens (and lazily creates) a chunk store below root.
func NewContentStore(root string) (*ContentStore, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("create chunk store: %w", err)
	}
	return &ContentStore{root: root}, nil
}

// Root returns the absolute root directory of the store.
func (s *ContentStore) Root() string { return s.root }

// Path returns the on-disk path for a chunk id without opening it.
func (s *ContentStore) Path(id []byte) (string, error) { return s.path(id) }

func hexID(id []byte) string { return hex.EncodeToString(id) }

// path returns the on-disk path for a chunk id. It refuses ids that are not
// 32-byte SHA-256 digests so ids can never escape the store root.
func (s *ContentStore) path(id []byte) (string, error) {
	if len(id) != sha256.Size {
		return "", fmt.Errorf("invalid chunk id length %d", len(id))
	}
	name := hexID(id)
	return filepath.Join(s.root, name[:2], name[2:]), nil
}

var errBadDigest = errors.New("chunk content does not match expected digest")

// Put stores b under id. If a chunk with the same id already exists and has
// the expected size and digest, nothing is written (deduplication). The
// digest of b is verified against id before the temp file is published.
func (s *ContentStore) Put(id []byte, b []byte) error {
	p, err := s.path(id)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(b)
	if !bytesEqual(sum[:], id) {
		return errBadDigest
	}
	if fi, err := os.Lstat(p); err == nil {
		if fi.Mode().IsRegular() && fi.Size() == int64(len(b)) {
			return nil // already present, content verified by id
		}
	}
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0o444); err != nil {
		return err
	}
	return os.Rename(tmpName, p)
}

// Has reports whether the chunk exists as a regular file with the expected
// length. It is the liveness check used during snapshot verification and
// missing-chunk diagnosis.
func (s *ContentStore) Has(id []byte, length int64) (bool, error) {
	p, err := s.path(id)
	if err != nil {
		return false, err
	}
	fi, err := os.Lstat(p)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return fi.Mode().IsRegular() && fi.Size() == length, nil
}

// Open opens a chunk for reading, verifying its digest as it streams. The
// caller must Close the reader. A digest mismatch is surfaced on Read.
func (s *ContentStore) Open(id []byte) (io.ReadCloser, error) {
	p, err := s.path(id)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	return &verifiedReader{file: f, h: sha256.New(), want: append([]byte(nil), id...)}, nil
}

// Remove deletes a chunk. It exists for the commit-interrupt demo, which
// simulates a chunk lost before the snapshot is finalized.
func (s *ContentStore) Remove(id []byte) error {
	p, err := s.path(id)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// AbsPath converts a stored hex id to its path; used by the diagnose report.
func (s *ContentStore) AbsPath(id []byte) (string, error) { return s.path(id) }

type verifiedReader struct {
	file *os.File
	h    hash.Hash
	want []byte
}

func (r *verifiedReader) Read(p []byte) (int, error) {
	n, err := r.file.Read(p)
	if n > 0 {
		r.h.Write(p[:n])
	}
	if errors.Is(err, io.EOF) {
		if got := r.h.Sum(nil); !bytesEqual(got, r.want) {
			return n, fmt.Errorf("%w: stored chunk digest %x, manifest says %x",
				errBadDigest, got, r.want)
		}
	}
	return n, err
}

func (r *verifiedReader) Close() error { return r.file.Close() }

func bytesEqual(a, b []byte) bool {
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

// IsBadDigest reports whether err is a content-digest verification failure.
func IsBadDigest(err error) bool { return errors.Is(err, errBadDigest) }

// joinSafe joins root with a relative manifest path and guarantees the
// result stays within root. Symlinks along the path are handled separately by
// callers (they are never followed during restore), this is a lexical check.
func joinSafe(root, rel string) (string, error) {
	clean := filepath.Clean("/" + rel) // strip any leading ".." segments
	root = filepath.Clean(root)
	joined := filepath.Join(root, clean)
	if joined != root && !strings.HasPrefix(joined, root+string(os.PathSeparator)) {
		return "", fmt.Errorf("path %q escapes repository root", rel)
	}
	return joined, nil
}
