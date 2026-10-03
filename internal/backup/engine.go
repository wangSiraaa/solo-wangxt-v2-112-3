package backup

import (
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/restic/chunker"

	"incbackup/internal/repo"
)

// Default demo-friendly chunk parameters. 8KiB average chunks mean a one-line
// edit in the middle of a file only produces 1-2 new chunks.
const (
	DefaultMinSize     = 2 * 1024
	DefaultMaxSize     = 64 * 1024
	DefaultAverageBits = 13 // ~8KiB
)

// Failpoints are deliberately injected faults for the demo and tests.
type Failpoints struct {
	// LoseChunkCount: after a successful scan, delete this many referenced
	// blob files from the content store before verification runs,
	// simulating a crash/loss during commit finalization.
	LoseChunkCount int
}

// Engine ties the manifest and content store together.
type Engine struct {
	Manifest *repo.Manifest
	Store    *repo.ContentStore
	Pol      chunker.Pol
	Fail     Failpoints

	mu sync.Mutex // serializes snapshots: scan + commit is one critical section

	scrubMu sync.Mutex   // guards scrub below
	scrub   *activeScrub // non-nil while a scrub worker runs in this process
}

// NewEngine opens an engine, loading the repository's chunking polynomial
// (or generating and persisting a fresh random one on first use).
func NewEngine(m *repo.Manifest, s *repo.ContentStore) (*Engine, error) {
	pol, ok, err := m.GetPolynomial()
	if err != nil {
		return nil, err
	}
	if !ok {
		p, err := chunker.RandomPolynomial()
		if err != nil {
			return nil, err
		}
		pol = uint64(p)
		if err := m.SetPolynomial(pol); err != nil {
			return nil, err
		}
	}
	return &Engine{Manifest: m, Store: s, Pol: chunker.Pol(pol)}, nil
}

// CreateSnapshotResult reports what happened for one snapshot request.
type CreateSnapshotResult struct {
	SnapshotID int64
	Status     string
	Errors     []repo.SnapshotError
	NewChunks  int64
	RefChunks  int64
}

// storeSink adapts the content store to backup.ChunkSink and counts new blobs.
type storeSink struct{ e *Engine }

func (s storeSink) Put(digest, data []byte) (bool, error) {
	p, err := s.e.Store.Path(digest)
	if err != nil {
		return false, err
	}
	_, statErr := os.Stat(p)
	exists := statErr == nil
	if err := s.e.Store.Put(digest, data); err != nil {
		return false, err
	}
	return !exists, nil
}

// ErrRejected is returned when a snapshot cannot be made (unstable file,
// unsupported entry...). The pending snapshot row is kept and marked failed
// so the reason is inspectable.
type ErrRejected struct {
	SnapshotID int64
	Reasons    []string
}

func (e *ErrRejected) Error() string {
	return fmt.Sprintf("snapshot %d rejected: %s", e.SnapshotID, strings.Join(e.Reasons, "; "))
}

// CreateSnapshot scans root, persists a pending manifest, verifies every
// referenced chunk against the live store, and only then commits. Any failure
// leaves a failed (or, on hard crash, pending) snapshot with detailed errors.
func (e *Engine) CreateSnapshot(root, message string, finish bool) (*CreateSnapshotResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(root) // Eval symlinks in the requested root itself
	if err != nil {
		return nil, fmt.Errorf("snapshot root: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("snapshot root %q is not a directory", root)
	}

	id, err := e.Manifest.BeginSnapshot(root, uint64(e.Pol), message)
	if err != nil {
		return nil, err
	}
	res := &CreateSnapshotResult{SnapshotID: id, Status: repo.StatusPending}

	scan, err := Scan(ScanOptions{
		Root:        root,
		Params:      ChunkParams{e.Pol, DefaultMinSize, DefaultMaxSize, DefaultAverageBits},
		MaxRetries:  2,
		RetryDelay:  50 * time.Millisecond,
		SettleDelay: 25 * time.Millisecond,
		Sink:        storeSink{e},
	})
	if err != nil {
		return res, e.fail(id, "scan", "", nil, err)
	}
	if len(scan.Errors) > 0 {
		var reasons []string
		for _, se := range scan.Errors {
			_ = e.Manifest.AddError(id, "scan", se.RelPath, nil, se.Message)
			reasons = append(reasons, se.RelPath+": "+se.Message)
		}
		_ = e.Manifest.MarkFailed(id)
		res.Status = repo.StatusFailed
		errs, _ := e.Manifest.ListErrors(id)
		res.Errors = errs
		return res, &ErrRejected{SnapshotID: id, Reasons: reasons}
	}

	if err := e.Manifest.SaveSnapshotContents(id, scan.Entries,
		scan.NewCount, scan.RefCount, scan.Bytes, scan.Files, scan.Dirs); err != nil {
		return res, e.fail(id, "commit", "", nil, err)
	}
	res.NewChunks = scan.NewCount
	res.RefChunks = scan.RefCount

	if !finish {
		// Demo fault: process dies right after the pending manifest exists.
		return res, nil
	}

	if err := e.injectChunkLoss(id); err != nil {
		return res, e.fail(id, "commit", "", nil, err)
	}

	return e.verifyAndFinalize(id)
}

// injectChunkLoss implements the lose-chunks failpoint.
func (e *Engine) injectChunkLoss(id int64) error {
	if e.Fail.LoseChunkCount <= 0 {
		return nil
	}
	refs, err := e.Manifest.SingletonChunks(id, e.Fail.LoseChunkCount)
	if err != nil {
		return err
	}
	for _, c := range refs {
		if err := e.Store.Remove(c.Digest); err != nil {
			return err
		}
	}
	if len(refs) < e.Fail.LoseChunkCount {
		return fmt.Errorf("failpoint: only %d singleton chunks available", len(refs))
	}
	return nil
}

func (e *Engine) fail(id int64, stage, rel string, digest []byte, cause error) error {
	_ = e.Manifest.AddError(id, stage, rel, digest, cause.Error())
	_ = e.Manifest.MarkFailed(id)
	return cause
}

// VerifyAndFinalize checks every content chunk of a pending snapshot exists
// in the store with the correct length, then commits. On any missing chunk
// the snapshot stays failed and each missing chunk is recorded with the
// affected file path and digest — that is the report maintenance follows.
func (e *Engine) VerifyAndFinalize(id int64) (*CreateSnapshotResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.verifyAndFinalize(id)
}

func (e *Engine) verifyAndFinalize(id int64) (*CreateSnapshotResult, error) {
	res := &CreateSnapshotResult{SnapshotID: id, Status: repo.StatusPending}
	info, err := e.Manifest.GetSnapshot(id)
	if err != nil {
		return res, err
	}
	res.NewChunks, res.RefChunks = info.ChunksNew, info.ChunksRef

	missing, err := e.Manifest.FindMissingChunks(id, func(digest []byte, length int64) (bool, error) {
		return e.Store.Has(digest, length)
	})
	if err != nil {
		return res, e.fail(id, "verify", "", nil, err)
	}
	for _, mc := range missing {
		msg := mc.Reason
		if mc.Length >= 0 {
			msg += fmt.Sprintf(" (declared length %d)", mc.Length)
		}
		_ = e.Manifest.AddError(id, "verify", mc.RelPath, mc.Digest, msg)
	}
	if len(missing) > 0 {
		_ = e.Manifest.MarkFailed(id)
		res.Status = repo.StatusFailed
		res.Errors, _ = e.Manifest.ListErrors(id)
		var reasons []string
		for _, mc := range missing {
			reasons = append(reasons, mc.RelPath+": chunk "+hex.EncodeToString(mc.Digest)+" ("+mc.Reason+")")
		}
		return res, &ErrRejected{SnapshotID: id, Reasons: reasons}
	}

	if err := e.Manifest.MarkCommitted(id); err != nil {
		return res, e.fail(id, "commit", "", nil, err)
	}
	res.Status = repo.StatusCommitted
	return res, nil
}

// RecoverPending finds snapshots abandoned before commit (e.g. server killed
// mid-finalize), verifies each now, and either commits or fails it.
func (e *Engine) RecoverPending() ([]CreateSnapshotResult, error) {
	pending, err := e.Manifest.PendingSnapshots()
	if err != nil {
		return nil, err
	}
	var out []CreateSnapshotResult
	for _, p := range pending {
		res, _ := e.VerifyAndFinalize(p.ID)
		if res != nil {
			out = append(out, *res)
		}
	}
	return out, nil
}

// ErrTargetExists is returned when the restore destination already exists:
// restore never overwrites.
var ErrTargetExists = errors.New("restore destination already exists")

// RestoreResult is the manifest-to-disk verification report of a restore.
type RestoreResult struct {
	SnapshotID int64
	Target     string
	Files      int
	Dirs       int
	Symlinks   int
	Bytes      int64
	Verified   []FileReport // per-file digest + length, recomputed from restored bytes
}

// FileReport is one restored file with its checked measurements.
type FileReport struct {
	RelPath    string      `json:"rel_path"`
	Size       int64       `json:"size"`
	Digest     string      `json:"digest"`
	Mode       os.FileMode `json:"mode"`
	ChunkCount int         `json:"chunk_count"`
}

// Restore writes a committed snapshot into a brand-new directory target.
// Target must not exist. Symlinks are recreated as links (never followed),
// no link target may escape target, and every regular file is assembled from
// verified chunks then checked against its stored length and SHA-256.
func (e *Engine) Restore(snapshotID int64, target string) (*RestoreResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	info, err := e.Manifest.GetSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}
	if info.Status != repo.StatusCommitted {
		return nil, fmt.Errorf("snapshot %d is %s, only committed snapshots can be restored",
			snapshotID, info.Status)
	}

	// Early, locatable refusal: if the most recent integrity scrub proved a
	// referenced chunk corrupt/missing, do not start writing a half tree —
	// name every bad chunk, its disk path and all affected snapshots/paths.
	if err := e.GuardSuspectSnapshot(snapshotID); err != nil {
		return nil, err
	}

	target, err = filepath.Abs(filepath.FromSlash(target))
	if err != nil {
		return nil, err
	}
	// Refuse to descend through a pre-existing symlink on any ancestor: the
	// destination must be a real path, not an alias into somewhere else.
	if err := noSymlinkAncestors(target); err != nil {
		return nil, err
	}
	// Never overwrite or merge with anything at the destination.
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			return nil, fmt.Errorf("%w: %s", ErrTargetExists, target)
		}
		return nil, err
	}

	entries, err := e.Manifest.EntriesOf(snapshotID)
	if err != nil {
		return nil, err
	}
	if err := validateEntryPaths(entries); err != nil {
		return nil, err
	}

	res := &RestoreResult{SnapshotID: snapshotID, Target: target}
	// Track created dirs for a post-order mtime/permission finalize; children
	// must be writable while we populate the tree.
	var createdDirs []string
	cleanup := true
	defer func() {
		if cleanup {
			os.RemoveAll(target) // do not leave a half-restored tree behind
		}
	}()

	if err := os.MkdirAll(target, 0o755); err != nil {
		return nil, err
	}

	for _, se := range entries {
		if se.RelPath == "." {
			// Root entry is represented by target itself, created above.
			continue
		}
		dst, err := safeJoin(target, se.RelPath)
		if err != nil {
			return nil, err
		}
		switch se.Kind {
		case repo.KindDir:
			if err := os.Mkdir(dst, os.FileMode(se.Mode).Perm()); err != nil {
				return nil, fmt.Errorf("mkdir %s: %w", se.RelPath, err)
			}
			createdDirs = append(createdDirs, dst)
			res.Dirs++
		case repo.KindSymlink:
			if err := validateLinkTarget(target, se.RelPath, se.LinkTarget); err != nil {
				return nil, err
			}
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return nil, err
			}
			// O_EXCL-equivalent: Symlink fails if the leaf already exists.
			if err := os.Symlink(se.LinkTarget, dst); err != nil {
				if errors.Is(err, os.ErrExist) {
					return nil, fmt.Errorf("refusing to overwrite existing file at %s", se.RelPath)
				}
				return nil, fmt.Errorf("symlink %s: %w", se.RelPath, err)
			}
			res.Symlinks++
		case repo.KindFile:
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return nil, err
			}
			rep, err := e.restoreFile(se, dst)
			if err != nil {
				return nil, fmt.Errorf("restore %s: %w", se.RelPath, err)
			}
			res.Verified = append(res.Verified, rep)
			res.Files++
			res.Bytes += rep.Size
		default:
			return nil, fmt.Errorf("unknown entry kind %q for %s", se.Kind, se.RelPath)
		}
	}

	// Metadata: files/symlinks first, then dirs bottom-up (children already
	// created, so restrictive dir modes are safe to apply now). Symlinks are
	// handled with lchown only: chmod/chtimes follow the link and would alter
	// the linked file instead of the link itself, and on Linux symlink
	// permission bits are not meaningful — preserving the link means
	// preserving the target string, which was done at creation.
	for _, se := range entries {
		if se.RelPath == "." || se.Kind == repo.KindDir {
			continue
		}
		dst, _ := safeJoin(target, se.RelPath)
		if se.Kind == repo.KindSymlink {
			if se.UID >= 0 {
				lchown(dst, se.UID, se.GID)
			}
			continue
		}
		chmod(dst, se.Mode)
		if se.UID >= 0 {
			chown(dst, se.UID, se.GID)
		}
		os.Chtimes(dst, se.ModTime, se.ModTime)
	}
	for i := len(createdDirs) - 1; i >= 0; i-- {
		d := createdDirs[i]
		_ = os.Chmod(d, 0o755) // writable while siblings still finalizing
	}
	// second pass: exact mode + mtime bottom-up, including the target root
	// which carries the mode/mtime of the source root entry (".").
	dirMode := map[string]uint32{}
	for _, se := range entries {
		if se.Kind != repo.KindDir {
			continue
		}
		if se.RelPath == "." {
			dirMode[target] = se.Mode
			continue
		}
		d, _ := safeJoin(target, se.RelPath)
		dirMode[d] = se.Mode
	}
	for i := len(createdDirs) - 1; i >= 0; i-- {
		d := createdDirs[i]
		chmod(d, dirMode[d])
		if se := findDir(entries, d, target); se != nil {
			if se.UID >= 0 {
				chown(d, se.UID, se.GID)
			}
			os.Chtimes(d, se.ModTime, se.ModTime)
		}
	}
	if root := findRoot(entries); root != nil {
		chmod(target, root.Mode)
		if root.UID >= 0 {
			chown(target, root.UID, root.GID)
		}
		os.Chtimes(target, root.ModTime, root.ModTime)
	}

	cleanup = false
	return res, nil
}

// restoreFile creates one regular file with O_CREATE|O_EXCL, streams every
// referenced chunk (each digest-checked by the store reader), then compares
// total length and the whole-file SHA-256 with the manifest.
func (e *Engine) restoreFile(se repo.StoredEntry, dst string) (FileReport, error) {
	// O_EXCL: never overwrite an existing leaf, even a pre-existing symlink
	// that would redirect the open outside target.
	f, err := openExclusiveFile(dst, os.FileMode(se.Mode).Perm())
	if err != nil {
		return FileReport{}, err
	}

	h := newFileHasher()
	var total int64
	var count int
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(dst)
		}
	}()

	for _, d := range se.ChunkDigests {
		rc, err := e.Store.Open(d) // verifies chunk digest while streaming
		if err != nil {
			return FileReport{}, err
		}
		n, err := copyChunks(f, rc, h)
		rc.Close()
		if err != nil {
			return FileReport{}, err
		}
		total += n
		count++
	}
	if err := f.Sync(); err != nil {
		return FileReport{}, err
	}
	if err := f.Close(); err != nil {
		return FileReport{}, err
	}

	if total != se.Size {
		return FileReport{}, fmt.Errorf("length mismatch: restored %d bytes, manifest says %d", total, se.Size)
	}
	got := h.checksum()
	if se.FileDigest != nil && !equalBytes(got, se.FileDigest) {
		return FileReport{}, fmt.Errorf("digest mismatch: restored %x, manifest %x", got, se.FileDigest)
	}
	ok = true
	return FileReport{
		RelPath:    se.RelPath,
		Size:       total,
		Digest:     hex.EncodeToString(got),
		Mode:       os.FileMode(se.Mode).Perm(),
		ChunkCount: count,
	}, nil
}

func findRoot(es []repo.StoredEntry) *repo.StoredEntry {
	for i := range es {
		if es[i].Kind == repo.KindDir && es[i].RelPath == "." {
			return &es[i]
		}
	}
	return nil
}

func findDir(es []repo.StoredEntry, dst, target string) *repo.StoredEntry {
	rel, err := filepath.Rel(target, dst)
	if err != nil {
		return nil
	}
	rel = filepath.ToSlash(rel)
	for i := range es {
		if es[i].Kind == repo.KindDir && es[i].RelPath == rel {
			return &es[i]
		}
	}
	return nil
}

// noSymlinkAncestors walks existing prefixes of p with Lstat and fails if any
// component is a symlink. Components that do not exist yet are fine (we create
// them ourselves).
func noSymlinkAncestors(p string) error {
	cur := p
	for {
		fi, err := os.Lstat(cur)
		if errors.Is(err, os.ErrNotExist) {
			parent := filepath.Dir(cur)
			if parent == cur {
				break
			}
			cur = parent
			continue
		}
		if err != nil {
			return err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing to restore through symlink ancestor %q", cur)
		}
		break
	}
	return nil
}

// safeJoin joins target with a manifest rel path and guarantees the result is
// target or below it. Manifest paths are slash-relative.
func safeJoin(target, rel string) (string, error) {
	if filepath.IsAbs(filepath.FromSlash(rel)) {
		return "", fmt.Errorf("absolute path in manifest: %q", rel)
	}
	clean := filepath.Clean("/" + filepath.FromSlash(rel))
	dst := filepath.Join(target, clean)
	if dst != target && !strings.HasPrefix(dst, target+string(os.PathSeparator)) {
		return "", fmt.Errorf("path %q escapes restore root", rel)
	}
	return dst, nil
}

// validateEntryPaths rejects absolute and ".."-escaping manifest paths before
// anything is created.
func validateEntryPaths(es []repo.StoredEntry) error {
	for _, se := range es {
		p := filepath.FromSlash(se.RelPath)
		if filepath.IsAbs(p) {
			return fmt.Errorf("absolute path in manifest: %q", se.RelPath)
		}
		clean := filepath.Clean(p)
		if clean == ".." || strings.HasPrefix(clean, ".."+string(os.PathSeparator)) {
			return fmt.Errorf("path escapes root: %q", se.RelPath)
		}
	}
	return nil
}

// validateLinkTarget resolves a symlink target lexically (relative to the
// directory containing the link) and requires it to stay within target. The
// link is created after this check, and restore never follows links.
func validateLinkTarget(target, rel, linkTo string) error {
	p := filepath.FromSlash(linkTo)
	var resolved string
	if filepath.IsAbs(p) {
		resolved = filepath.Clean(p)
	} else {
		resolved = filepath.Clean(filepath.Join(target, filepath.Dir(filepath.FromSlash(rel)), p))
	}
	if resolved != target && !strings.HasPrefix(resolved, target+string(os.PathSeparator)) {
		return fmt.Errorf("symlink %q -> %q escapes restore root", rel, linkTo)
	}
	return nil
}
