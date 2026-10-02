// Package backup contains the snapshot, verification and restore engine.
package backup

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/restic/chunker"

	"incbackup/internal/repo"
)

// ChunkParams configures content-defined chunking. Smaller boundaries make
// chunk reuse after tiny edits observable on small demo trees; production
// deployments would use chunker.MinSize/chunker.MaxSize (512KiB/8MiB).
type ChunkParams struct {
	Polynomial  chunker.Pol
	MinSize     uint
	MaxSize     uint
	AverageBits int
}

// ScanOptions controls the walk.
type ScanOptions struct {
	Root       string
	Params     ChunkParams
	MaxRetries int           // re-read attempts when a file changes mid-scan
	RetryDelay time.Duration // delay between re-read attempts
	// SettleDelay is a quiet-period check: after a full read passes, wait this
	// long and stat again. A small file can be read entirely between two
	// appends, so pre/post-read stats alone cannot detect an active writer;
	// if anything changed during the settle window the file is re-read.
	SettleDelay time.Duration
	Sink        ChunkSink
}

// ChunkSink receives chunks as files are read. Put stores data under digest
// and reports whether this is a newly created chunk (false = reused).
type ChunkSink interface {
	Put(digest []byte, data []byte) (isNew bool, err error)
}

// ScanError marks one path-level problem. A scan that collects any error is
// rejected wholesale; the engine never turns a partial walk into a success.
type ScanError struct {
	RelPath string
	Message string
}

func (e *ScanError) Error() string { return e.RelPath + ": " + e.Message }

// ScanResult is everything collected during one tree walk.
type ScanResult struct {
	Entries  []repo.Entry
	Errors   []ScanError
	Chunks   map[string]repo.ChunkRef // digest(hex) -> ref, dedup across files
	NewCount int64                    // distinct chunks newly written
	RefCount int64                    // distinct chunks referenced
	Bytes    int64
	Files    int64
	Dirs     int64
}

// relPath converts an absolute walked path into a slash-separated path
// relative to root.
func relPath(root, p string) string {
	rel, err := filepath.Rel(root, p)
	if err != nil {
		rel = p
	}
	return filepath.ToSlash(rel)
}

// Scan walks root without following symlinks, chunks every regular file with
// stable-read retries, and feeds the sink. Entries are emitted in
// filepath.WalkDir order (parent directories before their children).
func Scan(opts ScanOptions) (*ScanResult, error) {
	if opts.MaxRetries < 0 {
		opts.MaxRetries = 0
	}
	res := &ScanResult{Chunks: map[string]repo.ChunkRef{}}

	// filepath.WalkDir does not follow symlinks: dirent types come from
	// readdir, so a symlink-to-a-directory is visited as a symlink.
	err := filepath.WalkDir(opts.Root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			res.Errors = append(res.Errors, ScanError{relPath(opts.Root, path), err.Error()})
			return nil
		}
		info, err := os.Lstat(path)
		if err != nil {
			res.Errors = append(res.Errors, ScanError{relPath(opts.Root, path), err.Error()})
			return nil
		}
		rel := relPath(opts.Root, path)
		switch {
		case info.Mode().IsRegular():
			res.Files++
			e, errs := readStable(opts, path, rel, info, res)
			if len(errs) > 0 {
				res.Errors = append(res.Errors, errs...)
				return nil
			}
			res.Entries = append(res.Entries, e)
			res.Bytes += e.Size
		case info.Mode()&fs.ModeSymlink != 0:
			target, rerr := os.Readlink(path)
			if rerr != nil {
				res.Errors = append(res.Errors, ScanError{rel, rerr.Error()})
				return nil
			}
			res.Entries = append(res.Entries, metaEntry(rel, repo.KindSymlink, info, target))
		case info.IsDir():
			if rel != "." {
				res.Dirs++
			}
			res.Entries = append(res.Entries, metaEntry(rel, repo.KindDir, info, ""))
		default:
			// Sockets, devices, fifos: record explicitly instead of silently
			// skipping — a "successful" backup must not quietly omit things.
			res.Errors = append(res.Errors, ScanError{rel,
				"unsupported file type: " + info.Mode().String()})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for range res.Chunks {
		res.RefCount++
	}
	return res, nil
}

func metaEntry(rel, kind string, info fs.FileInfo, target string) repo.Entry {
	st := statOf(info)
	return repo.Entry{
		RelPath:    rel,
		Kind:       kind,
		Mode:       permBits(info.Mode()),
		UID:        st.uid,
		GID:        st.gid,
		ModTime:    info.ModTime(),
		Size:       info.Size(),
		LinkTarget: target,
	}
}

// readStable reads a regular file through the chunker. It re-reads the whole
// file when size or mtime change while scanning, on read error, or during the
// post-read settle window. After MaxRetries+1 attempts the file is reported
// as still-changing and the snapshot is rejected rather than guessing which
// version is current.
func readStable(opts ScanOptions, path, rel string, first fs.FileInfo, res *ScanResult) (repo.Entry, []ScanError) {
	var lastErr string
	for attempt := 0; attempt <= opts.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(opts.RetryDelay)
		}
		e, changed, errMsg := readAndChunk(opts, path, rel, res)
		if errMsg != "" || changed {
			if errMsg != "" {
				lastErr = errMsg
			} else {
				lastErr = "size or mtime changed during read"
			}
			continue
		}
		if opts.SettleDelay > 0 {
			time.Sleep(opts.SettleDelay)
			now, serr := os.Stat(path)
			if serr != nil {
				lastErr = serr.Error()
				continue
			}
			if now.Size() != e.Size || !now.ModTime().Equal(e.ModTime) {
				lastErr = "file changed during settle window"
				continue
			}
		}
		return e, nil
	}
	return repo.Entry{}, []ScanError{{rel, "file still being written after " +
		fmt.Sprintf("%d attempts: %s", opts.MaxRetries+1, lastErr)}}
}

// readAndChunk performs one pass. changed is true when the post-read stat
// disagrees with the pre-read stat, meaning the bytes captured may be torn.
func readAndChunk(opts ScanOptions, path, rel string, res *ScanResult) (e repo.Entry, changed bool, errMsg string) {
	f, err := os.Open(path)
	if err != nil {
		return repo.Entry{}, false, err.Error()
	}
	defer f.Close()

	pre, err := f.Stat()
	if err != nil {
		return repo.Entry{}, false, err.Error()
	}

	ch := chunker.New(f, opts.Params.Polynomial,
		chunker.WithBoundaries(opts.Params.MinSize, opts.Params.MaxSize))
	ch.SetAverageBits(opts.Params.AverageBits)

	fileHash := sha256.New()
	var refs []repo.ChunkRef
	seq := 0
	var readErr error
	for {
		c, err := ch.Next(nil)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			readErr = err
			break
		}
		if len(c.Data) == 0 {
			continue
		}
		sum := sha256.Sum256(c.Data)
		isNew, perr := opts.Sink.Put(sum[:], c.Data)
		if perr != nil {
			return repo.Entry{}, false, "store chunk: " + perr.Error()
		}
		fileHash.Write(c.Data)
		key := fmt.Sprintf("%x", sum)
		if _, ok := res.Chunks[key]; !ok {
			res.Chunks[key] = repo.ChunkRef{
				Digest: append([]byte(nil), sum[:]...),
				Length: int64(len(c.Data)),
			}
			if isNew {
				res.NewCount++
			}
		}
		refs = append(refs, repo.ChunkRef{
			Digest: append([]byte(nil), sum[:]...),
			Length: int64(len(c.Data)),
			Seq:    seq,
		})
		seq++
	}

	post, statErr := f.Stat()
	if statErr != nil {
		return repo.Entry{}, false, statErr.Error()
	}
	if readErr != nil {
		return repo.Entry{}, false, readErr.Error()
	}
	if pre.Size() != post.Size() || !pre.ModTime().Equal(post.ModTime()) {
		return repo.Entry{}, true, ""
	}

	st := statOf(post)
	e = repo.Entry{
		RelPath:    rel,
		Kind:       repo.KindFile,
		Mode:       permBits(post.Mode()),
		UID:        st.uid,
		GID:        st.gid,
		ModTime:    post.ModTime(),
		Size:       post.Size(),
		FileDigest: fileHash.Sum(nil),
		Chunks:     refs,
	}
	return e, false, ""
}

// unixStat carries owner ids; -1 when unavailable.
type unixStat struct{ uid, gid int }

func statOf(fi fs.FileInfo) unixStat {
	if s, ok := fi.Sys().(*syscall.Stat_t); ok {
		return unixStat{uid: int(s.Uid), gid: int(s.Gid)}
	}
	return unixStat{uid: -1, gid: -1}
}

func permBits(m os.FileMode) uint32 { return uint32(m.Perm()) }
