package repo

import "time"

// Entry kinds stored in the manifest.
const (
	KindFile    = "file"
	KindDir     = "dir"
	KindSymlink = "symlink"
)

// ChunkRef points at one content-addressed chunk that makes up part of a
// regular file, in seq order.
type ChunkRef struct {
	Digest []byte
	Length int64
	Seq    int
}

// Entry is one filesystem object recorded in a snapshot.
type Entry struct {
	RelPath    string // slash-separated, relative to snapshot root, never leading /
	Kind       string
	Mode       uint32 // permission bits only
	UID        int
	GID        int
	ModTime    time.Time
	Size       int64
	FileDigest []byte // whole-file SHA-256 (files, including empty files)
	LinkTarget string // symlinks only
	Chunks     []ChunkRef
}
