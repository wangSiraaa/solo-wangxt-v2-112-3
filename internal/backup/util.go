package backup

import (
	"crypto/sha256"
	"io"
	"os"
)

// fileHasher streams the whole-file SHA-256 while chunks are written to disk,
// so the restored digest is verified without reading the file back.
type fileHasher struct {
	h interface{ Write([]byte) (int, error) }
}

func newFileHasher() *fileHasher { return &fileHasher{h: sha256.New()} }

func (f *fileHasher) Write(p []byte) (int, error) { return f.h.Write(p) }
func (f *fileHasher) checksum() []byte            { return f.h.(interface{ Sum([]byte) []byte }).Sum(nil) }

func copyChunks(w io.Writer, r io.Reader, h *fileHasher) (int64, error) {
	buf := make([]byte, 128*1024)
	var n int64
	for {
		nr, er := r.Read(buf)
		if nr > 0 {
			nw, ew := w.Write(buf[:nr])
			_, _ = h.Write(buf[:nw])
			if nw < nr {
				return n, io.ErrShortWrite
			}
			n += int64(nw)
			if ew != nil {
				return n, ew
			}
		}
		if er == io.EOF {
			return n, nil
		}
		if er != nil {
			return n, er
		}
	}
}

func equalBytes(a, b []byte) bool {
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

func chmod(path string, mode uint32) { _ = os.Chmod(path, os.FileMode(mode).Perm()) }
