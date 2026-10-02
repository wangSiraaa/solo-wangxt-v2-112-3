package backup_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

func openEngine(t *testing.T) (*backup.Engine, string) {
	t.Helper()
	dir := t.TempDir()
	m, err := repo.OpenManifest(filepath.Join(dir, "manifest.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	s, err := repo.NewContentStore(filepath.Join(dir, "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	e, err := backup.NewEngine(m, s)
	if err != nil {
		t.Fatal(err)
	}
	return e, dir
}

func writeTree(t *testing.T, root string, files map[string]string, modes map[string]os.FileMode) {
	t.Helper()
	for rel, content := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		mode := os.FileMode(0o644)
		if m, ok := modes[rel]; ok {
			mode = m
		}
		if err := os.WriteFile(p, []byte(content), mode); err != nil {
			t.Fatal(err)
		}
	}
	for rel, mode := range modes {
		if _, ok := files[rel]; ok {
			_ = os.Chmod(filepath.Join(root, filepath.FromSlash(rel)), mode)
		}
	}
}

func TestSnapshotRestoreVerifiesDigestAndLength(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o750); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"a.txt":        "hello\n",
		"EMPTY":        "",
		"sub/run.sh":   "#!/bin/sh\necho x\n",
		"sub/notes.md": strings.Repeat("abcdefgh\n", 4000),
	}
	writeTree(t, src, files, map[string]os.FileMode{"sub": 0o750, "sub/run.sh": 0o755, "EMPTY": 0o600})

	res, err := e.CreateSnapshot(src, "base", true)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if res.Status != repo.StatusCommitted {
		t.Fatalf("status=%s", res.Status)
	}

	target := filepath.Join(dir, "restored")
	rr, err := e.Restore(res.SnapshotID, target)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if rr.Files != len(files) {
		t.Fatalf("files=%d want %d", rr.Files, len(files))
	}
	// every verified report must match an independent recomputation
	for _, rep := range rr.Verified {
		gotSum, gotLen := fileSHA(t, filepath.Join(target, filepath.FromSlash(rep.RelPath)))
		if gotLen != rep.Size || gotSum != rep.Digest {
			t.Fatalf("%s report mismatch: %d/%s vs disk %d/%s",
				rep.RelPath, rep.Size, rep.Digest, gotLen, gotSum)
		}
		want := files[rep.RelPath]
		if gotLen != int64(len(want)) {
			t.Fatalf("%s length %d want %d", rep.RelPath, gotLen, len(want))
		}
		if gotSum != sha256Hex([]byte(want)) {
			t.Fatalf("%s digest mismatch", rep.RelPath)
		}
	}
	// modes preserved
	for _, c := range []struct {
		rel  string
		mode os.FileMode
	}{
		{"sub", 0o750}, {"sub/run.sh", 0o755}, {"EMPTY", 0o600},
	} {
		fi, err := os.Lstat(filepath.Join(target, filepath.FromSlash(c.rel)))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != c.mode {
			t.Fatalf("%s mode=%o want %o", c.rel, fi.Mode().Perm(), c.mode)
		}
	}
}

func TestSmallEditReusesChunks(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	big := strings.Repeat("0123456789ABCDEF\n", 12000) // ~180KiB -> many chunks
	must(t, os.WriteFile(filepath.Join(src, "big.bin"), []byte(big), 0o644))

	r1, err := e.CreateSnapshot(src, "v1", true)
	if err != nil {
		t.Fatal(err)
	}
	if r1.NewChunks != r1.RefChunks || r1.NewChunks <= 1 {
		t.Fatalf("first snapshot new=%d ref=%d", r1.NewChunks, r1.RefChunks)
	}

	buf := []byte(big)
	copy(buf[90*1024:], []byte("PATCHED IN THE MIDDLE"))
	must(t, os.WriteFile(filepath.Join(src, "big.bin"), buf, 0o644))
	r2, err := e.CreateSnapshot(src, "v2", true)
	if err != nil {
		t.Fatal(err)
	}
	if r2.NewChunks != 1 {
		t.Fatalf("expected exactly 1 new chunk from a middle edit, got %d (ref=%d)",
			r2.NewChunks, r2.RefChunks)
	}
	if r2.RefChunks-r2.NewChunks != r1.RefChunks-1 {
		t.Fatalf("expected %d reused chunks, got %d",
			r1.RefChunks-1, r2.RefChunks-r2.NewChunks)
	}
}

func TestActivelyWrittenFileIsRejected(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	g := filepath.Join(src, "growing.log")
	must(t, os.WriteFile(g, []byte("x\n"), 0o644))

	var wg sync.WaitGroup
	wg.Add(1)
	stop := make(chan struct{})
	go func() {
		defer wg.Done()
		f, _ := os.OpenFile(g, os.O_APPEND|os.O_WRONLY, 0o644)
		defer f.Close()
		for {
			select {
			case <-stop:
				return
			default:
				fmt.Fprintf(f, "%s\n", strings.Repeat("z", 300))
				time.Sleep(time.Millisecond)
			}
		}
	}()
	time.Sleep(20 * time.Millisecond)
	_, err := e.CreateSnapshot(src, "race", true)
	close(stop)
	wg.Wait()

	var rej *backup.ErrRejected
	if !errors.As(err, &rej) {
		t.Fatalf("want ErrRejected, got %v", err)
	}
	found := false
	for _, r := range rej.Reasons {
		if strings.Contains(r, "growing.log") && strings.Contains(r, "still being written") {
			found = true
		}
	}
	if !found {
		t.Fatalf("growing.log not named in %v", rej.Reasons)
	}
}

func TestSettlingWriterIsRereadAndAccepted(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	g := filepath.Join(src, "g.log")
	must(t, os.WriteFile(g, []byte("start\n"), 0o644))
	go func() {
		f, _ := os.OpenFile(g, os.O_APPEND|os.O_WRONLY, 0o644)
		for i := 0; i < 4; i++ {
			fmt.Fprintf(f, "more %d\n", i)
			time.Sleep(15 * time.Millisecond)
		}
		f.Close()
	}()
	// Eventually the writer stops; repeated snapshots must succeed then.
	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		_, lastErr = e.CreateSnapshot(src, "settle", true)
		if lastErr == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("snapshot never succeeded after writer settled: %v", lastErr)
}

func TestRestoreRefusesExistingTargetAndEscapeSymlink(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(filepath.Join(src, "d"), 0o755))
	writeTree(t, src, map[string]string{"d/f.txt": "fine\n"}, nil)
	outside := filepath.Join(dir, "outside.txt")
	must(t, os.WriteFile(outside, []byte("secret"), 0o600))
	rel, _ := filepath.Rel(src, outside)
	must(t, os.Symlink(rel, filepath.Join(src, "escape_link")))

	r, err := e.CreateSnapshot(src, "links", true)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "out")
	must(t, os.MkdirAll(target, 0o755))
	if _, err := e.Restore(r.SnapshotID, target); !errors.Is(err, backup.ErrTargetExists) {
		t.Fatalf("want ErrTargetExists, got %v", err)
	}
	target2 := filepath.Join(dir, "out2")
	_, err = e.Restore(r.SnapshotID, target2)
	if err == nil || !strings.Contains(err.Error(), "escapes restore root") {
		t.Fatalf("want escape error, got %v", err)
	}
	if _, statErr := os.Lstat(target2); !os.IsNotExist(statErr) {
		t.Fatal("failed restore must not leave a tree behind")
	}
	if _, err := os.ReadFile(filepath.Join(target2, "escape_link")); err == nil {
		t.Fatal("outside file must not be reachable")
	}
}

func TestSymlinkMetadataDoesNotTouchTarget(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	secret := filepath.Join(src, "secret.txt")
	must(t, os.WriteFile(secret, []byte("0600 content"), 0o600))
	must(t, os.Symlink("secret.txt", filepath.Join(src, "l")))

	r, err := e.CreateSnapshot(src, "links", true)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(dir, "out")
	if _, err := e.Restore(r.SnapshotID, target); err != nil {
		t.Fatalf("restore: %v", err)
	}
	fi, err := os.Lstat(filepath.Join(target, "secret.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("symlink metadata pass changed target mode to %o", fi.Mode().Perm())
	}
	tgt, err := os.Readlink(filepath.Join(target, "l"))
	if err != nil || tgt != "secret.txt" {
		t.Fatalf("link target = %q, %v", tgt, err)
	}
}

func TestMissingChunkIsDiagnosed(t *testing.T) {
	e, dir := openEngine(t)
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	writeTree(t, src, map[string]string{"a.txt": strings.Repeat("a", 5000)}, nil)

	r, err := e.CreateSnapshot(src, "ok", true)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate commit interruption on a second snapshot: scan stores all
	// blobs, then one brand-new blob (unique to this snapshot) disappears
	// before verification.
	must(t, os.WriteFile(filepath.Join(src, "a.txt"),
		[]byte(strings.Repeat("a", 5000)+strings.Repeat("b", 9000)), 0o644))
	e.Fail.LoseChunkCount = 1
	r2, err := e.CreateSnapshot(src, "interrupted", true)
	e.Fail.LoseChunkCount = 0
	if !errors.As(err, new(*backup.ErrRejected)) {
		t.Fatalf("want rejection, got status=%v err=%v", r2, err)
	}
	missing, err := e.Manifest.FindMissingChunks(r2.SnapshotID,
		func(digest []byte, length int64) (bool, error) { return e.Store.Has(digest, length) })
	if err != nil {
		t.Fatal(err)
	}
	if len(missing) != 1 {
		t.Fatalf("want 1 missing chunk, got %d", len(missing))
	}
	mc := missing[0]
	if mc.RelPath != "a.txt" {
		t.Fatalf("missing chunk attributed to %q", mc.RelPath)
	}
	if hexLen := hex.EncodedLen(len(mc.Digest)); hexLen != 64 {
		t.Fatalf("digest len %d", hexLen)
	}
	p, _ := e.Store.Path(mc.Digest)
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatalf("blob %s should be gone", p)
	}
	if _, err := e.Restore(r2.SnapshotID, filepath.Join(dir, "nope")); err == nil {
		t.Fatal("failed snapshot must not restore")
	}
	// baseline snapshot remains intact and restorable
	if _, err := e.Restore(r.SnapshotID, filepath.Join(dir, "baseline-out")); err != nil {
		t.Fatalf("baseline restore: %v", err)
	}
}

func TestPendingRecoversAfterRestart(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	must(t, os.MkdirAll(src, 0o755))
	writeTree(t, src, map[string]string{"f": "data\n"}, nil)

	open := func() *backup.Engine {
		m, err := repo.OpenManifest(filepath.Join(dir, "manifest.sqlite"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { m.Close() })
		s, err := repo.NewContentStore(filepath.Join(dir, "chunks"))
		if err != nil {
			t.Fatal(err)
		}
		e, err := backup.NewEngine(m, s)
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := open()
	r, err := e.CreateSnapshot(src, "crash", false)
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != repo.StatusPending {
		t.Fatalf("want pending, got %s", r.Status)
	}
	e.Manifest.Close()

	e2 := open()
	out, err := e2.RecoverPending()
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Status != repo.StatusCommitted {
		t.Fatalf("recovery = %+v", out)
	}
}

func fileSHA(t *testing.T, p string) (string, int64) {
	t.Helper()
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil)), n
}

func sha256Hex(b []byte) string {
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
