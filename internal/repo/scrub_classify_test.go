package repo_test

import (
	"os"
	"path/filepath"
	"testing"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

func openScrubEngine(t *testing.T) (*backup.Engine, string) {
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

// The patrol classifies storage failures distinctly and locatably, not just
// digest mismatches: a deleted blob is missing_blob; a same-name resized blob
// is length_mismatch.
func TestScrubResultClassification(t *testing.T) {
	e, dir := openScrubEngine(t)
	src := filepath.Join(dir, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	// Two files -> distinct chunk sets in one snapshot.
	if err := os.WriteFile(filepath.Join(src, "gone.txt"), []byte("this blob will be deleted-xyz"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "resized.txt"), []byte("this blob will be resized-abcdef"), 0o644); err != nil {
		t.Fatal(err)
	}
	r, err := e.CreateSnapshot(src, "v", true)
	if err != nil {
		t.Fatal(err)
	}

	refs, err := e.Manifest.ChunkReferencesOfSnapshot(r.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	var goneDigest, resizedDigest []byte
	for _, ci := range refs {
		switch ci.RelPath {
		case "gone.txt":
			goneDigest = ci.Digest
		case "resized.txt":
			resizedDigest = ci.Digest
		}
	}
	if goneDigest == nil || resizedDigest == nil {
		t.Fatalf("chunks not located: gone=%v resized=%v", goneDigest != nil, resizedDigest != nil)
	}

	// Delete one blob outright; append bytes to the other (same name, wrong
	// size) while keeping it readable.
	if err := e.Store.Remove(goneDigest); err != nil {
		t.Fatal(err)
	}
	rp, err := e.Store.Path(resizedDigest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(rp, 0o644); err != nil {
		t.Fatal(err)
	}
	existing, err := os.ReadFile(rp)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rp, append(existing, []byte("EXTRA-BYTES")...), 0o644); err != nil {
		t.Fatal(err)
	}

	prog, _, err := e.StartScrub(backup.ScrubOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// wait for done by polling
	for {
		p, err := e.ScrubProgressOf(prog.RunID)
		if err != nil {
			t.Fatal(err)
		}
		if p.Status == repo.ScrubStatusDone && !p.Running {
			break
		}
	}

	want := map[string]string{
		"gone.txt":    repo.ChunkResultMissingBlob,
		"resized.txt": repo.ChunkResultLengthMismatch,
	}
	for rel, wantResult := range want {
		// Find the digest for this rel and inspect its verdict row.
		var digest []byte
		for _, ci := range refs {
			if ci.RelPath == rel {
				digest = ci.Digest
			}
		}
		result, affected, err := e.Manifest.ScrubAffected(prog.RunID, digest)
		if err != nil {
			t.Fatalf("affected %s: %v", rel, err)
		}
		if result != wantResult {
			t.Errorf("%s: result=%s want %s", rel, result, wantResult)
		}
		found := false
		for _, ref := range affected {
			if ref.SnapshotID == r.SnapshotID && ref.RelPath == rel {
				found = true
			}
		}
		if !found {
			t.Errorf("%s: affected list does not name snapshot/path", rel)
		}
	}

	// The snapshot is suspect, and restore is refused naming both modes.
	_, err = e.Restore(r.SnapshotID, filepath.Join(dir, "out"))
	if err == nil {
		t.Fatal("suspect snapshot restored")
	}
	bad, err := e.Manifest.ScrubBadChunksForSnapshot(prog.RunID, r.SnapshotID)
	if err != nil {
		t.Fatal(err)
	}
	results := map[string]bool{}
	for _, bc := range bad {
		results[bc.Result] = true
	}
	if !results[repo.ChunkResultMissingBlob] || !results[repo.ChunkResultLengthMismatch] {
		t.Fatalf("bad chunks missing classifications: %v", results)
	}
}
