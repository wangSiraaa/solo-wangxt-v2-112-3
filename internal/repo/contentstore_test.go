package repo

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
)

func TestContentStoreRoundTripAndVerify(t *testing.T) {
	dir := t.TempDir()
	s, err := NewContentStore(filepath.Join(dir, "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("content-defined chunking payload")
	sum := sha256.Sum256(data)

	if err := s.Put(sum[:], data); err != nil {
		t.Fatalf("put: %v", err)
	}
	// second Put is a no-op dedup hit
	if err := s.Put(sum[:], data); err != nil {
		t.Fatalf("dedup put: %v", err)
	}
	ok, err := s.Has(sum[:], int64(len(data)))
	if err != nil || !ok {
		t.Fatalf("has = %v, %v", ok, err)
	}
	// wrong length must fail Has
	ok, _ = s.Has(sum[:], int64(len(data))+1)
	if ok {
		t.Fatal("Has should reject wrong length")
	}
	// wrong digest must be rejected outright
	bad := sha256.Sum256([]byte("other"))
	if err := s.Put(bad[:], data); err == nil {
		t.Fatal("Put with mismatched digest must fail")
	}

	rc, err := s.Open(sum[:])
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	if _, err := got.ReadFrom(rc); err != nil {
		t.Fatalf("read verified: %v", err)
	}
	rc.Close()
	if !bytes.Equal(got.Bytes(), data) {
		t.Fatal("roundtrip mismatch")
	}
}

func TestContentStoreDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewContentStore(filepath.Join(dir, "chunks"))
	data := []byte("immutable chunk body")
	sum := sha256.Sum256(data)
	if err := s.Put(sum[:], data); err != nil {
		t.Fatal(err)
	}
	// corrupt the blob on disk (chunks are 0444, so chmod first)
	p, _ := s.Path(sum[:])
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("tampered"), 0o644); err != nil {
		t.Fatal(err)
	}
	rc, err := s.Open(sum[:])
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	var readErr error
	for {
		_, err := rc.Read(buf)
		if err != nil {
			readErr = err
			break
		}
	}
	if readErr == nil || !IsBadDigest(readErr) {
		t.Fatalf("want digest error, got %v", readErr)
	}
}
