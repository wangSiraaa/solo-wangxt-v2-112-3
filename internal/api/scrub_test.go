package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"incbackup/internal/api"
	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

func newTestServer(t *testing.T) (*httptest.Server, *backup.Engine, string) {
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
	srv := httptest.NewServer((&api.Server{Engine: e}).NewRouter())
	t.Cleanup(srv.Close)
	return srv, e, dir
}

func doJSON(t *testing.T, method, url string, body any) (int, map[string]any) {
	t.Helper()
	var rdr *bytes.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		rdr = bytes.NewReader(b)
	} else {
		rdr = bytes.NewReader(nil)
	}
	req, _ := http.NewRequest(method, url, rdr)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	out := map[string]any{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil && err.Error() != "EOF" {
		t.Fatal(err)
	}
	return resp.StatusCode, out
}

func waitLatestDone(t *testing.T, base string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		code, body := doJSON(t, "GET", base+"/v1/scrubs/latest", nil)
		if code != http.StatusOK {
			t.Fatalf("latest: %d %v", code, body)
		}
		if body["status"] == "done" && body["running"] == false {
			return body
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("scrub never finished")
	return nil
}

func asID(v any) int64 { return int64(v.(float64)) }

func TestScrubAPIEndToEnd(t *testing.T) {
	srv, e, dir := newTestServer(t)
	src := filepath.Join(dir, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("0123456789ABCDEF\n", 12000)
	if err := os.WriteFile(filepath.Join(src, "big.bin"), []byte(big), 0o644); err != nil {
		t.Fatal(err)
	}
	code, body := doJSON(t, "POST", srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "v1"})
	if code != http.StatusCreated {
		t.Fatalf("create: %d %v", code, body)
	}
	id1 := asID(body["snapshot_id"])

	// Before any patrol, latest returns 404 and snapshots report unscanned.
	code, body = doJSON(t, "GET", srv.URL+"/v1/scrubs/latest", nil)
	if code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", code)
	}
	code, body = doJSON(t, "GET", srv.URL+"/v1/snapshots/"+fmt.Sprint(id1), nil)
	if body["integrity"] != "unscanned" || body["status"] != "committed" {
		t.Fatalf("pre-scrub snapshot: %v", body)
	}

	// Start a patrol -> 201, then poll to done.
	code, body = doJSON(t, "POST", srv.URL+"/v1/scrubs", nil)
	if code != http.StatusCreated || body["action"] != "started" {
		t.Fatalf("start: %d %v", code, body)
	}
	run := body["progress"].(map[string]any)
	runID := asID(run["run_id"])
	done := waitLatestDone(t, srv.URL)
	if asID(done["high_watermark"]) < id1 {
		t.Fatalf("watermark %v", done["high_watermark"])
	}

	// Snapshot now presents clean while status stays committed.
	code, body = doJSON(t, "GET", srv.URL+"/v1/snapshots/"+fmt.Sprint(id1), nil)
	if body["integrity"] != repo.IntegrityClean || body["status"] != repo.StatusCommitted {
		t.Fatalf("post-scrub snapshot: %v", body)
	}
	if asID(body["integrity_run_id"]) != runID {
		t.Fatalf("integrity_run_id: %v", body["integrity_run_id"])
	}

	// Per-snapshot detail endpoint.
	code, body = doJSON(t, "GET",
		fmt.Sprintf("%s/v1/scrubs/%d/snapshots/%d", srv.URL, runID, id1), nil)
	if code != http.StatusOK || body["integrity"] != repo.IntegrityClean || body["covered"] != true {
		t.Fatalf("detail: %d %v", code, body)
	}
	// The /latest/ alias resolves to the same run.
	code, body = doJSON(t, "GET",
		fmt.Sprintf("%s/v1/scrubs/latest/snapshots/%d", srv.URL, id1), nil)
	if code != http.StatusOK || body["integrity"] != repo.IntegrityClean {
		t.Fatalf("latest snapshot alias: %d %v", code, body)
	}

	// Corrupt a shared-ish chunk; here snapshot id1 owns it. Take first
	// catalog chunk.
	rows, err := e.Manifest.DB().Query(`SELECT c.digest FROM entry_chunks ec
		JOIN chunks c ON c.digest = ec.chunk_digest WHERE ec.snapshot_id = ? LIMIT 1`, id1)
	if err != nil {
		t.Fatal(err)
	}
	var digest []byte
	if !rows.Next() {
		rows.Close()
		t.Fatal("no chunks")
	}
	if err := rows.Scan(&digest); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	p, _ := e.Store.Path(digest)
	data, _ := os.ReadFile(p)
	_ = os.Chmod(p, 0o644)
	for i := range data {
		data[i] ^= 0xff
	}
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatal(err)
	}

	// A second start is a fresh run -> 201 again once the prior is done.
	code, body = doJSON(t, "POST", srv.URL+"/v1/scrubs", nil)
	if code != http.StatusCreated {
		t.Fatalf("second start: %d %v", code, body)
	}
	done = waitLatestDone(t, srv.URL)
	runID = asID(done["run_id"])

	// Reverse-lookup endpoint reports the bad chunk, snapshot and path.
	dhex := fmt.Sprintf("%x", digest)
	code, body = doJSON(t, "GET",
		fmt.Sprintf("%s/v1/scrubs/%d/chunks/%s/affected", srv.URL, runID, dhex), nil)
	if code != http.StatusOK {
		t.Fatalf("affected: %d %v", code, body)
	}
	if body["healthy"] != false || body["result"] != repo.ChunkResultDigestMismatch {
		t.Fatalf("affected body: %v", body)
	}
	ids := body["affected_snapshot_ids"].([]any)
	if len(ids) != 1 || asID(ids[0]) != id1 {
		t.Fatalf("affected ids: %v", ids)
	}
	refs := body["references"].([]any)
	ref0 := refs[0].(map[string]any)
	if ref0["rel_path"] != "big.bin" {
		t.Fatalf("reference path: %v", ref0)
	}

	// Restore now refused with 409 snapshot_suspect and chunk details.
	code, body = doJSON(t, "POST",
		fmt.Sprintf("%s/v1/snapshots/%d/restore", srv.URL, id1),
		map[string]any{"target": filepath.Join(dir, "restored")})
	if code != http.StatusConflict || body["error"] != "snapshot_suspect" {
		t.Fatalf("restore: %d %v", code, body)
	}
	bad := body["affected_chunks"].([]any)
	if len(bad) == 0 || bad[0].(map[string]any)["chunk_digest"] != dhex {
		t.Fatalf("affected_chunks: %v", bad)
	}

	// Bad digest shape -> 400.
	code, _ = doJSON(t, "GET",
		fmt.Sprintf("%s/v1/scrubs/%d/chunks/zz/affected", srv.URL, runID), nil)
	if code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", code)
	}
}

func TestScrubAPIResumeAndInterrupt(t *testing.T) {
	srv, _, dir := newTestServer(t)
	src := filepath.Join(dir, "src")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "a"), []byte("hello\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code, _ := doJSON(t, "POST", srv.URL+"/v1/snapshots", map[string]any{"root": src}); code != http.StatusCreated {
		t.Fatalf("create snapshot: %d", code)
	}

	// Fast patrols complete immediately; a second start after completion
	// begins a new run, while interrupt on an idle server is a 409.
	doJSON(t, "POST", srv.URL+"/v1/scrubs", nil)
	waitLatestDone(t, srv.URL)
	code, body := doJSON(t, "POST", srv.URL+"/v1/scrubs/interrupt", nil)
	if code != http.StatusConflict {
		t.Fatalf("idle interrupt: %d %v", code, body)
	}
}
