package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
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
	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		bodyReader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var out map[string]any
	if len(data) > 0 {
		if err := json.Unmarshal(data, &out); err != nil {
			t.Fatalf("decode %s: %v: %s", url, err, data)
		}
	}
	if out == nil {
		out = map[string]any{}
	}
	return resp.StatusCode, out
}

func TestScrubEndToEndOverHTTP(t *testing.T) {
	srv, e, dir := newTestServer(t)
	src := filepath.Join(dir, "src")
	if err := os.MkdirAll(filepath.Join(src, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	content := strings.Repeat("http-shared-block-line-0123456789\n", 4000)
	if err := os.WriteFile(filepath.Join(src, "d", "big.log"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	code, b := doJSON(t, "POST", srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "s1"})
	if code != http.StatusCreated {
		t.Fatalf("create1 %d %v", code, b)
	}
	id1 := int64(b["snapshot_id"].(float64))
	if err := os.WriteFile(filepath.Join(src, "extra.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	code, b = doJSON(t, "POST", srv.URL+"/v1/snapshots", map[string]any{"root": src, "message": "s2"})
	if code != http.StatusCreated {
		t.Fatalf("create2 %d %v", code, b)
	}
	id2 := int64(b["snapshot_id"].(float64))

	// Before any scrub, snapshots are decorated never_scrubbed.
	code, b = doJSON(t, "GET", srv.URL+"/v1/snapshots/"+itoa(id1), nil)
	if code != http.StatusOK || b["integrity_status"] != "never_scrubbed" {
		t.Fatalf("pre-scrub integrity: code=%d body=%v", code, b)
	}

	// Corrupt a chunk shared by both snapshots directly on disk.
	refs, err := e.Manifest.ReferencedChunks(id1)
	if err != nil || len(refs) == 0 {
		t.Fatalf("refs: %v", err)
	}
	dg := refs[0].Digest
	p, _ := e.Store.Path(dg)
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("HTTP-CORRUPT!!"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Start the scrub.
	code, b = doJSON(t, "POST", srv.URL+"/v1/scrubs", nil)
	if code != http.StatusAccepted {
		t.Fatalf("start scrub %d %v", code, b)
	}
	scrubID := int64(b["id"].(float64))
	// Second start while running must be rejected (or after it finishes, the
	// job must resume/complete; retry once to absorb very fast machines).
	if b["status"] == "running" {
		if c2, b2 := doJSON(t, "POST", srv.URL+"/v1/scrubs", nil); c2 != http.StatusConflict {
			t.Fatalf("double start code=%d body=%v", c2, b2)
		}
	}

	// Poll progress until done.
	var final map[string]any
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		code, final = doJSON(t, "GET", srv.URL+"/v1/scrubs/"+itoa(scrubID), nil)
		if code != http.StatusOK {
			t.Fatalf("progress %d", code)
		}
		if final["status"] == "done" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if final["status"] != "done" {
		t.Fatalf("scrub did not finish: %v", final["status"])
	}
	if int64(final["bad_chunks"].(float64)) != 1 {
		t.Fatalf("bad_chunks=%v", final["bad_chunks"])
	}

	// Both snapshots suspect via the per-scrub snapshot endpoint.
	code, b = doJSON(t, "GET", srv.URL+"/v1/scrubs/"+itoa(scrubID)+"/snapshots", nil)
	if code != http.StatusOK {
		t.Fatalf("snapshots endpoint %d", code)
	}
	stat := map[int64]string{}
	for _, x := range b["snapshots"].([]any) {
		m := x.(map[string]any)
		stat[int64(m["snapshot_id"].(float64))] = m["integrity"].(string)
	}
	if stat[id1] != "suspect" || stat[id2] != "suspect" {
		t.Fatalf("verdicts: %d=%s %d=%s", id1, stat[id1], id2, stat[id2])
	}

	// Per-snapshot integrity query.
	code, b = doJSON(t, "GET", srv.URL+"/v1/scrubs/0/snapshots", nil)
	if code != http.StatusNotFound {
		t.Fatalf("missing scrub code=%d", code)
	}
	code, b = doJSON(t, "GET", srv.URL+"/v1/snapshots/"+itoa(id1)+"/integrity", nil)
	if code != http.StatusOK || b["integrity"] != "suspect" {
		t.Fatalf("integrity endpoint code=%d body=%v", code, b)
	}

	// Affected-paths endpoint lists both snapshots and rel paths.
	code, b = doJSON(t, "GET", srv.URL+"/v1/scrubs/"+itoa(scrubID)+"/affected", nil)
	if code != http.StatusOK {
		t.Fatalf("affected code=%d", code)
	}
	bads := b["bad_chunks"].([]any)
	if len(bads) != 1 {
		t.Fatalf("bad chunks %d", len(bads))
	}
	bm := bads[0].(map[string]any)
	if bm["blob_path"] == "" || bm["detail"] == "" {
		t.Fatalf("affected item not locatable: %v", bm)
	}
	owners := map[int64]bool{}
	for _, a := range bm["affected"].([]any) {
		am := a.(map[string]any)
		owners[int64(am["snapshot_id"].(float64))] = true
		if am["rel_path"] == "" {
			t.Fatal("empty rel_path in affected")
		}
	}
	if !owners[id1] || !owners[id2] {
		t.Fatalf("affected owners %v", owners)
	}

	// Restore of a suspect snapshot -> 409 snapshot_suspect, locatable.
	code, b = doJSON(t, "POST", srv.URL+"/v1/snapshots/"+itoa(id1)+"/restore",
		map[string]any{"target": filepath.Join(dir, "out")})
	if code != http.StatusConflict || b["error"] != "snapshot_suspect" {
		t.Fatalf("restore code=%d body=%v", code, b)
	}
	if _, ok := b["bad_chunks"]; !ok {
		t.Fatal("suspect response missing bad_chunks detail")
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
