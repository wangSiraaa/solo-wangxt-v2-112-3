package api

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

// POST /v1/scrubs — start a patrol, or resume the run left 'running' by an
// interrupted process. 201 = fresh run, 200 = resumed existing one,
// 409 = a patrol is already active in this process.
//
// Optional body: {"throttle_ms": 5} — idle time after each chunk.
func (s *Server) startScrub(w http.ResponseWriter, r *http.Request) {
	opts := backup.ScrubOptions{}
	if r.Body != nil && r.ContentLength != 0 {
		var req struct {
			ThrottleMS int64 `json:"throttle_ms"`
			GateMS     int64 `json:"gate_ms"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err.Error() != "EOF" {
			writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
			return
		}
		if req.ThrottleMS < 0 || req.GateMS < 0 {
			writeErr(w, http.StatusBadRequest, "bad_request", "throttle_ms and gate_ms must be >= 0", nil)
			return
		}
		opts.Throttle = time.Duration(req.ThrottleMS) * time.Millisecond
		if req.GateMS > 0 {
			ch := make(chan struct{})
			go func() {
				time.Sleep(time.Duration(req.GateMS) * time.Millisecond)
				close(ch)
			}()
			opts.Gate = ch
		}
	}
	prog, resumed, err := s.Engine.StartScrub(opts)
	if err != nil {
		if errors.Is(err, backup.ErrScrubRunning) {
			writeErr(w, http.StatusConflict, "scrub_running", err.Error(), nil)
			return
		}
		writeErr(w, http.StatusInternalServerError, "scrub_start_failed", err.Error(), nil)
		return
	}
	status := http.StatusCreated
	action := "started"
	if resumed {
		status = http.StatusOK
		action = "resumed"
	}
	writeJSON(w, status, map[string]any{"action": action, "progress": prog})
}

// POST /v1/scrubs/interrupt — request a cooperative stop of the in-process
// patrol. Checkpointed verdicts and the stable cursor stay in SQLite.
func (s *Server) interruptScrub(w http.ResponseWriter, r *http.Request) {
	if !s.Engine.InterruptScrub() {
		writeErr(w, http.StatusConflict, "no_scrub_running", "no integrity patrol is running in this process", nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "interrupt requested"})
}

// GET /v1/scrubs/latest — progress of the newest patrol run.
func (s *Server) latestScrub(w http.ResponseWriter, r *http.Request) {
	prog, err := s.Engine.LatestScrubProgress()
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "no_scrub", "no integrity patrol has ever run", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, prog)
}

// GET /v1/scrubs/{id} — progress of one run.
func (s *Server) getScrub(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	prog, err := s.Engine.ScrubProgressOf(id)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "scrub run does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, prog)
}

// GET /v1/scrubs/{id}/snapshots/{sid} — one snapshot's independent integrity
// state within the run, with the bad chunks and paths behind a suspect verdict.
func (s *Server) getScrubSnapshot(w http.ResponseWriter, r *http.Request) {
	runID, ok := parseIDPath(w, r, "id")
	if !ok {
		return
	}
	snapID, ok := parseIDPath(w, r, "sid")
	if !ok {
		return
	}
	detail, err := s.Engine.ScrubSnapshotDetail(runID, snapID)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "scrub run or snapshot does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

// GET /v1/scrubs/{id}/chunks/{digest}/affected — manifest reverse-lookup:
// every snapshot and file path referencing the chunk, with its patrol result.
func (s *Server) chunkAffected(w http.ResponseWriter, r *http.Request) {
	runID, ok := parseIDPath(w, r, "id")
	if !ok {
		return
	}
	digestHex := r.PathValue("digest")
	digest, err := hex.DecodeString(digestHex)
	if err != nil || len(digest) != 32 {
		writeErr(w, http.StatusBadRequest, "bad_digest", "chunk digest must be 64 hex characters", nil)
		return
	}
	result, refs, err := s.Engine.Manifest.ScrubAffected(runID, digest)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found",
			"chunk has no patrol verdict in this run (not in the run's work set or run unknown)", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	type refItem struct {
		SnapshotID int64  `json:"snapshot_id"`
		RelPath    string `json:"rel_path"`
	}
	items := make([]refItem, 0, len(refs))
	for _, ref := range refs {
		items = append(items, refItem{SnapshotID: ref.SnapshotID, RelPath: ref.RelPath})
	}
	blobPath, _ := s.Engine.Store.Path(digest)
	healthy := result == repo.ChunkResultOK
	resp := map[string]any{
		"scrub_run_id":          runID,
		"chunk_digest":          digestHex,
		"result":                result,
		"healthy":               healthy,
		"expected_blob_path":    blobPath,
		"affected_snapshot_ids": affectedIDs(refs),
		"references":            items,
	}
	writeJSON(w, http.StatusOK, resp)
}

func affectedIDs(refs []repo.AffectedRef) []int64 {
	seen := map[int64]bool{}
	var ids []int64
	for _, ref := range refs {
		if !seen[ref.SnapshotID] {
			seen[ref.SnapshotID] = true
			ids = append(ids, ref.SnapshotID)
		}
	}
	return ids
}

// GET /v1/scrubs/latest/snapshots/{sid} — same as the per-run endpoint but
// resolves the newest run id.
func (s *Server) latestScrubSnapshot(w http.ResponseWriter, r *http.Request) {
	runID, ok := s.latestRunID(w, r)
	if !ok {
		return
	}
	r.SetPathValue("id", strconv.FormatInt(runID, 10))
	s.getScrubSnapshot(w, r)
}

// GET /v1/scrubs/latest/chunks/{digest}/affected — newest-run reverse-lookup.
func (s *Server) latestChunkAffected(w http.ResponseWriter, r *http.Request) {
	runID, ok := s.latestRunID(w, r)
	if !ok {
		return
	}
	r.SetPathValue("id", strconv.FormatInt(runID, 10))
	s.chunkAffected(w, r)
}

// latestRunID resolves the newest run id for the "latest" aliases.
func (s *Server) latestRunID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	run, err := s.Engine.Manifest.LatestScrubRun()
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "no_scrub", "no integrity patrol has ever run", nil)
		return 0, false
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return 0, false
	}
	return run.ID, true
}

func parseIDPath(w http.ResponseWriter, r *http.Request, key string) (int64, bool) {
	id, err := parseInt64(r.PathValue(key))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_id", key+" must be an integer", nil)
		return 0, false
	}
	return id, true
}
