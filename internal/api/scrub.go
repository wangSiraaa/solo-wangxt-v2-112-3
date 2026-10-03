package api

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

// scrubJSON is the serialized shape of one scrub job.
func scrubJSON(sc repo.ScrubInfo, snapshotCount int) map[string]any {
	return map[string]any{
		"id":             sc.ID,
		"status":         sc.Status,
		"watermark":      sc.HighSnapshotID,
		"total_chunks":   sc.TotalChunks,
		"scanned_chunks": sc.ScannedChunks,
		"bad_chunks":     sc.BadChunks,
		"bytes_scanned":  sc.BytesScanned,
		"started_at":     sc.StartedAt,
		"paused_at":      sc.PausedAt,
		"finished_at":    sc.FinishedAt,
		"last_error":     sc.LastError,
		"snapshot_count": snapshotCount,
	}
}

type startScrubReq struct {
	// Force starts a fresh scrub even when an unfinished one can be resumed.
	Force bool `json:"force"`
}

// startScrub starts (or resumes) a background repository integrity scrub.
func (s *Server) startScrub(w http.ResponseWriter, r *http.Request) {
	var req startScrubReq
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
			return
		}
	}
	sc, err := s.Engine.StartScrub(req.Force, backup.ScrubHooks{})
	if err != nil {
		if errors.Is(err, backup.ErrScrubAlreadyRunning) {
			writeErr(w, http.StatusConflict, "scrub_running", err.Error(), nil)
			return
		}
		writeErr(w, http.StatusInternalServerError, "scrub_start_failed", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusAccepted, scrubJSON(sc, 0))
}

// listScrubs returns every scrub newest first.
func (s *Server) listScrubs(w http.ResponseWriter, r *http.Request) {
	all, err := s.Engine.Manifest.ListScrubs()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	out := make([]map[string]any, 0, len(all))
	for _, sc := range all {
		snaps, _ := s.Engine.Manifest.ScrubSnapshots(sc.ID)
		out = append(out, scrubJSON(sc, len(snaps)))
	}
	writeJSON(w, http.StatusOK, map[string]any{"scrubs": out})
}

// getScrub returns live progress of one scrub plus per-snapshot verdicts.
func (s *Server) getScrub(w http.ResponseWriter, r *http.Request) {
	id, ok := parseScrubID(w, r)
	if !ok {
		return
	}
	prog, err := s.Engine.ScrubProgress(id)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "scrub does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	body := scrubJSON(prog.Scrub, prog.SnapshotCount)
	body["snapshots"] = verdictItems(prog.Snapshots)
	writeJSON(w, http.StatusOK, body)
}

// cancelScrub asks the active worker to stop at the next stable boundary.
func (s *Server) cancelScrub(w http.ResponseWriter, r *http.Request) {
	id, ok := parseScrubID(w, r)
	if !ok {
		return
	}
	// Ensure the scrub exists even if no worker is active (already paused).
	if _, err := s.Engine.Manifest.GetScrub(id); errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "scrub does not exist", nil)
		return
	}
	cid, running := s.Engine.CancelScrub()
	if !running || cid != id {
		// Different (or no) active worker: return current state, no worker.
		prog, err := s.Engine.ScrubProgress(id)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
			return
		}
		writeJSON(w, http.StatusOK, scrubJSON(prog.Scrub, prog.SnapshotCount))
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"scrub_id": id,
		"status":   "cancelling",
		"message":  "scrub will pause after the current batch; resume with POST /v1/scrubs",
	})
}

func verdictItems(snaps []repo.SnapshotVerdict) []map[string]any {
	out := make([]map[string]any, 0, len(snaps))
	for _, v := range snaps {
		out = append(out, map[string]any{
			"snapshot_id": v.SnapshotID,
			"integrity":   v.Status,
			"bad_chunks":  v.BadChunks,
		})
	}
	return out
}

// scrubSnapshots returns per-snapshot integrity status of one scrub, with
// post-watermark snapshots explicitly listed as "uncovered".
func (s *Server) scrubSnapshots(w http.ResponseWriter, r *http.Request) {
	id, ok := parseScrubID(w, r)
	if !ok {
		return
	}
	snaps, err := s.Engine.Manifest.ScrubSnapshots(id)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "scrub does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"scrub_id":  id,
		"snapshots": verdictItems(snaps),
	})
}

type affectedItemResp struct {
	ChunkDigest    string             `json:"chunk_digest"`
	Result         string             `json:"result"`
	DeclaredLength int64              `json:"declared_length"`
	ObservedLength int64              `json:"observed_length"`
	ObservedDigest string             `json:"observed_digest,omitempty"`
	BlobPath       string             `json:"blob_path"`
	Detail         string             `json:"detail"`
	Affected       []repo.AffectedRef `json:"affected"`
}

// scrubAffected lists every bad chunk of a scrub and, via the manifest
// reverse lookup, every snapshot + file path referencing it — including
// snapshots outside the scrub's watermark that share the blob.
func (s *Server) scrubAffected(w http.ResponseWriter, r *http.Request) {
	id, ok := parseScrubID(w, r)
	if !ok {
		return
	}
	if _, err := s.Engine.Manifest.GetScrub(id); errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "scrub does not exist", nil)
		return
	}
	bad, err := s.Engine.Manifest.ScrubBadChunks(id)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "scrub does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	items := make([]affectedItemResp, 0, len(bad))
	for _, bc := range bad {
		it := affectedItemResp{
			ChunkDigest:    hex.EncodeToString(bc.Digest),
			Result:         bc.Result,
			DeclaredLength: bc.DeclaredLength,
			ObservedLength: bc.ObservedLength,
			Detail:         bc.Detail,
			Affected:       bc.Refs,
		}
		if len(bc.ObservedDigest) > 0 {
			it.ObservedDigest = hex.EncodeToString(bc.ObservedDigest)
		}
		if p, perr := s.Engine.Store.Path(bc.Digest); perr == nil {
			it.BlobPath = p
		}
		items = append(items, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"scrub_id":   id,
		"bad_count":  len(items),
		"bad_chunks": items,
	})
}

// snapshotIntegrity returns one snapshot's verdict against the latest scrub.
func (s *Server) snapshotIntegrity(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	if _, err := s.Engine.Manifest.GetSnapshot(id); errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "snapshot does not exist", nil)
		return
	}
	views, err := s.Engine.Manifest.IntegrityViews()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	v := views[id]
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot_id": v.SnapshotID,
		"scrub_id":    v.ScrubID,
		"integrity":   v.Status,
		"bad_chunks":  v.BadChunks,
	})
}

func parseScrubID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_id", "scrub id must be an integer", nil)
		return 0, false
	}
	return id, true
}
