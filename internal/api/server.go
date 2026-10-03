// Package api exposes the backup engine over a small local HTTP API.
package api

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"incbackup/internal/backup"
	"incbackup/internal/repo"
)

// Server wires the engine to HTTP.
type Server struct {
	Engine *backup.Engine
}

// NewRouter builds the mux.
func (s *Server) NewRouter() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /v1/recover", s.recover)
	mux.HandleFunc("GET /v1/snapshots", s.list)
	mux.HandleFunc("POST /v1/snapshots", s.create)
	mux.HandleFunc("GET /v1/snapshots/{id}", s.get)
	mux.HandleFunc("POST /v1/snapshots/{id}/verify", s.verify)
	mux.HandleFunc("GET /v1/snapshots/{id}/errors", s.listErrors)
	mux.HandleFunc("GET /v1/snapshots/{id}/missing", s.missing)
	mux.HandleFunc("GET /v1/snapshots/{id}/chunks", s.snapshotChunks)
	mux.HandleFunc("POST /v1/snapshots/{id}/restore", s.restore)

	// Repository-wide integrity patrol.
	mux.HandleFunc("POST /v1/scrubs", s.startScrub)
	mux.HandleFunc("POST /v1/scrubs/interrupt", s.interruptScrub)
	mux.HandleFunc("GET /v1/scrubs/latest", s.latestScrub)
	mux.HandleFunc("GET /v1/scrubs/latest/snapshots/{sid}", s.latestScrubSnapshot)
	mux.HandleFunc("GET /v1/scrubs/latest/chunks/{digest}/affected", s.latestChunkAffected)
	mux.HandleFunc("GET /v1/scrubs/{id}", s.getScrub)
	mux.HandleFunc("GET /v1/scrubs/{id}/snapshots/{sid}", s.getScrubSnapshot)
	mux.HandleFunc("GET /v1/scrubs/{id}/chunks/{digest}/affected", s.chunkAffected)
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, status int, code, msg string, details any) {
	writeJSON(w, status, map[string]any{
		"error":   code,
		"message": msg,
		"details": details,
	})
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339)})
}

type snapshotResp struct {
	ID             int64      `json:"id"`
	RootPath       string     `json:"root_path"`
	Status         string     `json:"status"`
	Integrity      string     `json:"integrity"`
	IntegrityRunID int64      `json:"integrity_run_id,omitempty"`
	FileCount      int64      `json:"file_count"`
	DirCount       int64      `json:"dir_count"`
	BytesTotal     int64      `json:"bytes_total"`
	ChunksNew      int64      `json:"chunks_new"`
	ChunksRef      int64      `json:"chunks_referenced"`
	Polynomial     string     `json:"polynomial"`
	CreatedAt      time.Time  `json:"created_at"`
	CommittedAt    *time.Time `json:"committed_at,omitempty"`
	Message        string     `json:"message"`
}

func toSnapshotResp(si repo.SnapshotInfo) snapshotResp {
	return snapshotResp{
		ID:          si.ID,
		RootPath:    si.RootPath,
		Status:      si.Status,
		Integrity:   repo.IntegrityUnscanned, // replaced with latest patrol verdict below when available
		FileCount:   si.FileCount,
		DirCount:    si.DirCount,
		BytesTotal:  si.BytesTotal,
		ChunksNew:   si.ChunksNew,
		ChunksRef:   si.ChunksRef,
		Polynomial:  "0x" + strconv.FormatUint(si.Polynomial, 16),
		CreatedAt:   si.CreatedAt,
		CommittedAt: si.CommittedAt,
		Message:     si.Message,
	}
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	all, err := s.Engine.Manifest.ListSnapshots()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	verdicts := s.latestIntegrityMap()
	out := make([]snapshotResp, 0, len(all))
	for _, si := range all {
		resp := toSnapshotResp(si)
		if v, ok := verdicts[si.ID]; ok {
			resp.Integrity = v.Integrity
			resp.IntegrityRunID = v.RunID
		}
		out = append(out, resp)
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshots": out})
}

// integrityView is the latest patrol verdict of one snapshot.
type integrityView struct {
	RunID     int64
	Integrity string
}

// latestIntegrityMap returns snapshot id -> latest patrol verdict. When no
// patrol ever ran, every snapshot stays "unscanned".
func (s *Server) latestIntegrityMap() map[int64]integrityView {
	prog, err := s.Engine.LatestScrubProgress()
	if err != nil || prog == nil {
		return nil
	}
	out := make(map[int64]integrityView, len(prog.Snapshots))
	for _, v := range prog.Snapshots {
		out[v.SnapshotID] = integrityView{RunID: prog.RunID, Integrity: v.Integrity}
	}
	return out
}

type createReq struct {
	Root          string `json:"root"`
	Message       string `json:"message"`
	Finish        *bool  `json:"finish"`      // default true; false = die before commit (demo)
	LoseChunks    int    `json:"lose_chunks"` // failpoint: delete N blobs pre-verify
	UnstableRetry int    `json:"-"`
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	var req createReq
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
			return
		}
	}
	if strings.TrimSpace(req.Root) == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "root is required", nil)
		return
	}
	finish := true
	if req.Finish != nil {
		finish = *req.Finish
	}
	s.Engine.Fail.LoseChunkCount = req.LoseChunks
	defer func() { s.Engine.Fail.LoseChunkCount = 0 }()

	res, err := s.Engine.CreateSnapshot(req.Root, req.Message, finish)
	if err != nil {
		var rej *backup.ErrRejected
		if errors.As(err, &rej) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"snapshot_id": rej.SnapshotID,
				"status":      repo.StatusFailed,
				"error":       "snapshot_rejected",
				"reasons":     rej.Reasons,
				"hint":        "GET /v1/snapshots/" + strconv.FormatInt(rej.SnapshotID, 10) + "/missing",
			})
			return
		}
		status := http.StatusInternalServerError
		if res == nil {
			writeErr(w, status, "snapshot_failed", err.Error(), nil)
			return
		}
		writeJSON(w, status, map[string]any{
			"snapshot_id": res.SnapshotID,
			"status":      res.Status,
			"error":       err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"snapshot_id":       res.SnapshotID,
		"status":            res.Status,
		"chunks_new":        res.NewChunks,
		"chunks_referenced": res.RefChunks,
	})
}

func parseID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := parseInt64(r.PathValue("id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, "bad_id", "snapshot id must be an integer", nil)
		return 0, false
	}
	return id, true
}

func parseInt64(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	si, err := s.Engine.Manifest.GetSnapshot(id)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "snapshot does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	resp := toSnapshotResp(si)
	if v, ok := s.latestIntegrityMap()[id]; ok {
		resp.Integrity = v.Integrity
		resp.IntegrityRunID = v.RunID
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) verify(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	res, err := s.Engine.VerifyAndFinalize(id)
	if err != nil {
		var rej *backup.ErrRejected
		if errors.As(err, &rej) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"snapshot_id": id,
				"status":      repo.StatusFailed,
				"error":       "verification_failed",
				"missing":     rej.Reasons,
			})
			return
		}
		writeErr(w, http.StatusInternalServerError, "verify_failed", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"snapshot_id": id,
		"status":      res.Status,
	})
}

type missingResp struct {
	SnapshotID int64             `json:"snapshot_id"`
	Status     string            `json:"status"`
	Missing    []missingItemResp `json:"missing"`
}

type missingItemResp struct {
	RelPath  string `json:"rel_path"`
	Digest   string `json:"chunk_digest"`
	Length   int64  `json:"declared_length"`
	BlobPath string `json:"expected_blob_path"`
	Reason   string `json:"reason"`
}

func (s *Server) missing(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	si, err := s.Engine.Manifest.GetSnapshot(id)
	if errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "snapshot does not exist", nil)
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	found, err := s.Engine.Manifest.FindMissingChunks(id, func(digest []byte, length int64) (bool, error) {
		return s.Engine.Store.Has(digest, length)
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	items := make([]missingItemResp, 0, len(found))
	for _, mc := range found {
		item := missingItemResp{
			RelPath: mc.RelPath,
			Digest:  hex.EncodeToString(mc.Digest),
			Length:  mc.Length,
			Reason:  mc.Reason,
		}
		if p, err := s.Engine.Store.Path(mc.Digest); err == nil {
			item.BlobPath = p
		}
		items = append(items, item)
	}
	writeJSON(w, http.StatusOK, missingResp{SnapshotID: id, Status: si.Status, Missing: items})
}

// GET /v1/snapshots/{id}/chunks — maintenance listing: every content chunk
// referenced by the snapshot with the file path(s) using it.
func (s *Server) snapshotChunks(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	if _, err := s.Engine.Manifest.GetSnapshot(id); errors.Is(err, repo.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not_found", "snapshot does not exist", nil)
		return
	} else if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	refs, err := s.Engine.Manifest.ChunkReferencesOfSnapshot(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	type item struct {
		Digest   string `json:"chunk_digest"`
		Length   int64  `json:"declared_length"`
		RelPath  string `json:"rel_path"`
		BlobPath string `json:"expected_blob_path"`
	}
	out := make([]item, 0, len(refs))
	for _, ci := range refs {
		it := item{Digest: hex.EncodeToString(ci.Digest), Length: ci.Length, RelPath: ci.RelPath}
		if p, err := s.Engine.Store.Path(ci.Digest); err == nil {
			it.BlobPath = p
		}
		out = append(out, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshot_id": id, "chunks": out})
}

func (s *Server) listErrors(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	errs, err := s.Engine.Manifest.ListErrors(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "db", err.Error(), nil)
		return
	}
	type item struct {
		Stage       string    `json:"stage"`
		RelPath     string    `json:"rel_path"`
		ChunkDigest string    `json:"chunk_digest,omitempty"`
		Message     string    `json:"message"`
		CreatedAt   time.Time `json:"created_at"`
	}
	out := make([]item, 0, len(errs))
	for _, e := range errs {
		it := item{Stage: e.Stage, RelPath: e.RelPath, Message: e.Message, CreatedAt: e.CreatedAt}
		if len(e.ChunkDigest) > 0 {
			it.ChunkDigest = hex.EncodeToString(e.ChunkDigest)
		}
		out = append(out, it)
	}
	writeJSON(w, http.StatusOK, map[string]any{"snapshot_id": id, "errors": out})
}

type restoreReq struct {
	Target string `json:"target"`
}

func (s *Server) restore(w http.ResponseWriter, r *http.Request) {
	id, ok := parseID(w, r)
	if !ok {
		return
	}
	var req restoreReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_json", err.Error(), nil)
		return
	}
	if strings.TrimSpace(req.Target) == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "target is required", nil)
		return
	}
	res, err := s.Engine.Restore(id, req.Target)
	if err != nil {
		var suspect *backup.ScrubSuspectError
		if errors.As(err, &suspect) {
			items := make([]map[string]any, 0, len(suspect.Bad))
			for _, bc := range suspect.Bad {
				item := map[string]any{
					"chunk_digest":    hex.EncodeToString(bc.Digest),
					"declared_length": bc.Length,
					"reason":          bc.Result,
					"rel_paths":       bc.RelPaths,
				}
				if p, perr := s.Engine.Store.Path(bc.Digest); perr == nil {
					item["expected_blob_path"] = p
				}
				items = append(items, item)
			}
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":           "snapshot_suspect",
				"message":         err.Error(),
				"snapshot_id":     suspect.SnapshotID,
				"scrub_run_id":    suspect.RunID,
				"affected_chunks": items,
				"hint":            "GET /v1/scrubs/latest/snapshots/" + strconv.FormatInt(suspect.SnapshotID, 10),
			})
			return
		}
		if errors.Is(err, backup.ErrTargetExists) {
			writeErr(w, http.StatusConflict, "target_exists", err.Error(), nil)
			return
		}
		if strings.Contains(err.Error(), "only committed snapshots") {
			writeErr(w, http.StatusConflict, "snapshot_not_committed", err.Error(), nil)
			return
		}
		if strings.Contains(err.Error(), "escapes restore root") {
			writeErr(w, http.StatusUnprocessableEntity, "unsafe_symlink", err.Error(), nil)
			return
		}
		writeErr(w, http.StatusInternalServerError, "restore_failed", err.Error(), nil)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"snapshot_id": res.SnapshotID,
		"target":      res.Target,
		"files":       res.Files,
		"directories": res.Dirs,
		"symlinks":    res.Symlinks,
		"bytes":       res.Bytes,
		"verified":    res.Verified,
	})
}

func (s *Server) recover(w http.ResponseWriter, r *http.Request) {
	out, err := s.Engine.RecoverPending()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "recover_failed", err.Error(), nil)
		return
	}
	ids := make([]map[string]any, 0, len(out))
	for _, r := range out {
		ids = append(ids, map[string]any{"snapshot_id": r.SnapshotID, "status": r.Status})
	}
	writeJSON(w, http.StatusOK, map[string]any{"recovered": ids})
}
