package node

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
)

// Server phục vụ API shard của một node.
type Server struct {
	ID    string
	Store ShardStore
	Log   *slog.Logger
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /shards/{hash}", s.putShard)
	mux.HandleFunc("GET /shards/{hash}", s.getShard)
	mux.HandleFunc("HEAD /shards/{hash}", s.headShard)
	mux.HandleFunc("DELETE /shards/{hash}", s.deleteShard)
	mux.HandleFunc("GET /shards", s.listShards)
	mux.HandleFunc("GET /healthz", s.healthz)
	return mux
}

func (s *Server) putShard(w http.ResponseWriter, r *http.Request) {
	hash := r.PathValue("hash")
	if err := s.Store.Put(r.Context(), hash, r.Body); err != nil {
		status := statusFor(err)
		if status == http.StatusBadRequest {
			// Hash không khớp là lỗi của client, không phải lỗi node.
			s.Log.Warn("từ chối shard", "hash", hash, "err", err)
		} else {
			s.Log.Error("ghi shard lỗi", "hash", hash, "err", err)
		}
		writeError(w, status, err.Error())
		return
	}
	s.Log.Debug("nhận shard", "hash", hash)
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) getShard(w http.ResponseWriter, r *http.Request) {
	f, size, err := s.Store.Open(r.Context(), r.PathValue("hash"))
	if err != nil {
		writeError(w, statusFor(err), err.Error())
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	io.Copy(w, f)
}

func (s *Server) headShard(w http.ResponseWriter, r *http.Request) {
	if !s.Store.Has(r.Context(), r.PathValue("hash")) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) deleteShard(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.Delete(r.Context(), r.PathValue("hash")); err != nil {
		writeError(w, statusFor(err), err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listShards(w http.ResponseWriter, r *http.Request) {
	hashes, err := s.Store.List(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if hashes == nil {
		hashes = []string{}
	}
	writeJSON(w, http.StatusOK, hashes)
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	used, count := s.Store.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":     s.ID,
		"used_bytes":  used,
		"shard_count": count,
	})
}

// statusFor quy lỗi của ShardStore ra mã HTTP.
func statusFor(err error) int {
	switch {
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrInvalidHash), errors.Is(err, ErrHashMismatch):
		return http.StatusBadRequest
	case errors.Is(err, ErrTooLarge):
		return http.StatusRequestEntityTooLarge
	default:
		return http.StatusInternalServerError
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
