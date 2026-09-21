package coordinator

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"dstore-demo/internal/protocol"
	"dstore-demo/internal/shardclient"
)

// Server là HTTP API của control plane.
type Server struct {
	Store  *Store
	Placer *Placer
	Health *HealthMonitor
	Shards *shardclient.Client
	Log    *slog.Logger
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /nodes/register", s.registerNode)
	mux.HandleFunc("POST /nodes/{id}/heartbeat", s.heartbeat)
	mux.HandleFunc("GET /nodes", s.listNodes)
	mux.HandleFunc("POST /nodes/pick", s.pickNodes)
	mux.HandleFunc("POST /files/plan", s.planFile)
	mux.HandleFunc("POST /files/{id}/commit", s.commitFile)
	mux.HandleFunc("GET /files/{id}", s.getManifest)
	mux.HandleFunc("GET /files", s.listFiles)
	mux.HandleFunc("DELETE /files/{id}", s.deleteFile)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

// --- Node ---

func (s *Server) registerNode(w http.ResponseWriter, r *http.Request) {
	var req protocol.RegisterRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Addr == "" {
		writeError(w, http.StatusBadRequest, "thiếu addr")
		return
	}
	id := req.NodeID
	if id == "" {
		id = "node-" + randomID(4)
	}

	returned := s.Store.RegisterNode(id, req.Addr, req.Capacity)
	s.Log.Info("node đăng ký", "node", id, "addr", req.Addr, "quay_lai", returned)
	if returned {
		// Node sống lại: đối chiếu shard còn trên đĩa ở nền, không chặn response.
		go s.Health.Reconcile(context.Background(), id, req.Addr)
	}
	writeJSON(w, http.StatusOK, protocol.RegisterResponse{NodeID: id})
}

func (s *Server) heartbeat(w http.ResponseWriter, r *http.Request) {
	var req protocol.HeartbeatRequest
	if !decode(w, r, &req) {
		return
	}
	id := r.PathValue("id")
	if !s.Store.Heartbeat(id, req.UsedBytes, req.ShardCount) {
		// Coordinator chưa biết node này (ví dụ vừa restart) -> node sẽ đăng ký lại.
		writeError(w, http.StatusNotFound, "node chưa đăng ký")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listNodes(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Store.Nodes())
}

// pickNodes cấp node thay thế khi client gặp node lỗi lúc upload.
func (s *Server) pickNodes(w http.ResponseWriter, r *http.Request) {
	var req protocol.PickRequest
	if !decode(w, r, &req) {
		return
	}
	if req.Count <= 0 {
		req.Count = 1
	}
	nodes, err := s.Placer.Pick(req.Count, 0, req.Exclude)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, protocol.PickResponse{Nodes: nodes})
}

// --- File ---

func (s *Server) planFile(w http.ResponseWriter, r *http.Request) {
	var req protocol.PlanRequest
	if !decode(w, r, &req) {
		return
	}
	if req.K <= 0 || req.M <= 0 || req.SegmentSize <= 0 || req.Size < 0 {
		writeError(w, http.StatusBadRequest, "k, m, segment_size phải > 0 và size >= 0")
		return
	}

	segmentCount := int((req.Size + int64(req.SegmentSize) - 1) / int64(req.SegmentSize))
	if segmentCount == 0 {
		segmentCount = 1 // file rỗng vẫn có một segment
	}
	// Shard lớn nhất có thể: segment chia cho k, làm tròn lên.
	shardSize := int64((req.SegmentSize + req.K - 1) / req.K)

	plans, err := s.Placer.PlanFile(segmentCount, req.K, req.M, shardSize)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	fileID := randomID(8)
	s.Store.CreateFile(fileID, req.Name, req.Size, req.K, req.M, req.SegmentSize, segmentCount)
	s.Log.Info("tạo kế hoạch upload",
		"file", fileID, "name", req.Name, "size", req.Size, "segments", segmentCount, "k", req.K, "m", req.M)

	writeJSON(w, http.StatusOK, protocol.PlanResponse{
		FileID: fileID, K: req.K, M: req.M, SegmentSize: req.SegmentSize, Segments: plans,
	})
}

func (s *Server) commitFile(w http.ResponseWriter, r *http.Request) {
	var req protocol.CommitRequest
	if !decode(w, r, &req) {
		return
	}
	id := r.PathValue("id")
	if err := s.Store.CommitFile(id, req.Segments); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.Log.Info("commit file", "file", id, "segments", len(req.Segments))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) getManifest(w http.ResponseWriter, r *http.Request) {
	man, ok := s.Store.Manifest(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "không có file này (hoặc chưa commit)")
		return
	}
	writeJSON(w, http.StatusOK, man)
}

func (s *Server) listFiles(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Store.Files())
}

func (s *Server) deleteFile(w http.ResponseWriter, r *http.Request) {
	locs, ok := s.Store.DeleteFile(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, "không có file này")
		return
	}
	// Xóa shard dưới node ở nền; node chết thì shard thừa sẽ được dọn khi đối chiếu sau.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, loc := range locs {
			if err := s.Shards.Delete(ctx, loc.NodeAddr, loc.Hash); err != nil {
				s.Log.Warn("xóa shard lỗi", "node_addr", loc.NodeAddr, "err", err)
			}
		}
	}()
	s.Log.Info("xóa file", "file", r.PathValue("id"), "shards", len(locs))
	w.WriteHeader(http.StatusNoContent)
}

// --- Helper ---

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "JSON không hợp lệ: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, protocol.ErrorResponse{Error: msg})
}

func randomID(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}
