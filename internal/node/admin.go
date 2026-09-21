package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// Admin là API riêng của web UI node: xem tình trạng, sửa cấu hình kết nối,
// thử kết nối tới coordinator, ngắt và nối lại heartbeat. main gắn nó dưới
// /ui/api/, sau lớp đăng nhập của webui.
type Admin struct {
	ID         string
	Store      ShardStore
	Storage    string // mô tả nơi lưu shard, ví dụ "s3://dstore-shards/nodes/node-1/"
	Reporter   *Reporter
	ConfigFile string // rỗng = không lưu cấu hình, restart là quay về env
	StartedAt  time.Time
	Log        *slog.Logger
}

func (a *Admin) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ui/api/status", a.status)
	mux.HandleFunc("PUT /ui/api/settings", a.updateSettings)
	mux.HandleFunc("POST /ui/api/test-connection", a.testConnection)
	mux.HandleFunc("POST /ui/api/disconnect", a.disconnect)
	mux.HandleFunc("POST /ui/api/connect", a.connect)
	return mux
}

func (a *Admin) status(w http.ResponseWriter, r *http.Request) {
	used, count := a.Store.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"node_id":     a.ID,
		"storage":     a.Storage,
		"used_bytes":  used,
		"shard_count": count,
		"started_at":  a.StartedAt,
		"config_file": a.ConfigFile,
		"settings":    a.Reporter.Settings(),
		"connection":  a.Reporter.Status(),
	})
}

// updateSettings lưu file trước, áp dụng sau: UI báo "đã lưu" nghĩa là restart
// cũng không mất.
func (a *Admin) updateSettings(w http.ResponseWriter, r *http.Request) {
	var s Settings
	if err := json.NewDecoder(r.Body).Decode(&s); err != nil {
		writeError(w, http.StatusBadRequest, "JSON không hợp lệ: "+err.Error())
		return
	}
	s = s.Clean()
	if err := s.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	saved := false
	if a.ConfigFile != "" {
		if err := SaveSettings(a.ConfigFile, s); err != nil {
			writeError(w, http.StatusInternalServerError, "không ghi được file cấu hình: "+err.Error())
			return
		}
		saved = true
	}
	a.Reporter.Apply(s)
	a.Log.Info("đổi cấu hình kết nối qua web UI",
		"coordinator", s.CoordinatorURL, "addr", s.Addr, "capacity", s.Capacity,
		"heartbeat_interval", s.HeartbeatInterval, "saved", saved)
	writeJSON(w, http.StatusOK, map[string]any{"settings": s, "saved": saved})
}

// testConnection thử gọi GET /nodes của một coordinator trước khi lưu. Chọn
// /nodes thay vì /healthz vì node và gateway cũng có /healthz: gõ nhầm địa
// chỉ một node thì phép thử phải báo sai.
func (a *Admin) testConnection(w http.ResponseWriter, r *http.Request) {
	var req struct {
		CoordinatorURL string `json:"coordinator_url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "JSON không hợp lệ: "+err.Error())
		return
	}
	base := Settings{CoordinatorURL: req.CoordinatorURL}.Clean().CoordinatorURL
	if err := checkURL(base); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	start := time.Now()
	nodes, err := countNodes(r.Context(), base)
	result := map[string]any{"ok": err == nil, "latency_ms": time.Since(start).Milliseconds()}
	if err != nil {
		result["error"] = err.Error()
	} else {
		result["nodes"] = nodes
	}
	// Kết quả phép thử là dữ liệu, không phải lỗi của request, nên luôn 200.
	writeJSON(w, http.StatusOK, result)
}

func countNodes(ctx context.Context, base string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/nodes", nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("trả về %s, có vẻ đây không phải coordinator", resp.Status)
	}
	var nodes []json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return 0, fmt.Errorf("trả lời không đúng dạng của coordinator: %w", err)
	}
	return len(nodes), nil
}

func (a *Admin) disconnect(w http.ResponseWriter, r *http.Request) {
	a.Reporter.SetPaused(true)
	a.Log.Warn("ngắt heartbeat theo yêu cầu từ web UI; coordinator sẽ coi node này là chết")
	w.WriteHeader(http.StatusNoContent)
}

func (a *Admin) connect(w http.ResponseWriter, r *http.Request) {
	a.Reporter.SetPaused(false)
	a.Log.Info("nối lại heartbeat theo yêu cầu từ web UI")
	w.WriteHeader(http.StatusNoContent)
}
