package coordinator

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"dstore-demo/internal/protocol"
)

// Admin là API riêng của web UI: tổng quan cụm, sửa Settings, chạy ngay
// repair/GC. Danh sách node, file và manifest thì UI đọc thẳng từ API control
// plane sẵn có (/nodes, /files...). main gắn Admin dưới /ui/api/, sau lớp
// đăng nhập của webui.
type Admin struct {
	Store     *Store
	Meta      MetaStore
	Settings  *SettingsStore
	Repairer  *Repairer
	Collector *Collector
	Metrics   *Metrics
	Startup   StartupInfo
	Log       *slog.Logger
}

// StartupInfo là cấu hình đọc từ biến môi trường lúc khởi động. UI chỉ hiển
// thị, muốn đổi thì sửa env rồi restart.
type StartupInfo struct {
	Listen         string            `json:"listen"`
	Database       string            `json:"database"` // chuỗi kết nối đã bỏ mật khẩu
	HealthInterval protocol.Duration `json:"health_interval"`
	RepairInterval protocol.Duration `json:"repair_interval"`
	GCInterval     protocol.Duration `json:"gc_interval"`
	StartedAt      time.Time         `json:"started_at"`
}

func (a *Admin) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ui/api/overview", a.overview)
	mux.HandleFunc("PUT /ui/api/settings", a.updateSettings)
	mux.HandleFunc("POST /ui/api/repair", a.runRepair)
	mux.HandleFunc("POST /ui/api/gc", a.runGC)
	return mux
}

type dbStatus struct {
	OK        bool   `json:"ok"`
	Error     string `json:"error,omitempty"`
	LatencyMS int64  `json:"latency_ms"`
}

func (a *Admin) overview(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	start := time.Now()
	db := dbStatus{OK: true}
	if err := a.Meta.Ping(ctx); err != nil {
		db = dbStatus{Error: err.Error()}
	}
	db.LatencyMS = time.Since(start).Milliseconds()

	writeJSON(w, http.StatusOK, map[string]any{
		"stats":    a.Store.Stats(),
		"counters": a.Metrics.Snapshot(),
		"settings": a.Settings.Get(),
		"startup":  a.Startup,
		"database": db,
	})
}

func (a *Admin) updateSettings(w http.ResponseWriter, r *http.Request) {
	var v Settings
	if !decode(w, r, &v) {
		return
	}
	if err := a.Settings.Update(r.Context(), v); err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, ErrStorage) {
			status = http.StatusServiceUnavailable
		}
		writeError(w, status, err.Error())
		return
	}
	a.Log.Info("đổi settings qua web UI", "settings", v)
	writeJSON(w, http.StatusOK, v)
}

func (a *Admin) runRepair(w http.ResponseWriter, r *http.Request) {
	a.Repairer.Trigger()
	a.Log.Info("chạy repair ngay theo yêu cầu từ web UI")
	w.WriteHeader(http.StatusAccepted)
}

func (a *Admin) runGC(w http.ResponseWriter, r *http.Request) {
	a.Collector.Trigger()
	a.Log.Info("chạy GC ngay theo yêu cầu từ web UI")
	w.WriteHeader(http.StatusAccepted)
}
