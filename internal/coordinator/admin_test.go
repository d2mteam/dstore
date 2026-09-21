package coordinator_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dstore-demo/internal/coordinator"
	"dstore-demo/internal/protocol"
	"dstore-demo/internal/shardclient"
)

func newAdmin(meta coordinator.MetaStore) (*coordinator.Admin, http.Handler) {
	a := &coordinator.Admin{
		Store:    coordinator.NewStore(meta),
		Meta:     meta,
		Settings: coordinator.NewSettingsStore(meta, defaultSettings),
		Log:      slog.New(slog.DiscardHandler),
	}
	return a, a.Routes()
}

func do(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

// Giá trị sai bị từ chối và không đổi gì; giá trị đúng được áp dụng và còn
// nguyên sau restart.
func TestAdminUpdateSettings(t *testing.T) {
	meta := coordinator.NewMemoryMeta()
	admin, h := newAdmin(meta)

	for _, body := range []string{
		`{"heartbeat_timeout":"abc","gc_grace":"2m"}`,
		`{"heartbeat_timeout":"20s","gc_grace":"1s"}`, // grace nhỏ thì xóa nhầm shard đang upload
		`{"heartbeat_timeout":"100ms","gc_grace":"2m"}`,
	} {
		if rec := do(h, http.MethodPut, "/ui/api/settings", body); rec.Code != http.StatusBadRequest {
			t.Errorf("PUT %s = %d, mong đợi 400", body, rec.Code)
		}
	}
	if got := admin.Settings.Get(); got != defaultSettings {
		t.Fatalf("request lỗi mà settings vẫn đổi: %+v", got)
	}

	rec := do(h, http.MethodPut, "/ui/api/settings", `{"heartbeat_timeout":"45s","gc_grace":"5m","repair_paused":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT hợp lệ = %d %s", rec.Code, rec.Body)
	}
	want := coordinator.Settings{
		HeartbeatTimeout: protocol.Duration(45 * time.Second),
		GCGrace:          protocol.Duration(5 * time.Minute),
		RepairPaused:     true,
	}
	if got := admin.Settings.Get(); got != want {
		t.Fatalf("settings = %+v, mong đợi %+v", got, want)
	}

	restarted := coordinator.NewSettingsStore(meta, defaultSettings)
	if found, err := restarted.Load(context.Background()); err != nil || !found {
		t.Fatalf("Load = %v, %v", found, err)
	}
	if got := restarted.Get(); got != want {
		t.Fatalf("sau restart settings = %+v, mong đợi %+v", got, want)
	}
}

func TestAdminWhenDatabaseDown(t *testing.T) {
	admin, h := newAdmin(brokenMeta{coordinator.NewMemoryMeta()})

	rec := do(h, http.MethodPut, "/ui/api/settings", `{"heartbeat_timeout":"45s","gc_grace":"5m"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("lưu settings khi DB chết = %d, mong đợi 503", rec.Code)
	}
	if got := admin.Settings.Get(); got != defaultSettings {
		t.Fatalf("chưa lưu được mà settings đã đổi: %+v", got)
	}

	var ov struct {
		Database struct {
			OK    bool   `json:"ok"`
			Error string `json:"error"`
		} `json:"database"`
	}
	rec = do(h, http.MethodGet, "/ui/api/overview", "")
	if err := json.Unmarshal(rec.Body.Bytes(), &ov); err != nil {
		t.Fatal(err)
	}
	if ov.Database.OK || ov.Database.Error == "" {
		t.Fatalf("overview không báo DB lỗi: %+v", ov.Database)
	}
}

func TestAdminOverview(t *testing.T) {
	admin, h := newAdmin(coordinator.NewMemoryMeta())
	admin.Store.RegisterNode(context.Background(), "node-1", "http://node-1:8080", 1000)
	commitTwoSegments(t, admin.Store, "f1")

	var ov struct {
		Stats    coordinator.Stats    `json:"stats"`
		Settings coordinator.Settings `json:"settings"`
		Database struct {
			OK bool `json:"ok"`
		} `json:"database"`
	}
	rec := do(h, http.MethodGet, "/ui/api/overview", "")
	if err := json.Unmarshal(rec.Body.Bytes(), &ov); err != nil {
		t.Fatalf("%v: %s", err, rec.Body)
	}
	if ov.Stats.Files != 1 || ov.Stats.Segments != 2 || ov.Stats.SegmentHealth[3] != 2 {
		t.Errorf("stats = %+v", ov.Stats)
	}
	if ov.Settings != defaultSettings || !ov.Database.OK {
		t.Errorf("settings = %+v, database ok = %v", ov.Settings, ov.Database.OK)
	}
	if !strings.Contains(rec.Body.String(), `"heartbeat_timeout":"20s"`) {
		t.Errorf("duration phải ở dạng chuỗi như env: %s", rec.Body)
	}
}

// Tạm dừng chỉ chặn vòng tự động; bấm "chạy ngay" thì vẫn chạy.
func TestPausedRepairRunsOnlyWhenTriggered(t *testing.T) {
	meta := coordinator.NewMemoryMeta()
	store := coordinator.NewStore(meta)
	paused := defaultSettings
	paused.RepairPaused = true
	metrics := &coordinator.Metrics{}
	r := &coordinator.Repairer{
		Store: store, Placer: coordinator.NewPlacer(store), Shards: shardclient.New(time.Second),
		Settings: coordinator.NewSettingsStore(meta, paused),
		Interval: 5 * time.Millisecond, Metrics: metrics, Log: slog.New(slog.DiscardHandler),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	time.Sleep(100 * time.Millisecond) // khoảng 20 nhịp ticker
	if last := metrics.Snapshot().LastRepair; !last.IsZero() {
		t.Fatal("repair vẫn tự chạy khi đang tạm dừng")
	}

	h := (&coordinator.Admin{Repairer: r, Log: slog.New(slog.DiscardHandler)}).Routes()
	if rec := do(h, http.MethodPost, "/ui/api/repair", ""); rec.Code != http.StatusAccepted {
		t.Fatalf("POST /ui/api/repair = %d", rec.Code)
	}
	deadline := time.Now().Add(2 * time.Second)
	for metrics.Snapshot().LastRepair.IsZero() {
		if time.Now().After(deadline) {
			t.Fatal("bấm chạy ngay mà repair không chạy")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
