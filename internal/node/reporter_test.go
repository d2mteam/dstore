package node

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"dstore-demo/internal/protocol"
)

// fakeCoordinator đếm số lần đăng ký và heartbeat mà node gửi tới.
type fakeCoordinator struct {
	URL string

	mu         sync.Mutex
	registers  int
	heartbeats int
	lastAddr   string
}

func newFakeCoordinator(t *testing.T) *fakeCoordinator {
	f := &fakeCoordinator{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /nodes/register", func(w http.ResponseWriter, r *http.Request) {
		var req protocol.RegisterRequest
		json.NewDecoder(r.Body).Decode(&req)
		f.mu.Lock()
		f.registers++
		f.lastAddr = req.Addr
		f.mu.Unlock()
		writeJSON(w, http.StatusOK, protocol.RegisterResponse{NodeID: req.NodeID})
	})
	mux.HandleFunc("POST /nodes/{id}/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.heartbeats++
		f.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /nodes", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, []protocol.NodeInfo{{ID: "node-1"}, {ID: "node-2"}})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	f.URL = srv.URL
	return f
}

func (f *fakeCoordinator) counts() (registers, heartbeats int, lastAddr string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registers, f.heartbeats, f.lastAddr
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("hết 3s mà chưa thấy: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func testSettings(coordURL string) Settings {
	return Settings{
		CoordinatorURL:    coordURL,
		Addr:              "http://node-1:8080",
		Capacity:          1 << 20,
		HeartbeatInterval: protocol.Duration(10 * time.Millisecond),
	}
}

func startReporter(t *testing.T, s Settings) *Reporter {
	t.Helper()
	store, err := NewDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := NewReporter("node-1", s, store, slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go r.Run(ctx)
	return r
}

func (r *Reporter) state() string { return r.Status().State }

// Đổi coordinator lúc đang chạy: đăng ký ngay với bên mới, thôi heartbeat bên cũ.
func TestReporterSwitchesCoordinator(t *testing.T) {
	a, b := newFakeCoordinator(t), newFakeCoordinator(t)
	r := startReporter(t, testSettings(a.URL))
	waitFor(t, "heartbeat tới coordinator a", func() bool {
		_, hb, _ := a.counts()
		return hb >= 2 && r.state() == StateConnected
	})

	s := testSettings(b.URL)
	s.Addr = "http://10.0.0.9:8080"
	r.Apply(s)
	waitFor(t, "đăng ký với coordinator b bằng địa chỉ mới", func() bool {
		reg, hb, addr := b.counts()
		return reg == 1 && hb >= 1 && addr == "http://10.0.0.9:8080"
	})

	_, before, _ := a.counts()
	time.Sleep(50 * time.Millisecond)
	if _, after, _ := a.counts(); after != before {
		t.Fatalf("vẫn heartbeat tới coordinator cũ: %d -> %d", before, after)
	}
}

func TestReporterPauseAndResume(t *testing.T) {
	a := newFakeCoordinator(t)
	r := startReporter(t, testSettings(a.URL))
	waitFor(t, "kết nối", func() bool { return r.state() == StateConnected })

	r.SetPaused(true)
	if got := r.state(); got != StatePaused {
		t.Fatalf("state = %s, mong đợi %s", got, StatePaused)
	}
	time.Sleep(20 * time.Millisecond) // cho request đang bay (nếu có) về hết
	_, before, _ := a.counts()
	time.Sleep(60 * time.Millisecond)
	if _, after, _ := a.counts(); after != before {
		t.Fatalf("đã ngắt mà vẫn gửi heartbeat: %d -> %d", before, after)
	}

	reg, _, _ := a.counts()
	r.SetPaused(false)
	// Nối lại thì phải đăng ký lại: coordinator có thể đã coi node là chết và
	// cần đối chiếu shard.
	waitFor(t, "đăng ký lại sau khi nối", func() bool {
		after, _, _ := a.counts()
		return after == reg+1 && r.state() == StateConnected
	})
}

func TestReporterRecordsErrors(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	r := startReporter(t, testSettings(dead.URL))
	waitFor(t, "ghi nhận lỗi liên tiếp", func() bool {
		st := r.Status()
		return st.Failures >= 2 && st.LastError != "" && st.State == StateConnecting
	})
}

func TestSettingsValidate(t *testing.T) {
	good := testSettings("http://coordinator:9000")
	good.HeartbeatInterval = protocol.Duration(time.Second)
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	bad := map[string]func(*Settings){
		"thiếu scheme":      func(s *Settings) { s.CoordinatorURL = "coordinator:9000" },
		"scheme lạ":         func(s *Settings) { s.CoordinatorURL = "ftp://coordinator" },
		"addr rỗng":         func(s *Settings) { s.Addr = "" },
		"capacity bằng 0":   func(s *Settings) { s.Capacity = 0 },
		"heartbeat quá dày": func(s *Settings) { s.HeartbeatInterval = protocol.Duration(time.Millisecond) },
	}
	for name, mutate := range bad {
		s := good
		mutate(&s)
		if s.Validate() == nil {
			t.Errorf("%s: Validate không báo lỗi", name)
		}
	}

	s := Settings{CoordinatorURL: " http://coordinator:9000/ ", Addr: "http://node-1:8080/"}.Clean()
	if s.CoordinatorURL != "http://coordinator:9000" || s.Addr != "http://node-1:8080" {
		t.Fatalf("Clean = %+v", s)
	}
}

func TestSettingsFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config", "node.json") // thư mục chưa tồn tại
	defaults := testSettings("http://from-env:9000")

	got, found, err := LoadSettings(path, defaults)
	if err != nil || found || got != defaults {
		t.Fatalf("chưa có file: %+v, %v, %v", got, found, err)
	}

	want := testSettings("http://from-ui:9000")
	want.Capacity = 5 << 20
	if err := SaveSettings(path, want); err != nil {
		t.Fatal(err)
	}
	if got, found, err = LoadSettings(path, defaults); err != nil || !found || got != want {
		t.Fatalf("đọc lại = %+v, %v, %v; mong đợi %+v", got, found, err, want)
	}

	// File thiếu trường thì trường đó lấy theo env.
	os.WriteFile(path, []byte(`{"coordinator_url":"http://partial:9000"}`), 0o644)
	got, _, _ = LoadSettings(path, defaults)
	if got.CoordinatorURL != "http://partial:9000" || got.Capacity != defaults.Capacity {
		t.Fatalf("file thiếu trường: %+v", got)
	}

	os.WriteFile(path, []byte(`{hỏng`), 0o644)
	if _, _, err := LoadSettings(path, defaults); err == nil {
		t.Fatal("file hỏng mà không báo lỗi")
	}
}
