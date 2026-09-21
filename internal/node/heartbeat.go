package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"dstore-demo/internal/protocol"
)

// Reporter lo việc đăng ký với coordinator và gửi heartbeat định kỳ.
//
// Cấu hình kết nối đổi được lúc đang chạy (web UI gọi Apply), và heartbeat có
// thể tạm ngắt (SetPaused) để mô phỏng node chết mà không phải tắt tiến trình:
// node vẫn phục vụ shard, nhưng coordinator sẽ coi nó là chết và chạy repair.
type Reporter struct {
	nodeID string
	store  ShardStore
	log    *slog.Logger
	client *http.Client
	wake   chan struct{} // đánh thức vòng Run khi cấu hình vừa đổi

	mu         sync.Mutex
	settings   Settings
	paused     bool
	registered bool
	// gen tăng mỗi lần cấu hình đổi. Request đang bay với cấu hình cũ trả về
	// sau đó thì kết quả bị bỏ qua, không được ghi đè trạng thái mới.
	gen    int
	status ConnStatus
}

// Trạng thái kết nối hiện trên web UI.
const (
	StateConnected  = "connected"
	StateConnecting = "connecting" // đang đăng ký, hoặc vừa mất kết nối
	StatePaused     = "paused"     // người dùng bấm "ngắt kết nối"
)

// ConnStatus là tình trạng kết nối tới coordinator.
type ConnStatus struct {
	State       string    `json:"state"`
	LastOK      time.Time `json:"last_ok,omitzero"` // lần cuối coordinator trả lời thành công
	LastError   string    `json:"last_error,omitempty"`
	LastErrorAt time.Time `json:"last_error_at,omitzero"`
	Failures    int       `json:"failures"` // số lần lỗi liên tiếp
}

// NewReporter tạo Reporter cho node nodeID với cấu hình ban đầu s. Reporter chưa
// làm gì cho tới khi gọi Run; store chỉ dùng để đọc Stats gửi kèm heartbeat.
func NewReporter(nodeID string, s Settings, store ShardStore, log *slog.Logger) *Reporter {
	return &Reporter{
		nodeID:   nodeID,
		store:    store,
		log:      log,
		client:   &http.Client{Timeout: 5 * time.Second},
		wake:     make(chan struct{}, 1),
		settings: s,
	}
}

// Settings trả cấu hình kết nối đang dùng.
func (r *Reporter) Settings() Settings {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.settings
}

// Status trả tình trạng kết nối hiện tại, để web UI hiển thị.
func (r *Reporter) Status() ConnStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.status
	switch {
	case r.paused:
		st.State = StatePaused
	case r.registered:
		st.State = StateConnected
	default:
		st.State = StateConnecting
	}
	return st
}

// Apply đổi cấu hình và đăng ký lại ngay, có thể với một coordinator khác.
func (r *Reporter) Apply(s Settings) {
	r.mu.Lock()
	r.settings = s
	r.resetLocked()
	r.mu.Unlock()
	r.poke()
}

// SetPaused ngừng (true) hoặc nối lại (false) heartbeat.
func (r *Reporter) SetPaused(paused bool) {
	r.mu.Lock()
	r.paused = paused
	r.resetLocked()
	r.mu.Unlock()
	r.poke()
}

// resetLocked buộc lượt sau đăng ký lại từ đầu; gọi khi đang giữ r.mu.
func (r *Reporter) resetLocked() {
	r.registered = false
	r.gen++
	r.status.Failures = 0
}

func (r *Reporter) poke() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run đăng ký rồi gửi heartbeat cho tới khi ctx bị hủy. Heartbeat lỗi thì
// đăng ký lại, vì coordinator có thể vừa restart và quên node này.
func (r *Reporter) Run(ctx context.Context) {
	for {
		wait := r.step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
		case <-time.After(wait):
		}
	}
}

// step làm một việc tùy trạng thái (đăng ký hoặc heartbeat) và trả về thời
// gian chờ tới lượt sau.
func (r *Reporter) step(ctx context.Context) time.Duration {
	r.mu.Lock()
	s, paused, registered, gen := r.settings, r.paused, r.registered, r.gen
	r.mu.Unlock()
	interval := time.Duration(s.HeartbeatInterval)

	switch {
	case paused:
		return time.Hour // chỉ chờ SetPaused(false) hoặc Apply đánh thức

	case !registered:
		err := r.postJSON(ctx, s.CoordinatorURL+"/nodes/register",
			protocol.RegisterRequest{Addr: s.Addr, Capacity: s.Capacity, NodeID: r.nodeID})
		if !r.record(gen, err) || ctx.Err() != nil {
			return 0
		}
		if err != nil {
			r.log.Warn("đăng ký thất bại, thử lại", "coordinator", s.CoordinatorURL, "err", err)
			return time.Second
		}
		r.log.Info("đã đăng ký với coordinator", "coordinator", s.CoordinatorURL, "addr", s.Addr)
		return interval

	default:
		used, count := r.store.Stats()
		err := r.postJSON(ctx, fmt.Sprintf("%s/nodes/%s/heartbeat", s.CoordinatorURL, r.nodeID),
			protocol.HeartbeatRequest{UsedBytes: used, ShardCount: count})
		if !r.record(gen, err) || ctx.Err() != nil {
			return 0
		}
		if err != nil {
			r.log.Warn("heartbeat lỗi, đăng ký lại", "err", err)
			return 0
		}
		return interval
	}
}

// record ghi kết quả một request vào trạng thái kết nối. Trả về false nếu cấu
// hình đã đổi trong lúc request đang chạy: kết quả đó thuộc cấu hình cũ.
func (r *Reporter) record(gen int, err error) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if gen != r.gen {
		return false
	}
	if err != nil {
		r.registered = false
		r.status.LastError = err.Error()
		r.status.LastErrorAt = time.Now()
		r.status.Failures++
		return true
	}
	r.registered = true
	r.status.LastOK = time.Now()
	r.status.Failures = 0
	return true
}

func (r *Reporter) postJSON(ctx context.Context, url string, in any) error {
	buf, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s trả về %s", url, resp.Status)
	}
	return nil
}
