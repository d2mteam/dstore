package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"dstore-demo/internal/protocol"
)

// Reporter lo việc đăng ký với coordinator và gửi heartbeat định kỳ.
type Reporter struct {
	CoordinatorURL string
	NodeID         string
	Addr           string
	Capacity       int64
	Interval       time.Duration
	Store          ShardStore
	Log            *slog.Logger

	client *http.Client
}

func (r *Reporter) httpClient() *http.Client {
	if r.client == nil {
		r.client = &http.Client{Timeout: 5 * time.Second}
	}
	return r.client
}

// Register đăng ký node với coordinator; thử lại cho tới khi coordinator sẵn sàng.
func (r *Reporter) Register(ctx context.Context) error {
	body := protocol.RegisterRequest{Addr: r.Addr, Capacity: r.Capacity, NodeID: r.NodeID}
	for attempt := 1; ; attempt++ {
		err := r.postJSON(ctx, r.CoordinatorURL+"/nodes/register", body, nil)
		if err == nil {
			r.Log.Info("đã đăng ký với coordinator", "node_id", r.NodeID, "addr", r.Addr)
			return nil
		}
		r.Log.Warn("đăng ký thất bại, thử lại", "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

// Run gửi heartbeat cho tới khi ctx bị hủy.
func (r *Reporter) Run(ctx context.Context) {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			used, count := r.Store.Stats()
			hb := protocol.HeartbeatRequest{UsedBytes: used, ShardCount: count}
			url := fmt.Sprintf("%s/nodes/%s/heartbeat", r.CoordinatorURL, r.NodeID)
			if err := r.postJSON(ctx, url, hb, nil); err != nil {
				r.Log.Warn("heartbeat lỗi", "err", err)
				// Coordinator có thể vừa restart và quên node này -> đăng ký lại.
				if regErr := r.postJSON(ctx, r.CoordinatorURL+"/nodes/register",
					protocol.RegisterRequest{Addr: r.Addr, Capacity: r.Capacity, NodeID: r.NodeID}, nil); regErr == nil {
					r.Log.Info("đã đăng ký lại với coordinator")
				}
			}
		}
	}
}

func (r *Reporter) postJSON(ctx context.Context, url string, in, out any) error {
	buf, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s trả về %s", url, resp.Status)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
