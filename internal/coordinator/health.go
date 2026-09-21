package coordinator

import (
	"context"
	"log/slog"
	"time"

	"dstore-demo/internal/shardclient"
)

// HealthMonitor quét định kỳ để phát hiện node quá hạn heartbeat.
type HealthMonitor struct {
	Store    *Store
	Shards   *shardclient.Client
	Interval time.Duration // tần suất quét, demo: 5s
	Timeout  time.Duration // quá hạn này không heartbeat thì coi là chết, demo: 20s
	Metrics  *Metrics
	Log      *slog.Logger
}

func (h *HealthMonitor) Run(ctx context.Context) {
	ticker := time.NewTicker(h.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, id := range h.Store.ExpiredNodes(h.Timeout) {
				lost := h.Store.MarkOffline(id)
				h.Metrics.NodeLost()
				h.Log.Warn("node mất heartbeat -> offline", "node", id, "shard_lost", lost)
			}
		}
	}
}

// Reconcile chạy khi một node quay lại: hỏi node đang giữ những shard nào
// rồi đánh dấu ok trở lại các shard tương ứng.
func (h *HealthMonitor) Reconcile(ctx context.Context, nodeID, addr string) {
	hashes, err := h.Shards.List(ctx, addr)
	if err != nil {
		h.Log.Warn("không đối chiếu được shard của node vừa quay lại", "node", nodeID, "err", err)
		return
	}
	present := make(map[string]bool, len(hashes))
	for _, hash := range hashes {
		present[hash] = true
	}
	restored := h.Store.RestoreShards(nodeID, present)
	h.Log.Info("node quay lại", "node", nodeID, "shard_on_disk", len(hashes), "shard_restored", restored)
}
