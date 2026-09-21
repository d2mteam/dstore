package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"dstore-demo/internal/shardclient"
)

// Collector dọn shard mồ côi: shard còn trên đĩa node nhưng metadata không còn
// tham chiếu tới. Thường gặp nhất là sau repair — shard đã được tái tạo ở node
// khác, rồi node chết sống lại và vẫn giữ bản cũ.
//
// Rủi ro duy nhất là xóa nhầm shard đang upload: client PUT shard lên node
// *trước khi* commit, nên trong khoảng đó coordinator chưa biết hash và shard
// trông y hệt shard mồ côi. Vì vậy một shard phải bị thấy là mồ côi liên tục
// suốt Grace rồi mới bị xóa; Grace cần lớn hơn thời gian upload một file.
type Collector struct {
	Store    *Store
	Shards   *shardclient.Client
	Interval time.Duration
	Grace    time.Duration
	Metrics  *Metrics
	Log      *slog.Logger

	mu sync.Mutex
	// suspects[nodeID][hash] = lần đầu thấy shard này mồ côi.
	suspects map[string]map[string]time.Time
}

func (c *Collector) Run(ctx context.Context) {
	ticker := time.NewTicker(c.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.RunOnce(ctx)
		}
	}
}

// RunOnce quét mọi node online một lượt, trả về số shard đã xóa.
func (c *Collector) RunOnce(ctx context.Context) int {
	deleted := 0
	for _, node := range c.Store.OnlineNodes() {
		if ctx.Err() != nil {
			return deleted
		}
		deleted += c.collectNode(ctx, node.ID, node.Addr)
	}
	return deleted
}

func (c *Collector) collectNode(ctx context.Context, nodeID, addr string) int {
	onDisk, err := c.Shards.List(ctx, addr)
	if err != nil {
		c.Log.Warn("không liệt kê được shard của node", "node", nodeID, "err", err)
		return 0
	}
	referenced := c.Store.ReferencedHashes(nodeID)

	now := time.Now()
	seen := c.suspectsOf(nodeID)
	next := make(map[string]time.Time)
	deleted := 0

	for _, hash := range onDisk {
		if referenced[hash] {
			continue // metadata vẫn cần shard này
		}
		firstSeen, wasSuspect := seen[hash]
		if !wasSuspect {
			firstSeen = now // lần đầu thấy: ghi nhận rồi để lượt sau xử lý
		}
		if now.Sub(firstSeen) < c.Grace {
			next[hash] = firstSeen
			continue
		}
		if err := c.Shards.Delete(ctx, addr, hash); err != nil {
			c.Log.Warn("xóa shard mồ côi lỗi", "node", nodeID, "err", err)
			next[hash] = firstSeen // giữ lại để lượt sau thử tiếp
			continue
		}
		deleted++
	}

	c.setSuspects(nodeID, next)
	if deleted > 0 {
		c.Metrics.OrphansDeleted(deleted)
		c.Log.Info("đã dọn shard mồ côi", "node", nodeID, "da_xoa", deleted, "con_theo_doi", len(next))
	}
	return deleted
}

func (c *Collector) suspectsOf(nodeID string) map[string]time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.suspects[nodeID]
}

func (c *Collector) setSuspects(nodeID string, s map[string]time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.suspects == nil {
		c.suspects = make(map[string]map[string]time.Time)
	}
	if len(s) == 0 {
		delete(c.suspects, nodeID)
		return
	}
	c.suspects[nodeID] = s
}
