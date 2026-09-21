package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"dstore-demo/internal/ec"
	"dstore-demo/internal/protocol"
	"dstore-demo/internal/shardclient"
)

// Repairer tìm segment thiếu shard và tái tạo shard bị mất lên node khác.
// Đây là đường duy nhất dữ liệu đi qua coordinator.
type Repairer struct {
	Store    *Store
	Placer   *Placer
	Shards   *shardclient.Client
	Interval time.Duration
	Metrics  *Metrics
	Log      *slog.Logger
}

func (r *Repairer) Run(ctx context.Context) {
	ticker := time.NewTicker(r.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.RunOnce(ctx)
		}
	}
}

// RunOnce sửa tất cả segment đang thiếu shard, segment yếu nhất trước.
func (r *Repairer) RunOnce(ctx context.Context) {
	for _, seg := range r.Store.DegradedSegments() {
		if ctx.Err() != nil {
			return
		}
		if !seg.Repairable {
			// Còn dưới k shard: không cách nào dựng lại được.
			r.Log.Error("segment mất quá nhiều shard, không sửa được",
				"file", seg.FileID, "segment", seg.SegIndex, "shard_ok", seg.OKCount, "k", seg.K)
			r.Store.MarkFileDamaged(seg.FileID)
			continue
		}
		if err := r.repairSegment(ctx, seg); err != nil {
			r.Metrics.RepairFailed()
			r.Log.Warn("repair segment thất bại",
				"file", seg.FileID, "segment", seg.SegIndex, "err", err)
		}
	}
}

func (r *Repairer) repairSegment(ctx context.Context, seg SegmentRef) error {
	coder, err := ec.New(seg.K, seg.M)
	if err != nil {
		return err
	}

	// 1. Tải k shard khỏe về.
	shards := make([][]byte, coder.Total())
	healthyNodes := make([]string, 0, seg.K) // node đang giữ shard khỏe -> tránh đặt lại vào đây
	downloaded := 0
	for _, sh := range seg.Shards {
		if sh.Status != protocol.ShardOK {
			continue
		}
		healthyNodes = append(healthyNodes, sh.NodeID)
		if downloaded >= seg.K {
			continue // đủ k shard để tái tạo rồi
		}
		node, ok := r.Store.Node(sh.NodeID)
		if !ok {
			continue
		}
		data, err := r.Shards.Get(ctx, node.Addr, sh.Hash)
		if err != nil {
			r.Log.Warn("tải shard để repair lỗi", "node", sh.NodeID, "err", err)
			continue
		}
		shards[sh.Index] = data
		downloaded++
	}
	if downloaded < seg.K {
		return fmt.Errorf("chỉ tải được %d/%d shard cần thiết", downloaded, seg.K)
	}

	// 2. Tái tạo các shard còn thiếu.
	if err := coder.Reconstruct(shards); err != nil {
		return err
	}

	// 3. Đặt shard tái tạo lên node mới, khác các node đang giữ segment này.
	missing := missingIndexes(seg, coder.Total())
	if len(missing) == 0 {
		return nil
	}
	shardSize := int64(len(shards[0]))
	targets, err := r.Placer.Pick(len(missing), shardSize, healthyNodes)
	if err != nil {
		return fmt.Errorf("không tìm được node để đặt shard tái tạo: %w", err)
	}

	for i, shardIndex := range missing {
		target := targets[i]
		data := shards[shardIndex]
		hash := ec.Hash(data)
		if err := r.Shards.Put(ctx, target.Addr, hash, data); err != nil {
			r.Log.Warn("ghi shard tái tạo lỗi", "node", target.ID, "err", err)
			continue
		}
		if err := r.Store.ReplaceShard(seg.FileID, seg.SegIndex, shardIndex, hash, target.ID); err != nil {
			return err
		}
		r.Metrics.RepairedShard()
		r.Log.Info("đã tái tạo shard",
			"file", seg.FileID, "segment", seg.SegIndex, "shard", shardIndex, "node_moi", target.ID)
	}
	return nil
}

// missingIndexes liệt kê shard_index chưa có bản khỏe nào.
func missingIndexes(seg SegmentRef, total int) []int {
	healthy := make(map[int]bool, len(seg.Shards))
	for _, sh := range seg.Shards {
		if sh.Status == protocol.ShardOK {
			healthy[sh.Index] = true
		}
	}
	var out []int
	for i := 0; i < total; i++ {
		if !healthy[i] {
			out = append(out, i)
		}
	}
	return out
}
