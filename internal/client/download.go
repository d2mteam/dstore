package client

import (
	"context"
	"fmt"
	"os"

	"dstore-demo/internal/ec"
	"dstore-demo/internal/protocol"
)

// Download lấy manifest rồi tải về; mỗi segment chỉ cần k shard bất kỳ.
func (c *Client) Download(ctx context.Context, fileID, outPath string) error {
	man, err := c.Manifest(ctx, fileID)
	if err != nil {
		return err
	}
	coder, err := ec.New(man.K, man.M)
	if err != nil {
		return err
	}

	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	for _, seg := range man.Segments {
		shards, err := c.fetchSegment(ctx, coder, seg)
		if err != nil {
			return fmt.Errorf("segment %d: %w", seg.Index, err)
		}
		data, err := coder.Decode(shards, seg.OrigSize)
		if err != nil {
			return fmt.Errorf("segment %d: %w", seg.Index, err)
		}
		if _, err := out.Write(data); err != nil {
			return err
		}
	}
	return out.Close()
}

// fetchSegment tải song song mọi shard của segment và dừng ngay khi đủ k shard;
// node chậm hoặc node chết không làm chậm cả quá trình.
func (c *Client) fetchSegment(ctx context.Context, coder *ec.Coder, seg protocol.ManifestSegment) ([][]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // hủy các request còn dở khi đã đủ k shard

	type result struct {
		index int
		data  []byte // nil nếu lấy không được
	}

	results := make(chan result, len(seg.Shards))
	inflight := 0
	for _, sh := range seg.Shards {
		if sh.NodeAddr == "" {
			continue
		}
		// Thử cả shard đang `lost`: node có thể đã sống lại mà coordinator
		// chưa kịp cập nhật, còn node chết thì từ chối kết nối rất nhanh.
		inflight++
		go func(sh protocol.ManifestShard) {
			data, err := c.shards.Get(ctx, sh.NodeAddr, sh.Hash)
			if err != nil {
				if ctx.Err() == nil {
					c.Log.Debug("lấy shard lỗi", "segment", seg.Index, "shard", sh.Index, "node", sh.NodeID, "err", err)
				}
				data = nil
			}
			results <- result{index: sh.Index, data: data}
		}(sh)
	}

	shards := make([][]byte, coder.Total())
	got := 0
	for i := 0; i < inflight && got < coder.K; i++ {
		r := <-results
		if r.data == nil || r.index >= len(shards) || shards[r.index] != nil {
			continue
		}
		shards[r.index] = r.data
		got++
	}
	if got < coder.K {
		return nil, fmt.Errorf("chỉ lấy được %d shard, cần tối thiểu %d", got, coder.K)
	}
	return shards, nil
}
