package client

import (
	"context"
	"fmt"
	"io"
	"os"

	"dstore-demo/internal/ec"
	"dstore-demo/internal/protocol"
)

// Download lấy manifest rồi ghi toàn bộ file ra đĩa.
func (c *Client) Download(ctx context.Context, fileID, outPath string) error {
	out, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer out.Close()

	if err := c.DownloadTo(ctx, fileID, out); err != nil {
		return err
	}
	return out.Close()
}

// DownloadTo ghi toàn bộ nội dung file vào w.
func (c *Client) DownloadTo(ctx context.Context, fileID string, w io.Writer) error {
	man, err := c.Manifest(ctx, fileID)
	if err != nil {
		return err
	}
	return c.WriteRange(ctx, man, 0, man.Size-1, w)
}

// WriteRange ghi đoạn byte [start, end] của file vào w; mỗi segment chỉ cần k
// shard bất kỳ. Segment nằm ngoài khoảng yêu cầu bị bỏ qua hoàn toàn — nhờ vậy
// đọc một đoạn giữa file lớn không phải tải cả file về.
func (c *Client) WriteRange(ctx context.Context, man protocol.Manifest, start, end int64, w io.Writer) error {
	if man.Size == 0 {
		return nil
	}
	if start < 0 || end >= man.Size || start > end {
		return fmt.Errorf("khoảng [%d, %d] nằm ngoài file %d byte", start, end, man.Size)
	}
	coder, err := ec.New(man.K, man.M)
	if err != nil {
		return err
	}

	segSize := int64(man.SegmentSize)
	first, last := start/segSize, end/segSize
	for _, seg := range man.Segments {
		i := int64(seg.Index)
		if i < first || i > last {
			continue
		}
		shards, err := c.fetchSegment(ctx, coder, seg)
		if err != nil {
			return fmt.Errorf("segment %d: %w", seg.Index, err)
		}
		data, err := coder.Decode(shards, seg.OrigSize)
		if err != nil {
			return fmt.Errorf("segment %d: %w", seg.Index, err)
		}

		// Cắt phần thừa ở segment đầu và segment cuối của khoảng.
		segStart := i * segSize
		from, to := int64(0), int64(len(data))
		if start > segStart {
			from = start - segStart
		}
		if end-segStart+1 < to {
			to = end - segStart + 1
		}
		if _, err := w.Write(data[from:to]); err != nil {
			return err
		}
	}
	return nil
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
