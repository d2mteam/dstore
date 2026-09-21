package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"

	"golang.org/x/sync/errgroup"

	"dstore-demo/internal/ec"
	"dstore-demo/internal/protocol"
)

// Upload đẩy một file trên đĩa lên hệ thống.
func (c *Client) Upload(ctx context.Context, path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	return c.UploadStream(ctx, filepath.Base(path), info.Size(), f)
}

// UploadStream đọc size byte từ r, chia thành segment, encode mỗi segment thành
// k+m shard rồi đẩy song song lên các node do coordinator chỉ định. File chỉ tồn
// tại sau khi commit.
//
// Phải biết trước size vì coordinator cần số segment để lập kế hoạch đặt shard.
// Mỗi lúc chỉ giữ một segment trong bộ nhớ, nên gateway stream được file lớn.
func (c *Client) UploadStream(ctx context.Context, name string, size int64, r io.Reader) (string, error) {
	coder, err := ec.New(c.K, c.M)
	if err != nil {
		return "", err
	}

	// 1. Xin kế hoạch đặt shard.
	var plan protocol.PlanResponse
	req := protocol.PlanRequest{
		Name: name, Size: size,
		K: c.K, M: c.M, SegmentSize: c.SegmentSize,
	}
	if err := c.call(ctx, http.MethodPost, "/files/plan", req, &plan); err != nil {
		return "", err
	}
	c.Log.Info("nhận kế hoạch", "file", plan.FileID, "segments", len(plan.Segments))

	// 2-4. Đọc, encode và đẩy từng segment.
	commit := protocol.CommitRequest{Segments: make([]protocol.CommitSegment, 0, len(plan.Segments))}
	buf := make([]byte, c.SegmentSize)
	for _, segPlan := range plan.Segments {
		n, err := io.ReadFull(r, buf)
		if err != nil && err != io.ErrUnexpectedEOF && err != io.EOF {
			return "", fmt.Errorf("đọc segment %d: %w", segPlan.Index, err)
		}
		segment := buf[:n]

		shards, err := coder.Encode(segment)
		if err != nil {
			return "", fmt.Errorf("segment %d: %w", segPlan.Index, err)
		}

		placed, err := c.uploadShards(ctx, segPlan, shards)
		if err != nil {
			return "", fmt.Errorf("segment %d: %w", segPlan.Index, err)
		}
		commit.Segments = append(commit.Segments, protocol.CommitSegment{
			Index: segPlan.Index, OrigSize: n, Shards: placed,
		})
		c.Log.Info("đã đẩy segment", "segment", segPlan.Index, "shards", len(placed), "bytes", n)
	}

	// 5. Commit metadata.
	if err := c.call(ctx, http.MethodPost, "/files/"+plan.FileID+"/commit", commit, nil); err != nil {
		return "", err
	}
	return plan.FileID, nil
}

// uploadShards đẩy k+m shard của một segment lên node, mỗi shard một goroutine.
func (c *Client) uploadShards(ctx context.Context, segPlan protocol.SegmentPlan, shards [][]byte) ([]protocol.CommitShard, error) {
	if len(segPlan.Nodes) != len(shards) {
		return nil, fmt.Errorf("plan có %d node nhưng segment có %d shard", len(segPlan.Nodes), len(shards))
	}

	// Mỗi goroutine chỉ ghi vào ô của riêng nó nên không cần khóa.
	placed := make([]protocol.CommitShard, len(shards))

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(c.Parallel)
	for i := range shards {
		shardIndex, data, node := i, shards[i], segPlan.Nodes[i]
		g.Go(func() error {
			hash := ec.Hash(data)
			err := c.shards.Put(gctx, node.Addr, hash, data)
			if err != nil {
				// Node hỏng: xin coordinator node thay thế rồi thử lại.
				c.Log.Warn("đẩy shard lỗi, xin node thay thế",
					"segment", segPlan.Index, "shard", shardIndex, "node", node.ID, "err", err)
				node, err = c.replacementNode(gctx, segPlan)
				if err != nil {
					return err
				}
				if err := c.shards.Put(gctx, node.Addr, hash, data); err != nil {
					return err
				}
			}
			placed[shardIndex] = protocol.CommitShard{Index: shardIndex, Hash: hash, NodeID: node.ID}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return placed, nil
}

// replacementNode xin một node ngoài danh sách node đã dành cho segment này,
// để k+m shard vẫn nằm trên k+m node khác nhau.
func (c *Client) replacementNode(ctx context.Context, segPlan protocol.SegmentPlan) (protocol.NodeRef, error) {
	exclude := make([]string, 0, len(segPlan.Nodes))
	for _, n := range segPlan.Nodes {
		exclude = append(exclude, n.ID)
	}

	var resp protocol.PickResponse
	err := c.call(ctx, http.MethodPost, "/nodes/pick",
		protocol.PickRequest{Count: 1, Exclude: exclude}, &resp)
	if err != nil {
		return protocol.NodeRef{}, err
	}
	if len(resp.Nodes) == 0 {
		return protocol.NodeRef{}, fmt.Errorf("không còn node thay thế")
	}
	return resp.Nodes[0], nil
}
