// Package client là phần logic của dsctl: nói chuyện với coordinator để lấy
// metadata, rồi trao đổi dữ liệu trực tiếp với node.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"dstore-demo/internal/protocol"
	"dstore-demo/internal/shardclient"
)

type Client struct {
	CoordinatorURL string
	K              int
	M              int
	SegmentSize    int
	Parallel       int // số request shard chạy song song tối đa
	Log            *slog.Logger

	http   *http.Client
	shards *shardclient.Client
}

func New(coordinatorURL string, k, m, segmentSize, parallel int, log *slog.Logger) *Client {
	if parallel <= 0 {
		parallel = 8
	}
	return &Client{
		CoordinatorURL: coordinatorURL,
		K:              k,
		M:              m,
		SegmentSize:    segmentSize,
		Parallel:       parallel,
		Log:            log,
		http:           &http.Client{Timeout: 30 * time.Second},
		shards:         shardclient.New(30 * time.Second),
	}
}

// Nodes lấy danh sách node cho `dsctl nodes`.
func (c *Client) Nodes(ctx context.Context) ([]protocol.NodeInfo, error) {
	var out []protocol.NodeInfo
	err := c.call(ctx, http.MethodGet, "/nodes", nil, &out)
	return out, err
}

// Files lấy danh sách file cho `dsctl ls`.
func (c *Client) Files(ctx context.Context) ([]protocol.FileInfo, error) {
	var out []protocol.FileInfo
	err := c.call(ctx, http.MethodGet, "/files", nil, &out)
	return out, err
}

// Manifest lấy metadata một file.
func (c *Client) Manifest(ctx context.Context, fileID string) (protocol.Manifest, error) {
	var out protocol.Manifest
	err := c.call(ctx, http.MethodGet, "/files/"+fileID, nil, &out)
	return out, err
}

// Delete xóa file khỏi hệ thống.
func (c *Client) Delete(ctx context.Context, fileID string) error {
	return c.call(ctx, http.MethodDelete, "/files/"+fileID, nil, nil)
}

// call gửi một request JSON tới coordinator.
func (c *Client) call(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		buf, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.CoordinatorURL+path, body)
	if err != nil {
		return err
	}
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("gọi coordinator %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		var errResp protocol.ErrorResponse
		json.NewDecoder(resp.Body).Decode(&errResp)
		if errResp.Error == "" {
			errResp.Error = resp.Status
		}
		return fmt.Errorf("coordinator %s %s: %s", method, path, errResp.Error)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}
