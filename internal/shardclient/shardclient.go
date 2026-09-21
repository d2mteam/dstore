// Package shardclient nói chuyện HTTP với node để đọc/ghi shard.
// Cả dsctl (upload/download) và coordinator (repair) đều dùng chung gói này.
package shardclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"dstore-demo/internal/ec"
)

type Client struct {
	HTTP *http.Client
}

func New(timeout time.Duration) *Client {
	return &Client{HTTP: &http.Client{Timeout: timeout}}
}

// Put ghi shard lên node. Node tự kiểm tra hash và từ chối nếu không khớp.
func (c *Client) Put(ctx context.Context, nodeAddr, hash string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		nodeAddr+"/shards/"+hash, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(data))

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("PUT %s/shards/%s: %s: %s", nodeAddr, short(hash), resp.Status, bytes.TrimSpace(body))
	}
	return nil
}

// Get đọc shard và kiểm tra lại hash; nội dung sai coi như không có shard.
func (c *Client) Get(ctx context.Context, nodeAddr, hash string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nodeAddr+"/shards/"+hash, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s/shards/%s: %s", nodeAddr, short(hash), resp.Status)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if got := ec.Hash(data); got != hash {
		return nil, fmt.Errorf("shard %s từ %s có hash %s: dữ liệu hỏng", short(hash), nodeAddr, short(got))
	}
	return data, nil
}

// List trả về hash của mọi shard node đang giữ.
func (c *Client) List(ctx context.Context, nodeAddr string) ([]string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, nodeAddr+"/shards", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s/shards: %s", nodeAddr, resp.Status)
	}
	var hashes []string
	if err := json.NewDecoder(resp.Body).Decode(&hashes); err != nil {
		return nil, err
	}
	return hashes, nil
}

func (c *Client) Delete(ctx context.Context, nodeAddr, hash string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, nodeAddr+"/shards/"+hash, nil)
	if err != nil {
		return err
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("DELETE %s/shards/%s: %s", nodeAddr, short(hash), resp.Status)
	}
	return nil
}

// short rút gọn hash cho dễ đọc trong log.
func short(hash string) string {
	if len(hash) > 8 {
		return hash[:8]
	}
	return hash
}
