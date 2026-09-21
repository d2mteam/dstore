// Package ec bọc thư viện reedsolomon cho đúng nhu cầu của demo:
// mã hóa một segment thành k+m shard và ghép ngược lại khi thiếu shard.
package ec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/klauspost/reedsolomon"
)

// Coder mã hóa/giải mã theo cấu hình k data shard + m parity shard.
type Coder struct {
	K   int
	M   int
	enc reedsolomon.Encoder
}

func New(k, m int) (*Coder, error) {
	if k <= 0 || m <= 0 {
		return nil, fmt.Errorf("ec: k và m phải > 0 (k=%d, m=%d)", k, m)
	}
	enc, err := reedsolomon.New(k, m)
	if err != nil {
		return nil, fmt.Errorf("ec: tạo encoder: %w", err)
	}
	return &Coder{K: k, M: m, enc: enc}, nil
}

// Total là số shard của một segment.
func (c *Coder) Total() int { return c.K + c.M }

// Encode cắt segment thành k data shard (có đệm) rồi tính m parity shard.
// Kết quả luôn có đúng k+m phần tử, phần tử i là shard có shard_index = i.
func (c *Coder) Encode(segment []byte) ([][]byte, error) {
	shards, err := c.enc.Split(segment)
	if err != nil {
		return nil, fmt.Errorf("ec: split: %w", err)
	}
	if err := c.enc.Encode(shards); err != nil {
		return nil, fmt.Errorf("ec: encode: %w", err)
	}
	return shards, nil
}

// Reconstruct điền lại các shard nil. Cần ít nhất k shard khác nil.
func (c *Coder) Reconstruct(shards [][]byte) error {
	if countPresent(shards) < c.K {
		return fmt.Errorf("ec: chỉ còn %d/%d shard, cần tối thiểu %d", countPresent(shards), len(shards), c.K)
	}
	if err := c.enc.Reconstruct(shards); err != nil {
		return fmt.Errorf("ec: reconstruct: %w", err)
	}
	return nil
}

// Decode khôi phục nội dung gốc của segment từ các shard (shard thiếu để nil).
// origSize là kích thước segment trước khi Split đệm thêm.
func (c *Coder) Decode(shards [][]byte, origSize int) ([]byte, error) {
	if len(shards) != c.Total() {
		return nil, fmt.Errorf("ec: cần %d shard, nhận %d", c.Total(), len(shards))
	}
	if err := c.Reconstruct(shards); err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	if err := c.enc.Join(&buf, shards, origSize); err != nil {
		return nil, fmt.Errorf("ec: join: %w", err)
	}
	return buf.Bytes(), nil
}

func countPresent(shards [][]byte) int {
	n := 0
	for _, s := range shards {
		if s != nil {
			n++
		}
	}
	return n
}

// Hash trả về SHA-256 dạng hex — chính là ID nội dung của shard.
func Hash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
