package node

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
)

// ShardStore là nơi node thực sự cất shard. Node lo phần giao thức (heartbeat,
// kiểm hash, API cho client); còn byte nằm ở đâu thì tùy ShardStore:
// DiskStore ghi xuống đĩa local, S3Store ghi vào MinIO.
type ShardStore interface {
	// Put ghi shard sau khi đã kiểm SHA-256 nội dung khớp hash.
	Put(ctx context.Context, hash string, r io.Reader) error
	// Open mở shard để đọc; caller phải Close.
	Open(ctx context.Context, hash string) (io.ReadCloser, int64, error)
	Has(ctx context.Context, hash string) bool
	Delete(ctx context.Context, hash string) error
	// List trả về hash của mọi shard đang giữ.
	List(ctx context.Context) ([]string, error)
	// Stats trả về số byte và số shard đang giữ.
	Stats() (used int64, count int)
}

var (
	ErrNotFound     = errors.New("không có shard này")
	ErrInvalidHash  = errors.New("hash không hợp lệ")
	ErrHashMismatch = errors.New("nội dung không khớp hash")
	ErrTooLarge     = errors.New("shard quá lớn")
)

func validHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil
}
