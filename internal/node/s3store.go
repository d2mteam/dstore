package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"sync"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// S3Config là thông tin kết nối MinIO (hoặc bất kỳ dịch vụ tương thích S3 nào).
type S3Config struct {
	Endpoint  string // ví dụ "minio:9000", không kèm http://
	AccessKey string
	SecretKey string
	Bucket    string // mỗi node một bucket riêng
	UseSSL    bool
}

// maxShardBytes giới hạn kích thước một shard. Shard thường cỡ segment/k (1 MB
// với cấu hình mặc định), nên 64 MB là rộng rãi mà vẫn chặn được body bất thường.
const maxShardBytes = 64 << 20

// S3Store cất shard trong một bucket MinIO, mỗi shard một object tên là hash.
type S3Store struct {
	client *minio.Client
	bucket string

	mu    sync.RWMutex
	used  int64
	count int
}

// NewS3Store kết nối MinIO, tạo bucket nếu chưa có, rồi đếm lại shard đang có.
func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("tạo client MinIO: %w", err)
	}

	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("kiểm tra bucket %s: %w", cfg.Bucket, err)
	}
	if !exists {
		err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{})
		// Hai tiến trình cùng tạo một lúc thì bên thua nhận lỗi này — không sao cả.
		if err != nil && minio.ToErrorResponse(err).Code != "BucketAlreadyOwnedByYou" {
			return nil, fmt.Errorf("tạo bucket %s: %w", cfg.Bucket, err)
		}
	}

	s := &S3Store{client: client, bucket: cfg.Bucket}
	for obj := range client.ListObjects(ctx, cfg.Bucket, minio.ListObjectsOptions{Recursive: true}) {
		if obj.Err != nil {
			return nil, fmt.Errorf("liệt kê bucket %s: %w", cfg.Bucket, obj.Err)
		}
		s.used += obj.Size
		s.count++
	}
	return s, nil
}

// Put đọc hết shard vào RAM để kiểm SHA-256 trước khi ghi, nên trong bucket
// không bao giờ có object sai hash. Không cần file tạm như DiskStore: PutObject
// của S3 là nguyên tử — object hoặc có đủ, hoặc không có.
func (s *S3Store) Put(ctx context.Context, hash string, r io.Reader) error {
	if !validHash(hash) {
		return fmt.Errorf("%w: %q", ErrInvalidHash, hash)
	}
	data, err := io.ReadAll(io.LimitReader(r, maxShardBytes+1))
	if err != nil {
		return err
	}
	if len(data) > maxShardBytes {
		return fmt.Errorf("%w: vượt %d byte", ErrTooLarge, maxShardBytes)
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != hash {
		return fmt.Errorf("%w: nhận %s", ErrHashMismatch, got)
	}

	if s.Has(ctx, hash) {
		return nil // content addressing: đã có đúng nội dung này
	}
	_, err = s.client.PutObject(ctx, s.bucket, hash, bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		return fmt.Errorf("ghi object %s: %w", hash[:8], err)
	}

	s.mu.Lock()
	s.used += int64(len(data))
	s.count++
	s.mu.Unlock()
	return nil
}

func (s *S3Store) Open(ctx context.Context, hash string) (io.ReadCloser, int64, error) {
	if !validHash(hash) {
		return nil, 0, fmt.Errorf("%w: %q", ErrInvalidHash, hash)
	}
	obj, err := s.client.GetObject(ctx, s.bucket, hash, minio.GetObjectOptions{})
	if err != nil {
		return nil, 0, err
	}
	// GetObject chưa gửi request; Stat mới thật sự hỏi MinIO object có tồn tại không.
	info, err := obj.Stat()
	if err != nil {
		obj.Close()
		if isNotFound(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, err
	}
	return obj, info.Size, nil
}

func (s *S3Store) Has(ctx context.Context, hash string) bool {
	if !validHash(hash) {
		return false
	}
	_, err := s.client.StatObject(ctx, s.bucket, hash, minio.StatObjectOptions{})
	return err == nil
}

func (s *S3Store) Delete(ctx context.Context, hash string) error {
	if !validHash(hash) {
		return fmt.Errorf("%w: %q", ErrInvalidHash, hash)
	}
	info, err := s.client.StatObject(ctx, s.bucket, hash, minio.StatObjectOptions{})
	if err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return err
	}
	if err := s.client.RemoveObject(ctx, s.bucket, hash, minio.RemoveObjectOptions{}); err != nil {
		return err
	}
	s.mu.Lock()
	s.used -= info.Size
	s.count--
	s.mu.Unlock()
	return nil
}

func (s *S3Store) List(ctx context.Context) ([]string, error) {
	var hashes []string
	for obj := range s.client.ListObjects(ctx, s.bucket, minio.ListObjectsOptions{Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if validHash(obj.Key) {
			hashes = append(hashes, obj.Key)
		}
	}
	return hashes, nil
}

func (s *S3Store) Stats() (used int64, count int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.used, s.count
}

func isNotFound(err error) bool {
	code := minio.ToErrorResponse(err).Code
	return code == "NoSuchKey" || code == "NoSuchObject"
}
