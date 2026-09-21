package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// S3Config là cấu hình kết nối tới Amazon S3 hoặc dịch vụ tương thích (MinIO).
// Thông tin đăng nhập và region lấy từ chuỗi xác thực mặc định của AWS: biến
// AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY/AWS_REGION, profile, hoặc IAM role của
// ECS/EC2/EKS — nên trên AWS không cần nhét khóa tĩnh vào cấu hình.
type S3Config struct {
	// Bucket dùng chung cho mọi node. Tên bucket trên AWS là duy nhất toàn cầu,
	// nên không thể mỗi node một bucket tên "node-1", "node-2"...
	Bucket string
	// Prefix tách shard của từng node, ví dụ "nodes/node-1/".
	Prefix string
	// Endpoint để trống khi dùng Amazon S3; đặt khi dùng MinIO, ví dụ "http://minio:9000".
	Endpoint string
	// UsePathStyle: MinIO cần true (http://minio:9000/bucket/key); Amazon S3 dùng
	// dạng virtual-hosted (bucket.s3.amazonaws.com) nên để false.
	UsePathStyle bool
	// CreateBucket tạo bucket nếu chưa có — tiện khi dev với MinIO. Trên AWS
	// bucket nên do hạ tầng tạo trước, và role của node không cần quyền tạo bucket.
	CreateBucket bool
}

// maxShardBytes giới hạn kích thước một shard. Shard thường cỡ segment/k (1 MB
// với cấu hình mặc định), nên 64 MB là rộng rãi mà vẫn chặn được body bất thường.
const maxShardBytes = 64 << 20

// S3Store cất mỗi shard thành một object <prefix><hash>.
type S3Store struct {
	client *s3.Client
	bucket string
	prefix string

	mu    sync.RWMutex
	used  int64
	count int
}

// NewS3Store kết nối S3, kiểm tra bucket dùng được, rồi đếm lại shard đang có.
func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("thiếu tên bucket")
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("đọc cấu hình AWS: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
		o.UsePathStyle = cfg.UsePathStyle
	})

	s := &S3Store{client: client, bucket: cfg.Bucket, prefix: cfg.Prefix}
	if err := s.checkBucket(ctx, cfg.CreateBucket, awsCfg.Region); err != nil {
		return nil, err
	}

	pages := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket), Prefix: aws.String(s.prefix),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("liệt kê %s/%s: %w", s.bucket, s.prefix, err)
		}
		for _, obj := range page.Contents {
			s.used += aws.ToInt64(obj.Size)
			s.count++
		}
	}
	return s, nil
}

// checkBucket báo lỗi sớm nếu bucket không tồn tại hoặc không có quyền, thay vì
// để lỗi lộ ra ở lần ghi shard đầu tiên.
func (s *S3Store) checkBucket(ctx context.Context, create bool, region string) error {
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.bucket)})
	if err == nil {
		return nil
	}
	if !isNotFound(err) || !create {
		return fmt.Errorf("không dùng được bucket %s: %w", s.bucket, err)
	}

	input := &s3.CreateBucketInput{Bucket: aws.String(s.bucket)}
	// us-east-1 là mặc định của S3 và không được ghi rõ; region khác thì bắt buộc.
	if region != "" && region != "us-east-1" {
		input.CreateBucketConfiguration = &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(region),
		}
	}
	_, err = s.client.CreateBucket(ctx, input)
	var owned *types.BucketAlreadyOwnedByYou
	if err != nil && !errors.As(err, &owned) { // node khác vừa tạo trước — không sao
		return fmt.Errorf("tạo bucket %s: %w", s.bucket, err)
	}
	return nil
}

func (s *S3Store) key(hash string) string { return s.prefix + hash }

// Put đọc hết shard vào RAM để kiểm SHA-256 trước khi ghi (shard chỉ cỡ 1 MB),
// nên trong bucket không bao giờ có object sai hash. Không cần file tạm như
// DiskStore: PutObject của S3 là nguyên tử — object hoặc có đủ, hoặc không có.
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
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.key(hash)),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
		ContentType:   aws.String("application/octet-stream"),
		// Gửi kèm chính SHA-256 đó để S3 kiểm lại phía server: byte hỏng trên
		// đường truyền bị từ chối, không lọt vào bucket.
		ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
		ChecksumSHA256:    aws.String(base64.StdEncoding.EncodeToString(sum[:])),
	})
	if err != nil {
		return fmt.Errorf("ghi object %s: %w", hash[:8], err)
	}

	s.mu.Lock()
	s.used += int64(len(data))
	s.count++
	s.mu.Unlock()
	return nil
}

// Open trả body của object dưới dạng stream; trả ErrNotFound nếu không có.
func (s *S3Store) Open(ctx context.Context, hash string) (io.ReadCloser, int64, error) {
	if !validHash(hash) {
		return nil, 0, fmt.Errorf("%w: %q", ErrInvalidHash, hash)
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(hash)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, err
	}
	return out.Body, aws.ToInt64(out.ContentLength), nil
}

// Has hỏi S3 bằng HeadObject, không tải nội dung.
func (s *S3Store) Has(ctx context.Context, hash string) bool {
	if !validHash(hash) {
		return false
	}
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(hash)),
	})
	return err == nil
}

// Delete xóa object; trả ErrNotFound nếu không có.
func (s *S3Store) Delete(ctx context.Context, hash string) error {
	if !validHash(hash) {
		return fmt.Errorf("%w: %q", ErrInvalidHash, hash)
	}
	// S3 xóa object không tồn tại vẫn trả thành công, nên hỏi trước để biết
	// kích thước và để trả ErrNotFound giống DiskStore.
	head, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(hash)),
	})
	if err != nil {
		if isNotFound(err) {
			return ErrNotFound
		}
		return err
	}
	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(s.key(hash)),
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.used -= aws.ToInt64(head.ContentLength)
	s.count--
	s.mu.Unlock()
	return nil
}

// List liệt kê object dưới prefix của node, mỗi lượt gọi S3 được 1000 object.
func (s *S3Store) List(ctx context.Context) ([]string, error) {
	var hashes []string
	pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket), Prefix: aws.String(s.prefix),
	})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, obj := range page.Contents {
			if hash := strings.TrimPrefix(aws.ToString(obj.Key), s.prefix); validHash(hash) {
				hashes = append(hashes, hash)
			}
		}
	}
	return hashes, nil
}

// Stats trả số byte và số shard đang giữ, đếm sẵn trong RAM nên không gọi S3.
func (s *S3Store) Stats() (used int64, count int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.used, s.count
}

// isNotFound nhận ra lỗi "không có": GetObject trả NoSuchKey, còn HeadObject và
// HeadBucket trả NotFound/NoSuchBucket vì response của HEAD không có body.
func isNotFound(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	switch apiErr.ErrorCode() {
	case "NoSuchKey", "NotFound", "NoSuchBucket":
		return true
	}
	return false
}
