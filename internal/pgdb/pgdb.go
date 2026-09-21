// Package pgdb mở kết nối PostgreSQL dùng chung cho coordinator và gateway.
// Chạy được với Postgres tự dựng lẫn Amazon RDS / Aurora PostgreSQL:
//
//   - TLS: đặt trong DATABASE_URL, ví dụ
//     ?sslmode=verify-full&sslrootcert=/etc/ssl/rds-global-bundle.pem
//   - Đăng nhập bằng IAM thay cho mật khẩu tĩnh: bật IAMAuth.
//   - Sau RDS Proxy: thêm default_query_exec_mode=exec vào URL để tránh bị
//     ghim phiên vì prepared statement.
package pgdb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Config struct {
	URL string

	// IAMAuth đăng nhập RDS bằng token IAM thay cho mật khẩu. Token chỉ sống
	// 15 phút nên được sinh mới mỗi khi mở kết nối. Quyền lấy từ chuỗi xác thực
	// mặc định của AWS (IAM role của ECS/EC2/EKS, biến môi trường, profile...).
	IAMAuth bool

	// Schema chạy lúc khởi động (CREATE TABLE IF NOT EXISTS ...). Để trống khi
	// user DB không có quyền DDL — trên RDS bảng thường do migration tạo trước.
	Schema string
}

// Open mở pool kết nối, kiểm tra kết nối được, rồi tạo bảng nếu có Schema.
func Open(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("DATABASE_URL không hợp lệ: %w", err)
	}

	if cfg.IAMAuth {
		// RDS từ chối đăng nhập IAM qua kết nối không mã hóa; báo sớm thay vì
		// để người dùng gặp lỗi "PAM authentication failed" khó hiểu.
		if poolCfg.ConnConfig.TLSConfig == nil {
			return nil, fmt.Errorf("đăng nhập IAM cần TLS: bỏ sslmode=disable khỏi DATABASE_URL")
		}
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, fmt.Errorf("đọc cấu hình AWS: %w", err)
		}
		if awsCfg.Region == "" {
			return nil, fmt.Errorf("đăng nhập IAM cần AWS_REGION")
		}
		poolCfg.BeforeConnect = iamPassword(awsCfg)
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if cfg.Schema != "" {
		// Exec không có tham số luôn đi bằng simple protocol, nên chạy được
		// nhiều câu lệnh một lúc, kể cả sau RDS Proxy.
		if _, err := pool.Exec(ctx, cfg.Schema); err != nil {
			pool.Close()
			return nil, fmt.Errorf("tạo bảng: %w", err)
		}
	}
	return pool, nil
}

// iamPassword thay mật khẩu bằng token IAM mới trước mỗi lần mở kết nối.
func iamPassword(awsCfg aws.Config) func(context.Context, *pgx.ConnConfig) error {
	return func(ctx context.Context, cc *pgx.ConnConfig) error {
		endpoint := fmt.Sprintf("%s:%d", cc.Host, cc.Port)
		token, err := auth.BuildAuthToken(ctx, endpoint, awsCfg.Region, cc.User, awsCfg.Credentials)
		if err != nil {
			return fmt.Errorf("tạo token IAM cho RDS: %w", err)
		}
		cc.Password = token
		return nil
	}
}
