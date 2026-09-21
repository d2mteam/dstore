package pgdb

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/jackc/pgx/v5"
)

// Token IAM chỉ là một URL ký SigV4, sinh được mà không cần gọi AWS — nên kiểm
// tra được hook có thay mật khẩu đúng cách không.
func TestIAMPasswordReplacesPasswordWithSignedToken(t *testing.T) {
	awsCfg := aws.Config{
		Region:      "ap-southeast-1",
		Credentials: credentials.NewStaticCredentialsProvider("AKIDEXAMPLE", "secret", ""),
	}
	cc, err := pgx.ParseConfig("postgres://dstore:mat-khau-cu@db.abc.ap-southeast-1.rds.amazonaws.com:5432/dstore?sslmode=require")
	if err != nil {
		t.Fatal(err)
	}

	if err := iamPassword(awsCfg)(context.Background(), cc); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{
		"db.abc.ap-southeast-1.rds.amazonaws.com:5432",
		"Action=connect",
		"DBUser=dstore",
		"X-Amz-Credential=AKIDEXAMPLE",
		"X-Amz-Signature=",
	} {
		if !strings.Contains(cc.Password, want) {
			t.Fatalf("token thiếu %q:\n%s", want, cc.Password)
		}
	}
	if strings.Contains(cc.Password, "mat-khau-cu") {
		t.Fatal("mật khẩu tĩnh vẫn còn")
	}
}

// Bật IAM mà tắt TLS thì báo lỗi ngay, không cần tới DB.
func TestIAMAuthRequiresTLS(t *testing.T) {
	_, err := Open(context.Background(), Config{
		URL:     "postgres://dstore@db.example.com:5432/dstore?sslmode=disable",
		IAMAuth: true,
	})
	if err == nil || !strings.Contains(err.Error(), "TLS") {
		t.Fatalf("mong đợi lỗi đòi TLS, nhận %v", err)
	}
}
