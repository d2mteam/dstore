package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"dstore-demo/internal/pgdb"
)

// Open trả về pool trỏ vào một schema mới đã tạo sẵn bảng. Bỏ qua test nếu
// chưa đặt DSTORE_TEST_DATABASE_URL (xem `make test-integration`).
func Open(t *testing.T, tables string) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("DSTORE_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("chưa đặt DSTORE_TEST_DATABASE_URL")
	}
	ctx := context.Background()

	b := make([]byte, 6)
	rand.Read(b)
	schema := "test_" + hex.EncodeToString(b)

	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if c, err := pgx.Connect(context.Background(), url); err == nil {
			c.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
			c.Close(context.Background())
		}
	})

	sep := "?"
	if strings.Contains(url, "?") {
		sep = "&"
	}
	pool, err := pgdb.Open(ctx, pgdb.Config{URL: url + sep + "search_path=" + schema, Schema: tables})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}
