package gateway

import (
	"context"
	"testing"

	"dstore-demo/internal/pgdb/pgtest"
)

// Cùng một bộ kiểm tra cho cả hai cách lưu: RAM và PostgreSQL.
func ownersBackends(t *testing.T) map[string]func(t *testing.T) Owners {
	return map[string]func(t *testing.T) Owners{
		"memory":   func(t *testing.T) Owners { return NewMemoryOwners() },
		"postgres": func(t *testing.T) Owners { return NewPostgresOwners(pgtest.Open(t, OwnersSchema)) },
	}
}

func TestOwners(t *testing.T) {
	for name, open := range ownersBackends(t) {
		t.Run(name, func(t *testing.T) {
			owners := open(t)
			ctx := context.Background()

			must(t, owners.Put(ctx, "f1", FileMeta{Tenant: "app-a", Name: "a.png", ContentType: "image/png"}))
			must(t, owners.Put(ctx, "f2", FileMeta{Tenant: "app-a", Name: "b.bin", ContentType: "application/octet-stream"}))
			must(t, owners.Put(ctx, "f3", FileMeta{Tenant: "app-b", Name: "c.bin", ContentType: "application/octet-stream"}))

			meta, ok, err := owners.Get(ctx, "f1", "app-a")
			must(t, err)
			if !ok || meta.Name != "a.png" || meta.ContentType != "image/png" {
				t.Fatalf("Get(f1, app-a) = %+v, %v", meta, ok)
			}

			// Tenant khác hỏi thì trông như file không tồn tại.
			if _, ok, _ := owners.Get(ctx, "f1", "app-b"); ok {
				t.Fatal("tenant app-b đọc được file của app-a")
			}
			if _, ok, _ := owners.Get(ctx, "khong-co", "app-a"); ok {
				t.Fatal("file không tồn tại mà vẫn tìm thấy")
			}

			owned, err := owners.OwnedBy(ctx, "app-a")
			must(t, err)
			if len(owned) != 2 || owned["f1"].Name != "a.png" || owned["f2"].Name != "b.bin" {
				t.Fatalf("OwnedBy(app-a) = %+v", owned)
			}

			must(t, owners.Remove(ctx, "f1"))
			if _, ok, _ := owners.Get(ctx, "f1", "app-a"); ok {
				t.Fatal("file đã xóa vẫn còn")
			}
			owned, _ = owners.OwnedBy(ctx, "app-a")
			if len(owned) != 1 {
				t.Fatalf("sau khi xóa còn %d file, mong đợi 1", len(owned))
			}
		})
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
