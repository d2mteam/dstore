package node

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"dstore-demo/internal/ec"
)

// backend mở một ShardStore mới tinh; reopen mở lại đúng nơi lưu đó như khi
// node restart.
type backend struct {
	name string
	open func(t *testing.T) (store ShardStore, reopen func() ShardStore)
}

// backends trả về mọi backend cần test.
func backends(t *testing.T) []backend {
	list := []backend{{
		name: "disk",
		open: func(t *testing.T) (ShardStore, func() ShardStore) {
			dir := t.TempDir()
			store, err := NewDiskStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			return store, func() ShardStore {
				s, err := NewDiskStore(dir)
				if err != nil {
					t.Fatal(err)
				}
				return s
			}
		},
	}}

	return list
}

func serve(t *testing.T, store ShardStore) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer((&Server{ID: "node-test", Store: store, Log: slog.New(slog.DiscardHandler)}).Routes())
	t.Cleanup(srv.Close)
	return srv
}

func do(t *testing.T, method, url string, body []byte) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestShardRoundTrip(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			store, _ := b.open(t)
			srv := serve(t, store)
			content := []byte("nội dung shard demo")
			hash := ec.Hash(content)

			resp := do(t, http.MethodPut, srv.URL+"/shards/"+hash, content)
			resp.Body.Close()
			if resp.StatusCode != http.StatusCreated {
				t.Fatalf("PUT: status %s", resp.Status)
			}

			resp = do(t, http.MethodHead, srv.URL+"/shards/"+hash, nil)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("HEAD: status %s", resp.Status)
			}

			resp = do(t, http.MethodGet, srv.URL+"/shards/"+hash, nil)
			got, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if !bytes.Equal(got, content) {
				t.Fatalf("GET trả về %q, mong đợi %q", got, content)
			}

			var hashes []string
			resp = do(t, http.MethodGet, srv.URL+"/shards", nil)
			json.NewDecoder(resp.Body).Decode(&hashes)
			resp.Body.Close()
			if len(hashes) != 1 || hashes[0] != hash {
				t.Fatalf("GET /shards trả về %v", hashes)
			}

			if used, count := store.Stats(); count != 1 || used != int64(len(content)) {
				t.Fatalf("Stats = (%d, %d), mong đợi (%d, 1)", used, count, len(content))
			}

			resp = do(t, http.MethodDelete, srv.URL+"/shards/"+hash, nil)
			resp.Body.Close()
			if resp.StatusCode != http.StatusNoContent {
				t.Fatalf("DELETE: status %s", resp.Status)
			}

			resp = do(t, http.MethodGet, srv.URL+"/shards/"+hash, nil)
			resp.Body.Close()
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("GET sau khi xóa: status %s, mong đợi 404", resp.Status)
			}
			if used, count := store.Stats(); count != 0 || used != 0 {
				t.Fatalf("Stats sau khi xóa = (%d, %d), mong đợi (0, 0)", used, count)
			}
		})
	}
}

// Node phải từ chối nội dung không khớp hash trong URL, và không lưu lại gì.
func TestPutRejectsWrongHash(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			store, _ := b.open(t)
			srv := serve(t, store)
			hash := ec.Hash([]byte("nội dung thật"))

			resp := do(t, http.MethodPut, srv.URL+"/shards/"+hash, []byte("nội dung bị sửa"))
			resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status %s, mong đợi 400", resp.Status)
			}
			if store.Has(context.Background(), hash) {
				t.Fatal("shard sai hash không được phép lưu lại")
			}
		})
	}
}

// Ghi lại đúng nội dung đã có không được đếm hai lần.
func TestPutSameShardTwice(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			store, _ := b.open(t)
			content := []byte("shard trùng lặp")
			for range 2 {
				if err := store.Put(context.Background(), ec.Hash(content), bytes.NewReader(content)); err != nil {
					t.Fatal(err)
				}
			}
			if _, count := store.Stats(); count != 1 {
				t.Fatalf("count = %d, mong đợi 1", count)
			}
		})
	}
}

// Sau khi node restart, shard cũ vẫn được đếm và đọc được.
func TestStoreRecountsAfterRestart(t *testing.T) {
	for _, b := range backends(t) {
		t.Run(b.name, func(t *testing.T) {
			store, reopen := b.open(t)
			content := []byte("shard sống sót qua restart")
			hash := ec.Hash(content)
			if err := store.Put(context.Background(), hash, bytes.NewReader(content)); err != nil {
				t.Fatal(err)
			}

			reopened := reopen()
			used, count := reopened.Stats()
			if count != 1 || used != int64(len(content)) {
				t.Fatalf("sau restart: count=%d used=%d", count, used)
			}
			rc, size, err := reopened.Open(context.Background(), hash)
			if err != nil {
				t.Fatal(err)
			}
			got, _ := io.ReadAll(rc)
			rc.Close()
			if size != int64(len(content)) || !bytes.Equal(got, content) {
				t.Fatal("shard đọc lại sau restart không khớp")
			}
		})
	}
}
