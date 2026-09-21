package node

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"dstore-demo/internal/ec"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	store, err := NewDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{ID: "node-test", Store: store, Log: slog.New(slog.DiscardHandler)}
	srv := httptest.NewServer(s.Routes())
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
	srv := newTestServer(t)
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
}

// Node phải từ chối nội dung không khớp hash trong URL.
func TestPutRejectsWrongHash(t *testing.T) {
	srv := newTestServer(t)
	hash := ec.Hash([]byte("nội dung thật"))

	resp := do(t, http.MethodPut, srv.URL+"/shards/"+hash, []byte("nội dung bị sửa"))
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %s, mong đợi 400", resp.Status)
	}

	resp = do(t, http.MethodGet, srv.URL+"/shards/"+hash, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatal("shard sai hash không được phép lưu lại trên đĩa")
	}
}

// Sau khi node restart, shard cũ trên đĩa vẫn được đếm và đọc được.
func TestStoreRescansAfterRestart(t *testing.T) {
	dir := t.TempDir()
	store, err := NewDiskStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("shard sống sót qua restart")
	if err := store.Put(ec.Hash(content), bytes.NewReader(content)); err != nil {
		t.Fatal(err)
	}

	reopened, err := NewDiskStore(dir) // như khi node khởi động lại
	if err != nil {
		t.Fatal(err)
	}
	used, count := reopened.Stats()
	if count != 1 || used != int64(len(content)) {
		t.Fatalf("sau restart: count=%d used=%d", count, used)
	}
	if !reopened.Has(ec.Hash(content)) {
		t.Fatal("shard cũ không tìm thấy sau restart")
	}
}
