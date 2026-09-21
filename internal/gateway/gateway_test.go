package gateway_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"testing"

	"dstore-demo/internal/gateway"
	"dstore-demo/internal/testcluster"
)

const (
	keyA = "key-cua-app-a"
	keyB = "key-cua-app-b"
)

// start dựng cụm dstore thật rồi đặt gateway trước nó.
func start(t *testing.T) *httptest.Server {
	t.Helper()
	cluster := testcluster.Start(t, 6)
	storage := cluster.Client(4, 2, 256<<10)

	keys, err := gateway.ParseKeys(fmt.Sprintf("%s:app-a,%s:app-b", keyA, keyB))
	if err != nil {
		t.Fatal(err)
	}
	gw := gateway.New(storage, keys, gateway.NewMemoryOwners(), 1<<20, 8, slog.New(slog.DiscardHandler))

	srv := httptest.NewServer(gw.Routes())
	t.Cleanup(srv.Close)
	return srv
}

func request(t *testing.T, srv *httptest.Server, method, path, key string, body []byte, headers ...string) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		r = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, r)
	if err != nil {
		t.Fatal(err)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Set(headers[i], headers[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// upload đẩy nội dung qua gateway và trả về file_id.
func upload(t *testing.T, srv *httptest.Server, key, name string, content []byte) string {
	t.Helper()
	resp := request(t, srv, http.MethodPut, "/v1/files/"+name, key, content, "Content-Type", "image/png")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT: %s: %s", resp.Status, body)
	}
	var out struct {
		FileID string `json:"file_id"`
		Size   int64  `json:"size"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Size != int64(len(content)) {
		t.Fatalf("size trả về %d, mong đợi %d", out.Size, len(content))
	}
	return out.FileID
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// Đẩy file lên rồi tải về phải ra đúng nội dung cũ, kèm đúng Content-Type.
func TestUploadDownloadRoundTrip(t *testing.T) {
	srv := start(t)
	content := randomBytes(700 << 10) // 700 KB: nhiều hơn một segment 256 KB
	fileID := upload(t, srv, keyA, "trang-01.png", content)

	resp := request(t, srv, http.MethodGet, "/v1/files/"+fileID, keyA, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET: %s", resp.Status)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "image/png" {
		t.Fatalf("Content-Type = %q", ct)
	}
	if resp.Header.Get("Accept-Ranges") != "bytes" {
		t.Fatal("thiếu Accept-Ranges")
	}
	got, _ := io.ReadAll(resp.Body)
	if !bytes.Equal(got, content) {
		t.Fatalf("tải về %d byte, khác bản gốc %d byte", len(got), len(content))
	}
}

// Không có API key, hoặc key sai, thì không vào được.
func TestAuthRequired(t *testing.T) {
	srv := start(t)

	resp := request(t, srv, http.MethodGet, "/v1/files", "", nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("không có key: %s, mong đợi 401", resp.Status)
	}
	if resp.Header.Get("WWW-Authenticate") == "" {
		t.Fatal("401 phải kèm WWW-Authenticate")
	}

	resp = request(t, srv, http.MethodGet, "/v1/files", "key-bia-dat", nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("key sai: %s, mong đợi 401", resp.Status)
	}
}

// Tenant này không được đọc, xóa hay nhìn thấy file của tenant kia.
func TestTenantIsolation(t *testing.T) {
	srv := start(t)
	fileID := upload(t, srv, keyA, "rieng-tu.bin", randomBytes(1000))

	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		resp := request(t, srv, method, "/v1/files/"+fileID, keyB, nil)
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s bằng key tenant khác: %s, mong đợi 404", method, resp.Status)
		}
	}

	resp := request(t, srv, http.MethodGet, "/v1/files", keyB, nil)
	defer resp.Body.Close()
	var out struct {
		Files []map[string]any `json:"files"`
	}
	json.NewDecoder(resp.Body).Decode(&out)
	if len(out.Files) != 0 {
		t.Fatalf("tenant khác thấy %d file", len(out.Files))
	}
}

// Range cho phép đọc một đoạn giữa file mà không tải cả file.
func TestRangeRequests(t *testing.T) {
	srv := start(t)
	content := randomBytes(600 << 10)
	fileID := upload(t, srv, keyA, "video.bin", content)

	cases := []struct {
		header     string
		wantStart  int
		wantEnd    int
		wantHeader string
	}{
		{"bytes=0-99", 0, 99, fmt.Sprintf("bytes 0-99/%d", len(content))},
		{"bytes=300000-400000", 300000, 400000, fmt.Sprintf("bytes 300000-400000/%d", len(content))},
		{"bytes=500000-", 500000, len(content) - 1, fmt.Sprintf("bytes 500000-%d/%d", len(content)-1, len(content))},
		{"bytes=-1024", len(content) - 1024, len(content) - 1, fmt.Sprintf("bytes %d-%d/%d", len(content)-1024, len(content)-1, len(content))},
	}
	for _, tc := range cases {
		resp := request(t, srv, http.MethodGet, "/v1/files/"+fileID, keyA, nil, "Range", tc.header)
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("%s: %s, mong đợi 206", tc.header, resp.Status)
		}
		if cr := resp.Header.Get("Content-Range"); cr != tc.wantHeader {
			t.Fatalf("%s: Content-Range = %q, mong đợi %q", tc.header, cr, tc.wantHeader)
		}
		if want := content[tc.wantStart : tc.wantEnd+1]; !bytes.Equal(got, want) {
			t.Fatalf("%s: nhận %d byte, mong đợi %d byte và phải khớp nội dung",
				tc.header, len(got), len(want))
		}
	}

	// Khoảng nằm ngoài file thì báo 416 kèm kích thước thật.
	resp := request(t, srv, http.MethodGet, "/v1/files/"+fileID, keyA, nil, "Range", "bytes=999999999-")
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("range ngoài file: %s, mong đợi 416", resp.Status)
	}
	if want := fmt.Sprintf("bytes */%d", len(content)); resp.Header.Get("Content-Range") != want {
		t.Fatalf("416 phải kèm Content-Range %q", want)
	}
}

// HEAD trả đủ thông tin mà không tốn băng thông đọc shard.
func TestHeadReturnsSizeWithoutBody(t *testing.T) {
	srv := start(t)
	content := randomBytes(300 << 10)
	fileID := upload(t, srv, keyA, "anh.bin", content)

	resp := request(t, srv, http.MethodHead, "/v1/files/"+fileID, keyA, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HEAD: %s", resp.Status)
	}
	if resp.ContentLength != int64(len(content)) {
		t.Fatalf("Content-Length = %d, mong đợi %d", resp.ContentLength, len(content))
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) != 0 {
		t.Fatalf("HEAD trả về %d byte body", len(body))
	}
}

// File vượt giới hạn bị chặn ngay từ header, không đọc body.
func TestUploadTooLarge(t *testing.T) {
	srv := start(t)
	resp := request(t, srv, http.MethodPut, "/v1/files/qua-lon.bin", keyA, randomBytes(2<<20))
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("%s, mong đợi 413", resp.Status)
	}
}

// Không có Content-Length thì không chia shard được, phải báo rõ.
func TestUploadWithoutContentLength(t *testing.T) {
	srv := start(t)
	req, _ := http.NewRequest(http.MethodPut, srv.URL+"/v1/files/chunked.bin",
		io.NopCloser(bytes.NewReader([]byte("nội dung"))))
	req.Header.Set("Authorization", "Bearer "+keyA)
	req.ContentLength = -1 // buộc Go gửi chunked

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusLengthRequired {
		t.Fatalf("%s, mong đợi 411", resp.Status)
	}
}

// Tên file chứa dấu / hoặc .. bị từ chối. Dạng thường như "/v1/files/.." đã bị
// chính ServeMux chặn; chỗ handler phải tự lo là dấu / được mã hóa thành %2F,
// vì lúc đó PathValue mới trả về chuỗi có dấu /.
func TestUploadRejectsBadName(t *testing.T) {
	srv := start(t)
	for _, name := range []string{"a%2Fb.bin", "%2e%2e", "..%2F..%2Fetc%2Fpasswd"} {
		resp := request(t, srv, http.MethodPut, "/v1/files/"+name, keyA, []byte("x"))
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("tên %q: %s, mong đợi 400", name, resp.Status)
		}
	}
}

// Xóa xong thì không đọc lại được nữa.
func TestDeleteFile(t *testing.T) {
	srv := start(t)
	fileID := upload(t, srv, keyA, "tam.bin", randomBytes(5000))

	resp := request(t, srv, http.MethodDelete, "/v1/files/"+fileID, keyA, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE: %s, mong đợi 204", resp.Status)
	}

	resp = request(t, srv, http.MethodGet, "/v1/files/"+fileID, keyA, nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET sau khi xóa: %s, mong đợi 404", resp.Status)
	}
}
