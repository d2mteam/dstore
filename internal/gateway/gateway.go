package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"dstore-demo/internal/client"
	"dstore-demo/internal/protocol"
)

// Storage là phần việc gateway cần ở tầng lưu trữ; đúng bằng những gì
// *client.Client làm được.
type Storage interface {
	UploadStream(ctx context.Context, name string, size int64, r io.Reader) (string, error)
	Manifest(ctx context.Context, fileID string) (protocol.Manifest, error)
	WriteRange(ctx context.Context, man protocol.Manifest, start, end int64, w io.Writer) error
	Files(ctx context.Context) ([]protocol.FileInfo, error)
	Delete(ctx context.Context, fileID string) error
}

// Server là HTTP handler của gateway. Tạo bằng New để có sẵn giới hạn đồng thời.
type Server struct {
	Storage Storage
	Keys    Keys
	Owners  Owners
	Log     *slog.Logger

	// MaxUploadBytes chặn file quá lớn ngay từ header, không đọc body.
	MaxUploadBytes int64
	// MaxConcurrent giới hạn số request dữ liệu chạy cùng lúc, để một đợt
	// tải đột biến không dập chết các node phía sau.
	MaxConcurrent int

	slots chan struct{}
}

// New tạo Server. maxConcurrent <= 0 thì lấy 32.
func New(storage Storage, keys Keys, owners Owners, maxUpload int64, maxConcurrent int, log *slog.Logger) *Server {
	if maxConcurrent <= 0 {
		maxConcurrent = 32
	}
	return &Server{
		Storage:        storage,
		Keys:           keys,
		Owners:         owners,
		Log:            log,
		MaxUploadBytes: maxUpload,
		MaxConcurrent:  maxConcurrent,
		slots:          make(chan struct{}, maxConcurrent),
	}
}

// Routes trả handler của API /v1/files/... và /healthz, kèm log từng request.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("PUT /v1/files/{name}", s.authenticated(s.upload))
	mux.Handle("GET /v1/files/{id}", s.authenticated(s.download))
	mux.Handle("HEAD /v1/files/{id}", s.authenticated(s.download))
	mux.Handle("DELETE /v1/files/{id}", s.authenticated(s.deleteFile))
	mux.Handle("GET /v1/files", s.authenticated(s.list))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return s.logRequests(mux)
}

// --- Middleware ---

type handlerWithTenant func(w http.ResponseWriter, r *http.Request, tenant string)

// authenticated đòi header Authorization: Bearer <api key>.
func (s *Server) authenticated(next handlerWithTenant) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := bearerToken(r.Header.Get("Authorization"))
		if token == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="dstore"`)
			writeError(w, http.StatusUnauthorized, "thiếu header Authorization: Bearer <api key>")
			return
		}
		tenant, ok := s.Keys.tenantFor(token)
		if !ok {
			writeError(w, http.StatusUnauthorized, "API key không hợp lệ")
			return
		}
		next(w, r, tenant)
	})
}

// logRequests ghi lại mọi request với mã trạng thái và thời gian xử lý.
func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.Log.Info("request",
			"method", r.Method, "path", r.URL.Path, "status", rec.status,
			"bytes", rec.written, "ms", time.Since(start).Milliseconds())
	})
}

// acquire giữ một suất xử lý dữ liệu; hết suất thì báo quá tải thay vì xếp hàng
// vô hạn.
func (s *Server) acquire(w http.ResponseWriter) bool {
	select {
	case s.slots <- struct{}{}:
		return true
	default:
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "gateway đang quá tải, thử lại sau")
		return false
	}
}

func (s *Server) release() { <-s.slots }

// --- Handler ---

func (s *Server) upload(w http.ResponseWriter, r *http.Request, tenant string) {
	name := r.PathValue("name")
	if strings.ContainsAny(name, "/\\") || name == "." || name == ".." {
		writeError(w, http.StatusBadRequest, "tên file không hợp lệ")
		return
	}

	// Coordinator cần biết kích thước để lập kế hoạch đặt shard, nên không nhận
	// body chunked không kèm Content-Length.
	size := r.ContentLength
	if size < 0 {
		writeError(w, http.StatusLengthRequired, "cần Content-Length để chia shard")
		return
	}
	if s.MaxUploadBytes > 0 && size > s.MaxUploadBytes {
		writeError(w, http.StatusRequestEntityTooLarge,
			fmt.Sprintf("file %d byte vượt giới hạn %d byte", size, s.MaxUploadBytes))
		return
	}

	if !s.acquire(w) {
		return
	}
	defer s.release()

	counted := &countingReader{r: io.LimitReader(r.Body, size)}
	fileID, err := s.Storage.UploadStream(r.Context(), name, size, counted)
	if err != nil {
		if counted.n < size {
			writeError(w, http.StatusBadRequest,
				fmt.Sprintf("client mới gửi %d/%d byte", counted.n, size))
			return
		}
		s.Log.Error("upload lỗi", "tenant", tenant, "name", name, "err", err)
		writeError(w, http.StatusBadGateway, "lưu file thất bại: "+err.Error())
		return
	}

	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	meta := FileMeta{Tenant: tenant, Name: name, ContentType: contentType}
	if err := s.Owners.Put(r.Context(), fileID, meta); err != nil {
		// Không ghi được chủ sở hữu thì file thành vô chủ, không ai đọc được nữa:
		// xóa luôn cho sạch thay vì để rác trong hệ thống.
		s.Log.Error("không lưu được chủ sở hữu file", "file", fileID, "err", err)
		if delErr := s.Storage.Delete(context.WithoutCancel(r.Context()), fileID); delErr != nil {
			s.Log.Error("xóa file vô chủ lỗi", "file", fileID, "err", delErr)
		}
		writeError(w, http.StatusServiceUnavailable, "không lưu được metadata, thử lại sau")
		return
	}

	w.Header().Set("Location", "/v1/files/"+fileID)
	writeJSON(w, http.StatusCreated, map[string]any{
		"file_id": fileID, "name": name, "size": size,
	})
}

func (s *Server) download(w http.ResponseWriter, r *http.Request, tenant string) {
	fileID := r.PathValue("id")
	meta, ok := s.owned(w, r, fileID, tenant)
	if !ok {
		return
	}

	man, err := s.Storage.Manifest(r.Context(), fileID)
	if err != nil {
		writeError(w, http.StatusNotFound, "không có file này")
		return
	}

	start, end, err := parseRange(r.Header.Get("Range"), man.Size)
	if err != nil {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", man.Size))
		writeError(w, http.StatusRequestedRangeNotSatisfiable, err.Error())
		return
	}

	w.Header().Set("Content-Type", meta.ContentType)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", `"`+fileID+`"`)
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))

	status := http.StatusOK
	if r.Header.Get("Range") != "" {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, man.Size))
		status = http.StatusPartialContent
	}
	w.WriteHeader(status)

	if r.Method == http.MethodHead || man.Size == 0 {
		return
	}

	if !s.acquire(w) {
		return
	}
	defer s.release()

	if err := s.Storage.WriteRange(r.Context(), man, start, end, w); err != nil {
		// Header đã gửi đi rồi nên không đổi được mã trạng thái nữa; chỉ log lại,
		// client sẽ thấy body ngắn hơn Content-Length và biết là hỏng.
		s.Log.Error("đọc file lỗi", "tenant", tenant, "file", fileID, "err", err)
	}
}

func (s *Server) list(w http.ResponseWriter, r *http.Request, tenant string) {
	files, err := s.Storage.Files(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	owned, err := s.Owners.OwnedBy(r.Context(), tenant)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "không đọc được metadata, thử lại sau")
		return
	}

	out := make([]map[string]any, 0, len(owned))
	for _, f := range files {
		meta, ok := owned[f.ID]
		if !ok {
			continue // file của tenant khác
		}
		out = append(out, map[string]any{
			"file_id": f.ID, "name": meta.Name, "size": f.Size,
			"content_type": meta.ContentType, "status": f.Status, "created_at": f.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"files": out})
}

func (s *Server) deleteFile(w http.ResponseWriter, r *http.Request, tenant string) {
	fileID := r.PathValue("id")
	if _, ok := s.owned(w, r, fileID, tenant); !ok {
		return
	}
	if err := s.Storage.Delete(r.Context(), fileID); err != nil {
		writeError(w, http.StatusBadGateway, err.Error())
		return
	}
	if err := s.Owners.Remove(r.Context(), fileID); err != nil {
		// Dữ liệu đã xóa; dòng chủ sở hữu sót lại chỉ làm file hiện trong danh
		// sách tới khi coordinator trả 404 cho nó.
		s.Log.Warn("không xóa được chủ sở hữu file", "file", fileID, "err", err)
	}
	w.WriteHeader(http.StatusNoContent)
}

// owned kiểm tra file có thuộc tenant không; nếu không thì tự trả lỗi.
func (s *Server) owned(w http.ResponseWriter, r *http.Request, fileID, tenant string) (FileMeta, bool) {
	meta, ok, err := s.Owners.Get(r.Context(), fileID, tenant)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "không đọc được metadata, thử lại sau")
		return FileMeta{}, false
	}
	if !ok {
		// Trả 404 chứ không phải 403: người ngoài không nên biết file có tồn tại.
		writeError(w, http.StatusNotFound, "không có file này")
		return FileMeta{}, false
	}
	return meta, true
}

// --- Helper ---

// parseRange đọc header Range dạng "bytes=0-1023", "bytes=1024-" hoặc "bytes=-512".
// Header rỗng nghĩa là lấy cả file.
func parseRange(header string, size int64) (start, end int64, err error) {
	if header == "" {
		if size == 0 {
			return 0, -1, nil
		}
		return 0, size - 1, nil
	}
	spec, ok := strings.CutPrefix(strings.TrimSpace(header), "bytes=")
	if !ok || strings.Contains(spec, ",") {
		return 0, 0, fmt.Errorf("chỉ hỗ trợ một khoảng dạng bytes=đầu-cuối")
	}
	from, to, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, fmt.Errorf("khoảng không hợp lệ: %q", header)
	}

	switch {
	case from == "": // bytes=-512: 512 byte cuối
		n, err := strconv.ParseInt(to, 10, 64)
		if err != nil || n <= 0 {
			return 0, 0, fmt.Errorf("khoảng không hợp lệ: %q", header)
		}
		if n > size {
			n = size
		}
		start, end = size-n, size-1
	default:
		start, err = strconv.ParseInt(from, 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("khoảng không hợp lệ: %q", header)
		}
		end = size - 1
		if to != "" {
			if end, err = strconv.ParseInt(to, 10, 64); err != nil {
				return 0, 0, fmt.Errorf("khoảng không hợp lệ: %q", header)
			}
		}
	}

	if end > size-1 {
		end = size - 1
	}
	if start < 0 || start >= size || start > end {
		return 0, 0, fmt.Errorf("khoảng [%d, %d] nằm ngoài file %d byte", start, end, size)
	}
	return start, end, nil
}

// countingReader đếm số byte đã đọc, để phân biệt client gửi thiếu với lỗi hệ thống.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// statusRecorder nhớ lại mã trạng thái và số byte đã ghi, cho phần log.
type statusRecorder struct {
	http.ResponseWriter
	status  int
	written int64
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	n, err := s.ResponseWriter.Write(b)
	s.written += int64(n)
	return n, err
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, protocol.ErrorResponse{Error: msg})
}

// Đảm bảo *client.Client dùng được làm Storage.
var _ Storage = (*client.Client)(nil)
