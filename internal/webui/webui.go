package webui

import (
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"io/fs"
	"net/http"
)

//go:embed assets
var assets embed.FS

// Page là file HTML của từng loại service trong assets/.
type Page string

// Các trang có sẵn, truyền cho Handler.
const (
	CoordinatorPage Page = "coordinator.html" // control plane: tổng quan cụm, file, cấu hình vận hành
	NodePage        Page = "node.html"        // một node: kết nối tới coordinator, shard đang giữ
)

// User là tên đăng nhập duy nhất; mật khẩu lấy từ biến môi trường UI_PASSWORD.
const User = "admin"

// Handler gắn trang, file tĩnh và API của trang dưới /ui/, bọc bởi Protect.
// api nhận nguyên đường dẫn (ví dụ /ui/api/status), không bị cắt tiền tố.
func Handler(page Page, api http.Handler, password string) http.Handler {
	static, err := fs.Sub(assets, "assets")
	if err != nil {
		panic(err) // assets nhúng lúc build, không thể thiếu
	}
	mux := http.NewServeMux()
	mux.Handle("/ui/api/", api)
	mux.Handle("GET /ui/static/", http.StripPrefix("/ui/static/", http.FileServerFS(static)))
	mux.HandleFunc("GET /ui/{$}", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFileFS(w, r, static, string(page))
	})
	return Protect(mux, password)
}

// Protect làm hai việc:
//   - Bắt đăng nhập HTTP Basic khi có mật khẩu. Để trống thì UI mở cho mọi
//     người trong mạng — chỉ hợp khi chạy demo trên máy mình.
//   - Chặn request ghi (POST/PUT/DELETE) đến từ trang web khác. Trình duyệt tự
//     đính kèm thông tin đăng nhập đã nhớ, nên thiếu bước này thì một trang lạ
//     cũng bấm được nút "ngắt kết nối" hộ người dùng (CSRF).
func Protect(h http.Handler, password string) http.Handler {
	h = http.NewCrossOriginProtection().Handler(h)
	if password == "" {
		return h
	}
	// So sánh hash để thời gian so sánh không lộ độ dài mật khẩu.
	want := sha256.Sum256([]byte(password))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		got := sha256.Sum256([]byte(pass))
		if !ok || user != User || subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="dstore", charset="UTF-8"`)
			http.Error(w, "cần đăng nhập", http.StatusUnauthorized)
			return
		}
		h.ServeHTTP(w, r)
	})
}
