package webui

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

var okHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
})

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

// Trang, file tĩnh và API đều nằm dưới /ui/; API nhận nguyên đường dẫn.
func TestHandlerRoutes(t *testing.T) {
	api := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("api " + r.URL.Path))
	})
	h := Handler(NodePage, api, "")

	for path, want := range map[string]string{
		"/ui/":                 "<title>dstore · Node</title>",
		"/ui/static/style.css": "--bg:",
		"/ui/api/status":       "api /ui/api/status",
	} {
		rec := get(t, h, path)
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), want) {
			t.Errorf("GET %s = %d %q, mong đợi 200 chứa %q", path, rec.Code, rec.Body.String(), want)
		}
	}
}

// Mọi file mà trang HTML và JS tham chiếu tới phải có trong assets; gõ sai
// tên file là trang trắng mà Go không báo lỗi gì lúc build.
func TestPagesReferenceExistingAssets(t *testing.T) {
	h := Handler(CoordinatorPage, okHandler, "")
	ref := regexp.MustCompile(`(?:static/|from '\./)([\w.-]+\.(?:css|js))`)

	for _, page := range []string{"coordinator.html", "node.html", "coordinator.js", "node.js"} {
		body, err := assets.ReadFile("assets/" + page)
		if err != nil {
			t.Fatal(err)
		}
		matches := ref.FindAllStringSubmatch(string(body), -1)
		if len(matches) == 0 {
			t.Errorf("%s không tham chiếu file tĩnh nào, regexp có còn đúng không?", page)
		}
		for _, m := range matches {
			if rec := get(t, h, "/ui/static/"+m[1]); rec.Code != http.StatusOK {
				t.Errorf("%s tham chiếu %s nhưng GET trả %d", page, m[1], rec.Code)
			}
		}
	}
}

func TestProtectRequiresPassword(t *testing.T) {
	h := Protect(okHandler, "s3cret")

	cases := []struct {
		name       string
		user, pass string
		auth       bool
		want       int
	}{
		{"không gửi gì", "", "", false, http.StatusUnauthorized},
		{"sai mật khẩu", User, "wrong", true, http.StatusUnauthorized},
		{"sai tên", "root", "s3cret", true, http.StatusUnauthorized},
		{"đúng", User, "s3cret", true, http.StatusOK},
	}
	for _, c := range cases {
		req := httptest.NewRequest(http.MethodGet, "/ui/", nil)
		if c.auth {
			req.SetBasicAuth(c.user, c.pass)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s: mã %d, mong đợi %d", c.name, rec.Code, c.want)
		}
		if c.want == http.StatusUnauthorized && rec.Header().Get("WWW-Authenticate") == "" {
			t.Errorf("%s: thiếu WWW-Authenticate nên trình duyệt sẽ không hiện hộp đăng nhập", c.name)
		}
	}

	if rec := get(t, Protect(okHandler, ""), "/ui/"); rec.Code != http.StatusOK {
		t.Errorf("không đặt mật khẩu mà vẫn bị chặn: %d", rec.Code)
	}
}

// Trang web lạ không được bấm nút hộ người dùng, kể cả khi trình duyệt đã nhớ
// mật khẩu; còn công cụ dòng lệnh (không có Sec-Fetch-Site) vẫn gọi được.
func TestProtectBlocksCrossSiteWrites(t *testing.T) {
	h := Protect(okHandler, "")

	cases := []struct {
		method, site string
		want         int
	}{
		{http.MethodPost, "cross-site", http.StatusForbidden},
		{http.MethodPut, "same-site", http.StatusForbidden},
		{http.MethodPost, "same-origin", http.StatusOK},
		{http.MethodGet, "cross-site", http.StatusOK},
		{http.MethodPost, "", http.StatusOK},
	}
	for _, c := range cases {
		req := httptest.NewRequest(c.method, "/ui/api/disconnect", nil)
		if c.site != "" {
			req.Header.Set("Sec-Fetch-Site", c.site)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s với Sec-Fetch-Site=%q: mã %d, mong đợi %d", c.method, c.site, rec.Code, c.want)
		}
	}
}
