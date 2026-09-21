package gateway

import (
	"crypto/subtle"
	"fmt"
	"strings"
)

// Keys ánh xạ API key sang tenant. Demo dùng key tĩnh nạp từ biến môi trường;
// bản thật nên là token ký có hạn dùng, nhưng phần còn lại của gateway không
// đổi vì chỗ nào cũng chỉ làm việc với tenant.
type Keys map[string]string

// ParseKeys đọc chuỗi dạng "key1:tenant1,key2:tenant2".
func ParseKeys(s string) (Keys, error) {
	keys := make(Keys)
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		key, tenant, ok := strings.Cut(pair, ":")
		if !ok || key == "" || tenant == "" {
			return nil, fmt.Errorf("API key sai định dạng: %q, cần dạng key:tenant", pair)
		}
		keys[key] = tenant
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("không có API key nào")
	}
	return keys, nil
}

// tenantFor tìm tenant của một API key. So sánh với mọi key bằng hàm so sánh
// thời gian hằng định để thời gian trả lời không tiết lộ key đúng tới đâu.
func (k Keys) tenantFor(key string) (string, bool) {
	tenant, found := "", false
	for candidate, owner := range k {
		if subtle.ConstantTimeCompare([]byte(candidate), []byte(key)) == 1 {
			tenant, found = owner, true
		}
	}
	return tenant, found
}

// bearerToken lấy token từ header "Authorization: Bearer <token>".
func bearerToken(header string) string {
	const prefix = "Bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}
