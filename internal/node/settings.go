package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dstore-demo/internal/protocol"
)

// Settings là cấu hình kết nối của node, sửa được lúc đang chạy qua web UI.
// Những thứ gắn với danh tính và dữ liệu của node (NODE_ID, nơi lưu shard) thì
// không nằm ở đây: đổi chúng khi đang chạy sẽ làm coordinator mất dấu shard.
type Settings struct {
	CoordinatorURL string `json:"coordinator_url"`
	// Addr là địa chỉ client và coordinator dùng để gọi tới node này.
	Addr              string            `json:"addr"`
	Capacity          int64             `json:"capacity"` // byte
	HeartbeatInterval protocol.Duration `json:"heartbeat_interval"`
}

// Clean bỏ khoảng trắng và dấu "/" cuối URL mà người dùng hay gõ thừa.
func (s Settings) Clean() Settings {
	s.CoordinatorURL = strings.TrimRight(strings.TrimSpace(s.CoordinatorURL), "/")
	s.Addr = strings.TrimRight(strings.TrimSpace(s.Addr), "/")
	return s
}

// Validate kiểm tra hai URL có dạng http(s)://host, dung lượng dương và nhịp
// heartbeat từ 100ms trở lên. Gọi sau Clean.
func (s Settings) Validate() error {
	if err := checkURL(s.CoordinatorURL); err != nil {
		return fmt.Errorf("coordinator_url: %w", err)
	}
	if err := checkURL(s.Addr); err != nil {
		return fmt.Errorf("addr: %w", err)
	}
	if s.Capacity <= 0 {
		return errors.New("capacity phải > 0")
	}
	if time.Duration(s.HeartbeatInterval) < 100*time.Millisecond {
		return errors.New("heartbeat_interval phải từ 100ms trở lên")
	}
	return nil
}

func checkURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%q phải có dạng http://host:port", raw)
	}
	return nil
}

// LoadSettings đọc file cấu hình do web UI ghi, đè lên defaults (lấy từ env).
// Trường nào file không có thì giữ giá trị của defaults. Chưa có file thì
// trả về defaults và found = false.
func LoadSettings(path string, defaults Settings) (s Settings, found bool, err error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return defaults, false, nil
	}
	if err != nil {
		return defaults, false, err
	}
	s = defaults
	if err := json.Unmarshal(data, &s); err != nil {
		return defaults, false, fmt.Errorf("file cấu hình %s hỏng: %w", path, err)
	}
	return s, true, nil
}

// SaveSettings ghi ra file tạm rồi đổi tên, để mất điện giữa chừng không để
// lại file cấu hình ghi dở.
func SaveSettings(path string, s Settings) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
