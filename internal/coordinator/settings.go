package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"dstore-demo/internal/protocol"
)

// Settings là các tham số vận hành sửa được lúc đang chạy qua web UI. Giá trị
// ban đầu lấy từ biến môi trường; một khi đã sửa qua UI thì bản lưu trong DB
// được dùng cho cả những lần khởi động sau.
type Settings struct {
	// HeartbeatTimeout: quá hạn này không heartbeat thì node bị coi là chết.
	HeartbeatTimeout protocol.Duration `json:"heartbeat_timeout"`
	// GCGrace: shard phải mồ côi liên tục chừng này mới bị xóa (xem gc.go).
	GCGrace protocol.Duration `json:"gc_grace"`
	// Tạm dừng vòng tự động, ví dụ khi bảo trì hàng loạt node và không muốn
	// repair chép dữ liệu đi. Nút "chạy ngay" trên UI vẫn chạy được.
	RepairPaused bool `json:"repair_paused"`
	GCPaused     bool `json:"gc_paused"`
}

// Validate chặn những giá trị làm hỏng cụm. GCGrace quá nhỏ là nguy hiểm nhất:
// GC sẽ xóa shard của file đang upload dở.
func (s Settings) Validate() error {
	if time.Duration(s.HeartbeatTimeout) < time.Second {
		return errors.New("heartbeat_timeout phải từ 1s trở lên")
	}
	if time.Duration(s.GCGrace) < 10*time.Second {
		return errors.New("gc_grace phải từ 10s trở lên, nhỏ hơn dễ xóa nhầm shard đang upload")
	}
	return nil
}

// SettingsStore giữ Settings hiện hành, dùng chung giữa API và các vòng nền.
// Các vòng nền đọc lại Get() mỗi lượt nên thay đổi có hiệu lực ngay lượt sau.
type SettingsStore struct {
	meta MetaStore

	mu  sync.RWMutex
	cur Settings
}

// NewSettingsStore tạo SettingsStore với giá trị ban đầu defaults (thường lấy
// từ env); gọi Load để đè bằng bản đã lưu trong meta.
func NewSettingsStore(meta MetaStore, defaults Settings) *SettingsStore {
	return &SettingsStore{meta: meta, cur: defaults}
}

// Load đè giá trị mặc định bằng bản đã lưu trong DB, nếu có. Trường nào bản lưu
// không có (ví dụ tham số mới thêm sau) thì giữ giá trị mặc định.
func (s *SettingsStore) Load(ctx context.Context) (found bool, err error) {
	data, err := s.meta.LoadSettings(ctx)
	if err != nil {
		return false, fmt.Errorf("%w: %v", ErrStorage, err)
	}
	if data == nil {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	merged := s.cur
	if err := json.Unmarshal(data, &merged); err != nil {
		return false, fmt.Errorf("settings trong DB hỏng: %w", err)
	}
	s.cur = merged
	return true, nil
}

// Get trả Settings đang có hiệu lực.
func (s *SettingsStore) Get() Settings {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cur
}

// Update kiểm tra rồi lưu xuống DB trước, áp dụng sau: UI báo "đã lưu" nghĩa
// là restart cũng không mất.
func (s *SettingsStore) Update(ctx context.Context, v Settings) error {
	if err := v.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := s.meta.SaveSettings(ctx, data); err != nil {
		return fmt.Errorf("%w: %v", ErrStorage, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur = v
	return nil
}

// trigger cho phép yêu cầu một vòng nền chạy thêm một lượt ngay, không đợi tới
// nhịp ticker. Lượt chạy vẫn nằm trong goroutine của vòng đó, nên không bao giờ
// có hai lượt repair (hay GC) chạy chồng lên nhau.
type trigger struct {
	once sync.Once
	ch   chan struct{}
}

func (t *trigger) C() chan struct{} {
	t.once.Do(func() { t.ch = make(chan struct{}, 1) })
	return t.ch
}

// Fire không chặn; bấm nhiều lần khi lượt trước chưa xong thì gộp làm một.
func (t *trigger) Fire() {
	select {
	case t.C() <- struct{}{}:
	default:
	}
}
