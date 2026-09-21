package gateway

import (
	"context"
	"sync"
)

// FileMeta là phần metadata chỉ gateway quan tâm: file thuộc về ai và trả về
// với Content-Type nào. Coordinator không biết tới khái niệm tenant.
type FileMeta struct {
	Tenant      string
	Name        string
	ContentType string
}

// Owners ghi nhớ file nào thuộc tenant nào.
//
// Gateway chạy nhiều bản song song (nó nằm trên đường dữ liệu nên phải scale
// ngang), upload qua bản này rồi tải qua bản khác là chuyện thường. Vì vậy bảng
// này phải nằm ở chỗ dùng chung — PostgreSQL — chứ không phải RAM của từng bản.
type Owners interface {
	Put(ctx context.Context, fileID string, meta FileMeta) error
	// Get trả về metadata nếu file thuộc về tenant này.
	Get(ctx context.Context, fileID, tenant string) (FileMeta, bool, error)
	Remove(ctx context.Context, fileID string) error
	// OwnedBy trả về file_id -> metadata của mọi file thuộc tenant.
	OwnedBy(ctx context.Context, tenant string) (map[string]FileMeta, error)
}

// --- Bộ nhớ ---

// MemoryOwners giữ bảng trong RAM: dùng cho test, và khi chỉ chạy một bản
// gateway không có DB (restart là mất).
type MemoryOwners struct {
	mu    sync.RWMutex
	files map[string]FileMeta
}

func NewMemoryOwners() *MemoryOwners {
	return &MemoryOwners{files: make(map[string]FileMeta)}
}

func (m *MemoryOwners) Put(_ context.Context, fileID string, meta FileMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[fileID] = meta
	return nil
}

func (m *MemoryOwners) Get(_ context.Context, fileID, tenant string) (FileMeta, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	meta, ok := m.files[fileID]
	if !ok || meta.Tenant != tenant {
		return FileMeta{}, false, nil
	}
	return meta, true, nil
}

func (m *MemoryOwners) Remove(_ context.Context, fileID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, fileID)
	return nil
}

func (m *MemoryOwners) OwnedBy(_ context.Context, tenant string) (map[string]FileMeta, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[string]FileMeta)
	for id, meta := range m.files {
		if meta.Tenant == tenant {
			out[id] = meta
		}
	}
	return out, nil
}
