package gateway

import "sync"

// fileMeta là phần metadata chỉ gateway quan tâm: file thuộc về ai và trả về
// với Content-Type nào. Coordinator không biết tới khái niệm tenant.
type fileMeta struct {
	Tenant      string
	Name        string
	ContentType string
}

// registry giữ quyền sở hữu file trong bộ nhớ. Gateway restart là mất, giống
// coordinator; bản thật để cùng chỗ với metadata file.
type registry struct {
	mu    sync.RWMutex
	files map[string]fileMeta
}

func newRegistry() *registry {
	return &registry{files: make(map[string]fileMeta)}
}

func (r *registry) put(fileID string, meta fileMeta) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.files[fileID] = meta
}

// get trả về metadata nếu file thuộc về tenant này.
func (r *registry) get(fileID, tenant string) (fileMeta, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	meta, ok := r.files[fileID]
	if !ok || meta.Tenant != tenant {
		return fileMeta{}, false
	}
	return meta, true
}

func (r *registry) remove(fileID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.files, fileID)
}

// ownedBy lọc ra các file của một tenant.
func (r *registry) ownedBy(tenant string) map[string]fileMeta {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]fileMeta)
	for id, meta := range r.files {
		if meta.Tenant == tenant {
			out[id] = meta
		}
	}
	return out
}
