package gateway

import (
	"context"
	_ "embed"
	"errors"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
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

// --- PostgreSQL ---

// OwnersSchema là bảng gateway cần; truyền cho pgdb.Open để tạo lúc khởi động.
//
//go:embed schema.sql
var OwnersSchema string

// PostgresOwners cài đặt Owners trên bảng file_owners, dùng chung giữa mọi bản gateway.
type PostgresOwners struct {
	pool *pgxpool.Pool
}

// NewPostgresOwners dùng một pool đã mở bằng pgdb.Open (chạy được cả với Amazon RDS).
func NewPostgresOwners(pool *pgxpool.Pool) *PostgresOwners {
	return &PostgresOwners{pool: pool}
}

// Put ghi chủ sở hữu của file, thay bản cũ nếu có.
func (p *PostgresOwners) Put(ctx context.Context, fileID string, meta FileMeta) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO file_owners (file_id, tenant, name, content_type) VALUES ($1, $2, $3, $4)
		ON CONFLICT (file_id) DO UPDATE
		SET tenant = EXCLUDED.tenant, name = EXCLUDED.name, content_type = EXCLUDED.content_type`,
		fileID, meta.Tenant, meta.Name, meta.ContentType)
	return err
}

// Get trả metadata nếu file thuộc tenant; file của tenant khác trả về y như
// file không tồn tại.
func (p *PostgresOwners) Get(ctx context.Context, fileID, tenant string) (FileMeta, bool, error) {
	// Lọc theo tenant ngay trong câu truy vấn: file của tenant khác và file
	// không tồn tại trông y hệt nhau.
	m := FileMeta{Tenant: tenant}
	err := p.pool.QueryRow(ctx,
		`SELECT name, content_type FROM file_owners WHERE file_id = $1 AND tenant = $2`,
		fileID, tenant).Scan(&m.Name, &m.ContentType)
	if errors.Is(err, pgx.ErrNoRows) {
		return FileMeta{}, false, nil
	}
	if err != nil {
		return FileMeta{}, false, err
	}
	return m, true, nil
}

// Remove xóa dòng của file; không có dòng nào cũng không lỗi.
func (p *PostgresOwners) Remove(ctx context.Context, fileID string) error {
	_, err := p.pool.Exec(ctx, `DELETE FROM file_owners WHERE file_id = $1`, fileID)
	return err
}

// OwnedBy trả mọi file của tenant, khóa theo file_id.
func (p *PostgresOwners) OwnedBy(ctx context.Context, tenant string) (map[string]FileMeta, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT file_id, name, content_type FROM file_owners WHERE tenant = $1`, tenant)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]FileMeta)
	for rows.Next() {
		var id string
		m := FileMeta{Tenant: tenant}
		if err := rows.Scan(&id, &m.Name, &m.ContentType); err != nil {
			return nil, err
		}
		out[id] = m
	}
	return out, rows.Err()
}

// --- Bộ nhớ ---

// MemoryOwners giữ bảng trong RAM: dùng cho test, và khi chỉ chạy một bản
// gateway không có DB (restart là mất).
type MemoryOwners struct {
	mu    sync.RWMutex
	files map[string]FileMeta
}

// NewMemoryOwners tạo MemoryOwners rỗng.
func NewMemoryOwners() *MemoryOwners {
	return &MemoryOwners{files: make(map[string]FileMeta)}
}

// Put ghi chủ sở hữu của file, thay bản cũ nếu có.
func (m *MemoryOwners) Put(_ context.Context, fileID string, meta FileMeta) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[fileID] = meta
	return nil
}

// Get trả metadata nếu file thuộc tenant; file của tenant khác trả về y như
// file không tồn tại.
func (m *MemoryOwners) Get(_ context.Context, fileID, tenant string) (FileMeta, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	meta, ok := m.files[fileID]
	if !ok || meta.Tenant != tenant {
		return FileMeta{}, false, nil
	}
	return meta, true, nil
}

// Remove xóa file khỏi bảng; không có cũng không lỗi.
func (m *MemoryOwners) Remove(_ context.Context, fileID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, fileID)
	return nil
}

// OwnedBy trả mọi file của tenant, khóa theo file_id.
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
