// Package metadb lưu metadata của coordinator trong PostgreSQL.
package metadb

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"dstore-demo/internal/coordinator"
)

// Schema là các bảng coordinator cần; truyền cho pgdb.Open để tạo lúc khởi động.
//
//go:embed schema.sql
var Schema string

// Postgres cài đặt coordinator.MetaStore.
type Postgres struct {
	pool *pgxpool.Pool
}

var _ coordinator.MetaStore = (*Postgres)(nil)

// New dùng một pool đã mở bằng pgdb.Open (chạy được cả với Amazon RDS).
func New(pool *pgxpool.Pool) *Postgres {
	return &Postgres{pool: pool}
}

func (p *Postgres) SaveNode(ctx context.Context, n coordinator.NodeRecord) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO nodes (id, addr, capacity) VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET addr = EXCLUDED.addr, capacity = EXCLUDED.capacity`,
		n.ID, n.Addr, n.Capacity)
	return err
}

// SaveFile ghi file cùng mọi segment và shard trong một transaction: hoặc có
// đủ, hoặc không có gì. Ghi lại lần hai (client retry commit) thì thay bản cũ.
func (p *Postgres) SaveFile(ctx context.Context, f coordinator.FileRecord) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // không có tác dụng nếu đã Commit

	// Gom mọi câu INSERT vào một lượt gửi thay vì hàng trăm lượt đi về.
	batch := &pgx.Batch{}
	batch.Queue(`DELETE FROM files WHERE id = $1`, f.ID)
	batch.Queue(`
		INSERT INTO files (id, name, size, segment_size, k, m, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		f.ID, f.Name, f.Size, f.SegmentSize, f.K, f.M, f.CreatedAt)
	for _, seg := range f.Segments {
		batch.Queue(`INSERT INTO segments (file_id, seg_index, orig_size) VALUES ($1, $2, $3)`,
			f.ID, seg.Index, seg.OrigSize)
		for _, sh := range seg.Shards {
			batch.Queue(`
				INSERT INTO shards (file_id, seg_index, shard_index, hash, node_id)
				VALUES ($1, $2, $3, $4, $5)`,
				f.ID, seg.Index, sh.Index, sh.Hash, sh.NodeID)
		}
	}
	if err := tx.SendBatch(ctx, batch).Close(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (p *Postgres) UpdateShard(ctx context.Context, fileID string, segIndex, shardIndex int, hash, nodeID string) error {
	_, err := p.pool.Exec(ctx, `
		INSERT INTO shards (file_id, seg_index, shard_index, hash, node_id)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (file_id, seg_index, shard_index)
		DO UPDATE SET hash = EXCLUDED.hash, node_id = EXCLUDED.node_id`,
		fileID, segIndex, shardIndex, hash, nodeID)
	return err
}

// DeleteFile xóa file; segment và shard đi theo nhờ ON DELETE CASCADE.
func (p *Postgres) DeleteFile(ctx context.Context, fileID string) error {
	_, err := p.pool.Exec(ctx, `DELETE FROM files WHERE id = $1`, fileID)
	return err
}

// Load đọc toàn bộ metadata bằng bốn câu truy vấn, bất kể có bao nhiêu file.
func (p *Postgres) Load(ctx context.Context) ([]coordinator.NodeRecord, []coordinator.FileRecord, error) {
	nodes, err := p.loadNodes(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("đọc nodes: %w", err)
	}

	files := make(map[string]*coordinator.FileRecord)
	var order []string // giữ thứ tự ổn định
	rows, err := p.pool.Query(ctx, `SELECT id, name, size, segment_size, k, m, created_at FROM files ORDER BY created_at`)
	if err != nil {
		return nil, nil, fmt.Errorf("đọc files: %w", err)
	}
	for rows.Next() {
		var f coordinator.FileRecord
		if err := rows.Scan(&f.ID, &f.Name, &f.Size, &f.SegmentSize, &f.K, &f.M, &f.CreatedAt); err != nil {
			rows.Close()
			return nil, nil, err
		}
		files[f.ID] = &f
		order = append(order, f.ID)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	// Segment đọc theo thứ tự seg_index, nên Segments[i] là segment số i.
	rows, err = p.pool.Query(ctx, `SELECT file_id, seg_index, orig_size FROM segments ORDER BY file_id, seg_index`)
	if err != nil {
		return nil, nil, fmt.Errorf("đọc segments: %w", err)
	}
	for rows.Next() {
		var fileID string
		var seg coordinator.SegmentRecord
		if err := rows.Scan(&fileID, &seg.Index, &seg.OrigSize); err != nil {
			rows.Close()
			return nil, nil, err
		}
		if f, ok := files[fileID]; ok {
			f.Segments = append(f.Segments, seg)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	rows, err = p.pool.Query(ctx, `
		SELECT file_id, seg_index, shard_index, hash, node_id
		FROM shards ORDER BY file_id, seg_index, shard_index`)
	if err != nil {
		return nil, nil, fmt.Errorf("đọc shards: %w", err)
	}
	for rows.Next() {
		var fileID string
		var segIndex int
		var sh coordinator.ShardRecord
		if err := rows.Scan(&fileID, &segIndex, &sh.Index, &sh.Hash, &sh.NodeID); err != nil {
			rows.Close()
			return nil, nil, err
		}
		f, ok := files[fileID]
		if !ok || segIndex >= len(f.Segments) || f.Segments[segIndex].Index != segIndex {
			rows.Close()
			return nil, nil, fmt.Errorf("shard trỏ tới segment %s/%d không tồn tại", fileID, segIndex)
		}
		f.Segments[segIndex].Shards = append(f.Segments[segIndex].Shards, sh)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	out := make([]coordinator.FileRecord, 0, len(order))
	for _, id := range order {
		out = append(out, *files[id])
	}
	return nodes, out, nil
}

func (p *Postgres) loadNodes(ctx context.Context) ([]coordinator.NodeRecord, error) {
	rows, err := p.pool.Query(ctx, `SELECT id, addr, capacity FROM nodes ORDER BY id`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (coordinator.NodeRecord, error) {
		var n coordinator.NodeRecord
		err := row.Scan(&n.ID, &n.Addr, &n.Capacity)
		return n, err
	})
}
