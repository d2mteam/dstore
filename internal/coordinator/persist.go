package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"dstore-demo/internal/protocol"
)

// Coordinator chia trạng thái làm hai loại:
//
//   - Trạng thái cứng: file gồm những segment nào, shard nào nằm trên node nào,
//     node ở địa chỉ nào. Mất là mất dữ liệu, nên được lưu bền qua MetaStore
//     (PostgreSQL khi chạy thật).
//   - Trạng thái mềm: node sống hay chết, shard ok hay lost, dung lượng đã dùng.
//     Không lưu, vì heartbeat dựng lại được trong vài giây sau khi coordinator
//     khởi động; lưu thì phải ghi DB mỗi 5 giây cho từng node mà chẳng được gì.
//
// Mọi truy vấn vẫn đọc từ bộ nhớ cho nhanh; DB chỉ để khôi phục sau restart.

// ErrStorage bọc lỗi khi ghi metadata xuống DB, để API phân biệt với lỗi do
// client gửi dữ liệu sai.
var ErrStorage = errors.New("không lưu được metadata")

// MetaStore lưu bền trạng thái cứng của coordinator.
type MetaStore interface {
	SaveNode(ctx context.Context, n NodeRecord) error
	// SaveFile ghi một file đã commit cùng toàn bộ segment và shard.
	SaveFile(ctx context.Context, f FileRecord) error
	// UpdateShard ghi lại vị trí mới của một shard sau khi repair.
	UpdateShard(ctx context.Context, fileID string, segIndex, shardIndex int, hash, nodeID string) error
	DeleteFile(ctx context.Context, fileID string) error
	// Load đọc lại mọi thứ khi coordinator khởi động.
	Load(ctx context.Context) ([]NodeRecord, []FileRecord, error)

	// SaveSettings/LoadSettings lưu Settings sửa qua web UI dưới dạng JSON.
	// LoadSettings trả nil khi chưa từng lưu.
	SaveSettings(ctx context.Context, data []byte) error
	LoadSettings(ctx context.Context) ([]byte, error)
	// Ping kiểm tra DB còn trả lời, để web UI hiện tình trạng kết nối.
	Ping(ctx context.Context) error
}

// NodeRecord là node như lưu trong DB: chỉ địa chỉ và dung lượng, không có
// trạng thái sống/chết.
type NodeRecord struct {
	ID       string
	Addr     string
	Capacity int64
}

// FileRecord là một file đã commit như lưu trong DB, kèm mọi segment và shard.
type FileRecord struct {
	ID          string
	Name        string
	Size        int64
	SegmentSize int
	K, M        int
	CreatedAt   time.Time
	Segments    []SegmentRecord
}

// SegmentRecord là một segment trong FileRecord; Segments[i] luôn có Index == i.
type SegmentRecord struct {
	Index    int
	OrigSize int
	Shards   []ShardRecord
}

// ShardRecord là vị trí một shard: shard_index nào, hash nào, trên node nào.
type ShardRecord struct {
	Index  int
	Hash   string
	NodeID string
}

// Load khôi phục metadata từ MetaStore, trả về số node và số file đọc được.
func (s *Store) Load(ctx context.Context) (nodes, files int, err error) {
	nodeRecs, fileRecs, err := s.meta.Load(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("%w: %v", ErrStorage, err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for _, r := range nodeRecs {
		// Coi như node vừa heartbeat. Node còn sống sẽ heartbeat tiếp trong vài
		// giây; node chết trong lúc coordinator tắt sẽ bị health monitor chuyển
		// offline sau một khoảng timeout, rồi repair chạy như bình thường.
		s.nodes[r.ID] = &Node{
			ID: r.ID, Addr: r.Addr, Capacity: r.Capacity,
			Status: protocol.NodeOnline, LastHeartbeat: now,
		}
	}
	for _, r := range fileRecs {
		s.files[r.ID] = fileFromRecord(r)
	}
	return len(nodeRecs), len(fileRecs), nil
}

// fileFromRecord dựng lại File đã commit; mọi shard tạm coi là ok cho tới khi
// health monitor nói khác.
func fileFromRecord(r FileRecord) *File {
	f := &File{
		ID: r.ID, Name: r.Name, Size: r.Size, SegmentSize: r.SegmentSize,
		K: r.K, M: r.M, Status: protocol.FileOK, Committed: true, CreatedAt: r.CreatedAt,
		Segments: make([]*Segment, len(r.Segments)),
	}
	for i, sr := range r.Segments {
		seg := &Segment{Index: sr.Index, OrigSize: sr.OrigSize}
		for _, sh := range sr.Shards {
			seg.Shards = append(seg.Shards, &Shard{
				Index: sh.Index, Hash: sh.Hash, NodeID: sh.NodeID, Status: protocol.ShardOK,
			})
		}
		f.Segments[i] = seg
	}
	return f
}

// commitRecord kiểm tra dữ liệu client commit và dựng bản ghi file hoàn chỉnh.
func commitRecord(f *File, segments []protocol.CommitSegment) (FileRecord, error) {
	if len(segments) != len(f.Segments) {
		return FileRecord{}, fmt.Errorf("commit %d segment nhưng plan có %d", len(segments), len(f.Segments))
	}
	rec := FileRecord{
		ID: f.ID, Name: f.Name, Size: f.Size, SegmentSize: f.SegmentSize,
		K: f.K, M: f.M, CreatedAt: f.CreatedAt,
		Segments: make([]SegmentRecord, len(f.Segments)),
	}
	for i := range rec.Segments {
		rec.Segments[i].Index = i
	}
	for _, cs := range segments {
		if cs.Index < 0 || cs.Index >= len(f.Segments) {
			return FileRecord{}, fmt.Errorf("segment index %d nằm ngoài phạm vi", cs.Index)
		}
		if len(cs.Shards) < f.K {
			return FileRecord{}, fmt.Errorf("segment %d chỉ commit %d shard, cần tối thiểu %d",
				cs.Index, len(cs.Shards), f.K)
		}
		seg := SegmentRecord{Index: cs.Index, OrigSize: cs.OrigSize}
		for _, sh := range cs.Shards {
			seg.Shards = append(seg.Shards, ShardRecord{Index: sh.Index, Hash: sh.Hash, NodeID: sh.NodeID})
		}
		rec.Segments[cs.Index] = seg
	}
	return rec, nil
}

// MemoryMeta giữ bản ghi trong RAM: dùng cho test, và khi chạy không có DB
// (restart là mất metadata, như bản đầu tiên của demo).
type MemoryMeta struct {
	mu       sync.Mutex
	nodes    map[string]NodeRecord
	files    map[string]FileRecord
	settings []byte
}

// NewMemoryMeta tạo MemoryMeta rỗng.
func NewMemoryMeta() *MemoryMeta {
	return &MemoryMeta{nodes: make(map[string]NodeRecord), files: make(map[string]FileRecord)}
}

// SaveNode ghi đè bản ghi node cùng ID.
func (m *MemoryMeta) SaveNode(_ context.Context, n NodeRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nodes[n.ID] = n
	return nil
}

// SaveFile lưu bản sao sâu của file, thay bản cũ nếu có.
func (m *MemoryMeta) SaveFile(_ context.Context, f FileRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[f.ID] = cloneFile(f)
	return nil
}

// UpdateShard đổi vị trí một shard, thêm mới nếu segment chưa có shard_index đó.
func (m *MemoryMeta) UpdateShard(_ context.Context, fileID string, segIndex, shardIndex int, hash, nodeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	f, ok := m.files[fileID]
	if !ok || segIndex >= len(f.Segments) {
		return fmt.Errorf("không có segment %s/%d", fileID, segIndex)
	}
	seg := &f.Segments[segIndex]
	for i := range seg.Shards {
		if seg.Shards[i].Index == shardIndex {
			seg.Shards[i].Hash, seg.Shards[i].NodeID = hash, nodeID
			return nil
		}
	}
	seg.Shards = append(seg.Shards, ShardRecord{Index: shardIndex, Hash: hash, NodeID: nodeID})
	return nil
}

// DeleteFile xóa file; không có file cũng không lỗi.
func (m *MemoryMeta) DeleteFile(_ context.Context, fileID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.files, fileID)
	return nil
}

// Load trả bản sao mọi node và file, theo thứ tự bất kỳ.
func (m *MemoryMeta) Load(_ context.Context) ([]NodeRecord, []FileRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	nodes := make([]NodeRecord, 0, len(m.nodes))
	for _, n := range m.nodes {
		nodes = append(nodes, n)
	}
	files := make([]FileRecord, 0, len(m.files))
	for _, f := range m.files {
		files = append(files, cloneFile(f))
	}
	return nodes, files, nil
}

// SaveSettings giữ bản sao của JSON settings.
func (m *MemoryMeta) SaveSettings(_ context.Context, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.settings = append([]byte(nil), data...)
	return nil
}

// LoadSettings trả JSON đã lưu, hoặc nil nếu chưa lưu lần nào.
func (m *MemoryMeta) LoadSettings(_ context.Context) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.settings...), nil
}

// Ping luôn thành công: không có DB nào để mất kết nối.
func (m *MemoryMeta) Ping(context.Context) error { return nil }

// cloneFile sao chép sâu để bản ghi trong MemoryMeta không dùng chung slice
// với bên ngoài — giống như dữ liệu đi qua DB thật.
func cloneFile(f FileRecord) FileRecord {
	out := f
	out.Segments = make([]SegmentRecord, len(f.Segments))
	for i, seg := range f.Segments {
		out.Segments[i] = seg
		out.Segments[i].Shards = append([]ShardRecord(nil), seg.Shards...)
	}
	return out
}
