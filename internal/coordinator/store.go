// Package coordinator là control plane: giữ registry node, metadata file/shard,
// phát hiện node chết và chạy repair.
package coordinator

import (
	"fmt"
	"sync"
	"time"

	"dstore-demo/internal/protocol"
)

// Demo dùng map trong bộ nhớ thay cho SQLite: đủ cho một coordinator,
// đổi sang SQLite sau này chỉ cần thay các phương thức của Store.

type Node struct {
	ID            string
	Addr          string
	Capacity      int64
	Used          int64
	ShardCount    int
	Status        string
	LastHeartbeat time.Time
}

func (n *Node) Free() int64 { return n.Capacity - n.Used }

type Shard struct {
	Index  int // shard_index: Reed-Solomon cần biết đây là shard số mấy
	Hash   string
	NodeID string
	Status string
}

type Segment struct {
	Index    int
	OrigSize int // kích thước trước khi Split đệm; cần cho Join
	Shards   []*Shard
}

// OKCount là số shard còn khỏe của segment.
func (s *Segment) OKCount() int {
	n := 0
	for _, sh := range s.Shards {
		if sh.Status == protocol.ShardOK {
			n++
		}
	}
	return n
}

type File struct {
	ID          string
	Name        string
	Size        int64
	SegmentSize int
	K           int
	M           int
	Status      string
	Committed   bool // chưa commit thì file coi như chưa tồn tại
	CreatedAt   time.Time
	Segments    []*Segment
}

type Store struct {
	mu    sync.RWMutex
	nodes map[string]*Node
	files map[string]*File
}

func NewStore() *Store {
	return &Store{
		nodes: make(map[string]*Node),
		files: make(map[string]*File),
	}
}

// --- Node ---

// RegisterNode thêm node mới hoặc đưa node cũ trở lại online.
// Trả về true nếu node vừa quay lại từ trạng thái offline.
func (s *Store) RegisterNode(id, addr string, capacity int64) (returned bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if n, ok := s.nodes[id]; ok {
		returned = n.Status == protocol.NodeOffline
		n.Addr = addr
		n.Capacity = capacity
		n.Status = protocol.NodeOnline
		n.LastHeartbeat = time.Now()
		return returned
	}
	s.nodes[id] = &Node{
		ID: id, Addr: addr, Capacity: capacity,
		Status: protocol.NodeOnline, LastHeartbeat: time.Now(),
	}
	return false
}

// Heartbeat cập nhật số liệu node. Trả về false nếu coordinator chưa biết node này.
func (s *Store) Heartbeat(id string, used int64, shardCount int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.nodes[id]
	if !ok {
		return false
	}
	n.Used = used
	n.ShardCount = shardCount
	n.Status = protocol.NodeOnline
	n.LastHeartbeat = time.Now()
	return true
}

func (s *Store) Node(id string) (Node, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	n, ok := s.nodes[id]
	if !ok {
		return Node{}, false
	}
	return *n, true
}

// Nodes trả về bản sao danh sách node, sắp theo ID.
func (s *Store) Nodes() []protocol.NodeInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]protocol.NodeInfo, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, protocol.NodeInfo{
			ID: n.ID, Addr: n.Addr, Capacity: n.Capacity, Used: n.Used,
			ShardCount: n.ShardCount, Status: n.Status, LastHeartbeat: n.LastHeartbeat,
		})
	}
	sortByID(out)
	return out
}

// OnlineNodes trả về các node đang online (bản sao).
func (s *Store) OnlineNodes() []Node {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []Node
	for _, n := range s.nodes {
		if n.Status == protocol.NodeOnline {
			out = append(out, *n)
		}
	}
	return out
}

// MarkOffline chuyển node sang offline và đánh dấu mọi shard trên đó là lost.
// Trả về số shard bị ảnh hưởng.
func (s *Store) MarkOffline(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, ok := s.nodes[id]
	if !ok || n.Status == protocol.NodeOffline {
		return 0
	}
	n.Status = protocol.NodeOffline

	lost := 0
	for _, f := range s.files {
		for _, seg := range f.Segments {
			for _, sh := range seg.Shards {
				if sh.NodeID == id && sh.Status == protocol.ShardOK {
					sh.Status = protocol.ShardLost
					lost++
				}
			}
		}
	}
	return lost
}

// ExpiredNodes trả về node online nhưng quá hạn heartbeat.
func (s *Store) ExpiredNodes(timeout time.Duration) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var ids []string
	deadline := time.Now().Add(-timeout)
	for _, n := range s.nodes {
		if n.Status == protocol.NodeOnline && n.LastHeartbeat.Before(deadline) {
			ids = append(ids, n.ID)
		}
	}
	return ids
}

// RestoreShards đánh dấu ok trở lại các shard mà node báo là vẫn còn trên đĩa.
// Trả về số shard được khôi phục.
func (s *Store) RestoreShards(nodeID string, present map[string]bool) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	restored := 0
	for _, f := range s.files {
		for _, seg := range f.Segments {
			for _, sh := range seg.Shards {
				if sh.NodeID == nodeID && sh.Status == protocol.ShardLost && present[sh.Hash] {
					sh.Status = protocol.ShardOK
					restored++
				}
			}
		}
	}
	s.refreshFileStatusLocked()
	return restored
}

// --- File ---

// CreateFile tạo metadata file ở trạng thái chưa commit.
func (s *Store) CreateFile(id, name string, size int64, k, m, segmentSize, segmentCount int) *File {
	s.mu.Lock()
	defer s.mu.Unlock()

	f := &File{
		ID: id, Name: name, Size: size, SegmentSize: segmentSize,
		K: k, M: m, Status: protocol.FileOK, CreatedAt: time.Now(),
		Segments: make([]*Segment, segmentCount),
	}
	for i := range f.Segments {
		f.Segments[i] = &Segment{Index: i, Shards: make([]*Shard, 0, k+m)}
	}
	s.files[id] = f
	return f
}

// CommitFile ghi nhận shard nào đã lên node nào; sau bước này file mới đọc được.
func (s *Store) CommitFile(id string, segments []protocol.CommitSegment) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, ok := s.files[id]
	if !ok {
		return fmt.Errorf("không có file %s", id)
	}
	if len(segments) != len(f.Segments) {
		return fmt.Errorf("commit %d segment nhưng plan có %d", len(segments), len(f.Segments))
	}

	for _, cs := range segments {
		if cs.Index < 0 || cs.Index >= len(f.Segments) {
			return fmt.Errorf("segment index %d nằm ngoài phạm vi", cs.Index)
		}
		if len(cs.Shards) < f.K {
			return fmt.Errorf("segment %d chỉ commit %d shard, cần tối thiểu %d", cs.Index, len(cs.Shards), f.K)
		}
		seg := f.Segments[cs.Index]
		seg.OrigSize = cs.OrigSize
		seg.Shards = seg.Shards[:0]
		for _, sh := range cs.Shards {
			seg.Shards = append(seg.Shards, &Shard{
				Index: sh.Index, Hash: sh.Hash, NodeID: sh.NodeID, Status: protocol.ShardOK,
			})
		}
	}
	f.Committed = true
	return nil
}

func (s *Store) File(id string) (*File, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, ok := s.files[id]
	return f, ok
}

// Manifest dựng bản sao metadata để client tải file.
func (s *Store) Manifest(id string) (protocol.Manifest, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	f, ok := s.files[id]
	if !ok || !f.Committed {
		return protocol.Manifest{}, false
	}
	man := protocol.Manifest{
		FileID: f.ID, Name: f.Name, Size: f.Size, K: f.K, M: f.M,
		SegmentSize: f.SegmentSize, Status: f.Status,
	}
	for _, seg := range f.Segments {
		ms := protocol.ManifestSegment{Index: seg.Index, OrigSize: seg.OrigSize}
		for _, sh := range seg.Shards {
			addr := ""
			if n, ok := s.nodes[sh.NodeID]; ok {
				addr = n.Addr
			}
			ms.Shards = append(ms.Shards, protocol.ManifestShard{
				Index: sh.Index, Hash: sh.Hash, NodeID: sh.NodeID, NodeAddr: addr, Status: sh.Status,
			})
		}
		man.Segments = append(man.Segments, ms)
	}
	return man, true
}

// Files liệt kê các file đã commit.
func (s *Store) Files() []protocol.FileInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]protocol.FileInfo, 0, len(s.files))
	for _, f := range s.files {
		if !f.Committed {
			continue
		}
		out = append(out, protocol.FileInfo{
			ID: f.ID, Name: f.Name, Size: f.Size, K: f.K, M: f.M,
			Status: f.Status, CreatedAt: f.CreatedAt,
		})
	}
	sortFilesByTime(out)
	return out
}

// DeleteFile bỏ metadata và trả về danh sách (node addr, hash) cần xóa trên đĩa.
func (s *Store) DeleteFile(id string) ([]ShardLocation, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, ok := s.files[id]
	if !ok {
		return nil, false
	}
	var locs []ShardLocation
	for _, seg := range f.Segments {
		for _, sh := range seg.Shards {
			if n, ok := s.nodes[sh.NodeID]; ok {
				locs = append(locs, ShardLocation{NodeAddr: n.Addr, Hash: sh.Hash})
			}
		}
	}
	delete(s.files, id)
	return locs, true
}

// ShardLocation là một shard cụ thể nằm ở một node cụ thể.
type ShardLocation struct {
	NodeAddr string
	Hash     string
}

// --- Dùng cho repair ---

// SegmentRef trỏ tới một segment cần sửa.
type SegmentRef struct {
	FileID     string
	SegIndex   int
	K, M       int
	OrigSize   int
	Shards     []Shard // bản sao
	OKCount    int
	Repairable bool // còn >= k shard khỏe
}

// DegradedSegments trả về các segment thiếu shard, segment yếu nhất xếp trước.
func (s *Store) DegradedSegments() []SegmentRef {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var out []SegmentRef
	for _, f := range s.files {
		if !f.Committed {
			continue
		}
		for _, seg := range f.Segments {
			ok := seg.OKCount()
			if ok >= f.K+f.M {
				continue
			}
			ref := SegmentRef{
				FileID: f.ID, SegIndex: seg.Index, K: f.K, M: f.M,
				OrigSize: seg.OrigSize, OKCount: ok, Repairable: ok >= f.K,
			}
			for _, sh := range seg.Shards {
				ref.Shards = append(ref.Shards, *sh)
			}
			out = append(out, ref)
		}
	}
	sortByOKCount(out) // ưu tiên segment ít shard khỏe nhất
	return out
}

// ReplaceShard cập nhật metadata sau khi repair đặt shard lên node mới.
func (s *Store) ReplaceShard(fileID string, segIndex, shardIndex int, hash, nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	f, ok := s.files[fileID]
	if !ok || segIndex >= len(f.Segments) {
		return fmt.Errorf("không có segment %s/%d", fileID, segIndex)
	}
	seg := f.Segments[segIndex]
	for _, sh := range seg.Shards {
		if sh.Index == shardIndex {
			sh.Hash = hash
			sh.NodeID = nodeID
			sh.Status = protocol.ShardOK
			s.refreshFileStatusLocked()
			return nil
		}
	}
	// Shard chưa từng tồn tại trong metadata (ví dụ upload thiếu) -> thêm mới.
	seg.Shards = append(seg.Shards, &Shard{
		Index: shardIndex, Hash: hash, NodeID: nodeID, Status: protocol.ShardOK,
	})
	s.refreshFileStatusLocked()
	return nil
}

// MarkFileDamaged đánh dấu file có segment mất quá nhiều shard.
func (s *Store) MarkFileDamaged(fileID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f, ok := s.files[fileID]; ok {
		f.Status = protocol.FileDamaged
	}
}

// refreshFileStatusLocked tính lại trạng thái file; gọi khi đã giữ s.mu.
func (s *Store) refreshFileStatusLocked() {
	for _, f := range s.files {
		status := protocol.FileOK
		for _, seg := range f.Segments {
			if f.Committed && seg.OKCount() < f.K {
				status = protocol.FileDamaged
				break
			}
		}
		f.Status = status
	}
}
