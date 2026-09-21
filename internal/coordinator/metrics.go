package coordinator

import (
	"fmt"
	"io"
	"sort"
	"sync/atomic"
	"time"

	"dstore-demo/internal/protocol"
)

// Metrics đếm các sự kiện của control plane. Mọi phương thức đều an toàn khi
// con trỏ nil, để test dựng Repairer/Collector mà không cần quan tâm metrics.
type Metrics struct {
	repairedShards atomic.Int64
	repairFailures atomic.Int64
	orphansDeleted atomic.Int64
	nodesLost      atomic.Int64
	lastRepair     atomic.Int64 // unix nano lúc xong lượt repair gần nhất
	lastGC         atomic.Int64
}

// RepairedShard đếm một shard đã được tái tạo và đặt lên node mới.
func (m *Metrics) RepairedShard() {
	if m != nil {
		m.repairedShards.Add(1)
	}
}

// RepairFailed đếm một lần repair một segment thất bại.
func (m *Metrics) RepairFailed() {
	if m != nil {
		m.repairFailures.Add(1)
	}
}

// OrphansDeleted cộng n shard mồ côi vừa bị GC xóa.
func (m *Metrics) OrphansDeleted(n int) {
	if m != nil {
		m.orphansDeleted.Add(int64(n))
	}
}

// NodeLost đếm một lần node bị chuyển offline vì mất heartbeat.
func (m *Metrics) NodeLost() {
	if m != nil {
		m.nodesLost.Add(1)
	}
}

// RepairRan ghi thời điểm vừa xong một lượt repair.
func (m *Metrics) RepairRan() {
	if m != nil {
		m.lastRepair.Store(time.Now().UnixNano())
	}
}

// GCRan ghi thời điểm vừa xong một lượt GC.
func (m *Metrics) GCRan() {
	if m != nil {
		m.lastGC.Store(time.Now().UnixNano())
	}
}

// Counters là ảnh chụp các bộ đếm, dùng cho /metrics và web UI.
type Counters struct {
	RepairedShards int64     `json:"repaired_shards"`
	RepairFailures int64     `json:"repair_failures"`
	OrphansDeleted int64     `json:"orphans_deleted"`
	NodesLost      int64     `json:"nodes_lost"`
	LastRepair     time.Time `json:"last_repair,omitzero"` // zero = chưa chạy lượt nào
	LastGC         time.Time `json:"last_gc,omitzero"`
}

// Snapshot đọc mọi bộ đếm cùng lúc; con trỏ nil thì trả Counters rỗng.
func (m *Metrics) Snapshot() Counters {
	if m == nil {
		return Counters{}
	}
	return Counters{
		RepairedShards: m.repairedShards.Load(),
		RepairFailures: m.repairFailures.Load(),
		OrphansDeleted: m.orphansDeleted.Load(),
		NodesLost:      m.nodesLost.Load(),
		LastRepair:     unixNano(m.lastRepair.Load()),
		LastGC:         unixNano(m.lastGC.Load()),
	}
}

func unixNano(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// Stats là ảnh chụp trạng thái cụm tại một thời điểm.
type Stats struct {
	NodesOnline   int   `json:"nodes_online"`
	NodesOffline  int   `json:"nodes_offline"`
	CapacityBytes int64 `json:"capacity_bytes"`
	UsedBytes     int64 `json:"used_bytes"`

	Files        int `json:"files"`
	FilesDamaged int `json:"files_damaged"`
	Segments     int `json:"segments"`
	ShardsOK     int `json:"shards_ok"`
	ShardsLost   int `json:"shards_lost"`

	// SegmentHealth[n] = số segment đang có đúng n shard khỏe.
	// Đây là số liệu quan trọng nhất: nó cho thấy cụm đang cách mất dữ liệu bao xa.
	SegmentHealth map[int]int `json:"segment_health"`
}

// Stats tính số liệu hiện tại của cụm.
func (s *Store) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	st := Stats{SegmentHealth: make(map[int]int)}
	for _, n := range s.nodes {
		if n.Status == protocol.NodeOnline {
			st.NodesOnline++
		} else {
			st.NodesOffline++
		}
		st.CapacityBytes += n.Capacity
		st.UsedBytes += n.Used
	}

	for _, f := range s.files {
		if !f.Committed {
			continue
		}
		st.Files++
		if f.Status == protocol.FileDamaged {
			st.FilesDamaged++
		}
		for _, seg := range f.Segments {
			st.Segments++
			ok := 0
			for _, sh := range seg.Shards {
				if sh.Status == protocol.ShardOK {
					ok++
				} else {
					st.ShardsLost++
				}
			}
			st.ShardsOK += ok
			st.SegmentHealth[ok]++
		}
	}
	return st
}

// WriteMetrics xuất số liệu theo định dạng text của Prometheus.
// Tự viết thay vì kéo client_golang về cho gọn; đổi sang thư viện đó sau này
// chỉ phải thay hàm này.
func (s *Server) WriteMetrics(w io.Writer) {
	st := s.Store.Stats()
	c := s.Metrics.Snapshot()

	fmt.Fprint(w, "# HELP dstore_nodes Số node theo trạng thái.\n# TYPE dstore_nodes gauge\n")
	fmt.Fprintf(w, "dstore_nodes{status=\"online\"} %d\n", st.NodesOnline)
	fmt.Fprintf(w, "dstore_nodes{status=\"offline\"} %d\n", st.NodesOffline)

	fmt.Fprint(w, "# HELP dstore_capacity_bytes Tổng dung lượng các node.\n# TYPE dstore_capacity_bytes gauge\n")
	fmt.Fprintf(w, "dstore_capacity_bytes %d\n", st.CapacityBytes)
	fmt.Fprint(w, "# HELP dstore_used_bytes Tổng dung lượng đã dùng.\n# TYPE dstore_used_bytes gauge\n")
	fmt.Fprintf(w, "dstore_used_bytes %d\n", st.UsedBytes)

	fmt.Fprint(w, "# HELP dstore_files Số file theo trạng thái.\n# TYPE dstore_files gauge\n")
	fmt.Fprintf(w, "dstore_files{status=\"ok\"} %d\n", st.Files-st.FilesDamaged)
	fmt.Fprintf(w, "dstore_files{status=\"damaged\"} %d\n", st.FilesDamaged)

	fmt.Fprint(w, "# HELP dstore_segments Tổng số segment.\n# TYPE dstore_segments gauge\n")
	fmt.Fprintf(w, "dstore_segments %d\n", st.Segments)

	fmt.Fprint(w, "# HELP dstore_shards Số shard theo trạng thái.\n# TYPE dstore_shards gauge\n")
	fmt.Fprintf(w, "dstore_shards{status=\"ok\"} %d\n", st.ShardsOK)
	fmt.Fprintf(w, "dstore_shards{status=\"lost\"} %d\n", st.ShardsLost)

	fmt.Fprint(w, "# HELP dstore_segments_health Số segment theo số shard còn khỏe.\n"+
		"# TYPE dstore_segments_health gauge\n")
	for _, health := range sortedKeys(st.SegmentHealth) {
		fmt.Fprintf(w, "dstore_segments_health{healthy=\"%d\"} %d\n", health, st.SegmentHealth[health])
	}

	fmt.Fprint(w, "# HELP dstore_repaired_shards_total Số shard đã tái tạo.\n"+
		"# TYPE dstore_repaired_shards_total counter\n")
	fmt.Fprintf(w, "dstore_repaired_shards_total %d\n", c.RepairedShards)
	fmt.Fprint(w, "# HELP dstore_repair_failures_total Số lần repair một segment thất bại.\n"+
		"# TYPE dstore_repair_failures_total counter\n")
	fmt.Fprintf(w, "dstore_repair_failures_total %d\n", c.RepairFailures)
	fmt.Fprint(w, "# HELP dstore_orphan_shards_deleted_total Số shard mồ côi đã dọn.\n"+
		"# TYPE dstore_orphan_shards_deleted_total counter\n")
	fmt.Fprintf(w, "dstore_orphan_shards_deleted_total %d\n", c.OrphansDeleted)
	fmt.Fprint(w, "# HELP dstore_nodes_lost_total Số lần node mất heartbeat.\n"+
		"# TYPE dstore_nodes_lost_total counter\n")
	fmt.Fprintf(w, "dstore_nodes_lost_total %d\n", c.NodesLost)
}

func sortedKeys(m map[int]int) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}
