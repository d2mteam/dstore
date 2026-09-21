// Package protocol chứa các struct JSON dùng chung giữa coordinator, node và client.
package protocol

import "time"

// Trạng thái node.
const (
	NodeOnline  = "online"
	NodeOffline = "offline"
)

// Trạng thái shard.
const (
	ShardOK        = "ok"
	ShardLost      = "lost"
	ShardRepairing = "repairing"
)

// Trạng thái file.
const (
	FileOK      = "ok"
	FileDamaged = "damaged" // có segment còn dưới k shard khỏe
)

// --- Node đăng ký và heartbeat ---

type RegisterRequest struct {
	Addr     string `json:"addr"`     // địa chỉ node để client/coordinator gọi tới, ví dụ "http://node-1:8080"
	Capacity int64  `json:"capacity"` // dung lượng tối đa (byte)
	NodeID   string `json:"node_id"`  // node tự đề xuất ID để giữ nguyên sau khi restart
}

type RegisterResponse struct {
	NodeID string `json:"node_id"`
}

type HeartbeatRequest struct {
	UsedBytes  int64 `json:"used_bytes"`
	ShardCount int   `json:"shard_count"`
}

// NodeInfo là thông tin node coordinator trả về cho `dsctl nodes`.
type NodeInfo struct {
	ID            string    `json:"id"`
	Addr          string    `json:"addr"`
	Capacity      int64     `json:"capacity"`
	Used          int64     `json:"used"`
	ShardCount    int       `json:"shard_count"`
	Status        string    `json:"status"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

// --- Upload ---

type PlanRequest struct {
	Name        string `json:"name"`
	Size        int64  `json:"size"`
	K           int    `json:"k"`
	M           int    `json:"m"`
	SegmentSize int    `json:"segment_size"`
}

// NodeRef là node được chọn để giữ một shard.
type NodeRef struct {
	ID   string `json:"id"`
	Addr string `json:"addr"`
}

// SegmentPlan: Nodes[i] là node dành cho shard thứ i của segment (0..k+m-1).
type SegmentPlan struct {
	Index int       `json:"index"`
	Nodes []NodeRef `json:"nodes"`
}

type PlanResponse struct {
	FileID      string        `json:"file_id"`
	K           int           `json:"k"`
	M           int           `json:"m"`
	SegmentSize int           `json:"segment_size"`
	Segments    []SegmentPlan `json:"segments"`
}

type CommitShard struct {
	Index  int    `json:"index"`
	Hash   string `json:"hash"`
	NodeID string `json:"node_id"`
}

type CommitSegment struct {
	Index    int           `json:"index"`
	OrigSize int           `json:"orig_size"` // kích thước segment trước khi Split đệm thêm
	Shards   []CommitShard `json:"shards"`
}

type CommitRequest struct {
	Segments []CommitSegment `json:"segments"`
}

// PickRequest dùng khi client gặp node lỗi và cần node thay thế.
type PickRequest struct {
	Count   int      `json:"count"`
	Exclude []string `json:"exclude"` // node ID không được chọn
}

type PickResponse struct {
	Nodes []NodeRef `json:"nodes"`
}

// --- Download ---

type ManifestShard struct {
	Index    int    `json:"index"`
	Hash     string `json:"hash"`
	NodeID   string `json:"node_id"`
	NodeAddr string `json:"node_addr"`
	Status   string `json:"status"`
}

type ManifestSegment struct {
	Index    int             `json:"index"`
	OrigSize int             `json:"orig_size"`
	Shards   []ManifestShard `json:"shards"`
}

type Manifest struct {
	FileID      string            `json:"file_id"`
	Name        string            `json:"name"`
	Size        int64             `json:"size"`
	K           int               `json:"k"`
	M           int               `json:"m"`
	SegmentSize int               `json:"segment_size"`
	Status      string            `json:"status"`
	Segments    []ManifestSegment `json:"segments"`
}

// FileInfo là một dòng trong `dsctl ls`.
type FileInfo struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Size      int64     `json:"size"`
	K         int       `json:"k"`
	M         int       `json:"m"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	Segments  int       `json:"segments"`
	// MinHealthy là số shard khỏe của segment yếu nhất: bằng k+m là đủ, dưới k là mất dữ liệu.
	MinHealthy int `json:"min_healthy"`
}

// ErrorResponse là body trả về khi có lỗi.
type ErrorResponse struct {
	Error string `json:"error"`
}

// Duration là time.Duration đọc/ghi JSON dạng chuỗi "20s", cùng cách viết với
// biến môi trường, để cấu hình sửa qua web UI dễ đọc.
type Duration time.Duration

// String để log in "15s" thay vì số nano giây.
func (d Duration) String() string { return time.Duration(d).String() }

func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}
