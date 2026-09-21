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

// RegisterRequest là body của POST /nodes/register: node báo địa chỉ, dung
// lượng và ID nó muốn giữ.
type RegisterRequest struct {
	Addr     string `json:"addr"`     // địa chỉ node để client/coordinator gọi tới, ví dụ "http://node-1:8080"
	Capacity int64  `json:"capacity"` // dung lượng tối đa (byte)
	NodeID   string `json:"node_id"`  // node tự đề xuất ID để giữ nguyên sau khi restart
}

// RegisterResponse trả ID mà coordinator dùng cho node: chính NodeID node đề
// xuất, hoặc một ID ngẫu nhiên nếu node không đề xuất.
type RegisterResponse struct {
	NodeID string `json:"node_id"`
}

// HeartbeatRequest là body của POST /nodes/{id}/heartbeat, node gửi định kỳ để
// báo còn sống và đang dùng bao nhiêu chỗ.
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

// PlanRequest là body của POST /files/plan: client cho biết file lớn bao nhiêu
// và muốn mã hóa với k, m, kích thước segment nào.
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

// PlanResponse là kế hoạch upload: file_id mới cấp và node dành cho từng shard
// của từng segment. File chưa đọc được cho tới khi được commit.
type PlanResponse struct {
	FileID      string        `json:"file_id"`
	K           int           `json:"k"`
	M           int           `json:"m"`
	SegmentSize int           `json:"segment_size"`
	Segments    []SegmentPlan `json:"segments"`
}

// CommitShard báo một shard đã nằm trên node nào, với hash nào.
type CommitShard struct {
	Index  int    `json:"index"`
	Hash   string `json:"hash"`
	NodeID string `json:"node_id"`
}

// CommitSegment là các shard đã upload xong của một segment; cần ít nhất k shard.
type CommitSegment struct {
	Index    int           `json:"index"`
	OrigSize int           `json:"orig_size"` // kích thước segment trước khi Split đệm thêm
	Shards   []CommitShard `json:"shards"`
}

// CommitRequest là body của POST /files/{id}/commit, gửi sau khi đã PUT xong
// shard của mọi segment.
type CommitRequest struct {
	Segments []CommitSegment `json:"segments"`
}

// PickRequest dùng khi client gặp node lỗi và cần node thay thế.
type PickRequest struct {
	Count   int      `json:"count"`
	Exclude []string `json:"exclude"` // node ID không được chọn
}

// PickResponse là các node thay thế mà coordinator cấp.
type PickResponse struct {
	Nodes []NodeRef `json:"nodes"`
}

// --- Download ---

// ManifestShard là một shard trong manifest: nằm ở node nào, địa chỉ nào, còn
// khỏe ([ShardOK]) hay đã mất ([ShardLost]).
type ManifestShard struct {
	Index    int    `json:"index"`
	Hash     string `json:"hash"`
	NodeID   string `json:"node_id"`
	NodeAddr string `json:"node_addr"`
	Status   string `json:"status"`
}

// ManifestSegment là các shard của một segment, kèm kích thước gốc để cắt phần
// đệm khi ghép.
type ManifestSegment struct {
	Index    int             `json:"index"`
	OrigSize int             `json:"orig_size"`
	Shards   []ManifestShard `json:"shards"`
}

// Manifest là mọi thứ client cần để tải một file: tham số mã hóa và vị trí từng
// shard. Coordinator trả nó ở GET /files/{id}.
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

// MarshalText ghi Duration dạng "20s".
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

// UnmarshalText đọc Duration theo cú pháp của time.ParseDuration, ví dụ "1m30s".
func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}
