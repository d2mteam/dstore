// Package protocol chứa các kiểu dữ liệu đi qua HTTP giữa coordinator, node,
// client (dsctl, gateway) và web UI. Đây là nguồn sự thật duy nhất cho định
// dạng JSON: các package khác chỉ gặp nhau qua đây và qua HTTP, không import
// lẫn nhau.
//
// # Nhóm kiểu
//
//   - Trạng thái: [NodeOnline] / [NodeOffline], [ShardOK] / [ShardLost],
//     [FileOK] / [FileDamaged].
//   - Node đăng ký và heartbeat: [RegisterRequest], [RegisterResponse],
//     [HeartbeatRequest]; coordinator trả danh sách node dạng [NodeInfo].
//   - Upload: [PlanRequest] → [PlanResponse], trong đó mỗi segment có một
//     [SegmentPlan] gồm k+m [NodeRef]; upload xong thì gửi [CommitRequest] gồm
//     các [CommitSegment] và [CommitShard]. [PickRequest] / [PickResponse] xin
//     node thay thế khi một node lỗi giữa chừng.
//   - Download: [Manifest] → [ManifestSegment] → [ManifestShard], đủ để client
//     tự tải shard thẳng từ node rồi giải mã.
//   - Danh sách và lỗi: [FileInfo], [ErrorResponse].
//   - [Duration]: time.Duration đọc/ghi JSON dạng "20s", dùng cho cấu hình sửa
//     qua web UI.
//
// # Quy ước
//
// Shard được định danh bằng SHA-256 của nội dung (hex, 64 ký tự). shard_index
// cho biết đây là shard số mấy trong k+m shard của segment — Reed-Solomon cần
// biết điều này khi ghép lại. orig_size là kích thước segment trước khi được
// đệm cho chia đều cho k; thiếu nó thì không cắt được phần đệm khi ghép.
//
// Thêm trường mới thì giá trị 0 của trường đó phải có nghĩa hợp lý, vì bên gửi
// còn chạy bản cũ sẽ không gửi trường ấy.
//
// Package chỉ có một file, protocol.go, và không chứa logic.
package protocol
