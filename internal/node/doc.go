// Package node là data plane của dstore: mỗi node giữ một phần shard, kiểm
// hash khi nhận, phục vụ đọc/ghi qua HTTP và báo tình trạng cho coordinator.
// Node không biết shard thuộc file nào — đó là việc của coordinator.
//
// # Thành phần
//
//   - [ShardStore]: nơi byte thật sự nằm. [DiskStore] ghi file dưới một thư
//     mục local (dùng cho test và chạy nhanh), [S3Store] ghi object vào Amazon
//     S3 hoặc MinIO. Cả hai kiểm SHA-256 trước khi nhận shard và tự đếm lại
//     shard đang có khi khởi động.
//   - [Server]: API shard mà client và coordinator gọi — PUT/GET/HEAD/DELETE
//     /shards/{hash}, GET /shards, GET /healthz.
//   - [Reporter]: đăng ký với coordinator rồi heartbeat định kỳ, tự đăng ký lại
//     khi mất kết nối. Cấu hình kết nối ([Settings]) đổi được lúc đang chạy, và
//     heartbeat tạm ngắt được để diễn một vụ node chết.
//   - [Admin]: API riêng của web UI dưới /ui/api/ (xem package webui).
//
// # Vòng đời
//
//	Settings từ env, CONFIG_FILE (nếu có) đè lên
//	→ mở ShardStore → Server và Admin nghe HTTP
//	→ Reporter.Run: đăng ký → heartbeat mỗi HeartbeatInterval → lỗi thì đăng ký lại
//
// Khi node quay lại sau một thời gian offline, coordinator gọi GET /shards để
// đối chiếu và đánh dấu khỏe lại những shard còn nguyên. Shard thừa — đã được
// repair sang node khác trong lúc node vắng mặt — sẽ bị GC của coordinator xóa
// sau thời gian ân hạn.
//
// # Các file
//
//   - store.go: interface ShardStore và các lỗi chung ([ErrNotFound],
//     [ErrInvalidHash], [ErrHashMismatch], [ErrTooLarge]).
//   - diskstore.go: DiskStore, shard nằm ở <dir>/ab/cd/<hash>, ghi file tạm rồi
//     đổi tên để không bao giờ lộ ra shard ghi dở.
//   - s3store.go: S3Store và S3Config, mỗi node một prefix trong bucket dùng chung.
//   - api.go: Server, quy lỗi của ShardStore ra mã HTTP.
//   - heartbeat.go: Reporter và ConnStatus.
//   - settings.go: Settings, đọc/ghi file cấu hình do web UI lưu.
//   - admin.go: Admin, API của web UI.
package node
