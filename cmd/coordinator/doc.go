// Command coordinator chạy control plane của dstore (package coordinator):
// registry node, metadata file/shard, chọn node khi upload, phát hiện node
// chết, repair, dọn shard mồ côi, và web UI quản trị ở /ui/.
//
// Mọi cấu hình đọc từ biến môi trường:
//
//	LISTEN_ADDR        :9000     địa chỉ nghe HTTP (API, /metrics, /ui/)
//	DATABASE_URL       (trống)   PostgreSQL lưu metadata; trống = chỉ trong RAM, restart là mất
//	DB_IAM_AUTH        false     đăng nhập RDS bằng IAM thay cho mật khẩu (cần TLS và AWS_REGION)
//	DB_AUTO_MIGRATE    true      tạo bảng lúc khởi động
//	HEALTH_INTERVAL    5s        chu kỳ quét node quá hạn heartbeat
//	REPAIR_INTERVAL    10s       chu kỳ repair
//	GC_INTERVAL        60s       chu kỳ dọn shard mồ côi
//	HEARTBEAT_TIMEOUT  20s       quá hạn này không heartbeat thì node bị coi là chết (*)
//	GC_GRACE           2m        shard phải mồ côi liên tục chừng này mới bị xóa (*)
//	UI_PASSWORD        (trống)   mật khẩu web UI, tên đăng nhập admin; trống = không cần đăng nhập
//
// (*) Chỉ là giá trị mặc định: đã sửa qua web UI thì bản lưu trong DB được dùng
// ở những lần khởi động sau.
//
// Khi khởi động, coordinator chờ tới khi kết nối được DB, nạp metadata, coi mọi
// node đã biết là online cho tới khi quá hạn heartbeat, rồi chạy HealthMonitor,
// Repairer, Collector và HTTP server. SIGINT/SIGTERM thì tắt HTTP server gọn gàng.
package main
