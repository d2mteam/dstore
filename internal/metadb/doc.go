// Package metadb cài đặt coordinator.MetaStore trên PostgreSQL, chạy được với
// Postgres tự dựng lẫn Amazon RDS / Aurora; kết nối được mở bằng package pgdb.
//
// # Bảng
//
// schema.sql, nhúng vào [Schema], tạo bốn bảng trạng thái cứng — nodes, files,
// segments, shards — và bảng settings một dòng cho tham số vận hành sửa qua web
// UI. segments và shards bị xóa theo file nhờ ON DELETE CASCADE. shards cố ý
// không có khóa ngoại tới nodes, để việc ghi node lỗi không làm hỏng commit file.
//
// # Ghi và đọc
//
//   - SaveFile ghi file cùng mọi segment và shard trong một transaction, gom các
//     câu INSERT vào một pgx.Batch; commit lại lần hai thì thay bản cũ.
//   - UpdateShard là upsert, dùng sau mỗi lần repair.
//   - Load đọc toàn bộ bằng bốn câu truy vấn, bất kể có bao nhiêu file.
//
// Package coordinator không import package này — chỉ cmd/coordinator nối hai
// bên lại — nên logic lõi không biết PostgreSQL tồn tại.
//
// # Các file
//
//   - postgres.go: Postgres và các phương thức của MetaStore.
//   - schema.sql: định nghĩa bảng, được tạo lúc khởi động khi DB_AUTO_MIGRATE=true.
package metadb
