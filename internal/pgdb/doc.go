// Package pgdb mở pool kết nối PostgreSQL dùng chung cho coordinator và
// gateway, chạy được với Postgres tự dựng lẫn Amazon RDS / Aurora PostgreSQL.
//
// # Cấu hình cho RDS
//
//   - TLS: đặt trong DATABASE_URL, ví dụ
//     ?sslmode=verify-full&sslrootcert=/etc/ssl/rds-global-bundle.pem
//   - Đăng nhập bằng IAM thay cho mật khẩu tĩnh: bật Config.IAMAuth. Token chỉ
//     sống 15 phút nên được ký mới trước mỗi lần mở kết nối. Bật IAM mà thiếu
//     TLS hoặc AWS_REGION thì [Open] báo lỗi ngay, thay vì để RDS trả lỗi khó hiểu.
//   - Sau RDS Proxy: thêm default_query_exec_mode=exec vào URL để tránh bị
//     ghim phiên vì prepared statement.
//
// [Open] còn ping thử, rồi chạy Config.Schema (các câu CREATE TABLE IF NOT
// EXISTS) nếu có, để service tự tạo bảng khi khởi động.
//
// # Các file
//
//   - pgdb.go: Config, Open và hàm ký token IAM.
//   - pgtest/: mở DB cho test trong một schema riêng (package pgtest).
package pgdb
