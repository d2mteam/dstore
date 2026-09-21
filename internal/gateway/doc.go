// Package gateway là API HTTP công khai của dstore: ứng dụng bên ngoài PUT/GET
// file bằng HTTP thường, không cần biết erasure coding, không thấy coordinator
// hay node.
//
// Khác với dsctl — chạy trong mạng nội bộ và tự encode/decode — gateway đứng
// giữa: nhận dữ liệu thô, tự chia shard, tự ghép lại khi đọc, thông qua
// [Storage] (do *client.Client cài đặt). Đổi lại, dữ liệu phải đi qua gateway,
// nên gateway stream (mỗi lúc chỉ giữ một segment) và phải chạy nhiều bản vì nó
// là điểm nghẽn băng thông. Gateway không giữ trạng thái riêng: mọi bản dùng
// chung bảng chủ sở hữu file trong PostgreSQL.
//
// # API
//
//	PUT    /v1/files/{name}   upload, bắt buộc Content-Length; trả {file_id, name, size}
//	GET    /v1/files/{id}     tải file, hỗ trợ Range (206 + Content-Range)
//	HEAD   /v1/files/{id}     kích thước và Content-Type, không đọc shard
//	GET    /v1/files          danh sách file của tenant
//	DELETE /v1/files/{id}     xóa file
//	GET    /healthz           health check, không cần API key
//
// # Bảo vệ
//
//   - Xác thực: header "Authorization: Bearer <key>"; [Keys] ánh xạ key sang
//     tenant, so sánh bằng hàm thời gian hằng.
//   - Cách ly tenant: file của tenant khác trả 404 chứ không phải 403, để không
//     lộ file_id nào đang tồn tại.
//   - Chống quá tải: thiếu Content-Length trả 411; file lớn hơn MaxUploadBytes bị
//     từ chối ngay từ header (413); quá MaxConcurrent request dữ liệu cùng lúc
//     thì trả 503 kèm Retry-After.
//   - Tên file chứa "/" hay "\", hoặc là "." / "..", bị từ chối.
//
// # Các file
//
//   - gateway.go: Server, Storage, các handler và phân tích header Range.
//   - auth.go: Keys, ParseKeys, đọc Bearer token.
//   - registry.go: Owners — bảng chủ sở hữu file, bản PostgreSQL và bản RAM.
//   - schema.sql: bảng file_owners, nhúng vào [OwnersSchema].
package gateway
