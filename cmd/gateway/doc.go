// Command gateway chạy API HTTP công khai của dstore (package gateway): ứng
// dụng bên ngoài PUT/GET file bằng HTTP thường, xác thực bằng API key.
//
// Mọi cấu hình đọc từ biến môi trường:
//
//	LISTEN_ADDR       :8000                  địa chỉ nghe HTTP
//	COORDINATOR_URL   http://localhost:9000
//	API_KEYS          demo-key:demo          danh sách key:tenant, cách nhau bằng dấu phẩy
//	DATABASE_URL      (trống)                PostgreSQL lưu chủ sở hữu file; trống = RAM, chỉ hợp khi chạy một bản
//	DB_IAM_AUTH       false                  đăng nhập RDS bằng IAM
//	DB_AUTO_MIGRATE   true                   tạo bảng file_owners lúc khởi động
//	EC_K / EC_M       4 / 2                  tham số erasure coding khi ghi file
//	SEGMENT_SIZE      4194304                kích thước segment (byte)
//	SHARD_PARALLEL    8                      số request shard song song cho mỗi upload/download
//	MAX_UPLOAD_BYTES  536870912              file lớn hơn bị từ chối ngay từ header
//	MAX_CONCURRENT    32                     số request dữ liệu cùng lúc, vượt thì trả 503
//
// Gateway không giữ trạng thái riêng nên chạy được nhiều bản sau load balancer,
// miễn là chúng dùng chung DATABASE_URL.
package main
