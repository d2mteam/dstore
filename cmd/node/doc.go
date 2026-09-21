// Command node chạy một node data plane (package node): cất shard trên đĩa
// local hoặc Amazon S3 / MinIO, phục vụ API shard, heartbeat cho coordinator,
// và web UI cấu hình kết nối ở /ui/.
//
// Mọi cấu hình đọc từ biến môi trường:
//
//	NODE_ID              node-1                 định danh cố định; đổi là coordinator mất dấu shard
//	LISTEN_ADDR          :8080                  địa chỉ nghe HTTP
//	NODE_ADDR            http://localhost:8080  địa chỉ người khác dùng để gọi node này (*)
//	COORDINATOR_URL      http://localhost:9000  coordinator để đăng ký và heartbeat (*)
//	CAPACITY_BYTES       1073741824             dung lượng cho phép (*)
//	HEARTBEAT_INTERVAL   5s                     phải nhỏ hơn HEARTBEAT_TIMEOUT của coordinator (*)
//	CONFIG_FILE          (trống)                nơi lưu cấu hình sửa qua web UI; trống = không lưu
//	UI_PASSWORD          (trống)                mật khẩu web UI, tên đăng nhập admin
//	STORAGE              disk                   disk hoặc s3
//	DATA_DIR             ./data                 thư mục shard khi STORAGE=disk
//	S3_BUCKET            (bắt buộc với s3)      bucket dùng chung cho mọi node
//	S3_PREFIX            nodes/<NODE_ID>/       prefix riêng của node trong bucket
//	S3_ENDPOINT          (trống = Amazon S3)    ví dụ http://minio:9000
//	S3_FORCE_PATH_STYLE  false                  MinIO cần true
//	S3_CREATE_BUCKET     false                  tạo bucket nếu chưa có, tiện khi dev
//
// Khóa và region S3 theo chuẩn AWS: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
// AWS_REGION, hoặc IAM role khi chạy trên AWS.
//
// (*) Chỉ là giá trị mặc định: có CONFIG_FILE và đã sửa qua web UI thì bản
// trong file được dùng thay.
//
// Node mở nơi lưu shard trước (với S3 thì thử lại tới khi kết nối được), rồi
// nghe HTTP, rồi mới đăng ký với coordinator ở nền: coordinator chưa lên thì node
// vẫn phục vụ shard và tự thử lại mỗi giây.
package main
