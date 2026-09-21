// Command dsctl là CLI vận hành của dstore (package client). Nó tự
// erasure-code file và nói chuyện thẳng với node, nên phải chạy trong mạng nhìn
// thấy node — trong docker compose là `docker compose run --rm dsctl ...`.
//
//	dsctl [tùy chọn] put <file>             upload, in ra file_id
//	dsctl [tùy chọn] get <file_id> <file>   tải file về
//	dsctl [tùy chọn] ls                     danh sách file
//	dsctl [tùy chọn] nodes                  danh sách node và trạng thái
//	dsctl [tùy chọn] rm <file_id>           xóa file
//	dsctl hash <file>                       in SHA-256 để so sánh trước/sau
//
// Tùy chọn, kèm giá trị mặc định và biến môi trường thay được mặc định:
//
//	-coordinator URL   http://localhost:9000   COORDINATOR_URL
//	-k N               4                       EC_K           số data shard khi upload
//	-m N               2                       EC_M           số parity shard khi upload
//	-segment BYTES     4194304                 SEGMENT_SIZE   kích thước segment khi upload
//	-parallel N        8                                      số request shard song song
//	-v                                                        log chi tiết
package main
