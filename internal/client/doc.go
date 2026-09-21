// Package client là phía client của dstore: tự erasure-code file rồi trao đổi
// shard thẳng với node, chỉ hỏi coordinator phần metadata. dsctl dùng nó trong
// mạng nội bộ; gateway dùng nó để phục vụ ứng dụng bên ngoài.
//
// # Upload
//
//  1. POST /files/plan: coordinator chọn k+m node cho từng segment.
//  2. Đọc lần lượt từng segment — mỗi lúc chỉ giữ một segment trong RAM, nên
//     stream được file lớn hơn bộ nhớ.
//  3. Encode segment thành k+m shard rồi PUT song song lên các node đã chọn,
//     tối đa Client.Parallel request cùng lúc. Node nào lỗi thì xin node thay
//     thế qua POST /nodes/pick.
//  4. POST /files/{id}/commit kèm hash và vị trí từng shard. Chỉ sau bước này
//     file mới đọc được.
//
// # Download
//
// Lấy manifest, rồi với mỗi segment gửi request tới mọi shard khỏe và hủy phần
// còn lại ngay khi đủ k shard — một node chậm hay chết không làm chậm cả lượt
// đọc. [Client.WriteRange] chỉ tải những segment chứa đoạn byte cần đọc, nên đọc
// một đoạn giữa file lớn (header Range ở gateway) không phải tải cả file.
//
// # Các file
//
//   - client.go: Client, New và các lời gọi metadata (Nodes, Files, Manifest, Delete).
//   - upload.go: Upload, UploadStream, xin node thay thế khi node lỗi.
//   - download.go: Download, DownloadTo, WriteRange.
//   - e2e_test.go: test đầu-cuối trên cụm dựng bằng package testcluster.
package client
