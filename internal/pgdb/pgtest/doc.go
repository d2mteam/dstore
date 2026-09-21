// Package pgtest mở PostgreSQL cho test. Mỗi lần gọi [Open] tạo một schema
// riêng tên test_<ngẫu nhiên>, trỏ search_path vào đó, tạo bảng, và xóa cả
// schema khi test xong. Nhờ vậy các test không giẫm lên nhau và chạy được cả
// trên DB đang có dữ liệu.
//
// Chưa đặt DSTORE_TEST_DATABASE_URL thì test tự bỏ qua; `make test-integration`
// bật sẵn một container PostgreSQL và đặt biến này.
//
// Package chỉ có một file, pgtest.go.
package pgtest
