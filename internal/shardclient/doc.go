// Package shardclient là client HTTP cho API shard của node:
// PUT/GET/DELETE /shards/{hash} và GET /shards.
//
// Hai nơi dùng chung package này: client (dsctl, gateway) khi upload/download,
// và coordinator khi repair (tải k shard khỏe, đặt shard tái tạo) và dọn rác
// (liệt kê, xóa shard mồ côi).
//
// [Client.Get] kiểm lại SHA-256 của dữ liệu nhận được: node trả dữ liệu hỏng
// thì coi như không có shard, để bên gọi chuyển sang shard khác thay vì giải mã
// ra dữ liệu sai. [Client.Delete] coi 404 là thành công, vì mục tiêu "shard
// không còn trên node" đã đạt.
//
// Package chỉ có một file, shardclient.go.
package shardclient
