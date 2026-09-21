// Package ec bọc thư viện github.com/klauspost/reedsolomon cho đúng nhu cầu
// của dstore: mã hóa một segment thành k+m shard, và dựng lại segment (hoặc các
// shard bị mất) từ bất kỳ k shard nào còn lại.
//
// # Cách dùng
//
//	coder, _ := ec.New(4, 2)            // 4 data shard + 2 parity shard
//	shards, _ := coder.Encode(segment)  // len(shards) == 6, shards[i] có shard_index i
//	shards[1], shards[4] = nil, nil     // mất tối đa m = 2 shard
//	data, _ := coder.Decode(shards, len(segment))
//
// Repair chỉ cần [Coder.Reconstruct] để điền lại shard thiếu, không cần ghép
// thành segment. [Hash] là SHA-256 dạng hex, dùng làm ID nội dung của shard ở
// mọi nơi: client, node, coordinator và cả checksum phía S3.
//
// # Giới hạn
//
// Mỗi lần gọi làm việc trên cả segment trong RAM; dstore dùng segment 4 MB nên
// không thành vấn đề. Các shard dài bằng nhau vì segment được đệm cho chia hết
// cho k, nên bên gọi phải nhớ kích thước gốc để [Coder.Decode] cắt phần đệm.
//
// Package chỉ có một file, ec.go.
package ec
