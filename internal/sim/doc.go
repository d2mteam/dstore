// Package sim mô phỏng một cụm hàng nghìn node để trả lời hai câu hỏi mà demo
// 8 node không trả lời được: chọn k/m bao nhiêu, và có nên repair ngay khi mất
// shard đầu tiên không.
//
// # Mô hình
//
// Mô phỏng theo bước thời gian (mặc định 1 giờ). Mỗi bước:
//
//   - Node hỏng theo phân phối mũ với trung bình Config.NodeMTTFHours. Phần lớn
//     lần hỏng chỉ là tạm nghỉ (restart, mất mạng): node quay lại cùng dữ liệu.
//     Phần còn lại là chết hẳn: dữ liệu mất, node được thay bằng node rỗng.
//   - Shard được coi là cần sửa khi đã vắng mặt quá Config.RepairDelayHours và
//     segment thiếu ít nhất Config.RepairThreshold shard.
//   - Coordinator sửa segment yếu nhất trước, trong giới hạn
//     Config.RepairShardsPerHour. Sửa một segment tốn băng thông đọc k shard
//     cộng ghi số shard được tái tạo.
//   - Segment còn dưới k shard có dữ liệu là mất vĩnh viễn.
//
// [Result] đếm segment mất, thời gian segment không đọc được, băng thông repair,
// và số lần repair thừa — sửa xong thì node cũ lại quay về với nguyên dữ liệu.
// Cùng Config.Seed thì cùng kết quả, nên so sánh giữa các cấu hình là công bằng.
//
// Đây là mô hình để so sánh các cấu hình với nhau, không phải dự báo tuyệt đối:
// nó giả định các node hỏng độc lập và bỏ qua hỏng theo cụm (mất điện cả rack,
// lỗi firmware hàng loạt). cmd/simulator chạy các phép so sánh; kết quả nằm ở
// mục 12 của README.
//
// Package chỉ có một file, sim.go.
package sim
