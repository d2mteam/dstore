// Command simulator chạy package sim: mô phỏng cụm hàng nghìn node để chọn k, m
// và ngưỡng repair.
//
//	simulator                     # chạy một cấu hình, in kết quả
//	simulator -compare km         # so sánh các cặp k/m
//	simulator -compare threshold  # so sánh ngưỡng repair và độ trễ repair
//	simulator -compare bandwidth  # so sánh năng lực repair của cụm
//
// Các phép so sánh quy về cùng lượng dữ liệu gốc (-data-gb, mặc định 200 GB),
// nếu không thì cấu hình k lớn sẽ lưu nhiều dữ liệu hơn và so băng thông là sai.
// Các cờ còn lại (-nodes, -k, -m, -years, -mttf, -transient, -repair-delay,
// -threshold, -repair-rate, -seed, ...) đặt các trường của sim.Config; xem
// `simulator -h`.
package main
