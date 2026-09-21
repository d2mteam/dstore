// Package webui phục vụ giao diện web quản trị của coordinator và node.
//
// Trang là HTML + JavaScript thuần nhúng thẳng vào binary bằng go:embed: không
// có bước build frontend, không tải gì từ internet. Trang gọi API JSON dưới
// /ui/api/ do từng service tự cung cấp (coordinator.Admin, node.Admin), và đọc
// thẳng API sẵn có của service cho những gì đã có (danh sách node, file, shard).
//
// # Gắn vào service
//
//	mux.Handle("/", api.Routes())                                   // API sẵn có giữ nguyên
//	mux.Handle("/ui/", webui.Handler(webui.NodePage, admin.Routes(), password))
//
// [Handler] đặt trang, file tĩnh và API riêng của trang chung dưới /ui/, nên chỉ
// cần một lớp bảo vệ, và trình duyệt gửi kèm thông tin đăng nhập cho cả trang
// lẫn API. [Protect] làm hai việc: bắt đăng nhập HTTP Basic (tên [User], mật khẩu
// từ UI_PASSWORD) khi có mật khẩu, và chặn request ghi đến từ trang web khác
// (CSRF) bằng http.CrossOriginProtection.
//
// # Các file
//
//   - webui.go: Handler, Protect, danh sách trang.
//   - assets/common.js: gọi API, định dạng số liệu, dựng HTML bằng html`...` tự
//     escape mọi giá trị chèn vào, tự làm mới định kỳ.
//   - assets/style.css: giao diện dùng chung, sáng/tối theo hệ thống.
//   - assets/coordinator.html, assets/coordinator.js: trang control plane.
//   - assets/node.html, assets/node.js: trang của một node.
package webui
