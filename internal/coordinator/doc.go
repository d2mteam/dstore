// Package coordinator là control plane của dstore: biết có những node nào,
// file gồm những segment nào, shard nào nằm trên node nào; chọn node khi
// upload; phát hiện node chết; tái tạo shard bị mất; dọn shard mồ côi. Dữ liệu
// file không đi qua coordinator, trừ lúc repair.
//
// # Trạng thái
//
// [Store] giữ toàn bộ trạng thái trong RAM và là nơi duy nhất mà API cùng các
// vòng nền đọc/ghi. Trạng thái chia làm hai loại:
//
//   - Cứng — file, segment, vị trí shard, địa chỉ node: mất là mất dữ liệu, nên
//     được ghi qua [MetaStore] (PostgreSQL khi chạy thật, [MemoryMeta] trong test).
//   - Mềm — node sống hay chết, shard ok hay lost, dung lượng đã dùng: không lưu,
//     vì heartbeat dựng lại được trong vài giây sau khi coordinator khởi động.
//
// Ghi DB luôn diễn ra ngoài khóa. Commit và xóa file ghi DB trước rồi mới đổi
// RAM — DB lỗi thì trả [ErrStorage] và API trả 503. Repair thì ngược lại: đặt
// shard lên node mới trước rồi mới ghi DB, vì shard đã nằm an toàn ở node mới.
//
// # Vòng nền
//
//   - [HealthMonitor]: node quá HeartbeatTimeout không heartbeat thì thành
//     offline, shard của nó thành lost. Node quay lại thì
//     [HealthMonitor.Reconcile] đối chiếu shard còn trên node.
//   - [Repairer]: segment thiếu shard thì tải k shard khỏe, tái tạo phần thiếu,
//     đặt sang node khác; segment yếu nhất được sửa trước.
//   - [Collector]: shard trên node mà metadata không còn tham chiếu thì bị xóa,
//     nhưng chỉ sau khi thấy nó mồ côi liên tục suốt GCGrace, để không xóa nhầm
//     shard của file đang upload.
//
// Các tham số này nằm trong [Settings], do [SettingsStore] giữ: sửa được lúc
// đang chạy qua web UI, lưu trong MetaStore, vòng nền đọc lại mỗi lượt.
//
// # HTTP
//
// [Server] là API control plane: đăng ký node, heartbeat, lập kế hoạch upload,
// commit, manifest, xóa file, /metrics. [Admin] là API riêng của web UI dưới
// /ui/api/. [Placer] chọn node cho shard: online, còn đủ chỗ, không trùng node
// trong một segment, ưu tiên node trống nhiều.
//
// # Các file
//
//   - store.go: Store và các kiểu Node, File, Segment, Shard; mọi thao tác trên
//     metadata.
//   - persist.go: MetaStore, các bản ghi lưu bền (NodeRecord, FileRecord, ...),
//     MemoryMeta và Store.Load.
//   - placement.go: Placer.
//   - api.go: Server, API control plane.
//   - health.go: HealthMonitor.
//   - repair.go: Repairer.
//   - gc.go: Collector.
//   - settings.go: Settings, SettingsStore và cơ chế "chạy ngay" cho vòng nền.
//   - metrics.go: Metrics, Stats và định dạng Prometheus cho /metrics.
//   - admin.go: Admin, API của web UI.
//   - sort.go: các hàm sắp xếp dùng chung.
package coordinator
