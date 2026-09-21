// Package testcluster dựng một cụm dstore — coordinator cùng nhiều node — ngay
// trong tiến trình test.
//
// Test đi qua đúng các luồng HTTP thật mà không cần Docker, PostgreSQL hay
// MinIO: metadata nằm trong RAM ([coordinator.MemoryMeta]), shard nằm trong thư
// mục tạm của test.
//
// # Cách dùng
//
//	c := testcluster.Start(t, 8)       // coordinator + 8 node, tự dọn khi test xong
//	cl := c.Client(4, 2, 64<<10)       // client k=4, m=2, segment 64 KB
//	id, _ := cl.Upload(ctx, path)
//	c.StopNode("node-2")               // như `docker stop`: dữ liệu còn trên đĩa
//	c.WaitNodeStatus("node-2", protocol.NodeOffline)
//	c.Repairer.RunOnce(ctx)            // repair chạy khi test muốn, không theo ticker
//	c.RestartNode("node-2")            // bật lại đúng thư mục dữ liệu cũ
//
// Nhịp heartbeat rút xuống [HeartbeatEvery] và timeout xuống [HeartbeatDead]
// để test phát hiện node chết trong vài trăm mili giây.
//
// Package chỉ có một file, cluster.go.
package testcluster
