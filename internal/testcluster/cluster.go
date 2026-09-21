package testcluster

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"dstore-demo/internal/client"
	"dstore-demo/internal/coordinator"
	"dstore-demo/internal/node"
	"dstore-demo/internal/protocol"
	"dstore-demo/internal/shardclient"
)

// Nhịp tim rút ngắn để test phát hiện node chết trong vài trăm mili giây.
const (
	HeartbeatEvery = 100 * time.Millisecond
	HeartbeatDead  = 500 * time.Millisecond
)

type testNode struct {
	id      string
	dir     string
	srv     *httptest.Server
	store   *node.DiskStore
	stopHB  context.CancelFunc
	stopped bool
}

// Cluster là một cụm đang chạy; mọi thứ tự dọn khi test kết thúc.
type Cluster struct {
	URL      string                // địa chỉ coordinator
	Store    *coordinator.Store    // metadata, để test soi trạng thái
	Repairer *coordinator.Repairer // test tự gọi RunOnce khi muốn repair

	t     *testing.T
	nodes map[string]*testNode
	log   *slog.Logger
}

// Start dựng coordinator và nodeCount node.
func Start(t *testing.T, nodeCount int) *Cluster {
	t.Helper()
	log := slog.New(slog.DiscardHandler)

	meta := coordinator.NewMemoryMeta()
	store := coordinator.NewStore(meta)
	settings := coordinator.NewSettingsStore(meta, coordinator.Settings{
		HeartbeatTimeout: protocol.Duration(HeartbeatDead),
		GCGrace:          protocol.Duration(time.Hour),
	})
	placer := coordinator.NewPlacer(store)
	shards := shardclient.New(5 * time.Second)
	health := &coordinator.HealthMonitor{
		Store: store, Shards: shards, Settings: settings,
		Interval: HeartbeatEvery, Log: log,
	}
	api := &coordinator.Server{Store: store, Placer: placer, Health: health, Shards: shards, Log: log}

	srv := httptest.NewServer(api.Routes())
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go health.Run(ctx)

	c := &Cluster{
		URL:   srv.URL,
		Store: store,
		Repairer: &coordinator.Repairer{
			Store: store, Placer: placer, Shards: shards, Settings: settings,
			Interval: time.Hour, Log: log,
		},
		t:     t,
		nodes: make(map[string]*testNode),
		log:   log,
	}
	for i := 1; i <= nodeCount; i++ {
		c.startNode(fmt.Sprintf("node-%d", i), t.TempDir())
	}
	return c
}

// Client tạo một client trỏ vào cụm này.
func (c *Cluster) Client(k, m, segmentSize int) *client.Client {
	return client.New(c.URL, k, m, segmentSize, 8, c.log)
}

func (c *Cluster) startNode(id, dir string) {
	c.t.Helper()
	store, err := node.NewDiskStore(dir)
	if err != nil {
		c.t.Fatal(err)
	}
	srv := httptest.NewServer((&node.Server{ID: id, Store: store, Log: c.log}).Routes())

	ctx, cancel := context.WithCancel(context.Background())
	reporter := node.NewReporter(id, node.Settings{
		CoordinatorURL: c.URL, Addr: srv.URL, Capacity: 1 << 30,
		HeartbeatInterval: protocol.Duration(HeartbeatEvery),
	}, store, c.log)
	go reporter.Run(ctx)

	n := &testNode{id: id, dir: dir, srv: srv, store: store, stopHB: cancel}
	c.nodes[id] = n
	c.t.Cleanup(func() {
		if !n.stopped {
			cancel()
			srv.Close()
		}
	})
	c.waitRegistered(id, srv.URL)
}

// waitRegistered chờ coordinator nhận đăng ký của node ở đúng địa chỉ mới.
// Chỉ chờ "online" là chưa đủ: node vừa restart có thể vẫn đang online với
// địa chỉ cũ vì coordinator chưa kịp phát hiện nó đã tắt.
func (c *Cluster) waitRegistered(id, addr string) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n, ok := c.Store.Node(id); ok && n.Addr == addr && n.Status == protocol.NodeOnline {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("node %s không đăng ký được với coordinator trong 5s", id)
}

// StopNode mô phỏng `docker stop node-x`: node ngừng phục vụ và ngừng heartbeat,
// dữ liệu trên đĩa vẫn còn để có thể bật lại.
func (c *Cluster) StopNode(id string) {
	c.t.Helper()
	n, ok := c.nodes[id]
	if !ok || n.stopped {
		c.t.Fatalf("node %s không chạy", id)
	}
	n.stopHB()
	n.srv.Close()
	n.stopped = true
}

// RestartNode bật lại node với đúng thư mục dữ liệu cũ.
func (c *Cluster) RestartNode(id string) {
	c.t.Helper()
	n := c.nodes[id]
	if !n.stopped {
		c.t.Fatalf("node %s đang chạy", id)
	}
	c.startNode(id, n.dir)
}

// WaitNodeStatus chờ coordinator cập nhật trạng thái node.
func (c *Cluster) WaitNodeStatus(id, status string) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.Store.Nodes() {
			if n.ID == id && n.Status == status {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("node %s không chuyển sang %s trong 5s", id, status)
}

// HealthyShards đếm shard còn khỏe của từng segment trong file.
func (c *Cluster) HealthyShards(fileID string) []int {
	c.t.Helper()
	man, ok := c.Store.Manifest(fileID)
	if !ok {
		c.t.Fatalf("không có manifest cho %s", fileID)
	}
	counts := make([]int, len(man.Segments))
	for i, seg := range man.Segments {
		for _, sh := range seg.Shards {
			if sh.Status == protocol.ShardOK {
				counts[i]++
			}
		}
	}
	return counts
}
