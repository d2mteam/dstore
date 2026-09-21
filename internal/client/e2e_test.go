package client_test

// Test end-to-end: dựng coordinator + nhiều node ngay trong tiến trình test,
// rồi chạy đúng luồng upload / download / repair như khi chạy bằng Docker.

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dstore-demo/internal/client"
	"dstore-demo/internal/coordinator"
	"dstore-demo/internal/node"
	"dstore-demo/internal/protocol"
	"dstore-demo/internal/shardclient"
)

const (
	testK           = 4
	testM           = 2
	testSegmentSize = 256 << 10 // 256 KB cho test chạy nhanh
	heartbeatEvery  = 100 * time.Millisecond
	heartbeatDead   = 500 * time.Millisecond
)

type testNode struct {
	id      string
	dir     string
	srv     *httptest.Server
	store   *node.DiskStore
	stopHB  context.CancelFunc
	stopped bool
}

type cluster struct {
	t        *testing.T
	url      string
	store    *coordinator.Store
	repairer *coordinator.Repairer
	nodes    map[string]*testNode
	log      *slog.Logger
}

func startCluster(t *testing.T, nodeCount int) *cluster {
	t.Helper()
	log := slog.New(slog.DiscardHandler)

	store := coordinator.NewStore()
	placer := coordinator.NewPlacer(store)
	shards := shardclient.New(5 * time.Second)
	health := &coordinator.HealthMonitor{
		Store: store, Shards: shards,
		Interval: heartbeatEvery, Timeout: heartbeatDead, Log: log,
	}
	api := &coordinator.Server{Store: store, Placer: placer, Health: health, Shards: shards, Log: log}

	srv := httptest.NewServer(api.Routes())
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go health.Run(ctx)

	c := &cluster{
		t:     t,
		url:   srv.URL,
		store: store,
		repairer: &coordinator.Repairer{
			Store: store, Placer: placer, Shards: shards,
			Interval: time.Hour, Log: log, // test tự gọi RunOnce khi cần
		},
		nodes: make(map[string]*testNode),
		log:   log,
	}
	for i := 1; i <= nodeCount; i++ {
		c.startNode(nodeID(i), t.TempDir())
	}
	return c
}

func nodeID(i int) string { return fmt.Sprintf("node-%d", i) }

func (c *cluster) startNode(id, dir string) {
	c.t.Helper()
	store, err := node.NewDiskStore(dir)
	if err != nil {
		c.t.Fatal(err)
	}
	srv := httptest.NewServer((&node.Server{ID: id, Store: store, Log: c.log}).Routes())

	ctx, cancel := context.WithCancel(context.Background())
	reporter := &node.Reporter{
		CoordinatorURL: c.url, NodeID: id, Addr: srv.URL,
		Capacity: 1 << 30, Interval: heartbeatEvery, Store: store, Log: c.log,
	}
	if err := reporter.Register(ctx); err != nil {
		cancel()
		c.t.Fatal(err)
	}
	go reporter.Run(ctx)

	n := &testNode{id: id, dir: dir, srv: srv, store: store, stopHB: cancel}
	c.nodes[id] = n
	c.t.Cleanup(func() {
		if !n.stopped {
			cancel()
			srv.Close()
		}
	})
}

// stopNode mô phỏng `docker stop node-x`: node ngừng phục vụ và ngừng heartbeat,
// dữ liệu trên đĩa vẫn còn để có thể bật lại.
func (c *cluster) stopNode(id string) {
	c.t.Helper()
	n, ok := c.nodes[id]
	if !ok || n.stopped {
		c.t.Fatalf("node %s không chạy", id)
	}
	n.stopHB()
	n.srv.Close()
	n.stopped = true
}

// restartNode bật lại node với đúng thư mục dữ liệu cũ.
func (c *cluster) restartNode(id string) {
	c.t.Helper()
	n := c.nodes[id]
	if !n.stopped {
		c.t.Fatalf("node %s đang chạy", id)
	}
	c.startNode(id, n.dir)
}

// waitNodeStatus chờ coordinator cập nhật trạng thái node.
func (c *cluster) waitNodeStatus(id, status string) {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range c.store.Nodes() {
			if n.ID == id && n.Status == status {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("node %s không chuyển sang %s trong 5s", id, status)
}

func (c *cluster) client() *client.Client {
	return client.New(c.url, testK, testM, testSegmentSize, 8, c.log)
}

// countHealthyShards đếm shard ok của từng segment trong file.
func (c *cluster) countHealthyShards(fileID string) []int {
	c.t.Helper()
	man, ok := c.store.Manifest(fileID)
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

func writeTempFile(t *testing.T, size int) (path string, content []byte) {
	t.Helper()
	content = make([]byte, size)
	rand.Read(content)
	path = filepath.Join(t.TempDir(), "input.bin")
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatal(err)
	}
	return path, content
}

func mustDownload(t *testing.T, c *client.Client, fileID string, want []byte) {
	t.Helper()
	out := filepath.Join(t.TempDir(), "output.bin")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Download(ctx, fileID, out); err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("file tải về khác bản gốc (%d byte vs %d byte)", len(got), len(want))
	}
}

// Giai đoạn 4: upload rồi download phải ra đúng file ban đầu,
// và k+m shard của mỗi segment phải nằm trên k+m node khác nhau.
func TestUploadDownloadRoundTrip(t *testing.T) {
	c := startCluster(t, 8)
	dsctl := c.client()
	path, content := writeTempFile(t, 5<<20) // 5 MB -> 20 segment 256 KB

	ctx := context.Background()
	fileID, err := dsctl.Upload(ctx, path)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}

	man, _ := c.store.Manifest(fileID)
	if len(man.Segments) != 20 {
		t.Fatalf("có %d segment, mong đợi 20", len(man.Segments))
	}
	for _, seg := range man.Segments {
		if len(seg.Shards) != testK+testM {
			t.Fatalf("segment %d có %d shard, mong đợi %d", seg.Index, len(seg.Shards), testK+testM)
		}
		used := make(map[string]bool)
		for _, sh := range seg.Shards {
			if used[sh.NodeID] {
				t.Fatalf("segment %d có 2 shard trên cùng node %s", seg.Index, sh.NodeID)
			}
			used[sh.NodeID] = true
		}
	}

	mustDownload(t, dsctl, fileID, content)

	files, err := dsctl.Files(ctx)
	if err != nil || len(files) != 1 || files[0].ID != fileID {
		t.Fatalf("ls trả về %v (err=%v)", files, err)
	}
}

// Giai đoạn 5: tắt m node thì vẫn đọc được file.
func TestDownloadWithNodesDown(t *testing.T) {
	c := startCluster(t, 8)
	dsctl := c.client()
	path, content := writeTempFile(t, 2<<20)

	fileID, err := dsctl.Upload(context.Background(), path)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}

	c.stopNode("node-2")
	c.stopNode("node-5")
	c.waitNodeStatus("node-2", protocol.NodeOffline)
	c.waitNodeStatus("node-5", protocol.NodeOffline)

	mustDownload(t, dsctl, fileID, content)
}

// Giai đoạn 6: sau khi repair, tắt tiếp 2 node khác vẫn đọc được,
// vì độ dư thừa đã được khôi phục về k+m.
func TestRepairRestoresRedundancy(t *testing.T) {
	c := startCluster(t, 8)
	dsctl := c.client()
	path, content := writeTempFile(t, 1<<20)

	fileID, err := dsctl.Upload(context.Background(), path)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}

	c.stopNode("node-1")
	c.stopNode("node-3")
	c.waitNodeStatus("node-1", protocol.NodeOffline)
	c.waitNodeStatus("node-3", protocol.NodeOffline)

	degraded := false
	for _, n := range c.countHealthyShards(fileID) {
		if n < testK+testM {
			degraded = true
		}
	}
	if !degraded {
		t.Fatal("tắt 2 node mà không segment nào bị thiếu shard")
	}

	c.repairer.RunOnce(context.Background())

	for i, n := range c.countHealthyShards(fileID) {
		if n != testK+testM {
			t.Fatalf("sau repair segment %d còn %d shard khỏe, mong đợi %d", i, n, testK+testM)
		}
	}

	// Mất thêm 2 node nữa vẫn đọc được vì repair đã bù đủ shard.
	c.stopNode("node-2")
	c.stopNode("node-4")
	c.waitNodeStatus("node-2", protocol.NodeOffline)
	c.waitNodeStatus("node-4", protocol.NodeOffline)

	mustDownload(t, dsctl, fileID, content)
}

// Mất quá m node trước khi repair kịp chạy thì segment không đọc được:
// minh họa giới hạn chịu lỗi của m.
func TestDownloadFailsBeyondM(t *testing.T) {
	c := startCluster(t, 8)
	dsctl := c.client()
	path, _ := writeTempFile(t, 1<<20)

	fileID, err := dsctl.Upload(context.Background(), path)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}

	// 8 node, mỗi segment dùng 6 node -> tắt 5 node chắc chắn mất > m shard.
	for _, id := range []string{"node-1", "node-2", "node-3", "node-4", "node-5"} {
		c.stopNode(id)
		c.waitNodeStatus(id, protocol.NodeOffline)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	out := filepath.Join(t.TempDir(), "output.bin")
	if err := dsctl.Download(ctx, fileID, out); err == nil {
		t.Fatal("mong đợi download thất bại khi mất quá m shard")
	}
}

// Node sống lại thì shard còn trên đĩa của nó được đánh dấu ok trở lại.
func TestNodeReturnRestoresShards(t *testing.T) {
	c := startCluster(t, 8)
	dsctl := c.client()
	path, content := writeTempFile(t, 1<<20)

	fileID, err := dsctl.Upload(context.Background(), path)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	before := c.countHealthyShards(fileID)

	c.stopNode("node-6")
	c.waitNodeStatus("node-6", protocol.NodeOffline)

	c.restartNode("node-6")
	c.waitNodeStatus("node-6", protocol.NodeOnline)

	// Reconcile chạy ở nền sau khi node đăng ký lại.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if equalCounts(c.countHealthyShards(fileID), before) {
			mustDownload(t, dsctl, fileID, content)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("shard của node quay lại không được khôi phục: %v, mong đợi %v",
		c.countHealthyShards(fileID), before)
}

func equalCounts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
