package client_test

// Test end-to-end: dựng coordinator + nhiều node ngay trong tiến trình test,
// rồi chạy đúng luồng upload / download / repair như khi chạy bằng Docker.

import (
	"bytes"
	"context"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"dstore-demo/internal/client"
	"dstore-demo/internal/protocol"
	"dstore-demo/internal/testcluster"
)

const (
	testK           = 4
	testM           = 2
	testSegmentSize = 256 << 10 // 256 KB cho test chạy nhanh
)

func startCluster(t *testing.T, nodes int) *testcluster.Cluster {
	return testcluster.Start(t, nodes)
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
	dsctl := c.Client(testK, testM, testSegmentSize)
	path, content := writeTempFile(t, 5<<20) // 5 MB -> 20 segment 256 KB

	ctx := context.Background()
	fileID, err := dsctl.Upload(ctx, path)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}

	man, _ := c.Store.Manifest(fileID)
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
	dsctl := c.Client(testK, testM, testSegmentSize)
	path, content := writeTempFile(t, 2<<20)

	fileID, err := dsctl.Upload(context.Background(), path)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}

	c.StopNode("node-2")
	c.StopNode("node-5")
	c.WaitNodeStatus("node-2", protocol.NodeOffline)
	c.WaitNodeStatus("node-5", protocol.NodeOffline)

	mustDownload(t, dsctl, fileID, content)
}

// Giai đoạn 6: sau khi repair, tắt tiếp 2 node khác vẫn đọc được,
// vì độ dư thừa đã được khôi phục về k+m.
func TestRepairRestoresRedundancy(t *testing.T) {
	c := startCluster(t, 8)
	dsctl := c.Client(testK, testM, testSegmentSize)
	path, content := writeTempFile(t, 1<<20)

	fileID, err := dsctl.Upload(context.Background(), path)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}

	c.StopNode("node-1")
	c.StopNode("node-3")
	c.WaitNodeStatus("node-1", protocol.NodeOffline)
	c.WaitNodeStatus("node-3", protocol.NodeOffline)

	degraded := false
	for _, n := range c.HealthyShards(fileID) {
		if n < testK+testM {
			degraded = true
		}
	}
	if !degraded {
		t.Fatal("tắt 2 node mà không segment nào bị thiếu shard")
	}

	c.Repairer.RunOnce(context.Background())

	for i, n := range c.HealthyShards(fileID) {
		if n != testK+testM {
			t.Fatalf("sau repair segment %d còn %d shard khỏe, mong đợi %d", i, n, testK+testM)
		}
	}

	// Mất thêm 2 node nữa vẫn đọc được vì repair đã bù đủ shard.
	c.StopNode("node-2")
	c.StopNode("node-4")
	c.WaitNodeStatus("node-2", protocol.NodeOffline)
	c.WaitNodeStatus("node-4", protocol.NodeOffline)

	mustDownload(t, dsctl, fileID, content)
}

// Mất quá m node trước khi repair kịp chạy thì segment không đọc được:
// minh họa giới hạn chịu lỗi của m.
func TestDownloadFailsBeyondM(t *testing.T) {
	c := startCluster(t, 8)
	dsctl := c.Client(testK, testM, testSegmentSize)
	path, _ := writeTempFile(t, 1<<20)

	fileID, err := dsctl.Upload(context.Background(), path)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}

	// 8 node, mỗi segment dùng 6 node -> tắt 5 node chắc chắn mất > m shard.
	for _, id := range []string{"node-1", "node-2", "node-3", "node-4", "node-5"} {
		c.StopNode(id)
		c.WaitNodeStatus(id, protocol.NodeOffline)
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
	dsctl := c.Client(testK, testM, testSegmentSize)
	path, content := writeTempFile(t, 1<<20)

	fileID, err := dsctl.Upload(context.Background(), path)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	before := c.HealthyShards(fileID)

	c.StopNode("node-6")
	c.WaitNodeStatus("node-6", protocol.NodeOffline)

	c.RestartNode("node-6")
	c.WaitNodeStatus("node-6", protocol.NodeOnline)

	// Reconcile chạy ở nền sau khi node đăng ký lại.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if equalCounts(c.HealthyShards(fileID), before) {
			mustDownload(t, dsctl, fileID, content)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("shard của node quay lại không được khôi phục: %v, mong đợi %v",
		c.HealthyShards(fileID), before)
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
