package coordinator_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"dstore-demo/internal/coordinator"
	"dstore-demo/internal/ec"
	"dstore-demo/internal/node"
	"dstore-demo/internal/protocol"
	"dstore-demo/internal/shardclient"
)

// gcFixture dựng một coordinator và một node thật (httptest) để test GC.
type gcFixture struct {
	t     *testing.T
	store *coordinator.Store
	gc    *coordinator.Collector
	disk  *node.DiskStore
	addr  string
}

func newGCFixture(t *testing.T, grace time.Duration) *gcFixture {
	t.Helper()
	log := slog.New(slog.DiscardHandler)

	disk, err := node.NewDiskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer((&node.Server{ID: "node-1", Store: disk, Log: log}).Routes())
	t.Cleanup(srv.Close)

	store := coordinator.NewStore()
	store.RegisterNode("node-1", srv.URL, 1<<30)

	return &gcFixture{
		t:     t,
		store: store,
		disk:  disk,
		addr:  srv.URL,
		gc: &coordinator.Collector{
			Store: store, Shards: shardclient.New(5 * time.Second),
			Interval: time.Hour, Grace: grace, Log: log,
		},
	}
}

// put ghi một shard lên node và trả về hash.
func (f *gcFixture) put(content string) string {
	f.t.Helper()
	hash := ec.Hash([]byte(content))
	if err := f.disk.Put(hash, bytes.NewReader([]byte(content))); err != nil {
		f.t.Fatal(err)
	}
	return hash
}

// commit tạo metadata nói node-1 đang giữ các hash này.
func (f *gcFixture) commit(fileID string, hashes ...string) {
	f.t.Helper()
	f.store.CreateFile(fileID, fileID+".bin", 100, 1, 0, 100, 1)
	shards := make([]protocol.CommitShard, 0, len(hashes))
	for i, h := range hashes {
		shards = append(shards, protocol.CommitShard{Index: i, Hash: h, NodeID: "node-1"})
	}
	err := f.store.CommitFile(fileID, []protocol.CommitSegment{
		{Index: 0, OrigSize: 100, Shards: shards},
	})
	if err != nil {
		f.t.Fatal(err)
	}
}

// Shard metadata không còn tham chiếu thì bị xóa, shard đang dùng thì giữ nguyên.
func TestCollectorRemovesOrphanKeepsReferenced(t *testing.T) {
	f := newGCFixture(t, 20*time.Millisecond)
	kept := f.put("shard đang được dùng")
	orphan := f.put("shard mồ côi sau repair")
	f.commit("file-1", kept)

	ctx := context.Background()

	// Lượt đầu chỉ ghi nhận nghi ngờ, chưa xóa gì.
	if n := f.gc.RunOnce(ctx); n != 0 {
		t.Fatalf("lượt quét đầu đã xóa %d shard, lẽ ra phải chờ hết thời gian ân hạn", n)
	}
	if !f.disk.Has(orphan) {
		t.Fatal("shard bị xóa ngay trong lượt quét đầu")
	}

	time.Sleep(40 * time.Millisecond)
	if n := f.gc.RunOnce(ctx); n != 1 {
		t.Fatalf("xóa %d shard, mong đợi 1", n)
	}
	if f.disk.Has(orphan) {
		t.Fatal("shard mồ côi vẫn còn trên đĩa")
	}
	if !f.disk.Has(kept) {
		t.Fatal("shard đang được tham chiếu bị xóa nhầm")
	}
}

// Shard vừa PUT lên nhưng chưa commit không được xóa: đây chính là shard đang upload.
func TestCollectorSpareShardCommittedDuringGrace(t *testing.T) {
	f := newGCFixture(t, 50*time.Millisecond)
	uploading := f.put("shard đang upload, chưa commit")

	ctx := context.Background()
	f.gc.RunOnce(ctx) // client mới PUT xong, coordinator chưa biết hash

	// Client commit trong thời gian ân hạn.
	f.commit("file-1", uploading)

	time.Sleep(70 * time.Millisecond)
	if n := f.gc.RunOnce(ctx); n != 0 {
		t.Fatalf("xóa %d shard vừa được commit", n)
	}
	if !f.disk.Has(uploading) {
		t.Fatal("shard vừa commit bị xóa mất")
	}
}

// Sau khi repair chuyển shard sang node khác, bản cũ trở thành mồ côi và bị dọn.
func TestCollectorCleansUpAfterRepair(t *testing.T) {
	f := newGCFixture(t, 10*time.Millisecond)
	old := f.put("bản shard cũ")
	f.commit("file-1", old)

	// Repair tái tạo shard 0 sang node khác.
	if err := f.store.ReplaceShard("file-1", 0, 0, "aa"+old[2:], "node-2"); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	f.gc.RunOnce(ctx)
	time.Sleep(20 * time.Millisecond)
	if n := f.gc.RunOnce(ctx); n != 1 {
		t.Fatalf("xóa %d shard, mong đợi dọn 1 bản cũ sau repair", n)
	}
	if f.disk.Has(old) {
		t.Fatal("bản shard cũ vẫn chiếm đĩa sau repair")
	}
}

// Node offline thì bỏ qua, không được coi shard của nó là mồ côi.
func TestCollectorSkipsOfflineNodes(t *testing.T) {
	f := newGCFixture(t, 0)
	orphan := f.put("shard trên node sắp offline")
	f.store.MarkOffline("node-1")

	if n := f.gc.RunOnce(context.Background()); n != 0 {
		t.Fatalf("đã xóa %d shard của node offline", n)
	}
	if !f.disk.Has(orphan) {
		t.Fatal("shard trên node offline bị xóa")
	}
}
