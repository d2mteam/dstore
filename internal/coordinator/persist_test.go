package coordinator_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"dstore-demo/internal/coordinator"
	"dstore-demo/internal/protocol"
)

// commitTwoSegments tạo một file 2 segment, mỗi segment 3 shard trên node-1..3.
func commitTwoSegments(t *testing.T, store *coordinator.Store, fileID string) {
	t.Helper()
	ctx := context.Background()
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		if _, err := store.RegisterNode(ctx, id, "http://"+id+":8080", 1000); err != nil {
			t.Fatal(err)
		}
	}
	store.CreateFile(fileID, fileID+".bin", 500, 2, 1, 300, 2)
	var segs []protocol.CommitSegment
	for seg := range 2 {
		cs := protocol.CommitSegment{Index: seg, OrigSize: 300 - seg*100}
		for i, node := range []string{"node-1", "node-2", "node-3"} {
			cs.Shards = append(cs.Shards, protocol.CommitShard{
				Index: i, Hash: strings.Repeat(string(rune('a'+seg*3+i)), 64), NodeID: node,
			})
		}
		segs = append(segs, cs)
	}
	if err := store.CommitFile(ctx, fileID, segs); err != nil {
		t.Fatal(err)
	}
}

// Coordinator restart thì khôi phục được đúng file, segment, shard và vị trí
// shard sau repair — những thứ mất là mất dữ liệu.
func TestStoreSurvivesRestart(t *testing.T) {
	meta := coordinator.NewMemoryMeta()
	before := coordinator.NewStore(meta)
	commitTwoSegments(t, before, "f1")

	// Repair chuyển shard 1 của segment 0 sang node-4.
	newHash := strings.Repeat("z", 64)
	if err := before.ReplaceShard(context.Background(), "f1", 0, 1, newHash, "node-4"); err != nil {
		t.Fatal(err)
	}
	want, _ := before.Manifest("f1")

	after := coordinator.NewStore(meta) // coordinator mới, cùng DB
	nodes, files, err := after.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if nodes != 3 || files != 1 {
		t.Fatalf("khôi phục %d node, %d file; mong đợi 3 node, 1 file", nodes, files)
	}

	got, ok := after.Manifest("f1")
	if !ok {
		t.Fatal("mất file sau restart")
	}
	if got.Size != want.Size || got.K != want.K || got.M != want.M || len(got.Segments) != 2 {
		t.Fatalf("metadata file khác sau restart:\n%+v\n%+v", got, want)
	}
	for i := range want.Segments {
		if got.Segments[i].OrigSize != want.Segments[i].OrigSize {
			t.Fatalf("segment %d: orig_size %d, mong đợi %d", i, got.Segments[i].OrigSize, want.Segments[i].OrigSize)
		}
		for j := range want.Segments[i].Shards {
			g, w := got.Segments[i].Shards[j], want.Segments[i].Shards[j]
			if g.Index != w.Index || g.Hash != w.Hash || g.NodeID != w.NodeID {
				t.Fatalf("segment %d shard %d: %+v, mong đợi %+v", i, j, g, w)
			}
		}
	}
	if got.Segments[0].Shards[1].NodeID != "node-4" {
		t.Fatal("vị trí mới của shard sau repair không được lưu")
	}
}

// Node chết trong lúc coordinator tắt: sau khi khôi phục, nó không heartbeat
// nên phải bị chuyển offline để repair còn chạy.
func TestNodeGoneWhileCoordinatorDown(t *testing.T) {
	meta := coordinator.NewMemoryMeta()
	commitTwoSegments(t, coordinator.NewStore(meta), "f1")

	after := coordinator.NewStore(meta)
	if _, _, err := after.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	// node-1 và node-3 heartbeat lại; node-2 thì không bao giờ quay lại.
	time.Sleep(30 * time.Millisecond)
	after.Heartbeat("node-1", 0, 0)
	after.Heartbeat("node-3", 0, 0)

	expired := after.ExpiredNodes(20 * time.Millisecond)
	if len(expired) != 1 || expired[0] != "node-2" {
		t.Fatalf("node quá hạn = %v, mong đợi [node-2]", expired)
	}
	if lost := after.MarkOffline("node-2"); lost != 2 {
		t.Fatalf("mất %d shard, mong đợi 2 (mỗi segment một shard trên node-2)", lost)
	}
}

// brokenMeta mô phỏng DB chết: đọc được nhưng không ghi được.
type brokenMeta struct{ *coordinator.MemoryMeta }

var errDBDown = errors.New("mất kết nối tới DB")

func (brokenMeta) SaveFile(context.Context, coordinator.FileRecord) error { return errDBDown }
func (brokenMeta) DeleteFile(context.Context, string) error               { return errDBDown }
func (brokenMeta) SaveSettings(context.Context, []byte) error             { return errDBDown }
func (brokenMeta) Ping(context.Context) error                             { return errDBDown }

// DB chết thì commit phải thất bại: client không được nhận "thành công" cho một
// file mà coordinator restart là quên.
func TestCommitFailsWhenDatabaseDown(t *testing.T) {
	store := coordinator.NewStore(brokenMeta{coordinator.NewMemoryMeta()})
	srv := &coordinator.Server{Store: store, Log: slog.New(slog.DiscardHandler)}
	store.RegisterNode(context.Background(), "node-1", "http://node-1:8080", 1000)
	store.CreateFile("f1", "f1.bin", 100, 1, 1, 100, 1)

	body := `{"segments":[{"index":0,"orig_size":100,"shards":[{"index":0,"hash":"` +
		strings.Repeat("a", 64) + `","node_id":"node-1"}]}]}`
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/files/f1/commit", strings.NewReader(body)))

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("commit khi DB chết trả %d, mong đợi 503", rec.Code)
	}
	if _, ok := store.Manifest("f1"); ok {
		t.Fatal("file vẫn đọc được dù chưa lưu được xuống DB")
	}
}
