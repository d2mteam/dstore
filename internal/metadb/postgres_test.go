package metadb

import (
	"context"
	"strings"
	"testing"
	"time"

	"dstore-demo/internal/coordinator"
	"dstore-demo/internal/pgdb/pgtest"
)

func openTestDB(t *testing.T) *Postgres {
	return New(pgtest.Open(t, Schema))
}

func sampleFile(id string) coordinator.FileRecord {
	f := coordinator.FileRecord{
		ID: id, Name: id + ".bin", Size: 5000, SegmentSize: 4096, K: 2, M: 1,
		CreatedAt: time.Now().UTC().Truncate(time.Microsecond), // Postgres lưu tới micro giây
	}
	for seg := range 2 {
		sr := coordinator.SegmentRecord{Index: seg, OrigSize: 4096 - seg*3192}
		for i := range 3 {
			sr.Shards = append(sr.Shards, coordinator.ShardRecord{
				Index: i, Hash: strings.Repeat(string(rune('a'+seg*3+i)), 64), NodeID: "node-" + string(rune('1'+i)),
			})
		}
		f.Segments = append(f.Segments, sr)
	}
	return f
}

// Ghi vào rồi đọc ra phải y nguyên, kể cả thứ tự segment và shard.
func TestSaveAndLoad(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()

	if err := db.SaveNode(ctx, coordinator.NodeRecord{ID: "node-1", Addr: "http://node-1:8080", Capacity: 100}); err != nil {
		t.Fatal(err)
	}
	// Đăng ký lại với địa chỉ mới thì cập nhật chứ không nhân đôi.
	if err := db.SaveNode(ctx, coordinator.NodeRecord{ID: "node-1", Addr: "http://10.0.0.5:8080", Capacity: 200}); err != nil {
		t.Fatal(err)
	}
	want := sampleFile("f1")
	if err := db.SaveFile(ctx, want); err != nil {
		t.Fatal(err)
	}

	nodes, files, err := db.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].Addr != "http://10.0.0.5:8080" || nodes[0].Capacity != 200 {
		t.Fatalf("nodes = %+v", nodes)
	}
	if len(files) != 1 {
		t.Fatalf("đọc được %d file, mong đợi 1", len(files))
	}
	got := files[0]
	if got.ID != want.ID || got.Size != want.Size || got.K != want.K || !got.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("file = %+v\nmong đợi %+v", got, want)
	}
	for i, seg := range want.Segments {
		if got.Segments[i].OrigSize != seg.OrigSize {
			t.Fatalf("segment %d orig_size = %d", i, got.Segments[i].OrigSize)
		}
		for j, sh := range seg.Shards {
			if got.Segments[i].Shards[j] != sh {
				t.Fatalf("segment %d shard %d = %+v, mong đợi %+v", i, j, got.Segments[i].Shards[j], sh)
			}
		}
	}
}

// Repair ghi lại vị trí shard; commit lại lần hai (client retry) thay bản cũ.
func TestUpdateShardAndRecommit(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	f := sampleFile("f1")
	if err := db.SaveFile(ctx, f); err != nil {
		t.Fatal(err)
	}

	newHash := strings.Repeat("z", 64)
	if err := db.UpdateShard(ctx, "f1", 1, 2, newHash, "node-9"); err != nil {
		t.Fatal(err)
	}
	_, files, _ := db.Load(ctx)
	if sh := files[0].Segments[1].Shards[2]; sh.Hash != newHash || sh.NodeID != "node-9" {
		t.Fatalf("shard sau repair = %+v", sh)
	}

	if err := db.SaveFile(ctx, f); err != nil {
		t.Fatalf("commit lại lần hai lỗi: %v", err)
	}
	_, files, _ = db.Load(ctx)
	if len(files) != 1 || len(files[0].Segments[1].Shards) != 3 {
		t.Fatal("commit lại lần hai làm nhân đôi dữ liệu")
	}
}

// Xóa file thì segment và shard đi theo.
func TestDeleteCascades(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	db.SaveFile(ctx, sampleFile("f1"))
	db.SaveFile(ctx, sampleFile("f2"))

	if err := db.DeleteFile(ctx, "f1"); err != nil {
		t.Fatal(err)
	}
	_, files, err := db.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].ID != "f2" {
		t.Fatalf("còn lại %d file", len(files))
	}

	var orphans int
	db.pool.QueryRow(ctx, `SELECT count(*) FROM shards WHERE file_id = 'f1'`).Scan(&orphans)
	if orphans != 0 {
		t.Fatalf("còn %d shard mồ côi của file đã xóa", orphans)
	}
}
