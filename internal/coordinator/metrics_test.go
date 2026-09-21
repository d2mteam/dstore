package coordinator_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"dstore-demo/internal/coordinator"
	"dstore-demo/internal/protocol"
)

// metricValue lấy giá trị của một dòng metric trong body Prometheus.
func metricValue(t *testing.T, body, series string) string {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		if name, value, ok := strings.Cut(line, " "); ok && name == series {
			return value
		}
	}
	t.Fatalf("không tìm thấy %q trong:\n%s", series, body)
	return ""
}

func scrape(t *testing.T, srv *coordinator.Server) string {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /metrics: %d", rec.Code)
	}
	return rec.Body.String()
}

// Metrics phải phản ánh đúng trạng thái cụm, kể cả mức độ khỏe của từng segment.
func TestMetricsReflectClusterState(t *testing.T) {
	store := coordinator.NewStore()
	srv := &coordinator.Server{Store: store, Metrics: &coordinator.Metrics{}, Log: slog.New(slog.DiscardHandler)}

	for _, id := range []string{"node-1", "node-2", "node-3"} {
		store.RegisterNode(id, "http://"+id+":8080", 1000)
	}
	store.CreateFile("f1", "f1.bin", 300, 2, 1, 300, 1)
	err := store.CommitFile("f1", []protocol.CommitSegment{{
		Index: 0, OrigSize: 300,
		Shards: []protocol.CommitShard{
			{Index: 0, Hash: "a", NodeID: "node-1"},
			{Index: 1, Hash: "b", NodeID: "node-2"},
			{Index: 2, Hash: "c", NodeID: "node-3"},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}

	body := scrape(t, srv)
	if got := metricValue(t, body, `dstore_nodes{status="online"}`); got != "3" {
		t.Fatalf("node online = %s, mong đợi 3", got)
	}
	if got := metricValue(t, body, `dstore_segments_health{healthy="3"}`); got != "1" {
		t.Fatalf("segment đủ 3 shard = %s, mong đợi 1", got)
	}

	// Một node chết: segment tụt xuống mức 2 shard khỏe.
	store.MarkOffline("node-2")

	body = scrape(t, srv)
	if got := metricValue(t, body, `dstore_nodes{status="offline"}`); got != "1" {
		t.Fatalf("node offline = %s, mong đợi 1", got)
	}
	if got := metricValue(t, body, `dstore_segments_health{healthy="2"}`); got != "1" {
		t.Fatalf("segment còn 2 shard khỏe = %s, mong đợi 1", got)
	}
	if got := metricValue(t, body, `dstore_shards{status="lost"}`); got != "1" {
		t.Fatalf("shard lost = %s, mong đợi 1", got)
	}
}

// Counter tăng theo sự kiện và an toàn khi Metrics chưa được gắn.
func TestMetricsCounters(t *testing.T) {
	store := coordinator.NewStore()
	m := &coordinator.Metrics{}
	srv := &coordinator.Server{Store: store, Metrics: m, Log: slog.New(slog.DiscardHandler)}

	m.RepairedShard()
	m.RepairedShard()
	m.OrphansDeleted(5)
	m.NodeLost()

	body := scrape(t, srv)
	if got := metricValue(t, body, "dstore_repaired_shards_total"); got != "2" {
		t.Fatalf("shard đã tái tạo = %s, mong đợi 2", got)
	}
	if got := metricValue(t, body, "dstore_orphan_shards_deleted_total"); got != "5" {
		t.Fatalf("shard mồ côi đã dọn = %s, mong đợi 5", got)
	}
	if got := metricValue(t, body, "dstore_nodes_lost_total"); got != "1" {
		t.Fatalf("node mất heartbeat = %s, mong đợi 1", got)
	}

	// Không gắn Metrics thì vẫn xuất được, giá trị 0, không panic.
	plain := &coordinator.Server{Store: store, Log: slog.New(slog.DiscardHandler)}
	if got := metricValue(t, scrape(t, plain), "dstore_repaired_shards_total"); got != "0" {
		t.Fatalf("coordinator không gắn metrics trả về %s, mong đợi 0", got)
	}
}
