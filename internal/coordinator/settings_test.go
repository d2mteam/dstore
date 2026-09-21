package coordinator_test

import (
	"context"
	"testing"
	"time"

	"dstore-demo/internal/coordinator"
	"dstore-demo/internal/protocol"
)

var defaultSettings = coordinator.Settings{
	HeartbeatTimeout: protocol.Duration(20 * time.Second),
	GCGrace:          protocol.Duration(2 * time.Minute),
}

// Giá trị sai bị từ chối và không đổi gì; giá trị đúng còn nguyên sau restart.
func TestSettingsUpdateAndReload(t *testing.T) {
	ctx := context.Background()
	meta := coordinator.NewMemoryMeta()
	s := coordinator.NewSettingsStore(meta, defaultSettings)

	tooShortGrace := defaultSettings
	tooShortGrace.GCGrace = protocol.Duration(time.Second) // xóa nhầm shard đang upload
	if err := s.Update(ctx, tooShortGrace); err == nil {
		t.Fatal("GC grace 1s mà không bị từ chối")
	}
	if got := s.Get(); got != defaultSettings {
		t.Fatalf("Update lỗi mà settings vẫn đổi: %+v", got)
	}

	want := coordinator.Settings{
		HeartbeatTimeout: protocol.Duration(45 * time.Second),
		GCGrace:          protocol.Duration(5 * time.Minute),
		RepairPaused:     true,
	}
	if err := s.Update(ctx, want); err != nil {
		t.Fatal(err)
	}
	restarted := coordinator.NewSettingsStore(meta, defaultSettings)
	if found, err := restarted.Load(ctx); err != nil || !found {
		t.Fatalf("Load = %v, %v", found, err)
	}
	if got := restarted.Get(); got != want {
		t.Fatalf("sau restart settings = %+v, mong đợi %+v", got, want)
	}
}

// Bản lưu từ phiên bản cũ thiếu tham số mới thì tham số đó giữ giá trị env.
func TestSettingsLoadKeepsDefaultsForMissingFields(t *testing.T) {
	meta := coordinator.NewMemoryMeta()
	meta.SaveSettings(context.Background(), []byte(`{"repair_paused":true}`))

	s := coordinator.NewSettingsStore(meta, defaultSettings)
	if _, err := s.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := defaultSettings
	want.RepairPaused = true
	if got := s.Get(); got != want {
		t.Fatalf("settings = %+v, mong đợi %+v", got, want)
	}
}
