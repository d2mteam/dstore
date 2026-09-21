package sim

import "testing"

// base là cấu hình nhỏ để test chạy nhanh.
func base() Config {
	return Config{
		Nodes:               100,
		Segments:            2000,
		K:                   4,
		M:                   2,
		ShardSizeMB:         1,
		Years:               1,
		NodeMTTFHours:       8760,
		TransientFraction:   0.8,
		TransientMeanHours:  2,
		ReplaceHours:        24,
		RepairDelayHours:    1,
		RepairThreshold:     1,
		RepairShardsPerHour: 500,
		Seed:                7,
	}
}

// Node không bao giờ hỏng thì không có repair và không mất dữ liệu.
func TestNoFailuresNoRepair(t *testing.T) {
	c := base()
	c.NodeMTTFHours = 1e12
	r := Run(c)

	if r.NodeFailures != 0 || r.RepairedShards != 0 || r.LostSegments != 0 {
		t.Fatalf("cụm không hỏng mà vẫn có hoạt động: %+v", r)
	}
	if r.Durability != 1 {
		t.Fatalf("độ bền %.6f, mong đợi 1", r.Durability)
	}
}

// Cùng điều kiện khắc nghiệt, m lớn hơn thì mất ít dữ liệu hơn.
func TestMoreParityLosesLess(t *testing.T) {
	harsh := func(m int) Result {
		c := base()
		c.M = m
		c.NodeMTTFHours = 300   // node hỏng liên tục
		c.TransientFraction = 0 // hỏng là mất dữ liệu
		c.RepairShardsPerHour = 20
		return Run(c)
	}

	m1, m3 := harsh(1), harsh(3)
	if m1.LostSegments == 0 {
		t.Fatal("cấu hình khắc nghiệt với m=1 lẽ ra phải mất dữ liệu")
	}
	if m3.LostSegments >= m1.LostSegments {
		t.Fatalf("m=3 mất %d segment, m=1 mất %d: nhiều parity phải bền hơn",
			m3.LostSegments, m1.LostSegments)
	}
}

// Chờ lâu hơn trước khi repair thì bớt sửa oan những node chỉ tạm nghỉ.
func TestRepairDelayReducesWaste(t *testing.T) {
	withDelay := func(h float64) Result {
		c := base()
		c.TransientFraction = 1 // mọi lần hỏng đều là tạm nghỉ
		c.TransientMeanHours = 3
		c.RepairDelayHours = h
		return Run(c)
	}

	eager, patient := withDelay(0), withDelay(24)
	if eager.WastedRepairs == 0 {
		t.Fatal("repair ngay lập tức lẽ ra phải sửa oan rất nhiều")
	}
	if patient.RepairedShards >= eager.RepairedShards {
		t.Fatalf("chờ 24h vẫn sửa %d shard, không ít hơn %d",
			patient.RepairedShards, eager.RepairedShards)
	}
	if patient.RepairTrafficGB >= eager.RepairTrafficGB {
		t.Fatalf("chờ 24h tốn %.1f GB, không ít hơn %.1f GB",
			patient.RepairTrafficGB, eager.RepairTrafficGB)
	}
}

// Ngưỡng cao hơn thì repair ít hơn nhưng segment phơi nhiễm lâu hơn.
func TestHigherThresholdTradesExposureForTraffic(t *testing.T) {
	withThreshold := func(n int) Result {
		c := base()
		c.RepairThreshold = n
		return Run(c)
	}

	eager, lazy := withThreshold(1), withThreshold(2)
	if lazy.RepairTrafficGB >= eager.RepairTrafficGB {
		t.Fatalf("ngưỡng 2 tốn %.1f GB, không ít hơn ngưỡng 1 (%.1f GB)",
			lazy.RepairTrafficGB, eager.RepairTrafficGB)
	}
	if lazy.UnreadableHours < eager.UnreadableHours {
		t.Fatalf("ngưỡng 2 chỉ phơi nhiễm %.0f seg·h, ít hơn cả ngưỡng 1 (%.0f seg·h)",
			lazy.UnreadableHours, eager.UnreadableHours)
	}
}

// Cùng seed thì kết quả phải lặp lại y hệt.
func TestDeterministic(t *testing.T) {
	a, b := Run(base()), Run(base())
	if a != b {
		t.Fatalf("cùng seed ra hai kết quả khác nhau:\n%+v\n%+v", a, b)
	}
}
