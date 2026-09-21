package main

import (
	"flag"
	"fmt"
	"os"
	"text/tabwriter"

	"dstore-demo/internal/sim"
)

func main() {
	cfg := sim.Config{}
	flag.IntVar(&cfg.Nodes, "nodes", 1000, "số node trong cụm")
	var dataGB float64
	flag.Float64Var(&dataGB, "data-gb", 200, "tổng dữ liệu gốc của người dùng (GB)")
	flag.IntVar(&cfg.Segments, "segments", 0, "số segment (mặc định suy ra từ -data-gb)")
	flag.IntVar(&cfg.K, "k", 4, "số data shard")
	flag.IntVar(&cfg.M, "m", 2, "số parity shard")
	flag.Float64Var(&cfg.ShardSizeMB, "shard-mb", 1, "kích thước một shard (MB)")
	flag.Float64Var(&cfg.Years, "years", 3, "số năm mô phỏng")
	flag.Float64Var(&cfg.NodeMTTFHours, "mttf", 8760, "trung bình bao lâu một node hỏng (giờ)")
	flag.Float64Var(&cfg.TransientFraction, "transient", 0.8, "tỉ lệ lần hỏng chỉ là tạm nghỉ")
	flag.Float64Var(&cfg.TransientMeanHours, "transient-hours", 2, "node tạm nghỉ trung bình bao lâu")
	flag.Float64Var(&cfg.ReplaceHours, "replace-hours", 24, "bao lâu thì node chết được thay mới")
	flag.Float64Var(&cfg.RepairDelayHours, "repair-delay", 1, "chờ bao lâu mới coi shard là cần sửa")
	flag.IntVar(&cfg.RepairThreshold, "threshold", 1, "chỉ sửa khi segment thiếu >= ngần này shard")
	flag.Float64Var(&cfg.RepairShardsPerHour, "repair-rate", 2000, "năng lực repair của cụm (shard/giờ)")
	flag.Int64Var(&cfg.Seed, "seed", 1, "seed ngẫu nhiên")
	compare := flag.String("compare", "", "chế độ so sánh: km | threshold | bandwidth")
	flag.Parse()

	// Số segment suy ra từ lượng dữ liệu gốc, để các cấu hình k khác nhau vẫn
	// lưu đúng chừng ấy dữ liệu và so băng thông repair mới công bằng.
	fixedSegments := cfg.Segments > 0
	segmentsFor := func(c sim.Config) int {
		if fixedSegments {
			return c.Segments
		}
		n := int(dataGB * 1024 / (float64(c.K) * c.ShardSizeMB))
		if n < 1 {
			n = 1
		}
		return n
	}
	cfg.Segments = segmentsFor(cfg)

	switch *compare {
	case "":
		printConfig(cfg, dataGB)
		printTable([]sim.Result{sim.Run(cfg)})
	case "km":
		printConfig(cfg, dataGB)
		var results []sim.Result
		for _, km := range [][2]int{{4, 1}, {4, 2}, {4, 3}, {6, 2}, {6, 3}, {8, 2}, {8, 4}, {10, 4}} {
			c := cfg
			c.K, c.M = km[0], km[1]
			c.Segments = segmentsFor(c)
			results = append(results, sim.Run(c))
		}
		printTable(results)
		fmt.Println("\nGhi chú: cùng lượng dữ liệu gốc, m càng lớn càng bền nhưng tốn dung lượng;")
		fmt.Println("k càng lớn thì mỗi lần repair phải đọc càng nhiều shard.")
	case "threshold":
		printConfig(cfg, dataGB)
		var results []sim.Result
		for _, t := range []int{1, 2, 3} {
			for _, delay := range []float64{0, 1, 6} {
				c := cfg
				c.RepairThreshold = t
				c.RepairDelayHours = delay
				if t > c.M {
					continue // sửa muộn hơn m là đã mất dữ liệu
				}
				results = append(results, sim.Run(c))
			}
		}
		printTable(results)
		fmt.Println("\nGhi chú: ngưỡng và độ trễ cao thì ít repair thừa (node chỉ tạm nghỉ)")
		fmt.Println("nhưng segment nằm ở mức thiếu shard lâu hơn, rủi ro mất dữ liệu tăng.")
	case "bandwidth":
		printConfig(cfg, dataGB)
		var results []sim.Result
		for _, rate := range []float64{10, 20, 35, 50, 100, 500} {
			c := cfg
			c.RepairShardsPerHour = rate
			results = append(results, sim.Run(c))
		}
		printTable(results)
		fmt.Printf("\nCụm này cần khoảng %.0f shard/giờ chỉ để theo kịp node hỏng.\n", needRate(cfg))
		fmt.Println("Chậm hơn mức đó thì hàng đợi dồn lại, segment phơi nhiễm lâu và bắt đầu mất dữ liệu;")
		fmt.Println("nhanh hơn thì không cải thiện thêm.")
	default:
		fmt.Fprintf(os.Stderr, "chế độ so sánh không hợp lệ: %s\n", *compare)
		os.Exit(2)
	}
}

func printConfig(c sim.Config, dataGB float64) {
	fmt.Printf("Cụm %d node, %.0f GB dữ liệu gốc (shard %.0f MB), mô phỏng %.0f năm\n",
		c.Nodes, dataGB, c.ShardSizeMB, c.Years)
	fmt.Printf("Node hỏng trung bình mỗi %.0f giờ, %.0f%% chỉ là tạm nghỉ (~%.0f giờ), "+
		"node chết được thay sau %.0f giờ\n\n",
		c.NodeMTTFHours, c.TransientFraction*100, c.TransientMeanHours, c.ReplaceHours)
}

func printTable(results []sim.Result) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "K/M\tDƯ THỪA\tSEGMENT\tNGƯỠNG\tCHỜ\tREPAIR/H\tMẤT\tĐỘ BỀN\tTHIẾU SHARD\tKHÔNG ĐỌC ĐƯỢC\tREPAIR\tTHỪA\tHÀNG ĐỢI")
	for _, r := range results {
		c := r.Config
		fmt.Fprintf(w, "%d/%d\t%.2fx\t%d\t%d\t%.0fh\t%.0f\t%d\t%s\t%s\t%s\t%.0f GB\t%.0f%%\t%d\n",
			c.K, c.M,
			float64(c.K+c.M)/float64(c.K),
			c.Segments,
			c.RepairThreshold,
			c.RepairDelayHours,
			c.RepairShardsPerHour,
			r.LostSegments,
			nines(r.Durability),
			segmentHours(r.HoursDegraded),
			segmentHours(r.UnreadableHours),
			r.RepairTrafficGB,
			r.WastedRatio()*100,
			r.PeakBacklog,
		)
	}
	w.Flush()
}

// needRate ước lượng năng lực repair tối thiểu: mỗi giờ trung bình có bao nhiêu
// shard biến mất vì node chết hẳn.
func needRate(c sim.Config) float64 {
	shardsPerNode := float64(c.Segments*(c.K+c.M)) / float64(c.Nodes)
	failuresPerHour := float64(c.Nodes) / c.NodeMTTFHours
	return failuresPerHour * shardsPerNode * (1 - c.TransientFraction)
}

// nines quy độ bền ra dạng "số chín" cho dễ so sánh.
func nines(d float64) string {
	if d >= 1 {
		return "100%"
	}
	n := 0
	for x := 1 - d; x < 1; x *= 10 {
		n++
	}
	return fmt.Sprintf("%.6f%% (%d số 9)", d*100, n+1)
}

func segmentHours(h float64) string {
	if h == 0 {
		return "0"
	}
	if h < 1000 {
		return fmt.Sprintf("%.0f seg·h", h)
	}
	return fmt.Sprintf("%.1fk seg·h", h/1000)
}
