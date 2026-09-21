package sim

import (
	"math"
	"math/rand"
	"sort"
)

// Config là các tham số của một lần chạy mô phỏng.
type Config struct {
	Nodes    int // số node trong cụm
	Segments int // số segment dữ liệu (mỗi segment = k+m shard)
	K, M     int

	ShardSizeMB float64 // để quy ra băng thông repair
	Years       float64 // khoảng thời gian mô phỏng
	TickHours   float64 // bước thời gian, mặc định 1 giờ

	// Node hỏng
	NodeMTTFHours      float64 // trung bình bao lâu một node hỏng một lần
	TransientFraction  float64 // tỉ lệ lần hỏng chỉ là tạm nghỉ (restart, mất mạng)
	TransientMeanHours float64 // node tạm nghỉ trung bình bao lâu thì quay lại
	ReplaceHours       float64 // node chết hẳn bao lâu thì được thay bằng node rỗng

	// Chính sách repair
	RepairDelayHours    float64 // chờ bao lâu mới coi shard là cần sửa
	RepairThreshold     int     // chỉ sửa khi segment thiếu >= ngần này shard
	RepairShardsPerHour float64 // năng lực repair của cả cụm

	Seed int64
}

// Result là kết quả một lần chạy.
type Result struct {
	Config Config

	LostSegments    int     // mất vĩnh viễn: còn dưới k shard có dữ liệu
	Durability      float64 // tỉ lệ segment sống sót
	UnreadableHours float64 // tổng segment-giờ không đọc được (kể cả chỉ tạm thời)

	RepairedShards  int     // số shard đã tái tạo
	WastedRepairs   int     // trong đó, số shard sửa xong thì node cũ vẫn quay lại
	RepairTrafficGB float64 // băng thông repair đã tiêu

	PeakBacklog   int     // lúc cao điểm có bao nhiêu segment chờ sửa
	NodeFailures  int     // tổng số lần node hỏng
	PermanentLoss int     // trong đó bao nhiêu lần mất dữ liệu hẳn
	HoursDegraded float64 // tổng segment-giờ ở trạng thái thiếu shard
}

// WastedRatio là tỉ lệ repair lẽ ra không cần làm (node chỉ tạm nghỉ).
func (r Result) WastedRatio() float64 {
	if r.RepairedShards == 0 {
		return 0
	}
	return float64(r.WastedRepairs) / float64(r.RepairedShards)
}

type shardState uint8

const (
	shardOK          shardState = iota // node giữ shard đang online
	shardUnavailable                   // node tạm nghỉ, dữ liệu vẫn còn
	shardLost                          // node chết hẳn, dữ liệu mất
)

type segment struct {
	nodes      []int // nodes[i] giữ shard thứ i
	state      []shardState
	downSince  []float64 // thời điểm shard rời trạng thái OK
	lost       bool      // đã mất vĩnh viễn
	unreadable bool      // đang không đủ k shard đọc được
}

func (s *segment) count(want shardState) int {
	n := 0
	for _, st := range s.state {
		if st == want {
			n++
		}
	}
	return n
}

// survivable là số shard dữ liệu còn tồn tại, kể cả đang nằm trên node tạm nghỉ.
func (s *segment) survivable() int {
	n := 0
	for _, st := range s.state {
		if st != shardLost {
			n++
		}
	}
	return n
}

type node struct {
	online   bool
	backAt   float64    // thời điểm quay lại (tạm nghỉ) hoặc được thay mới
	keepData bool       // quay lại có còn dữ liệu không
	shards   []shardRef // shard node này đang giữ
}

type shardRef struct{ seg, idx int }

type simulation struct {
	cfg      Config
	rnd      *rand.Rand
	nodes    []node
	segments []segment

	degraded map[int]bool // segment đang thiếu shard
	budget   float64      // ngân sách repair còn dư của bước trước

	res Result
}

// Run chạy một lần mô phỏng với cấu hình đã cho.
func Run(cfg Config) Result {
	cfg = withDefaults(cfg)
	s := &simulation{
		cfg:      cfg,
		rnd:      rand.New(rand.NewSource(cfg.Seed)),
		nodes:    make([]node, cfg.Nodes),
		segments: make([]segment, cfg.Segments),
		degraded: make(map[int]bool),
	}
	s.res.Config = cfg

	for i := range s.nodes {
		s.nodes[i].online = true
	}
	total := cfg.K + cfg.M
	for i := range s.segments {
		seg := &s.segments[i]
		seg.nodes = s.pickDistinctNodes(total)
		seg.state = make([]shardState, total)
		seg.downSince = make([]float64, total)
		for j, n := range seg.nodes {
			s.nodes[n].shards = append(s.nodes[n].shards, shardRef{seg: i, idx: j})
		}
	}

	horizon := cfg.Years * 365 * 24
	for t := 0.0; t < horizon; t += cfg.TickHours {
		s.processReturns(t)
		s.injectFailures(t)
		s.repair(t)
		s.collect()
	}

	s.res.Durability = 1 - float64(s.res.LostSegments)/float64(cfg.Segments)
	return s.res
}

func withDefaults(c Config) Config {
	if c.TickHours == 0 {
		c.TickHours = 1
	}
	if c.ShardSizeMB == 0 {
		c.ShardSizeMB = 1
	}
	if c.TransientMeanHours == 0 {
		c.TransientMeanHours = 2
	}
	if c.ReplaceHours == 0 {
		c.ReplaceHours = 24
	}
	if c.RepairThreshold < 1 {
		c.RepairThreshold = 1
	}
	if c.Seed == 0 {
		c.Seed = 1
	}
	return c
}

// processReturns đưa node tạm nghỉ (và node thay thế) trở lại online.
func (s *simulation) processReturns(t float64) {
	for i := range s.nodes {
		n := &s.nodes[i]
		if n.online || n.backAt > t {
			continue
		}
		n.online = true
		if !n.keepData {
			n.shards = nil // node mới thay, đĩa trống
			continue
		}
		// Node tạm nghỉ quay lại: shard còn trên đĩa được dùng lại.
		for _, ref := range n.shards {
			seg := &s.segments[ref.seg]
			if seg.state[ref.idx] == shardUnavailable && seg.nodes[ref.idx] == i {
				seg.state[ref.idx] = shardOK
			}
		}
		s.refreshSegments(n.shards)
	}
}

// injectFailures cho một số node hỏng trong bước thời gian này.
func (s *simulation) injectFailures(t float64) {
	online := 0
	for i := range s.nodes {
		if s.nodes[i].online {
			online++
		}
	}
	if online == 0 {
		return
	}
	lambda := float64(online) * s.cfg.TickHours / s.cfg.NodeMTTFHours
	for f := poisson(s.rnd, lambda); f > 0; f-- {
		s.failRandomNode(t)
	}
}

func (s *simulation) failRandomNode(t float64) {
	// Bốc ngẫu nhiên tới khi gặp node đang online.
	var idx int
	for try := 0; ; try++ {
		idx = s.rnd.Intn(len(s.nodes))
		if s.nodes[idx].online {
			break
		}
		if try > 100 {
			return
		}
	}
	n := &s.nodes[idx]
	n.online = false
	s.res.NodeFailures++

	transient := s.rnd.Float64() < s.cfg.TransientFraction
	if transient {
		n.keepData = true
		n.backAt = t + expRand(s.rnd, s.cfg.TransientMeanHours)
	} else {
		n.keepData = false
		n.backAt = t + s.cfg.ReplaceHours
		s.res.PermanentLoss++
	}

	for _, ref := range n.shards {
		seg := &s.segments[ref.seg]
		if seg.lost || seg.nodes[ref.idx] != idx || seg.state[ref.idx] != shardOK {
			continue
		}
		if transient {
			seg.state[ref.idx] = shardUnavailable
		} else {
			seg.state[ref.idx] = shardLost
		}
		seg.downSince[ref.idx] = t
	}
	s.refreshSegments(n.shards)
}

// refreshSegments cập nhật lại trạng thái các segment vừa bị ảnh hưởng.
func (s *simulation) refreshSegments(refs []shardRef) {
	for _, ref := range refs {
		seg := &s.segments[ref.seg]
		if seg.lost {
			continue
		}
		// Mất vĩnh viễn khi số shard còn dữ liệu tụt dưới k.
		if seg.survivable() < s.cfg.K {
			seg.lost = true
			seg.unreadable = false
			delete(s.degraded, ref.seg)
			s.res.LostSegments++
			continue
		}
		ok := seg.count(shardOK)
		seg.unreadable = ok < s.cfg.K
		if ok < s.cfg.K+s.cfg.M {
			s.degraded[ref.seg] = true
		} else {
			delete(s.degraded, ref.seg)
		}
	}
}

// repair tái tạo shard thiếu trong giới hạn băng thông, ưu tiên segment yếu nhất.
func (s *simulation) repair(t float64) {
	s.budget += s.cfg.RepairShardsPerHour * s.cfg.TickHours
	if s.budget < 1 || len(s.degraded) == 0 {
		return
	}

	// Chỉ xét segment đã quá thời gian chờ và thiếu đủ số shard theo ngưỡng.
	type candidate struct {
		seg int
		ok  int
	}
	var queue []candidate
	for idx := range s.degraded {
		seg := &s.segments[idx]
		ok := seg.count(shardOK)
		if s.cfg.K+s.cfg.M-ok < s.cfg.RepairThreshold || ok < s.cfg.K {
			continue // chưa đủ ngưỡng, hoặc không còn đủ k shard để tái tạo
		}
		if !s.anyShardReady(seg, t) {
			continue
		}
		queue = append(queue, candidate{seg: idx, ok: ok})
	}
	if len(queue) > s.res.PeakBacklog {
		s.res.PeakBacklog = len(queue)
	}
	// Ưu tiên segment yếu nhất; so thêm chỉ số segment để thứ tự không phụ thuộc
	// thứ tự duyệt map (nếu không, cùng seed vẫn ra kết quả khác nhau).
	sort.Slice(queue, func(i, j int) bool {
		if queue[i].ok != queue[j].ok {
			return queue[i].ok < queue[j].ok
		}
		return queue[i].seg < queue[j].seg
	})

	for _, c := range queue {
		if s.budget < 1 {
			return
		}
		s.repairSegment(c.seg, t)
	}
}

func (s *simulation) anyShardReady(seg *segment, t float64) bool {
	for i, st := range seg.state {
		if st != shardOK && t-seg.downSince[i] >= s.cfg.RepairDelayHours {
			return true
		}
	}
	return false
}

func (s *simulation) repairSegment(idx int, t float64) {
	seg := &s.segments[idx]
	repaired := 0
	for i, st := range seg.state {
		if s.budget < 1 {
			break
		}
		if st == shardOK || t-seg.downSince[i] < s.cfg.RepairDelayHours {
			continue
		}
		target, ok := s.pickTarget(seg)
		if !ok {
			break // không còn node trống để đặt shard mới
		}
		if st == shardUnavailable {
			// Node cũ chỉ tạm nghỉ: bản sao này lẽ ra không cần tái tạo.
			s.res.WastedRepairs++
		}
		s.detach(seg, i)
		seg.nodes[i] = target
		seg.state[i] = shardOK
		s.nodes[target].shards = append(s.nodes[target].shards, shardRef{seg: idx, idx: i})

		s.budget--
		repaired++
		s.res.RepairedShards++
	}
	if repaired > 0 {
		// Đọc k shard một lần rồi ghi ra các shard vừa tái tạo.
		s.res.RepairTrafficGB += float64(s.cfg.K+repaired) * s.cfg.ShardSizeMB / 1024
		s.refreshSegments([]shardRef{{seg: idx, idx: 0}})
	}
}

// detach gỡ shard khỏi danh sách của node đang giữ nó.
func (s *simulation) detach(seg *segment, i int) {
	old := seg.nodes[i]
	if old < 0 {
		return
	}
	list := s.nodes[old].shards
	for j, ref := range list {
		if ref.idx == i && &s.segments[ref.seg] == seg {
			s.nodes[old].shards = append(list[:j], list[j+1:]...)
			return
		}
	}
}

// pickTarget chọn node online chưa giữ shard nào của segment này.
func (s *simulation) pickTarget(seg *segment) (int, bool) {
	for try := 0; try < 50; try++ {
		cand := s.rnd.Intn(len(s.nodes))
		if !s.nodes[cand].online {
			continue
		}
		used := false
		for _, n := range seg.nodes {
			if n == cand {
				used = true
				break
			}
		}
		if !used {
			return cand, true
		}
	}
	return 0, false
}

func (s *simulation) pickDistinctNodes(count int) []int {
	out := make([]int, 0, count)
	for len(out) < count {
		cand := s.rnd.Intn(len(s.nodes))
		dup := false
		for _, n := range out {
			if n == cand {
				dup = true
				break
			}
		}
		if !dup {
			out = append(out, cand)
		}
	}
	return out
}

// collect cộng dồn số liệu theo từng bước thời gian.
func (s *simulation) collect() {
	for idx := range s.degraded {
		s.res.HoursDegraded += s.cfg.TickHours
		if s.segments[idx].unreadable {
			s.res.UnreadableHours += s.cfg.TickHours
		}
	}
}

// expRand sinh số ngẫu nhiên theo phân phối mũ với giá trị trung bình mean.
func expRand(r *rand.Rand, mean float64) float64 {
	return -mean * math.Log(1-r.Float64())
}

// poisson sinh số sự kiện trong một bước thời gian (thuật toán Knuth, đủ dùng
// vì lambda ở đây rất nhỏ).
func poisson(r *rand.Rand, lambda float64) int {
	limit := math.Exp(-lambda)
	p, k := 1.0, 0
	for {
		k++
		p *= r.Float64()
		if p <= limit {
			return k - 1
		}
	}
}
