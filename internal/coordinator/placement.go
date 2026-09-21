package coordinator

import (
	"fmt"
	"math/rand"
	"sort"

	"dstore-demo/internal/protocol"
)

// Placer chọn node để đặt shard.
type Placer struct {
	store *Store
}

// NewPlacer tạo Placer chọn node từ danh sách node đang online trong s.
func NewPlacer(s *Store) *Placer { return &Placer{store: s} }

// Pick chọn count node khác nhau, đang online, không nằm trong exclude,
// còn đủ chỗ cho need byte. Ưu tiên node trống nhiều nhất, có xáo trộn nhẹ
// để shard trải đều thay vì dồn hết vào một node.
func (p *Placer) Pick(count int, need int64, exclude []string) ([]protocol.NodeRef, error) {
	excluded := make(map[string]bool, len(exclude))
	for _, id := range exclude {
		excluded[id] = true
	}

	candidates := make([]Node, 0)
	for _, n := range p.store.OnlineNodes() {
		if !excluded[n.ID] && n.Free() >= need {
			candidates = append(candidates, n)
		}
	}
	if len(candidates) < count {
		return nil, fmt.Errorf("cần %d node khả dụng nhưng chỉ có %d (online, còn đủ %d byte trống)",
			count, len(candidates), need)
	}

	// Xáo trước rồi sort ổn định theo dung lượng trống: node trống ngang nhau
	// sẽ được chọn ngẫu nhiên thay vì luôn theo một thứ tự.
	rand.Shuffle(len(candidates), func(i, j int) {
		candidates[i], candidates[j] = candidates[j], candidates[i]
	})
	sort.SliceStable(candidates, func(i, j int) bool { return candidates[i].Free() > candidates[j].Free() })

	out := make([]protocol.NodeRef, 0, count)
	for _, n := range candidates[:count] {
		out = append(out, protocol.NodeRef{ID: n.ID, Addr: n.Addr})
	}
	return out, nil
}

// PlanFile chọn k+m node cho từng segment của một file.
func (p *Placer) PlanFile(segmentCount, k, m int, shardSize int64) ([]protocol.SegmentPlan, error) {
	plans := make([]protocol.SegmentPlan, 0, segmentCount)
	for i := 0; i < segmentCount; i++ {
		nodes, err := p.Pick(k+m, shardSize, nil)
		if err != nil {
			return nil, fmt.Errorf("segment %d: %w", i, err)
		}
		plans = append(plans, protocol.SegmentPlan{Index: i, Nodes: nodes})
	}
	return plans, nil
}
