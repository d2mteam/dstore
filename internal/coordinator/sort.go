package coordinator

import (
	"sort"

	"dstore-demo/internal/protocol"
)

func sortByID(nodes []protocol.NodeInfo) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
}

func sortFilesByTime(files []protocol.FileInfo) {
	sort.Slice(files, func(i, j int) bool { return files[i].CreatedAt.After(files[j].CreatedAt) })
}

func sortByOKCount(segs []SegmentRef) {
	sort.Slice(segs, func(i, j int) bool { return segs[i].OKCount < segs[j].OKCount })
}
