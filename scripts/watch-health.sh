#!/usr/bin/env bash
# Theo dõi số segment theo mức độ khỏe theo thời gian — dùng chung với chaos.sh
# để thấy cụm tụt xuống mức thiếu shard rồi được repair kéo lên lại.
#
#   ./scripts/watch-health.sh [giây giữa hai lần đo] [địa chỉ coordinator]
set -euo pipefail

INTERVAL=${1:-5}
COORD=${2:-http://localhost:9000}

printf '%-8s %-6s %-9s %-9s %s\n' "GIỜ" "NODE" "TÁI TẠO" "DỌN RÁC" "SEGMENT THEO SỐ SHARD KHỎE"
while true; do
  metrics=$(curl -sf "$COORD/metrics") || { echo "không gọi được $COORD/metrics"; sleep "$INTERVAL"; continue; }

  online=$(grep '^dstore_nodes{status="online"}' <<<"$metrics" | awk '{print $2}')
  repaired=$(grep '^dstore_repaired_shards_total' <<<"$metrics" | awk '{print $2}')
  cleaned=$(grep '^dstore_orphan_shards_deleted_total' <<<"$metrics" | awk '{print $2}')

  # Mỗi mức khỏe in thành "số_shard:số_segment", mức thấp nhất là nơi đáng lo nhất.
  health=$(grep '^dstore_segments_health' <<<"$metrics" |
    sed -E 's/dstore_segments_health\{healthy="([0-9]+)"\} ([0-9]+)/\1:\2/' | tr '\n' ' ')

  printf '%-8s %-6s %-9s %-9s %s\n' "$(date +%T)" "$online" "$repaired" "$cleaned" "${health:-(chưa có dữ liệu)}"
  sleep "$INTERVAL"
done
