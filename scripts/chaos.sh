#!/usr/bin/env bash
# Tắt/bật node ngẫu nhiên để xem hệ thống tự phát hiện node chết và repair.
#
#   ./scripts/chaos.sh [số giây chạy] [số node tắt cùng lúc]
#
# Luôn giữ lại tối thiểu k+m node đang sống để file vẫn đọc và sửa được.
set -euo pipefail

DURATION=${1:-120}
KILL_COUNT=${2:-2}
NODES=(node-1 node-2 node-3 node-4 node-5 node-6 node-7 node-8)
MIN_ALIVE=6 # k + m

log() { echo "[chaos $(date +%T)] $*"; }

alive_count() {
  docker ps --filter "name=node-" --filter "status=running" --format '{{.Names}}' | wc -l
}

end=$((SECONDS + DURATION))
while [ $SECONDS -lt $end ]; do
  # Chọn ngẫu nhiên vài node đang chạy để tắt.
  mapfile -t running < <(docker ps --filter "name=node-" --filter "status=running" --format '{{.Names}}' | shuf)
  victims=()
  for n in "${running[@]}"; do
    [ ${#victims[@]} -lt "$KILL_COUNT" ] || break
    [ $(( ${#running[@]} - ${#victims[@]} )) -gt $MIN_ALIVE ] || break
    victims+=("$n")
  done

  if [ ${#victims[@]} -gt 0 ]; then
    log "tắt: ${victims[*]} (đang sống: $(alive_count))"
    docker stop "${victims[@]}" >/dev/null
  else
    log "không tắt thêm, giữ tối thiểu $MIN_ALIVE node"
  fi

  # Đợi coordinator phát hiện node chết (timeout 20s) và repair (mỗi 10s).
  sleep 40

  if [ ${#victims[@]} -gt 0 ]; then
    log "bật lại: ${victims[*]}"
    docker start "${victims[@]}" >/dev/null
  fi
  sleep 20
done

log "xong; bật lại toàn bộ node"
docker start "${NODES[@]}" >/dev/null 2>&1 || true
