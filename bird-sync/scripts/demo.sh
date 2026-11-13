#!/usr/bin/env bash
# 端到端演示：离线更正 -> 联网同步 -> 冲突 -> 裁定 -> 再同步 -> 分析取数
# 用法：先 `docker compose up -d --build`，再 `bash scripts/demo.sh`
set -euo pipefail

A="${ALPHA_URL:-http://localhost:8001}"
B="${BRAVO_URL:-http://localhost:8002}"

j() { python3 -m json.tool; }

echo "================ 0. 两站初始数据（同一份 CSV） ================"
curl -s "$A/records" | j >/dev/null
echo "alpha 记录数: $(curl -s "$A/records" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')"
echo "bravo 记录数: $(curl -s "$B/records" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')"

echo
echo "================ 1. 断网：两站各自离线更正 ================"
echo "-- alpha: R001 黑腹滨鹬 计数 -> 50"
curl -s -X POST "$A/records/R001/corrections" -H 'Content-Type: application/json' \
  -d '{"count": 50}' | j
echo "-- bravo: R001 计数 -> 70（并发、未读到彼此修改）"
curl -s -X POST "$B/records/R001/corrections" -H 'Content-Type: application/json' \
  -d '{"count": 70}' | j
echo "-- alpha: R002 计数 -> 99；bravo: 删除 R002（改值 vs 删除冲突）"
curl -s -X POST "$A/records/R002/corrections" -H 'Content-Type: application/json' -d '{"count": 99}' >/dev/null
curl -s -X DELETE "$B/records/R002" -H 'Content-Type: application/json' -d '{}' >/dev/null
echo "-- 两站都把 R003 改成 11（同值并发，自动合并不冲突）"
curl -s -X POST "$A/records/R003/corrections" -H 'Content-Type: application/json' -d '{"count": 11}' >/dev/null
curl -s -X POST "$B/records/R003/corrections" -H 'Content-Type: application/json' -d '{"count": 11}' >/dev/null
echo "-- alpha 删除 R007"
curl -s -X DELETE "$A/records/R007" -H 'Content-Type: application/json' -d '{}' >/dev/null

echo
echo "================ 2. 联网：alpha 发起双向同步 ================"
curl -s -X POST "$A/sync/run" | j

echo
echo "================ 3. 冲突清单（两站一致） ================"
curl -s "$A/conflicts" | j
echo "bravo 冲突数: $(curl -s "$B/conflicts" | python3 -c 'import json,sys; print(len(json.load(sys.stdin)))')"

echo
echo "================ 4. 分析取数自动排除冲突与删除 ================"
curl -s "$A/analysis/counts" | python3 -c '
import json, sys
rows = json.load(sys.stdin)
print(f"可用计数 {len(rows)} 条：", [(r["record_id"], r["count"]) for r in rows])
assert "R001" not in {r["record_id"] for r in rows}
assert "R002" not in {r["record_id"] for r in rows}
assert "R007" not in {r["record_id"] for r in rows}
print("R001/R002（冲突）与 R007（删除）已排除")
'

echo
echo "================ 5. 裁定：R001 取计数 70；R002 裁定删除 ================"
curl -s -X POST "$A/conflicts/R001/resolve" -H 'Content-Type: application/json' \
  -d '{"choice": "count", "count": 70}' | j
curl -s -X POST "$A/conflicts/R002/resolve" -H 'Content-Type: application/json' \
  -d '{"choice": "delete"}' | j

echo
echo "================ 6. 裁定同步到 bravo，两站收敛 ================"
curl -s -X POST "$A/sync/run" | j
curl -s -X POST "$B/sync/run" | j
echo "alpha 向量时钟: $(curl -s "$A/status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["version_vv"])')"
echo "bravo 向量时钟: $(curl -s "$B/status" | python3 -c 'import json,sys; print(json.load(sys.stdin)["version_vv"])')"
echo "bravo R001: $(curl -s "$B/records/R001" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["count"], "conflicted=", d["conflicted"])')"
echo "bravo R002: $(curl -s "$B/records/R002" | python3 -c 'import json,sys; d=json.load(sys.stdin); print("deleted=", d["deleted"], "conflicted=", d["conflicted"])')"

echo
echo "================ 7. 重复同步幂等 ================"
curl -s -X POST "$A/sync/run" | j

echo
echo "演示完成。Swagger 入口：$A/docs 与 $B/docs"
