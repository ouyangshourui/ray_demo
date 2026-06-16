#!/usr/bin/env bash
# 路演前 60 秒自检脚本（M1 D5）
# 用途：上台前 1 分钟跑一遍，所有关键端点 + SSE + ROI 数据全部 OK 才上场
#
# 用法：
#   bash scripts/preflight.sh                # 默认 :15556
#   PORT=8000 bash scripts/preflight.sh      # 自定义端口

set -e
PORT="${PORT:-15556}"
BASE="http://127.0.0.1:${PORT}"

echo "========================================"
echo " Ray Demo 路演前自检 · ${BASE}"
echo "========================================"

# 1. 健康检查
echo
echo "[1/8] /api/health"
curl -fs "${BASE}/api/health" | python3 -m json.tool || { echo "✗ Flask 未启动"; exit 1; }

# 2. 三个静态页面
echo
echo "[2/8] 主页 / 故事 / 卡片 静态页"
for path in "/" "/stories" "/cards" "/scenarios"; do
    code=$(curl -fs -o /dev/null -w "%{http_code}" "${BASE}${path}")
    if [ "$code" != "200" ]; then
        echo "✗ ${path} HTTP ${code}"
        exit 1
    fi
    echo "  ${path}  HTTP 200 ✓"
done

# 3. 故事元数据
echo
echo "[3/8] /api/stories"
curl -fs "${BASE}/api/stories" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert isinstance(d, list) and len(d) >= 2, '故事数 < 2'
for s in d:
    print(f\"  {s['id']}: {s['title']} · speedup={s['speedup']}×\")
"

# 4. 卡片元数据
echo
echo "[4/8] /api/cards/manifest"
curl -fs "${BASE}/api/cards/manifest" | python3 -c "
import sys, json
d = json.load(sys.stdin)
assert isinstance(d, list) and len(d) == 5, f'卡片数 {len(d)} ≠ 5'
for c in d:
    print(f\"  {c['id']}: {c['title']} · {c['metric']['spark']} → {c['metric']['ray']}\")
"

# 5. 预热（关键：消除首次冷启）
echo
echo "[5/8] /api/stories/warmup"
curl -fs -X POST "${BASE}/api/stories/warmup" | python3 -m json.tool

# 6. 故事 A 端到端 SSE
echo
echo "[6/8] 故事 A llm_dedup SSE"
T=$(curl -fs -X POST "${BASE}/api/story/llm_dedup/start" \
    -H 'Content-Type: application/json' \
    -d '{"token_size":1000000000,"gpu_count":8,"region":"ap-guangzhou"}' \
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["task_id"])')
curl -fs --max-time 25 -N "${BASE}/api/story/llm_dedup/${T}/stream" 2>&1 \
    | grep -E '"speedup"|"saved_usd"|"saved_percent"|"headline"' | head -3

# 7. 故事 B 端到端 SSE
echo
echo "[7/8] 故事 B video_tagging SSE"
T=$(curl -fs -X POST "${BASE}/api/story/video_tagging/start" \
    -H 'Content-Type: application/json' \
    -d '{"video_count":1000,"gpu_count":8,"region":"ap-guangzhou"}' \
    | python3 -c 'import sys,json;print(json.load(sys.stdin)["task_id"])')
curl -fs --max-time 25 -N "${BASE}/api/story/video_tagging/${T}/stream" 2>&1 \
    | grep -E '"speedup"|"saved_usd"|"saved_percent"|"headline"|"actor_pool_size"' | head -3

# 8. ROI 接口直接调用
echo
echo "[8/8] /api/pricing/calculate"
curl -fs -X POST "${BASE}/api/pricing/calculate" \
    -H 'Content-Type: application/json' \
    -d '{"scenario":"video_tagging","ray_ms":770,"spark_ms":9160,"gpu_count":8,"region":"ap-guangzhou"}' \
    | python3 -m json.tool | head -20

echo
echo "========================================"
echo " ✓ 全部通过，可以上台了"
echo " · 主页:     ${BASE}/"
echo " · 故事舞台: ${BASE}/stories"
echo " · 金句卡:   ${BASE}/cards"
echo " · 场景库:   ${BASE}/scenarios"
echo "========================================"
