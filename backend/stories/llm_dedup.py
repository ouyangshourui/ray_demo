"""故事 A：大模型数据清洗。

D1 阶段：mock 实现，generator 推 SSE 事件（progress / metric / done）。
D2 阶段：把 dedup / quality_score / tokenize 改为 Ray Data 真跑。
"""
from __future__ import annotations

import time
import random
from typing import Iterator, Dict, Any

from pricing import calculate_roi


PIPELINE_STAGES = [
    ("read",            "读取 COS://llm_corpus", 0.05),
    ("dedup",           "CPU dedup 去重", 0.30),
    ("quality_score",   "GPU Actor 质量打分", 0.45),
    ("tokenize",        "CPU tokenize 切分", 0.15),
    ("write",           "写入 parquet", 0.05),
]


def run_llm_dedup_mock(
    token_size: int = 1_000_000_000,
    speedup_target: float = 5.6,
    engine_compare: bool = True,
    region: str = "ap-guangzhou",
    gpu_count: int = 8,
) -> Iterator[Dict[str, Any]]:
    """以 generator 形式推送 SSE 事件。

    每个 yield 的 dict 形如 {"event": "progress"|"metric"|"done", "data": {...}}。
    上层 SSE 路由按 event/data 编码即可。
    """
    # 模拟整体耗时（演示用，单位秒）：故事真跑时被 Ray 实测耗时替换
    ray_total_seconds = 6.0
    spark_total_seconds = ray_total_seconds * speedup_target

    cumulative = 0.0
    ray_throughput_peak = token_size / ray_total_seconds  # tokens/s 峰值

    for stage_id, stage_label, stage_weight in PIPELINE_STAGES:
        stage_seconds = ray_total_seconds * stage_weight
        steps = max(int(stage_seconds * 5), 3)  # 每秒 ~5 帧
        for i in range(steps):
            time.sleep(stage_seconds / steps)
            cumulative += stage_seconds / steps
            percent = min(int(cumulative / ray_total_seconds * 100), 99)
            throughput = int(ray_throughput_peak * (0.6 + 0.4 * random.random()))
            yield {
                "event": "progress",
                "data": {
                    "stage": stage_id,
                    "stage_label": stage_label,
                    "percent": percent,
                    "ray_throughput_tps": throughput,
                    "ray_gpu_util": 0.85 + 0.08 * random.random(),
                    "spark_gpu_util": 0.15 + 0.06 * random.random(),
                    "elapsed_sec": round(cumulative, 2),
                },
            }

    ray_ms = ray_total_seconds * 1000
    spark_ms = spark_total_seconds * 1000

    roi = calculate_roi(
        scenario="llm_dedup",
        ray_ms=ray_ms,
        spark_ms=spark_ms,
        gpu_count=gpu_count,
        region=region,
    ) if engine_compare else None

    yield {
        "event": "metric",
        "data": {
            "ray_ms": ray_ms,
            "spark_ms": spark_ms,
            "speedup": speedup_target,
            "token_size": token_size,
            "roi": roi,
        },
    }

    yield {
        "event": "done",
        "data": {
            "summary": {
                "story": "llm_dedup",
                "headline": (
                    f"Ray Data 单作业 {ray_total_seconds:.1f}s · "
                    f"Spark 等价耗时 {spark_total_seconds:.1f}s · "
                    f"{speedup_target}× 加速"
                ),
                "ray_ms": ray_ms,
                "spark_ms": spark_ms,
                "roi": roi,
            }
        },
    }
