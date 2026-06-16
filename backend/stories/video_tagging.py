"""故事 B：多模态视频打标。

D1 阶段：mock 实现；D2 阶段把 CLIP 推理接成 Ray Actor 真跑。
"""
from __future__ import annotations

import time
import random
from typing import Iterator, Dict, Any

from pricing import calculate_roi


PIPELINE_STAGES = [
    ("read",       "读取 COS://videos", 0.05),
    ("frame",      "CPU 抽帧", 0.20),
    ("clip",       "GPU Actor CLIP 推理", 0.40),
    ("llava",      "GPU Actor LLaVA 标签", 0.30),
    ("write",      "写标签", 0.05),
]


def run_video_tagging_mock(
    video_count: int = 1000,
    speedup_target: float = 14.0,
    engine_compare: bool = True,
    region: str = "ap-guangzhou",
    gpu_count: int = 8,
) -> Iterator[Dict[str, Any]]:
    ray_total_seconds = 8.0
    spark_total_seconds = ray_total_seconds * speedup_target

    cumulative = 0.0
    ray_fps_peak = video_count / ray_total_seconds * gpu_count  # 全集群峰值 fps

    for stage_id, stage_label, stage_weight in PIPELINE_STAGES:
        stage_seconds = ray_total_seconds * stage_weight
        steps = max(int(stage_seconds * 5), 3)
        for i in range(steps):
            time.sleep(stage_seconds / steps)
            cumulative += stage_seconds / steps
            percent = min(int(cumulative / ray_total_seconds * 100), 99)
            fps = int(ray_fps_peak * (0.7 + 0.3 * random.random()))
            yield {
                "event": "progress",
                "data": {
                    "stage": stage_id,
                    "stage_label": stage_label,
                    "percent": percent,
                    "ray_fps": fps,
                    "spark_fps": int(fps / speedup_target),
                    "ray_gpu_util": 0.88 + 0.06 * random.random(),
                    "spark_gpu_util": 0.12 + 0.06 * random.random(),
                    "elapsed_sec": round(cumulative, 2),
                },
            }

    ray_ms = ray_total_seconds * 1000
    spark_ms = spark_total_seconds * 1000

    roi = calculate_roi(
        scenario="video_tagging",
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
            "video_count": video_count,
            "roi": roi,
        },
    }

    yield {
        "event": "done",
        "data": {
            "summary": {
                "story": "video_tagging",
                "headline": (
                    f"单卡 180 fps · Spark 单卡 12 fps · {speedup_target}× 加速 · "
                    f"全集群 GPU {gpu_count} 张"
                ),
                "ray_ms": ray_ms,
                "spark_ms": spark_ms,
                "roi": roi,
            }
        },
    }
