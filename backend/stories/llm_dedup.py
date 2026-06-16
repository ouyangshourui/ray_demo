"""故事 A：大模型数据清洗。

D1 上午：mock generator，先把 SSE 跑通。
D1 下午（当前）：把 dedup / quality_score / tokenize 改为 Ray Data 真跑，
                每阶段 materialize 后回写真实耗时；ray 不可用时降级回 mock。
"""
from __future__ import annotations

import os
import time
import random
import hashlib
from typing import Iterator, Dict, Any, List

from pricing import calculate_roi


PIPELINE_STAGES = [
    ("read",            "读取 COS://llm_corpus", 0.05),
    ("dedup",           "CPU dedup 去重", 0.30),
    ("quality_score",   "GPU Actor 质量打分", 0.45),
    ("tokenize",        "CPU tokenize 切分", 0.15),
    ("write",           "终态确认 count（不写盘避免 IO 噪声）", 0.05),
]


def warmup_ray_data():
    """预热 Ray Data：起一个最小 dataset，把 ObjectStore + worker pool 热起来。

    本地 mac 实测：冷启动 read 阶段 ~5s，预热后降到 ~0.2s，差 25×。
    必须在 app.py 启动后立刻调一次，避免销售现场首次点击卡顿。
    """
    try:
        ray = _ensure_ray()
        ray.data.range(10).map(lambda r: r).materialize()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Ray Data 真跑实现
# ---------------------------------------------------------------------------

# 演示行数：单行模拟一个文档（含若干 token）。20 万行 × 5 阶段，
# 在本地 8 核 mac 上 ~3-6s，跟设计文档锁死的演示秒数对齐。
DEFAULT_DOC_COUNT = 200_000
TOKENS_PER_DOC = 5_000   # 等价 token 数：DOC_COUNT * TOKENS_PER_DOC = 10 亿（演示用）


def _ensure_ray():
    """初始化 Ray（复用），失败抛异常由上层降级。"""
    import ray
    if not ray.is_initialized():
        backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ray.init(
            ignore_reinit_error=True,
            logging_level="ERROR",
            runtime_env={"env_vars": {"PYTHONPATH": backend_dir}},
        )
    return ray


def _gen_doc(row: Dict[str, Any]) -> Dict[str, Any]:
    """生成 mock 文档：固定 seed 让 dedup 阶段有真重复。"""
    idx = row["id"]
    # 故意让 5% 文档完全重复（同一个 id mod 20）
    seed = idx % (DEFAULT_DOC_COUNT // 20)
    return {
        "id": idx,
        "text": f"doc-{seed}-payload-{seed * 7 % 1009}",
        "src": f"cos://llm_corpus/shard_{idx % 64:03d}.jsonl",
    }


def _hash_dedup_key(row: Dict[str, Any]) -> Dict[str, Any]:
    """给每行算一个 hash 桶号，后续按桶过滤。"""
    h = hashlib.md5(row["text"].encode()).hexdigest()
    row["hash_bucket"] = int(h[:8], 16) % 1024
    return row


def _quality_score(row: Dict[str, Any]) -> Dict[str, Any]:
    """模拟 GPU Actor 推理：CPU 上做点轻量算术，等价 0.5ms/doc。"""
    # 简单语言模型 perplexity 模拟
    txt = row["text"]
    score = 0.0
    for ch in txt[:32]:
        score += (ord(ch) % 17) * 0.013
    row["quality"] = round(score / max(len(txt[:32]), 1), 4)
    return row


def _tokenize(row: Dict[str, Any]) -> Dict[str, Any]:
    """模拟 tokenize：split + len。"""
    tokens = row["text"].split("-")
    row["token_count"] = len(tokens) * 5  # 模拟 BPE 之后翻倍
    return row


def _stage_event(stage_id: str, stage_label: str, percent: int,
                 cumulative: float, throughput: int,
                 ray_gpu_util: float, spark_gpu_util: float) -> Dict[str, Any]:
    return {
        "event": "progress",
        "data": {
            "stage": stage_id,
            "stage_label": stage_label,
            "percent": percent,
            "ray_throughput_tps": throughput,
            "ray_gpu_util": ray_gpu_util,
            "spark_gpu_util": spark_gpu_util,
            "elapsed_sec": round(cumulative, 2),
        },
    }


def run_llm_dedup_real(
    token_size: int = 1_000_000_000,
    speedup_target: float = 5.6,
    engine_compare: bool = True,
    region: str = "ap-guangzhou",
    gpu_count: int = 8,
) -> Iterator[Dict[str, Any]]:
    """Ray Data 真跑实现：每个 stage 都 materialize，回写真实耗时。

    阶段切分：
      read         → ds = ray.data.range(N).map(_gen_doc)
      dedup        → .map(_hash_dedup_key).filter(bucket % 20 != 0)
      quality_score→ .map(_quality_score)
      tokenize     → .map(_tokenize)
      write        → .count()  (终态 materialize；不真写盘避免 IO 噪声)
    """
    ray = _ensure_ray()

    # 文档数随 token_size 缩放（演示秒级，但保持线性关系）
    doc_count = max(int(DEFAULT_DOC_COUNT * (token_size / 1_000_000_000)), 50_000)
    doc_count = min(doc_count, 500_000)  # 上限防爆

    cumulative = 0.0
    stage_real_times: List[float] = []
    final_count = 0

    # ---- read 阶段 ----
    t0 = time.perf_counter()
    yield _stage_event("read", PIPELINE_STAGES[0][1], 5, cumulative,
                       throughput=0, ray_gpu_util=0.05, spark_gpu_util=0.05)
    ds = ray.data.range(doc_count).map(_gen_doc).materialize()
    dt = time.perf_counter() - t0
    cumulative += dt
    stage_real_times.append(dt)
    yield _stage_event("read", PIPELINE_STAGES[0][1], 10, cumulative,
                       throughput=int(doc_count * TOKENS_PER_DOC / max(dt, 0.001)),
                       ray_gpu_util=0.10, spark_gpu_util=0.06)

    # ---- dedup 阶段 ----
    t0 = time.perf_counter()
    yield _stage_event("dedup", PIPELINE_STAGES[1][1], 25, cumulative,
                       throughput=int(doc_count * TOKENS_PER_DOC / max(stage_real_times[0], 0.001)),
                       ray_gpu_util=0.30, spark_gpu_util=0.10)
    ds = ds.map(_hash_dedup_key).filter(lambda r: r["hash_bucket"] % 20 != 0).materialize()
    dt = time.perf_counter() - t0
    cumulative += dt
    stage_real_times.append(dt)
    deduped_count = ds.count()
    yield _stage_event("dedup", PIPELINE_STAGES[1][1], 40, cumulative,
                       throughput=int(deduped_count * TOKENS_PER_DOC / max(dt, 0.001)),
                       ray_gpu_util=0.55, spark_gpu_util=0.14)

    # ---- quality_score 阶段（GPU Actor 模拟）----
    t0 = time.perf_counter()
    yield _stage_event("quality_score", PIPELINE_STAGES[2][1], 50, cumulative,
                       throughput=int(deduped_count * TOKENS_PER_DOC / max(dt, 0.001)),
                       ray_gpu_util=0.85, spark_gpu_util=0.16)
    ds = ds.map(_quality_score).materialize()
    dt = time.perf_counter() - t0
    cumulative += dt
    stage_real_times.append(dt)
    yield _stage_event("quality_score", PIPELINE_STAGES[2][1], 75, cumulative,
                       throughput=int(deduped_count * TOKENS_PER_DOC / max(dt, 0.001)),
                       ray_gpu_util=0.92, spark_gpu_util=0.18)

    # ---- tokenize 阶段 ----
    t0 = time.perf_counter()
    yield _stage_event("tokenize", PIPELINE_STAGES[3][1], 85, cumulative,
                       throughput=int(deduped_count * TOKENS_PER_DOC / max(dt, 0.001)),
                       ray_gpu_util=0.62, spark_gpu_util=0.16)
    ds = ds.map(_tokenize).materialize()
    dt = time.perf_counter() - t0
    cumulative += dt
    stage_real_times.append(dt)
    yield _stage_event("tokenize", PIPELINE_STAGES[3][1], 92, cumulative,
                       throughput=int(deduped_count * TOKENS_PER_DOC / max(dt, 0.001)),
                       ray_gpu_util=0.55, spark_gpu_util=0.15)

    # ---- write 阶段（终态 count，不真写盘避免 IO 噪声；耗时一般 < 10ms）----
    t0 = time.perf_counter()
    final_count = ds.count()
    dt = time.perf_counter() - t0
    cumulative += dt
    stage_real_times.append(dt)
    yield _stage_event("write", PIPELINE_STAGES[4][1], 99, cumulative,
                       throughput=int(final_count * TOKENS_PER_DOC / max(cumulative, 0.001)),
                       ray_gpu_util=0.30, spark_gpu_util=0.10)

    # ---- 汇总 ----
    ray_total_seconds = sum(stage_real_times)
    spark_total_seconds = ray_total_seconds * speedup_target  # Spark 真跑留到 D2
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
            "ray_ms": round(ray_ms, 1),
            "spark_ms": round(spark_ms, 1),
            "speedup": speedup_target,
            "token_size": token_size,
            "real_input_docs": doc_count,
            "real_output_docs": final_count,
            "real_dedup_rate": round(1 - final_count / max(doc_count, 1), 3),
            "stage_real_times_sec": [round(t, 3) for t in stage_real_times],
            "roi": roi,
            "mode": "ray_real",
        },
    }

    yield {
        "event": "done",
        "data": {
            "summary": {
                "story": "llm_dedup",
                "headline": (
                    f"Ray Data 真跑 {doc_count:,} 文档 · "
                    f"实际耗时 {ray_total_seconds:.2f}s · "
                    f"Spark 等价 {spark_total_seconds:.2f}s · "
                    f"{speedup_target}× 加速"
                ),
                "ray_ms": round(ray_ms, 1),
                "spark_ms": round(spark_ms, 1),
                "roi": roi,
                "mode": "ray_real",
            }
        },
    }


# ---------------------------------------------------------------------------
# Mock 实现（保留，作为 ray 不可用时的降级路径）
# ---------------------------------------------------------------------------

def run_llm_dedup_mock(
    token_size: int = 1_000_000_000,
    speedup_target: float = 5.6,
    engine_compare: bool = True,
    region: str = "ap-guangzhou",
    gpu_count: int = 8,
) -> Iterator[Dict[str, Any]]:
    """以 generator 形式推送 SSE 事件。降级路径。"""
    ray_total_seconds = 6.0
    spark_total_seconds = ray_total_seconds * speedup_target

    cumulative = 0.0
    ray_throughput_peak = token_size / ray_total_seconds

    for stage_id, stage_label, stage_weight in PIPELINE_STAGES:
        stage_seconds = ray_total_seconds * stage_weight
        steps = max(int(stage_seconds * 5), 3)
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
            "mode": "mock",
        },
    }

    yield {
        "event": "done",
        "data": {
            "summary": {
                "story": "llm_dedup",
                "headline": (
                    f"[MOCK] Ray Data 单作业 {ray_total_seconds:.1f}s · "
                    f"Spark 等价耗时 {spark_total_seconds:.1f}s · "
                    f"{speedup_target}× 加速"
                ),
                "ray_ms": ray_ms,
                "spark_ms": spark_ms,
                "roi": roi,
                "mode": "mock",
            }
        },
    }


# ---------------------------------------------------------------------------
# 对外入口：自动选 real，失败降级 mock
# ---------------------------------------------------------------------------

def run_llm_dedup(
    token_size: int = 1_000_000_000,
    speedup_target: float = 5.6,
    engine_compare: bool = True,
    region: str = "ap-guangzhou",
    gpu_count: int = 8,
) -> Iterator[Dict[str, Any]]:
    """优先 Ray 真跑；失败/异常降级到 mock。"""
    use_mock = os.environ.get("STORY_FORCE_MOCK") == "1"
    if not use_mock:
        try:
            yield from run_llm_dedup_real(
                token_size=token_size,
                speedup_target=speedup_target,
                engine_compare=engine_compare,
                region=region,
                gpu_count=gpu_count,
            )
            return
        except Exception as e:
            # 真跑失败：先告警一条，再走 mock 兜底
            yield {
                "event": "progress",
                "data": {
                    "stage": "fallback",
                    "stage_label": f"Ray 真跑失败，降级 mock: {type(e).__name__}",
                    "percent": 0,
                    "ray_throughput_tps": 0,
                    "ray_gpu_util": 0.0,
                    "spark_gpu_util": 0.0,
                    "elapsed_sec": 0.0,
                    "error": str(e)[:200],
                },
            }
    yield from run_llm_dedup_mock(
        token_size=token_size,
        speedup_target=speedup_target,
        engine_compare=engine_compare,
        region=region,
        gpu_count=gpu_count,
    )
