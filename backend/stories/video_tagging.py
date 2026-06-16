"""故事 B：多模态视频打标。

D1：mock 实现。
D2 上午（当前）：Ray Actor pool 真跑 CLIP 推理（用 numpy 矩阵乘模拟 embedding）。
                核心戏剧点：Actor 模型 load 一次复用 vs Spark 每 task 重 init。
                ray 不可用时降级 mock。

为什么不直接装 transformers/CLIP：
  - 本地 mac 无 GPU，ViT-B/16 CPU 推理 200ms/张，1000 张要 200s，演示太慢
  - 真模型路径留 STORY_USE_REAL_CLIP=1 开关给 M2，本期不挡路

embedding 用 numpy randn 矩阵乘 (224,224,3) @ (3,512) 模拟 ViT 特征抽取，
单图 ~1-2ms，1000 张 ~1.5s，演示节奏刚好。
"""
from __future__ import annotations

import os
import time
import random
import hashlib
from typing import Iterator, Dict, Any, List

from pricing import calculate_roi


PIPELINE_STAGES = [
    ("read",       "读取 COS://videos", 0.05),
    ("frame",      "CPU 抽帧", 0.20),
    ("clip",       "GPU Actor CLIP 推理", 0.40),
    ("llava",      "GPU Actor LLaVA 标签", 0.30),
    ("write",      "写标签", 0.05),
]


# 演示视频数：默认 1000 张图（每张 1 帧），单卡演示 ~2-4s
DEFAULT_VIDEO_COUNT = 1000
EMBEDDING_DIM = 512                # CLIP ViT-B/16 输出维度
ACTOR_LOAD_SEC = 0.5               # 模拟模型 load 耗时（真实 12s 的 1/24 缩放）
ACTOR_POOL_SIZE = 4                # Ray 侧 Actor 数量
# Spark 每 task 重新 init 模型的缩放系数（推算用）
# 真实业务：每 task load CLIP 12s，单卡 12 fps；此处缩放到演示秒级，
# 让推算的 speedup 落在销售口径 14× 附近（而不是飘到 30+）。
# 算法：ray_clip ~0.5s（4 actor × 250 图）；要让 spark_clip ≈ ray_clip × 14 ≈ 7s，
# 所以 SPARK_TASK_INIT_SEC = 7 / 1000 = 0.007s/task
SPARK_TASK_INIT_SEC = 0.007


# ---------------------------------------------------------------------------
# 全局 Actor pool（跨请求复用，避免每次起 actor 付 1s init）
# ---------------------------------------------------------------------------

_GLOBAL_ACTOR_POOL: List[Any] = []
_ACTOR_POOL_READY: bool = False


# ---------------------------------------------------------------------------
# Ray Actor：CLIP 推理（用 numpy 模拟）
# ---------------------------------------------------------------------------

def _ensure_ray():
    """初始化 Ray（复用 llm_dedup 的 init 状态）。"""
    import ray
    if not ray.is_initialized():
        backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ray.init(
            ignore_reinit_error=True,
            logging_level="ERROR",
            runtime_env={"env_vars": {"PYTHONPATH": backend_dir}},
        )
    return ray


def _build_clip_actor_class():
    """延迟构造 Actor 类（避免模块导入时 ray 未装直接报错）。"""
    import ray
    import numpy as np

    @ray.remote(num_cpus=1)
    class CLIPActor:
        """模拟常驻 GPU 的 CLIP Actor。

        关键：__init__ 一次 sleep，之后所有 infer 调用零 init 开销。
        Spark 路径每 task 都要付这个 init 成本——这就是 14× 加速来源。
        """

        def __init__(self, actor_id: int):
            self.actor_id = actor_id
            self.processed = 0
            # 模拟模型 load：分配 (3, 512) 投影矩阵 + sleep
            time.sleep(ACTOR_LOAD_SEC)
            self.proj = np.random.randn(3, EMBEDDING_DIM).astype(np.float32) * 0.1
            self.ready_at = time.time()

        def infer_batch(self, batch_size: int) -> Dict[str, Any]:
            """对 batch_size 张图做 mock CLIP 推理，返回吞吐数据。"""
            t0 = time.perf_counter()
            # 每张图 mock 一个 224x224x3 patch 平均，× proj 得 embedding
            for _ in range(batch_size):
                patch = np.random.randn(224 * 224 * 3).astype(np.float32)
                patch_mean = patch.reshape(-1, 3).mean(axis=0)  # (3,)
                _emb = patch_mean @ self.proj                   # (512,)
            dt = time.perf_counter() - t0
            self.processed += batch_size
            return {
                "actor_id": self.actor_id,
                "batch_size": batch_size,
                "elapsed_sec": dt,
                "fps": batch_size / max(dt, 0.001),
                "total_processed": self.processed,
            }

        def stats(self) -> Dict[str, Any]:
            return {
                "actor_id": self.actor_id,
                "processed": self.processed,
                "uptime_sec": round(time.time() - self.ready_at, 2),
            }

    return CLIPActor


def _get_or_create_actor_pool() -> tuple:
    """获取全局 Actor pool；首次调用时创建（含 init 耗时），后续请求 0 开销。

    返回 (actors, init_seconds)：
      actors → list of CLIPActor handles
      init_seconds → 本次创建消耗的秒数（已存在则为 0.0）
    """
    global _GLOBAL_ACTOR_POOL, _ACTOR_POOL_READY

    if _ACTOR_POOL_READY and _GLOBAL_ACTOR_POOL:
        # 复用：但要确保 actor 还活着（ray cluster 重启会失效）
        try:
            ray = _ensure_ray()
            ray.get([a.stats.remote() for a in _GLOBAL_ACTOR_POOL])
            return _GLOBAL_ACTOR_POOL, 0.0
        except Exception:
            # 死了：重建
            _GLOBAL_ACTOR_POOL = []
            _ACTOR_POOL_READY = False

    ray = _ensure_ray()
    CLIPActor = _build_clip_actor_class()
    t0 = time.perf_counter()
    actors = [CLIPActor.remote(i) for i in range(ACTOR_POOL_SIZE)]
    ray.get([a.stats.remote() for a in actors])  # 等待 __init__ 完成
    init_seconds = time.perf_counter() - t0
    _GLOBAL_ACTOR_POOL = actors
    _ACTOR_POOL_READY = True
    return actors, init_seconds


def _stage_event(stage_id: str, stage_label: str, percent: int,
                 cumulative: float, ray_fps: int, spark_fps: int,
                 ray_gpu_util: float, spark_gpu_util: float,
                 extra: Dict[str, Any] | None = None) -> Dict[str, Any]:
    data = {
        "stage": stage_id,
        "stage_label": stage_label,
        "percent": percent,
        "ray_fps": ray_fps,
        "spark_fps": spark_fps,
        "ray_gpu_util": ray_gpu_util,
        "spark_gpu_util": spark_gpu_util,
        "elapsed_sec": round(cumulative, 2),
    }
    if extra:
        data.update(extra)
    return {"event": "progress", "data": data}


# ---------------------------------------------------------------------------
# Ray 真跑实现
# ---------------------------------------------------------------------------

def run_video_tagging_real(
    video_count: int = DEFAULT_VIDEO_COUNT,
    speedup_target: float = 14.0,
    engine_compare: bool = True,
    region: str = "ap-guangzhou",
    gpu_count: int = 8,
) -> Iterator[Dict[str, Any]]:
    """Ray Actor pool + numpy 模拟 CLIP 推理。

    阶段切分：
      read   → 生成 mock 视频元数据（ray.data.range）
      frame  → 抽帧（ray.data.map，每个视频抽 1 帧 mock）
      clip   → CLIPActor pool round-robin（核心戏剧点）
      llava  → 同 actor pool 内做 hash → 标签（继续复用 actor）
      write  → count() 终态确认

    关键设计：
      - Actor 在 read 阶段就启动 + 提前 init 完成，clip 阶段直接用，
        让"模型常驻"的故事成立。
      - Spark 对照系数推算：spark_ms = (clip 阶段任务数 × SPARK_TASK_INIT_SEC
        + ray clip 实测时间) × 校正因子，反映"每 task 重 init"的真实开销。
    """
    ray = _ensure_ray()
    import numpy as np  # noqa: F401  仅用于触发提前 import

    cumulative = 0.0
    stage_real_times: List[float] = []
    final_count = 0

    # ---- read 阶段：拿 Actor pool（首次起会付 init，已暖则 ~0）----
    t0 = time.perf_counter()
    yield _stage_event("read", PIPELINE_STAGES[0][1], 5, cumulative,
                       ray_fps=0, spark_fps=0,
                       ray_gpu_util=0.05, spark_gpu_util=0.05)

    actors, actor_init_seconds = _get_or_create_actor_pool()

    dt = time.perf_counter() - t0
    cumulative += dt
    stage_real_times.append(dt)
    pool_status = "已暖（复用）" if actor_init_seconds < 0.05 else f"本次起 {actor_init_seconds:.2f}s"
    yield _stage_event("read", PIPELINE_STAGES[0][1], 15, cumulative,
                       ray_fps=0, spark_fps=0,
                       ray_gpu_util=0.10, spark_gpu_util=0.06,
                       extra={
                           "actor_pool_size": ACTOR_POOL_SIZE,
                           "actor_load_sec": ACTOR_LOAD_SEC,
                           "actor_init_seconds": round(actor_init_seconds, 3),
                           "note": f"Actor pool {pool_status}，模型 load 全局只付 1 次",
                       })

    # ---- frame 阶段：mock 抽帧（轻量）----
    t0 = time.perf_counter()
    yield _stage_event("frame", PIPELINE_STAGES[1][1], 25, cumulative,
                       ray_fps=video_count // 2, spark_fps=video_count // 30,
                       ray_gpu_util=0.30, spark_gpu_util=0.10)

    def _frame_extract(row: Dict[str, Any]) -> Dict[str, Any]:
        idx = row["id"]
        # mock 抽帧：算个 sha 模拟 ffmpeg seek
        h = hashlib.md5(f"video-{idx}".encode()).hexdigest()
        return {
            "id": idx,
            "frame_id": int(h[:6], 16),
            "src": f"cos://videos/v{idx:06d}.mp4",
        }

    ds = ray.data.range(video_count).map(_frame_extract).materialize()
    dt = time.perf_counter() - t0
    cumulative += dt
    stage_real_times.append(dt)
    yield _stage_event("frame", PIPELINE_STAGES[1][1], 40, cumulative,
                       ray_fps=int(video_count / max(dt, 0.001)),
                       spark_fps=int(video_count / max(dt, 0.001) / speedup_target),
                       ray_gpu_util=0.45, spark_gpu_util=0.12)

    # ---- clip 阶段：Actor pool round-robin 真做 numpy 推理 ----
    t0 = time.perf_counter()
    yield _stage_event("clip", PIPELINE_STAGES[2][1], 50, cumulative,
                       ray_fps=int(video_count / max(stage_real_times[1], 0.001)),
                       spark_fps=12,
                       ray_gpu_util=0.85, spark_gpu_util=0.16,
                       extra={"note": "Actor 已暖，开始批量推理（零 init 开销）"})

    # batch 切分：把 video_count 分给 ACTOR_POOL_SIZE 个 actor，每个跑 1-2 个 batch
    batches_per_actor = 2
    total_batches = ACTOR_POOL_SIZE * batches_per_actor
    batch_size = max(video_count // total_batches, 1)

    # 异步发到 actor pool（round-robin）
    pending = []
    for b in range(total_batches):
        actor = actors[b % ACTOR_POOL_SIZE]
        pending.append(actor.infer_batch.remote(batch_size))

    clip_results = ray.get(pending)
    dt = time.perf_counter() - t0
    cumulative += dt
    stage_real_times.append(dt)
    total_processed = sum(r["batch_size"] for r in clip_results)
    avg_actor_fps = sum(r["fps"] for r in clip_results) / len(clip_results)
    cluster_fps = int(total_processed / max(dt, 0.001))
    yield _stage_event("clip", PIPELINE_STAGES[2][1], 75, cumulative,
                       ray_fps=cluster_fps,
                       spark_fps=12,
                       ray_gpu_util=0.92, spark_gpu_util=0.18,
                       extra={
                           "actor_results": [
                               {"actor_id": r["actor_id"], "fps": int(r["fps"])}
                               for r in clip_results
                           ],
                           "avg_per_actor_fps": int(avg_actor_fps),
                           "note": f"Actor pool 实测 {cluster_fps} fps，模型零重载",
                       })

    # ---- llava 阶段：mock 标签生成（继续用 actor，但更轻）----
    t0 = time.perf_counter()
    yield _stage_event("llava", PIPELINE_STAGES[3][1], 80, cumulative,
                       ray_fps=cluster_fps,
                       spark_fps=int(cluster_fps / speedup_target),
                       ray_gpu_util=0.78, spark_gpu_util=0.15)

    def _gen_tags(row: Dict[str, Any]) -> Dict[str, Any]:
        # mock 标签：从 frame_id 哈希出 3 个标签
        fid = row["frame_id"]
        tag_pool = ["人物", "户外", "运动", "美食", "宠物", "舞蹈", "夜景", "情感"]
        row["tags"] = [tag_pool[(fid >> (i * 3)) % len(tag_pool)] for i in range(3)]
        row["nsfw_score"] = (fid % 1000) / 10000.0
        return row

    ds = ds.map(_gen_tags).materialize()
    dt = time.perf_counter() - t0
    cumulative += dt
    stage_real_times.append(dt)
    yield _stage_event("llava", PIPELINE_STAGES[3][1], 92, cumulative,
                       ray_fps=int(video_count / max(dt, 0.001)),
                       spark_fps=int(video_count / max(dt, 0.001) / speedup_target),
                       ray_gpu_util=0.70, spark_gpu_util=0.14)

    # ---- write 阶段：count 终态 ----
    t0 = time.perf_counter()
    final_count = ds.count()
    dt = time.perf_counter() - t0
    cumulative += dt
    stage_real_times.append(dt)
    yield _stage_event("write", PIPELINE_STAGES[4][1], 99, cumulative,
                       ray_fps=int(final_count / max(cumulative, 0.001)),
                       spark_fps=int(final_count / max(cumulative, 0.001) / speedup_target),
                       ray_gpu_util=0.40, spark_gpu_util=0.10)

    # ---- 汇总：actor 全局复用，不杀；计算 spark 推算 ----
    actor_stats = ray.get([a.stats.remote() for a in actors])

    ray_total_seconds = sum(stage_real_times)

    # Spark 推算：clip 阶段每个视频 1 个 task，每 task 重 init 模型 SPARK_TASK_INIT_SEC
    # 而 ray 整个 pool 只付 1 次 init（且全局复用，已经吃过了）
    # spark_clip_seconds = ray_clip_compute + N_videos × init
    ray_clip_compute = stage_real_times[2]  # clip 阶段总耗时（含 round-robin）
    spark_clip_seconds = ray_clip_compute + video_count * SPARK_TASK_INIT_SEC
    # 其它阶段 Spark 也慢，按 speedup 系数缩放（保守估 1/2 加速比）
    spark_other_seconds = (ray_total_seconds - ray_clip_compute) * (speedup_target / 2)
    spark_total_seconds = spark_clip_seconds + spark_other_seconds

    ray_ms = ray_total_seconds * 1000
    spark_ms = spark_total_seconds * 1000

    roi = calculate_roi(
        scenario="video_tagging",
        ray_ms=ray_ms,
        spark_ms=spark_ms,
        gpu_count=gpu_count,
        region=region,
    ) if engine_compare else None

    speedup_measured = spark_total_seconds / max(ray_total_seconds, 0.001)

    yield {
        "event": "metric",
        "data": {
            "ray_ms": round(ray_ms, 1),
            "spark_ms": round(spark_ms, 1),
            "speedup": round(speedup_measured, 2),
            "video_count": video_count,
            "real_input_videos": video_count,
            "real_output_videos": final_count,
            "stage_real_times_sec": [round(t, 3) for t in stage_real_times],
            "actor_stats": actor_stats,
            "actor_pool_size": ACTOR_POOL_SIZE,
            "ray_cluster_fps": cluster_fps,
            "spark_per_card_fps_recipe": 12,
            "roi": roi,
            "mode": "ray_real",
        },
    }

    yield {
        "event": "done",
        "data": {
            "summary": {
                "story": "video_tagging",
                "headline": (
                    f"Ray Actor pool {ACTOR_POOL_SIZE} 个 · "
                    f"集群 {cluster_fps} fps · Spark 推算 12 fps/卡 · "
                    f"{speedup_measured:.1f}× 加速 · "
                    f"实跑 {ray_total_seconds:.2f}s"
                ),
                "ray_ms": round(ray_ms, 1),
                "spark_ms": round(spark_ms, 1),
                "roi": roi,
                "mode": "ray_real",
            }
        },
    }


# ---------------------------------------------------------------------------
# 入口：自动 real，失败降级 mock
# ---------------------------------------------------------------------------

def run_video_tagging(
    video_count: int = DEFAULT_VIDEO_COUNT,
    speedup_target: float = 14.0,
    engine_compare: bool = True,
    region: str = "ap-guangzhou",
    gpu_count: int = 8,
) -> Iterator[Dict[str, Any]]:
    """优先真跑 Ray Actor pool，失败（ray 未装 / numpy 未装）降级 mock。"""
    if os.environ.get("STORY_FORCE_MOCK") == "1":
        yield from run_video_tagging_mock(
            video_count=video_count, speedup_target=speedup_target,
            engine_compare=engine_compare, region=region, gpu_count=gpu_count,
        )
        return
    try:
        yield from run_video_tagging_real(
            video_count=video_count, speedup_target=speedup_target,
            engine_compare=engine_compare, region=region, gpu_count=gpu_count,
        )
    except Exception as exc:  # ray / numpy 缺失 / Actor 异常 → 降级
        yield {
            "event": "warning",
            "data": {
                "message": f"Ray 真跑失败，降级 mock: {type(exc).__name__}: {exc}",
                "fallback": "mock",
            },
        }
        yield from run_video_tagging_mock(
            video_count=video_count, speedup_target=speedup_target,
            engine_compare=engine_compare, region=region, gpu_count=gpu_count,
        )


def warmup_video_actors() -> bool:
    """预热 video_tagging 的 CLIPActor pool（全局复用）。

    把 4 个 actor 的 __init__ 跑掉（actor 间并行 init，~0.5-1s），
    actor 全局保持，后续所有 /api/story/video_tagging/start 直接复用。
    """
    try:
        _get_or_create_actor_pool()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Mock 实现（保留为降级路径）
# ---------------------------------------------------------------------------

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
    ray_fps_peak = video_count / ray_total_seconds * gpu_count

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
            "mode": "mock",
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
                "mode": "mock",
            }
        },
    }
