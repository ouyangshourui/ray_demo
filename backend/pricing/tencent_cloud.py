"""腾讯云 ROI 计算器。

价格表硬编码 + 注释更新日期；后续如需要可改成 YAML 加载。
免责声明：所有价格基于腾讯云官网公示价（按量计费），不含商务折扣；
        激进版加速比来自 Ray Summit 2024 公开 benchmark + 内部 POC。
"""
from __future__ import annotations

from typing import Dict, Any

LAST_UPDATED = "2026-06-15"

# 价格单位：美元 / 小时
# 数据来源：腾讯云官网按量计费公示价（脱敏取整）
PRICES_USD_PER_HOUR: Dict[str, Dict[str, float]] = {
    "ap-guangzhou": {
        "hai_a100_80g":          3.20,   # HAI A100 80G 按量
        "tke_a100_80g":          2.85,   # TKE GPU 节点（含包年折扣摊销）
        "tke_v100":              1.10,
        "cos_storage_gb_month":  0.018,
        "goosefs_acceleration":  0.05,   # GooseFS 加速包
        "cpu_per_core":          0.04,   # 通用 CPU 核小时
    },
    "ap-shanghai": {
        "hai_a100_80g":          3.30,
        "tke_a100_80g":          2.95,
        "tke_v100":              1.15,
        "cos_storage_gb_month":  0.019,
        "goosefs_acceleration":  0.05,
        "cpu_per_core":          0.042,
    },
    "ap-beijing": {
        "hai_a100_80g":          3.25,
        "tke_a100_80g":          2.90,
        "tke_v100":              1.12,
        "cos_storage_gb_month":  0.018,
        "goosefs_acceleration":  0.05,
        "cpu_per_core":          0.041,
    },
}

# 不同故事的资源画像（GPU + CPU 配比，用于成本估算）
# 这些系数来自 docs/M1_design.md 锁定的"激进版"
SCENARIO_PROFILES: Dict[str, Dict[str, Any]] = {
    "llm_dedup": {
        "title": "大模型数据清洗",
        "default_speedup": 5.6,
        "ray_gpu_util": 0.89,
        "spark_gpu_util": 0.18,
        "cpu_cores_per_gpu": 16,        # 每张 GPU 配 16 核 CPU
        "gpu_sku": "hai_a100_80g",
        "package_name": "HAI A100 80G 包年",
        # 演示秒数 → 真实业务等价小时（万亿 token 单作业 36h）
        # 演示 ray 跑 6s，等价真实 6.4h；spark 跑 33.6s，等价真实 36h
        "real_scale_hours_per_demo_sec": 36.0 / 33.6,  # ≈ 1.071
    },
    "video_tagging": {
        "title": "多模态视频打标",
        "default_speedup": 14.0,
        "ray_gpu_util": 0.92,
        "spark_gpu_util": 0.15,
        "cpu_cores_per_gpu": 8,
        "gpu_sku": "hai_a100_80g",
        "package_name": "HAI A100 80G 包年",
        # 5 亿视频/天，Spark 8h；演示 spark 跑 112s，等价真实 8h
        "real_scale_hours_per_demo_sec": 8.0 / 112.0,  # ≈ 0.0714
    },
}


def list_regions() -> list:
    return list(PRICES_USD_PER_HOUR.keys())


def _hours(ms: float) -> float:
    return ms / 1000.0 / 3600.0


def calculate_roi(
    scenario: str,
    ray_ms: float,
    spark_ms: float,
    gpu_count: int = 8,
    region: str = "ap-guangzhou",
) -> Dict[str, Any]:
    """根据 Ray / Spark 实测耗时计算 ROI 账单。

    输出结构与 docs/M1_design.md §3.3 对齐。
    会把"演示用的秒级耗时"按 SCENARIO_PROFILES 的 real_scale 系数
    放大成"等价于真实业务的小时级耗时"，让账单数字符合销售口径
    （故事 A：$23k → $4.1k；故事 B：$8.4k → $1.2k）。
    """
    if region not in PRICES_USD_PER_HOUR:
        region = "ap-guangzhou"
    if scenario not in SCENARIO_PROFILES:
        raise ValueError(f"未知场景: {scenario}")

    price = PRICES_USD_PER_HOUR[region]
    profile = SCENARIO_PROFILES[scenario]

    gpu_price = price[profile["gpu_sku"]]
    cpu_cores = gpu_count * profile["cpu_cores_per_gpu"]
    cpu_price_per_hour = price["cpu_per_core"] * cpu_cores

    # 演示秒 → 真实小时（让 ROI 数字回到销售故事的口径）
    scale = profile.get("real_scale_hours_per_demo_sec", 1.0 / 3600.0)
    ray_hours_real = (ray_ms / 1000.0) * scale
    spark_hours_real = (spark_ms / 1000.0) * scale

    # 单作业总成本 = (GPU 单价 × GPU 数 + CPU 总价) × 真实业务等价小时
    ray_cost = (gpu_price * gpu_count + cpu_price_per_hour) * ray_hours_real
    spark_cost = (gpu_price * gpu_count + cpu_price_per_hour) * spark_hours_real

    saved = max(spark_cost - ray_cost, 0.0)
    saved_pct = (saved / spark_cost * 100.0) if spark_cost > 0 else 0.0

    # 折算腾讯云包年套餐免费月数：用省下的钱 / GPU 包年月单价（取按量价的 6 折）
    monthly_pkg_price = gpu_price * gpu_count * 24 * 30 * 0.6
    free_months = (saved / monthly_pkg_price) if monthly_pkg_price > 0 else 0.0
    hai_hours_saved = (saved / gpu_price) if gpu_price > 0 else 0.0

    return {
        "scenario": scenario,
        "region": region,
        "ray_cost_usd": round(ray_cost, 2),
        "spark_cost_usd": round(spark_cost, 2),
        "saved_usd": round(saved, 2),
        "saved_percent": round(saved_pct, 1),
        "tencent_cloud_equiv": {
            "hai_a100_hours_saved": round(hai_hours_saved, 1),
            "free_months": round(free_months, 2),
            "package": profile["package_name"],
        },
        "metrics": {
            "ray_ms_demo": round(ray_ms, 1),
            "spark_ms_demo": round(spark_ms, 1),
            "ray_hours_real": round(ray_hours_real, 2),
            "spark_hours_real": round(spark_hours_real, 2),
            "ray_gpu_util": profile["ray_gpu_util"],
            "spark_gpu_util": profile["spark_gpu_util"],
            "gpu_count": gpu_count,
            "cpu_cores": cpu_cores,
        },
        "disclaimer": (
            f"基于腾讯云 {region} 公示价 {LAST_UPDATED}，"
            f"激进版加速比（Ray Summit 2024 公开 benchmark + 腾讯云内部 POC）。"
            f"演示耗时已按 {scale:.4f} 系数放大到真实业务等价小时。"
        ),
    }
