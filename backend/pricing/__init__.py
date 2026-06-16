"""腾讯云 ROI 计算器：HAI / TKE-GPU / COS / GooseFS 公示价。

D1 阶段：先用硬编码价格表 + 简单成本模型，让 /api/pricing/calculate 跑通。
后续 D2 接入故事 A/B 的真实耗时输出。
"""
from .tencent_cloud import (
    LAST_UPDATED,
    PRICES_USD_PER_HOUR,
    calculate_roi,
    list_regions,
)

__all__ = [
    "LAST_UPDATED",
    "PRICES_USD_PER_HOUR",
    "calculate_roi",
    "list_regions",
]
