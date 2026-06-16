"""
场景注册聚合层（薄文件）。

历史背景：早期所有场景写在本文件中，map_batches 闭包通过 cloudpickle 序列化时
按模块路径 `scenarios.catalog._hash_int` 引用模块级符号；为了兼容已存在的 worker
缓存 / 老调用方，保留 `_hash_int` 与 `_ensure_ray` 的 re-export。

新增场景请到 inference.py / training.py / etl.py / pipeline.py 之一编写，
然后在下方 ALL_SCENARIOS 注册（顺序即前端展示顺序）。
"""
# 兼容性 re-export：确保 `from .catalog import _hash_int` 仍可用
# （早期 cloudpickle 闭包按 `scenarios.catalog._hash_int` 序列化）
from ._common import _ensure_ray, _hash_int

from .inference import (
    BatchInferenceScenario,
    LLMGenerationScenario,
    EmbeddingScenario,
    EvalScoringScenario,
)
from .training import (
    LastMileScenario,
    MultiModalDecodeScenario,
    AugmentationScenario,
)
from .etl import (
    ETLScenario,
    FormatConvertScenario,
    CleanDedupScenario,
    ShuffleSampleScenario,
)
from .pipeline import (
    StreamingScenario,
    HeterogeneousScenario,
    MediaProcessScenario,
    TrainIntegrationScenario,
)


# ============================================================
# 注册表（顺序即前端展示顺序）
# ============================================================

ALL_SCENARIOS = [
    # 一、推理 / 生成
    BatchInferenceScenario(),
    LLMGenerationScenario(),
    EmbeddingScenario(),
    EvalScoringScenario(),
    # 二、训练数据供给
    LastMileScenario(),
    MultiModalDecodeScenario(),
    AugmentationScenario(),
    # 三、数据工程 / ETL
    ETLScenario(),
    FormatConvertScenario(),
    CleanDedupScenario(),
    ShuffleSampleScenario(),
    # 四、流式 / 异构 / 集成
    StreamingScenario(),
    HeterogeneousScenario(),
    MediaProcessScenario(),
    TrainIntegrationScenario(),
]


# 显式 __all__：兼容 re-export 的 _ensure_ray / _hash_int 是公共契约，
# 防止静态检查工具把它们当未使用导入。
__all__ = [
    "ALL_SCENARIOS",
    "_ensure_ray",
    "_hash_int",
]
