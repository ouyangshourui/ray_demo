"""
引擎抽象基类
定义统一的数据处理接口，Ray Data 和 Spark 都实现此接口
方便后续扩展新引擎（如 Dask、Flink 等）
"""
from abc import ABC, abstractmethod


class BaseEngine(ABC):
    """数据处理引擎抽象基类"""

    name = "base"
    display_name = "Base Engine"

    @abstractmethod
    def get_version(self) -> str:
        """返回引擎版本"""
        raise NotImplementedError

    @abstractmethod
    def pipeline(self, num_rows: int, threshold: int, logger=None) -> dict:
        """
        统一数据处理流水线（用于对比）：
        1. 创建 num_rows 行数据 (id, value=id)
        2. Map: value * 2 + 1
        3. Filter: value > threshold
        4. 聚合: count / sum / avg

        返回: {count, sum, avg, elapsed_ms, steps}
        """
        raise NotImplementedError
