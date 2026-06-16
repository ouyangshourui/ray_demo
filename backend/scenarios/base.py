"""
场景插件抽象基类
每个 Ray Data 典型场景实现此接口，前端按 meta() 动态渲染卡片。
"""
import inspect
import textwrap
from abc import ABC, abstractmethod

# 单场景样本数上限，防止本地资源耗尽
MAX_ROWS = 500_000


class BaseScenario(ABC):
    """Ray Data 场景抽象基类"""

    id = "base"
    title = "Base Scenario"
    category = "通用"
    summary = ""
    purpose = ""            # 测试目的：这个场景验证什么、为什么需要它
    logic = ""              # 实现逻辑：用了哪些 Ray Data API、关键步骤是什么
    vs_spark = ""           # 相对 Spark 的架构/性能优势（仅写公认事实，不杜撰）
    ray_apis = []           # 本场景实际调用的 Ray Data API 列表
    params = []             # [{"name","label","type":"number/text","default", "min", "max"}]

    def source(self) -> str:
        """返回本场景 run() 方法的真实源码（用于前端代码展示，零维护、始终与实际运行一致）"""
        try:
            src = inspect.getsource(self.run)
            return textwrap.dedent(src)
        except (OSError, TypeError):
            return "# 源码不可用"

    def meta(self) -> dict:
        return {
            "id": self.id,
            "title": self.title,
            "category": self.category,
            "summary": self.summary,
            "purpose": self.purpose,
            "logic": self.logic,
            "vs_spark": self.vs_spark,
            "ray_apis": self.ray_apis,
            "params": self.params,
            "source": self.source(),
        }

    def get_int(self, params: dict, name: str, default: int,
                min_v: int = 1, max_v: int = MAX_ROWS) -> int:
        """安全读取整型参数并做边界校验"""
        try:
            v = int(params.get(name, default))
        except (TypeError, ValueError):
            raise ValueError(f"参数 {name} 必须为整数")
        if v < min_v or v > max_v:
            raise ValueError(f"参数 {name} 必须在 [{min_v}, {max_v}] 范围内")
        return v

    @abstractmethod
    def run(self, params: dict, log, progress) -> dict:
        """
        执行场景。
        log(msg)        追加一条日志
        progress(pct)   更新进度 (0-100)
        返回: 结果 dict（关键指标 + 样本）
        """
        raise NotImplementedError
