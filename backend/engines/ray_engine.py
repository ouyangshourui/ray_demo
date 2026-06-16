"""
Ray Data 引擎实现
"""
import time
from .base import BaseEngine


class RayEngine(BaseEngine):
    """Ray Data 数据处理引擎"""

    name = "ray"
    display_name = "Ray Data"

    _initialized = False

    def _ensure_init(self):
        import ray
        if not ray.is_initialized():
            ray.init(ignore_reinit_error=True, logging_level="ERROR")
        RayEngine._initialized = True

    def get_version(self) -> str:
        import ray
        return ray.__version__

    def pipeline(self, num_rows: int, threshold: int, logger=None) -> dict:
        import ray.data
        import pandas as pd

        def log(msg):
            if logger:
                logger(f"[Ray] {msg}")

        self._ensure_init()
        steps = []
        t0 = time.time()

        # 1. 创建数据集
        log(f"创建数据集 {num_rows} 行...")
        df = pd.DataFrame({"value": range(num_rows)})
        ds = ray.data.from_pandas(df)
        steps.append({"step": "创建数据集", "api": "ray.data.from_pandas(df)"})

        # 2. Map
        log("执行 Map: value * 2 + 1")
        ds = ds.map(lambda x: {"value": x["value"] * 2 + 1})
        steps.append({"step": "Map", "api": "ds.map(lambda x: {...})"})

        # 3. Filter
        log(f"执行 Filter: value > {threshold}")
        ds = ds.filter(lambda x: x["value"] > threshold)
        steps.append({"step": "Filter", "api": "ds.filter(lambda x: ...)"})

        # 4. 聚合（单次 action：用 ds.aggregate 一次算完 count/sum/mean，
        #    避免 count()/sum()/mean() 三个独立 action 把上游 map+filter 重算 3 遍）
        log("执行聚合: count / sum / avg（单次 action）")
        from ray.data.aggregate import Count, Sum, Mean
        agg = ds.aggregate(Count(), Sum("value"), Mean("value"))

        # 不同 Ray 版本聚合结果的 key 命名略有差异，按子串匹配取值更健壮
        def _pick(d, keyword):
            for k, v in d.items():
                if keyword in k.lower():
                    return v
            raise KeyError(f"聚合结果缺少 {keyword}，实际 keys={list(d.keys())}")

        count = _pick(agg, "count")
        total = _pick(agg, "sum")
        avg = _pick(agg, "mean")
        steps.append({"step": "聚合", "api": "ds.aggregate(Count(), Sum(), Mean())"})

        elapsed_ms = round((time.time() - t0) * 1000, 1)
        log(f"完成，耗时 {elapsed_ms} ms")

        return {
            "count": int(count),
            "sum": int(total),
            "avg": round(float(avg), 2),
            "elapsed_ms": elapsed_ms,
            "steps": steps,
        }
