"""
场景共享工具：Ray 初始化 + 通用哈希函数。

注意：
`_hash_int` 被多个场景的 map_batches 闭包以模块级符号引用，
cloudpickle 按 (module_name, qualname) 序列化 → worker 反序列化时
必须能从 `scenarios._common` 导入到同名函数。
为了兼容历史路径（早期版本闭包按 `scenarios.catalog._hash_int` 序列化），
catalog.py 仍 re-export 此函数。
"""
import os
import hashlib


def _ensure_ray():
    import ray
    if not ray.is_initialized():
        # 把 backend 目录注入 worker 的 PYTHONPATH，保证 worker 能 import scenarios
        backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ray.init(
            ignore_reinit_error=True,
            logging_level="ERROR",
            runtime_env={"env_vars": {"PYTHONPATH": backend_dir}},
        )


def _hash_int(s: str) -> int:
    return int(hashlib.md5(str(s).encode()).hexdigest()[:8], 16)
