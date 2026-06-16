"""
引擎注册中心
方便扩展：新增引擎只需在此注册
"""
from .ray_engine import RayEngine
from .spark_engine import SparkEngine

# 引擎单例注册表
_ENGINES = {
    "ray": RayEngine(),
    "spark": SparkEngine(),
}


def get_engine(name: str):
    """根据名称获取引擎实例"""
    return _ENGINES.get(name)


def list_engines():
    """列出所有已注册引擎"""
    return [
        {"name": e.name, "display_name": e.display_name}
        for e in _ENGINES.values()
    ]
