"""
场景注册中心。
新增场景：在 catalog.py 写一个 BaseScenario 子类并加入 ALL_SCENARIOS 即可，
前端会按 /api/scenarios 自动渲染卡片。
"""
from .catalog import ALL_SCENARIOS

# id -> scenario 实例
_REGISTRY = {s.id: s for s in ALL_SCENARIOS}


def get_scenario(scenario_id: str):
    """按 id 获取场景实例，不存在返回 None"""
    return _REGISTRY.get(scenario_id)


def list_scenarios():
    """返回所有场景的元数据列表（用于前端渲染）"""
    return [s.meta() for s in ALL_SCENARIOS]
