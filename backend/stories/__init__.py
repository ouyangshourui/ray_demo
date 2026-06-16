"""行业故事后端模块。

D1 阶段：提供 mock 流式 runner（用 generator 推进度），让 SSE 通道先跑通；
D2 起把 Ray 真跑 pipeline 接入。
"""
from .llm_dedup import run_llm_dedup, run_llm_dedup_mock, warmup_ray_data
from .video_tagging import (
    run_video_tagging,
    run_video_tagging_mock,
    warmup_video_actors,
)

STORY_RUNNERS = {
    "llm_dedup": run_llm_dedup,           # D1 下午：自动 real，失败降级 mock
    "video_tagging": run_video_tagging,   # D2 上午：Actor pool 真跑，失败降级 mock
}


def get_story_runner(story_id: str):
    return STORY_RUNNERS.get(story_id)


def list_stories():
    return [
        {
            "id": "llm_dedup",
            "title": "大模型数据清洗",
            "subtitle": "Top3 大模型公司，万亿 token 预训练清洗",
            "speedup": 5.6,
            "narrative": (
                "Spark UDF 单作业 36h、$23k；Ray Data 把 dedup(CPU)/score(GPU)/"
                "tokenize(CPU) 拆成异构算子，CPU+GPU 同时打满，模型 Actor 常驻不重 load。"
            ),
        },
        {
            "id": "video_tagging",
            "title": "多模态视频打标",
            "subtitle": "头部短视频平台，5 亿视频/天 NSFW+主体+情感打标",
            "speedup": 14.0,
            "narrative": (
                "Spark 每 task 重 load CLIP 12s，单卡 12 fps，200 张 A100 跑不完；"
                "Ray Data 流式无 barrier，CLIP/LLaVA 做 GPU Actor 常驻，单卡 180 fps。"
            ),
        },
    ]


__all__ = [
    "get_story_runner",
    "list_stories",
    "STORY_RUNNERS",
    "warmup_ray_data",
    "warmup_video_actors",
]
