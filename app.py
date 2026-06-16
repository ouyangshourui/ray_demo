"""
Ray Data Demo Server
提供 REST API 供前端调用，执行 Ray Data 演示任务
"""
import os
import sys
import time
import json
import threading
import uuid
from datetime import datetime
from flask import Flask, jsonify, request, Response, stream_with_context
from flask_cors import CORS
import ray

# 接入 backend 模块（引擎抽象 + 任务管理器）
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "backend"))
from core.task_manager import task_manager
from engines import get_engine, list_engines
from scenarios import get_scenario, list_scenarios
from stories import get_story_runner, list_stories, warmup_ray_data
from pricing import calculate_roi, list_regions, LAST_UPDATED as PRICING_LAST_UPDATED

app = Flask(__name__)
CORS(app)


@app.after_request
def add_no_cache_headers(response):
    """禁用 HTML/JS/CSS 缓存，避免前端改动后浏览器仍用旧版本。

    只对页面和静态资源生效；API 的 JSON 响应也加上没有副作用。
    """
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


# 对比任务的数据规模上限（防止资源耗尽）
MAX_COMPARE_ROWS = 5_000_000

# 任务状态存储
TASKS = {}

# Ray 初始化状态
RAY_STATUS = {"initialized": False, "version": None}


@app.route('/')
def index():
    """提供演示页面"""
    return app.send_static_file('index.html')


@app.route('/scenarios')
def scenarios_page():
    """Ray Data 15 场景实验室子页面"""
    return app.send_static_file('scenarios.html')


@app.route('/stories')
def stories_page():
    """M1 行业故事舞台（D3 起前端落地）"""
    # D1 阶段 stories.html 还没建，先回个占位 JSON 提示
    if os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'stories.html')):
        return app.send_static_file('stories.html')
    return jsonify({
        "status": "pending",
        "msg": "stories.html 将在 M1 D3 上午落地，目前只能调 /api/story/* 后端接口",
        "stories": list_stories(),
    })


@app.route('/cards')
def cards_page():
    """M1 销售金句卡轮播（D4 起前端落地）"""
    if os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static', 'cards.html')):
        return app.send_static_file('cards.html')
    return jsonify({
        "status": "pending",
        "msg": "cards.html 将在 M1 D4 落地",
    })


def init_ray():
    """初始化 Ray

    关键：把 backend 目录注入 worker 进程的 PYTHONPATH。
    否则 map_batches 里引用的 scenarios.catalog 模块级函数（如 _hash_int）
    被 cloudpickle 按引用序列化后，worker 进程 import scenarios 会失败
    （No module named 'scenarios'）。
    """
    if not ray.is_initialized():
        backend_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backend")
        ray.init(
            ignore_reinit_error=True,
            runtime_env={"env_vars": {"PYTHONPATH": backend_dir}},
        )
        RAY_STATUS["initialized"] = True
        RAY_STATUS["version"] = ray.__version__


@app.route('/api/health', methods=['GET'])
def health():
    """健康检查"""
    return jsonify({"status": "ok", "ray_initialized": RAY_STATUS["initialized"]})


@app.route('/api/ray/init', methods=['POST'])
def init_ray_api():
    """初始化 Ray"""
    init_ray()
    return jsonify({
        "status": "ok",
        "initialized": RAY_STATUS["initialized"],
        "version": RAY_STATUS["version"]
    })


@app.route('/api/demo1/start', methods=['POST'])
def start_demo1():
    """启动 Demo 1: 数据预处理流水线"""
    init_ray()
    task_id = str(uuid.uuid4())
    TASKS[task_id] = {
        "id": task_id,
        "demo": 1,
        "status": "running",
        "progress": 0,
        "logs": [],
        "result": None,
        "error": None,
        "start_time": datetime.now().isoformat()
    }

    thread = threading.Thread(target=run_demo1, args=(task_id,))
    thread.daemon = True
    thread.start()

    return jsonify({"task_id": task_id, "status": "started"})


def run_demo1(task_id):
    """执行 Demo 1: 数据预处理流水线"""
    try:
        import ray.data
        import pandas as pd

        task = TASKS[task_id]
        def log(msg):
            task["logs"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

        log("开始创建数据集...")
        task["progress"] = 10

        # 创建数据集（使用 Pandas DataFrame 确保数据结构清晰）
        df = pd.DataFrame({"value": range(10000)})
        ds = ray.data.from_pandas(df)
        log(f"数据集创建完成，共 {ds.count()} 条记录")
        task["progress"] = 20

        log("执行 Map 操作: value * 2 + 1...")
        ds = ds.map(lambda x: {"value": x["value"] * 2 + 1})
        task["progress"] = 40

        log("执行 Filter 操作: value > 1000...")
        ds = ds.filter(lambda x: x["value"] > 1000)
        task["progress"] = 60

        log("执行 FlatMap 操作: 每条数据拆分为 3 条...")
        ds = ds.flat_map(lambda x: [
            {"value": x["value"]},
            {"value": x["value"] * 2},
            {"value": x["value"] * 3}
        ])
        task["progress"] = 80

        log("统计结果...")
        result = ds.count()
        task["progress"] = 100

        task["result"] = {
            "total_records": result,
            "sample_data": ds.take(5)
        }
        task["status"] = "completed"

    except Exception as e:
        import traceback
        TASKS[task_id]["status"] = "failed"
        TASKS[task_id]["error"] = str(e)
        TASKS[task_id]["logs"].append(f"错误: {traceback.format_exc()}")


@app.route('/api/demo2/start', methods=['POST'])
def start_demo2():
    """启动 Demo 2: 批量推理"""
    init_ray()
    task_id = str(uuid.uuid4())
    TASKS[task_id] = {
        "id": task_id,
        "demo": 2,
        "status": "running",
        "progress": 0,
        "logs": [],
        "result": None,
        "error": None,
        "start_time": datetime.now().isoformat()
    }

    thread = threading.Thread(target=run_demo2, args=(task_id,))
    thread.daemon = True
    thread.start()

    return jsonify({"task_id": task_id, "status": "started"})


def mock_llm_inference(text: str) -> str:
    """模拟 LLM 推理"""
    import hashlib
    hash_val = int(hashlib.md5(text.encode()).hexdigest()[:8], 16)
    sentiment = "正面" if hash_val % 3 == 0 else ("负面" if hash_val % 3 == 1 else "中性")
    confidence = 0.5 + (hash_val % 50) / 100
    return f"情感: {sentiment}, 置信度: {confidence:.2f}"


def run_demo2(task_id):
    """执行 Demo 2: 批量推理"""
    try:
        import ray.data
        import pandas as pd

        task = TASKS[task_id]
        def log(msg):
            task["logs"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

        log("加载文本数据...")
        task["progress"] = 10

        # 创建示例数据
        texts = [
            "这个产品非常好用，强烈推荐！",
            "服务态度很差，不会再用了",
            "一般般，没有特别的感觉",
            "产品质量优秀，性价比高",
            "物流太慢了，等了好久",
            "界面设计美观，操作流畅",
            "功能不够强大，需要改进",
            "客服很耐心，问题解决了"
        ] * 100  # 扩展数据量

        log(f"共 {len(texts)} 条文本待处理")
        task["progress"] = 20

        log("使用 Ray Data 执行批量推理...")

        # 创建 Ray Dataset
        df = pd.DataFrame({"text": texts})
        ds = ray.data.from_pandas(df)

        task["progress"] = 30

        # 批量推理
        def batch_inference(batch):
            import pandas as pd
            results = []
            for text in batch["text"]:
                results.append(mock_llm_inference(text))
            return pd.DataFrame({
                "text": batch["text"],
                "inference_result": results
            })

        log("开始分布式推理...")
        ds = ds.map_batches(batch_inference, batch_size=10)
        task["progress"] = 70

        log("收集结果...")
        results = ds.take_all()
        task["progress"] = 90

        # 统计
        positive_count = sum(1 for r in results if "正面" in r["inference_result"])
        negative_count = sum(1 for r in results if "负面" in r["inference_result"])
        neutral_count = sum(1 for r in results if "中性" in r["inference_result"])

        task["result"] = {
            "total": len(results),
            "sentiment_distribution": {
                "positive": positive_count,
                "negative": negative_count,
                "neutral": neutral_count
            },
            "sample_results": results[:5]
        }
        task["progress"] = 100
        task["status"] = "completed"

    except Exception as e:
        import traceback
        TASKS[task_id]["status"] = "failed"
        TASKS[task_id]["error"] = str(e)
        TASKS[task_id]["logs"].append(f"错误: {traceback.format_exc()}")


@app.route('/api/demo3/start', methods=['POST'])
def start_demo3():
    """启动 Demo 3: 流式数据加载"""
    init_ray()
    task_id = str(uuid.uuid4())
    TASKS[task_id] = {
        "id": task_id,
        "demo": 3,
        "status": "running",
        "progress": 0,
        "logs": [],
        "result": None,
        "error": None,
        "start_time": datetime.now().isoformat()
    }

    thread = threading.Thread(target=run_demo3, args=(task_id,))
    thread.daemon = True
    thread.start()

    return jsonify({"task_id": task_id, "status": "started"})


def run_demo3(task_id):
    """执行 Demo 3: 流式数据加载"""
    try:
        import ray.data
        import pandas as pd
        import numpy as np

        task = TASKS[task_id]
        def log(msg):
            task["logs"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

        log("创建大规模数据集...")
        task["progress"] = 10

        # 创建较大的数据集
        ds = ray.data.range(50000)
        log(f"数据集大小: {ds.count()} 条记录")
        task["progress"] = 20

        log("配置流式读取参数...")
        task["progress"] = 30

        # 模拟数据预处理
        def process_batch(batch):
            import pandas as pd
            # 模拟数据处理
            processed = [x * 2 for x in batch]
            return pd.DataFrame({"value": processed})

        log("开始流式处理 (预取 + 批处理)...")
        task["progress"] = 40

        # 使用流式 API
        # 配置预取
        ds = ds.map_batches(
            process_batch,
            batch_size=100,
            num_cpus=1
        )

        task["progress"] = 60

        log("统计处理结果...")
        result = ds.sum(on="value")
        
        # 计算处理速度
        task["progress"] = 80
        
        task["result"] = {
            "total_records": ds.count(),
            "sum_value": result,
            "avg_value": ds.mean(on="value"),
            "min_value": ds.min(on="value"),
            "max_value": ds.max(on="value")
        }
        task["progress"] = 100
        task["status"] = "completed"

    except Exception as e:
        import traceback
        TASKS[task_id]["status"] = "failed"
        TASKS[task_id]["error"] = str(e)
        TASKS[task_id]["logs"].append(f"错误: {traceback.format_exc()}")


@app.route('/api/engines', methods=['GET'])
def get_engines():
    """列出已注册的数据处理引擎"""
    return jsonify(list_engines())


@app.route('/api/compare/start', methods=['POST'])
def start_compare():
    """启动 Ray vs Spark 实测对比：同一流水线在两个引擎上各跑一遍"""
    body = request.get_json(silent=True) or {}
    try:
        num_rows = int(body.get("num_rows", 2000))
        threshold = int(body.get("threshold", 100))
    except (TypeError, ValueError):
        return jsonify({"error": "num_rows / threshold 必须为整数"}), 400

    if num_rows <= 0 or threshold < 0:
        return jsonify({"error": "num_rows 必须 > 0，threshold 必须 >= 0"}), 400
    if num_rows > MAX_COMPARE_ROWS:
        return jsonify({"error": f"num_rows 不能超过 {MAX_COMPARE_ROWS}"}), 400

    task_id = task_manager.create_task(
        "compare", {"num_rows": num_rows, "threshold": threshold}
    )
    task_manager.run_async(task_id, run_compare, num_rows, threshold)
    return jsonify({"task_id": task_id, "status": "started"})


def run_compare(task_id, num_rows, threshold):
    """在 Ray 和 Spark 上分别执行统一流水线并对比结果"""
    def log(msg):
        task_manager.log(task_id, msg)

    log(f"对比开始：数据规模 {num_rows} 行，阈值 {threshold}")
    results = {}

    # 1. Ray Data
    log("====== 引擎 1/2：Ray Data ======")
    task_manager.set_progress(task_id, 5)
    ray_engine = get_engine("ray")
    ray_result = ray_engine.pipeline(num_rows, threshold, logger=log)
    ray_result["version"] = ray_engine.get_version()
    results["ray"] = ray_result
    task_manager.set_progress(task_id, 50)

    # 2. Spark SQL
    log("====== 引擎 2/2：Spark SQL ======")
    spark_engine = get_engine("spark")
    spark_result = spark_engine.pipeline(num_rows, threshold, logger=log)
    spark_result["version"] = spark_engine.get_version()
    results["spark"] = spark_result
    task_manager.set_progress(task_id, 95)

    # 3. 一致性校验 + 速度对比
    ray_ms = ray_result["elapsed_ms"]
    spark_ms = spark_result["elapsed_ms"]
    consistent = (
        ray_result["count"] == spark_result["count"]
        and ray_result["sum"] == spark_result["sum"]
    )
    faster = "ray" if ray_ms <= spark_ms else "spark"
    slower_ms = max(ray_ms, spark_ms)
    faster_ms = min(ray_ms, spark_ms)
    speedup = round(slower_ms / faster_ms, 2) if faster_ms > 0 else 1.0

    log(f"结果一致性: {'通过' if consistent else '不一致'}")
    log(f"Ray 耗时 {ray_ms} ms / Spark 耗时 {spark_ms} ms，{faster} 更快 {speedup}x")

    task_manager.complete(task_id, {
        "num_rows": num_rows,
        "threshold": threshold,
        "ray": ray_result,
        "spark": spark_result,
        "consistent": consistent,
        "faster": faster,
        "speedup": speedup,
    })


@app.route('/api/scenarios', methods=['GET'])
def api_scenarios():
    """列出 Ray Data 15 个典型场景的元数据"""
    return jsonify(list_scenarios())


@app.route('/api/scenarios/<scenario_id>/start', methods=['POST'])
def api_scenario_start(scenario_id):
    """启动指定场景的演示任务"""
    scenario = get_scenario(scenario_id)
    if scenario is None:
        return jsonify({"error": f"场景 {scenario_id} 不存在"}), 404

    body = request.get_json(silent=True) or {}
    init_ray()
    task_id = task_manager.create_task(f"scenario:{scenario_id}", body)
    task_manager.run_async(task_id, run_scenario, scenario_id, body)
    return jsonify({"task_id": task_id, "status": "started"})


def run_scenario(task_id, scenario_id, params):
    """在后台线程执行场景"""
    def log(msg):
        task_manager.log(task_id, msg)

    def progress(pct):
        task_manager.set_progress(task_id, pct)

    scenario = get_scenario(scenario_id)
    log(f"场景启动：{scenario.title}")
    progress(5)
    t0 = time.time()
    result = scenario.run(params, log, progress)
    elapsed_ms = round((time.time() - t0) * 1000, 1)
    result["_meta"] = {
        "id": scenario.id,
        "title": scenario.title,
        "ray_apis": scenario.ray_apis,
        "elapsed_ms": elapsed_ms,
    }
    log(f"场景完成，耗时 {elapsed_ms} ms")
    task_manager.complete(task_id, result)


# =============================================================================
# M1 新增：行业故事 + ROI + 金句卡 路由
# =============================================================================

# 故事任务的内存缓存：task_id -> {"runner_kwargs": ..., "status": ...}
# SSE 是单连接长流，不需要持久化，简单 dict 足够
STORY_TASKS = {}


@app.route('/api/stories', methods=['GET'])
def api_stories_list():
    """列出已注册的行业故事元数据。"""
    return jsonify(list_stories())


@app.route('/api/stories/warmup', methods=['POST'])
def api_stories_warmup():
    """预热 Ray Data：消除首次点击的 5s 冷启动。

    路演前应当在浏览器加载完页面时静默触发一次。
    返回 {ok: bool, elapsed_ms: int}。
    """
    t0 = time.perf_counter()
    ok = warmup_ray_data()
    return jsonify({
        "ok": ok,
        "elapsed_ms": int((time.perf_counter() - t0) * 1000),
    })


def _sse_format(event: str, data: dict) -> str:
    """编码 SSE 单条消息。"""
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def _stream_story(story_id: str, kwargs: dict):
    """通用 SSE generator，runner 是无状态的，可直接边跑边推。"""
    runner = get_story_runner(story_id)
    if runner is None:
        yield _sse_format("error", {"msg": f"未知故事 {story_id}"})
        return
    try:
        yield _sse_format("start", {"story": story_id, "params": kwargs})
        for evt in runner(**kwargs):
            yield _sse_format(evt["event"], evt["data"])
    except Exception as e:
        import traceback
        yield _sse_format("error", {
            "msg": str(e),
            "trace": traceback.format_exc(),
        })


def _parse_story_kwargs(body: dict, story_id: str) -> dict:
    """从请求体抽取 runner 需要的参数，做一次安全的类型转换。"""
    out = {
        "speedup_target": float(body.get("speedup_target") or
                                (5.6 if story_id == "llm_dedup" else 14.0)),
        "engine_compare": bool(body.get("engine_compare", True)),
        "region": str(body.get("region") or "ap-guangzhou"),
        "gpu_count": int(body.get("gpu_count") or 8),
    }
    if story_id == "llm_dedup":
        out["token_size"] = int(body.get("token_size") or 1_000_000_000)
    elif story_id == "video_tagging":
        out["video_count"] = int(body.get("video_count") or 1000)
    return out


@app.route('/api/story/<story_id>/start', methods=['POST'])
def api_story_start(story_id):
    """登记一个故事任务，返回 task_id；具体流通过 stream 端点拉取。"""
    if get_story_runner(story_id) is None:
        return jsonify({"error": f"未知故事 {story_id}"}), 404
    body = request.get_json(silent=True) or {}
    try:
        kwargs = _parse_story_kwargs(body, story_id)
    except (TypeError, ValueError) as e:
        return jsonify({"error": f"参数非法: {e}"}), 400

    task_id = str(uuid.uuid4())
    STORY_TASKS[task_id] = {
        "id": task_id,
        "story": story_id,
        "kwargs": kwargs,
        "status": "ready",
        "start_time": datetime.now().isoformat(),
    }
    return jsonify({"task_id": task_id, "status": "started", "kwargs": kwargs})


@app.route('/api/story/<story_id>/<task_id>/stream', methods=['GET'])
def api_story_stream(story_id, task_id):
    """SSE 流：浏览器/curl 直接挂连接读 progress/metric/done 事件。"""
    task = STORY_TASKS.get(task_id)
    if task is None or task["story"] != story_id:
        return jsonify({"error": "task 不存在或与 story 不匹配"}), 404
    task["status"] = "streaming"

    def gen():
        for chunk in _stream_story(story_id, task["kwargs"]):
            yield chunk
        task["status"] = "completed"
        # SSE 终止信号（部分客户端依赖空 event 兜底）
        yield "event: close\ndata: {}\n\n"

    return Response(
        stream_with_context(gen()),
        mimetype='text/event-stream',
        headers={
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no',  # 关闭 nginx 缓冲
        },
    )


@app.route('/api/pricing/calculate', methods=['POST'])
def api_pricing_calculate():
    """ROI 账单接口：输入 ray_ms / spark_ms / gpu_count / region。"""
    body = request.get_json(silent=True) or {}
    try:
        scenario = str(body.get("scenario") or "")
        ray_ms = float(body.get("ray_ms") or 0)
        spark_ms = float(body.get("spark_ms") or 0)
        gpu_count = int(body.get("gpu_count") or 8)
        region = str(body.get("region") or "ap-guangzhou")
    except (TypeError, ValueError) as e:
        return jsonify({"error": f"参数非法: {e}"}), 400

    if ray_ms <= 0 or spark_ms <= 0:
        return jsonify({"error": "ray_ms / spark_ms 必须为正数"}), 400

    try:
        result = calculate_roi(scenario, ray_ms, spark_ms, gpu_count, region)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify(result)


@app.route('/api/pricing/regions', methods=['GET'])
def api_pricing_regions():
    """返回支持的腾讯云地域 + 价格表更新日期。"""
    return jsonify({
        "regions": list_regions(),
        "last_updated": PRICING_LAST_UPDATED,
    })


@app.route('/api/cards/manifest', methods=['GET'])
def api_cards_manifest():
    """5 张销售金句卡的元数据清单（前端 D4 渲染用）。"""
    return jsonify([
        {
            "id": "card1",
            "title": "GPU 不是堆出来的，是用出来的",
            "metric": {"spark": "18%", "ray": "89%"},
            "subtitle": "同样 8 张 A100，Spark 利用率 18% / Ray Actor 复用 89%",
            "duration_sec": 10,
        },
        {
            "id": "card2",
            "title": "模型不该每次冷启动",
            "metric": {"spark": "12s", "ray": "0ms"},
            "subtitle": "Spark UDF 每 task 重 load 模型 12s / Ray Actor 常驻 0ms",
            "duration_sec": 10,
        },
        {
            "id": "card3",
            "title": "长尾决定下班时间",
            "metric": {"spark": "barrier", "ray": "流式"},
            "subtitle": "Spark stage barrier 拖慢全局 / Ray 流式 pipeline 无 barrier",
            "duration_sec": 10,
        },
        {
            "id": "card4",
            "title": "AI 数据是流，不是表",
            "metric": {"spark": "DataFrame", "ray": "Dataset"},
            "subtitle": "60% 非结构化数据，视频/图像/文本流过 DAG，表格挡不住",
            "duration_sec": 10,
        },
        {
            "id": "card5",
            "title": "算账：100 万视频打标",
            "metric": {"spark": "$8.4k", "ray": "$1.2k"},
            "subtitle": "Ray Data 14× 加速，账单一年省 $86k，等于免费用 HAI 4.2 个月",
            "duration_sec": 10,
        },
    ])


# =============================================================================


@app.route('/api/tasks/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """获取任务状态（先查旧的 TASKS，再查 task_manager）"""
    if task_id in TASKS:
        return jsonify(TASKS[task_id])
    task = task_manager.get(task_id)
    if task is not None:
        return jsonify(task)
    return jsonify({"error": "Task not found"}), 404


@app.route('/api/tasks', methods=['GET'])
def list_tasks():
    """列出所有任务（旧 demo 任务 + 对比任务）"""
    demo_tasks = [{
        "id": t["id"],
        "demo": t["demo"],
        "status": t["status"],
        "progress": t["progress"],
        "start_time": t["start_time"]
    } for t in TASKS.values()]
    return jsonify(demo_tasks + task_manager.list_all())


if __name__ == '__main__':
    print("Ray Data Demo Server starting...")
    # 关掉 reloader 避免双进程导致 Ray init 状态错位（预热只暖父进程，请求落子进程）
    # debug=True 仍保留：错误页 + 调试器 PIN 都能用
    app.run(host='0.0.0.0', port=15556, debug=True, use_reloader=False)
