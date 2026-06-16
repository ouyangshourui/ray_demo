"""
Ray Data Demo Server
提供 REST API 供前端调用，执行 Ray Data 演示任务
"""
import os
import sys
import time
import threading
import uuid
from datetime import datetime
from flask import Flask, jsonify, request
from flask_cors import CORS
import ray

# 接入 backend 模块（引擎抽象 + 任务管理器）
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "backend"))
from core.task_manager import task_manager
from engines import get_engine, list_engines
from scenarios import get_scenario, list_scenarios

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
    app.run(host='0.0.0.0', port=15556, debug=True)
