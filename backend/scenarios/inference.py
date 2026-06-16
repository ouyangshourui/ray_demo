"""
推理 / 生成类场景：批量推理、LLM 生成、Embedding、模型评估。
"""
from collections import Counter

from .base import BaseScenario
from ._common import _ensure_ray, _hash_int


class BatchInferenceScenario(BaseScenario):
    id = "batch_inference"
    title = "1. 大规模离线批量推理"
    category = "推理 / 生成"
    summary = "用 Actor 池让模型常驻，map_batches 对文本批量打分（情感分类），CPU 模拟 GPU 推理。"
    purpose = (
        "验证「模型常驻 + 批量推理」模式：模型只加载一次，被多个 batch 复用，"
        "避免每条样本都重启进程/重载模型的巨大开销。这是离线批量推理（如全量打标、"
        "评分、内容审核）的标准做法。"
    )
    logic = (
        "1) from_items 构造 N 条文本样本；"
        "2) 定义 Classifier 类（__call__ 接收一个 batch，写入 label 列）；"
        "3) map_batches(Classifier, concurrency=2) 启动 2 个常驻 Actor，每个 Actor "
        "在第一次被调用时构造一次模型，之后复用；"
        "4) Ray 自动把数据切块、分发给 Actor 池，每个 Actor 串行处理本地 batch；"
        "5) take_all 物化结果，统计标签分布。关键参数：concurrency 决定 Actor 数，"
        "batch_size 决定每次推理多少条（影响 GPU 利用率）。"
    )
    vs_spark = (
        "Spark 的 UDF（包括 Pandas UDF）以「每个 task 一个 Python 进程」为基本单位，"
        "模型权重要么每 task 重新加载，要么通过 broadcast 序列化，难以做到「常驻 + 复用」；"
        "Ray Data 的 map_batches(Class, concurrency=) 直接复用 Ray 的 Actor 模型，"
        "Actor 启动一次、模型加载一次，后续所有 batch 复用同一份显存权重，"
        "在批量推理这类「重初始化、轻每条计算」场景吞吐显著更高。"
        "此外 Spark 原生不感知 GPU，无法把 Actor 钉到 GPU；Ray 调度器一等公民支持 num_gpus。"
    )
    ray_apis = ["ray.data.from_items", "map_batches(Class, concurrency=)", "take_all"]
    params = [
        {"name": "num_rows", "label": "样本数", "type": "number", "default": 2000},
        {"name": "batch_size", "label": "批大小", "type": "number", "default": 64},
    ]

    def run(self, params, log, progress):
        import ray.data
        _ensure_ray()
        n = self.get_int(params, "num_rows", 2000)
        bs = self.get_int(params, "batch_size", 64, max_v=10000)

        log(f"构造 {n} 条文本样本")
        progress(15)
        ds = ray.data.from_items([{"text": f"sample-{i}"} for i in range(n)])

        class Classifier:
            """模拟常驻模型的 Actor（构造一次，复用多个 batch）"""
            def __call__(self, batch):
                labels = ["正面", "负面", "中性"]
                batch["label"] = [labels[_hash_int(t) % 3] for t in batch["text"]]
                return batch

        log(f"map_batches 批量推理（batch_size={bs}, concurrency=2 Actor 池）")
        progress(45)
        ds = ds.map_batches(Classifier, batch_size=bs, concurrency=2,
                            batch_format="pandas")
        rows = ds.take_all()
        progress(85)
        dist = Counter(r["label"] for r in rows)
        return {
            "total": len(rows),
            "batch_size": bs,
            "distribution": dict(dist),
            "samples": rows[:5],
        }


class LLMGenerationScenario(BaseScenario):
    id = "llm_generation"
    title = "2. LLM 批量离线生成 / 数据合成"
    category = "推理 / 生成"
    summary = "批量跑 prompt 生成回答（模拟 LLM），用于合成 SFT 语料。map_batches 模拟多卡生成。"
    purpose = (
        "验证「LLM 离线生成」流水线：把大量 prompt 喂给模型批量产出 completion，"
        "用于构造 SFT/RLHF 训练语料、知识库扩充、自动评测集生成等。"
        "关键诉求是吞吐而非延迟，与在线推理形成鲜明对比。"
    )
    logic = (
        "1) 构造 N 条 prompt 样本；"
        "2) Generator Actor 接收 batch 中的 prompt 列表，逐条「生成」回答（真实场景里"
        "是 vLLM/TGI 的批量 generate 调用）；"
        "3) map_batches(Generator, concurrency=2) 启动 2 个 Actor，"
        "每个 Actor 常驻 1 份模型权重；"
        "4) batch_size 决定每次输入多少条 prompt（关乎 KV cache 利用率）；"
        "5) take_all 取出全部生成结果，可继续 write_parquet 落盘。"
    )
    vs_spark = (
        "LLM 离线生成的瓶颈在 GPU 显存与 KV cache，要求模型常驻 + 大 batch + GPU 调度。"
        "Spark 没有原生 GPU 调度（要靠 Spark RAPIDS 等额外组件），且 PySpark UDF 通过 socket "
        "在 JVM/Python 间传数据，对 LLM 这种大对象 IO 损耗明显；"
        "Ray Data 在同一进程内把 batch 直接交给 Python Actor，零序列化开销，"
        "并能与 vLLM / TGI 这类生产级推理引擎天然集成（社区已有 ray-vllm 范式）。"
    )
    ray_apis = ["ray.data.from_items", "map_batches(Class, concurrency=)", "take"]
    params = [
        {"name": "num_rows", "label": "Prompt 数", "type": "number", "default": 1000},
        {"name": "batch_size", "label": "批大小", "type": "number", "default": 32},
    ]

    def run(self, params, log, progress):
        import ray.data
        _ensure_ray()
        n = self.get_int(params, "num_rows", 1000)
        bs = self.get_int(params, "batch_size", 32, max_v=10000)

        log(f"构造 {n} 条 prompt")
        progress(15)
        ds = ray.data.from_items([{"prompt": f"请解释概念 #{i}"} for i in range(n)])

        class Generator:
            def __call__(self, batch):
                outs = []
                for p in batch["prompt"]:
                    seed = _hash_int(p)
                    tokens = 20 + seed % 80
                    outs.append(f"[生成] 关于「{p}」的回答，约 {tokens} tokens。")
                batch["completion"] = outs
                return batch

        log(f"map_batches 批量生成（batch_size={bs}, concurrency=2）")
        progress(50)
        ds = ds.map_batches(Generator, batch_size=bs, concurrency=2,
                            batch_format="pandas")
        rows = ds.take_all()
        progress(85)
        return {
            "total": len(rows),
            "batch_size": bs,
            "samples": rows[:5],
        }


class EmbeddingScenario(BaseScenario):
    id = "embedding"
    title = "3. Embedding 批量生成"
    category = "推理 / 生成"
    summary = "把海量文档批量编码成向量（模拟 encoder），输出维度统一，可直接写入 Lance/向量库。"
    purpose = (
        "验证「文档 → 向量」的离线编码流水线：为 RAG / 语义检索 / 推荐召回 准备底库。"
        "重点是高吞吐、维度对齐、可直接对接向量库（Lance / Faiss / Milvus）的字段格式。"
    )
    logic = (
        "1) from_items 构造 N 篇文档；"
        "2) Encoder Actor 接收 batch，对每条 text 用 numpy 生成 dim 维向量并 L2 归一化；"
        "3) map_batches(Encoder, concurrency=2) 启动 2 个常驻 encoder Actor；"
        "4) 输出 schema 包含 doc_id + embedding（list[float]）两列，"
        "可直接 ds.write_lance / write_parquet 落库；"
        "5) take 抽样 + count 统计总数返回前端展示。"
    )
    vs_spark = (
        "Embedding 输出是定长向量数组，Spark 的 ArrayType 列在序列化/Parquet 写入时\n"
        "效率不如列存原生张量格式。Ray Data 输出 numpy / Arrow tensor 列后，\n"
        "可直接 write_lance 落到 Lance（向量原生列存），无需 array→string→json 的中间转换；\n"
        "且 encoder 模型通过 Actor 常驻显存，不像 Spark 每 task 重载。"
    )
    ray_apis = ["ray.data.from_items", "map_batches(Class, concurrency=)", "schema"]
    params = [
        {"name": "num_rows", "label": "文档数", "type": "number", "default": 3000},
        {"name": "dim", "label": "向量维度", "type": "number", "default": 128},
    ]

    def run(self, params, log, progress):
        import ray.data
        import numpy as np
        _ensure_ray()
        n = self.get_int(params, "num_rows", 3000)
        dim = self.get_int(params, "dim", 128, max_v=4096)

        log(f"构造 {n} 篇文档")
        progress(15)
        ds = ray.data.from_items([{"doc_id": i, "text": f"doc-{i}"} for i in range(n)])

        class Encoder:
            def __call__(self, batch):
                vecs = []
                for t in batch["text"]:
                    rng = np.random.RandomState(_hash_int(t) % (2**32))
                    v = rng.rand(dim).astype("float32")
                    v = v / (np.linalg.norm(v) + 1e-9)  # L2 归一化
                    vecs.append(v.tolist())
                batch["embedding"] = vecs
                return batch

        log(f"map_batches 编码为 {dim} 维向量（concurrency=2）")
        progress(50)
        ds = ds.map_batches(Encoder, batch_size=128, concurrency=2,
                            batch_format="pandas")
        rows = ds.take(3)
        total = ds.count()
        progress(85)
        return {
            "total": total,
            "dim": dim,
            "note": "向量已 L2 归一化，可直接 write_lance 落库",
            "samples": [{"doc_id": r["doc_id"], "embedding_head": r["embedding"][:4]} for r in rows],
        }


class EvalScoringScenario(BaseScenario):
    id = "eval_scoring"
    title = "4. 模型评估 / 批量打分"
    category = "推理 / 生成"
    summary = "对验证集批量预测并与标签比对，aggregate 汇总准确率等指标。"
    purpose = (
        "验证「批量预测 + 指标聚合」模式：在验证/测试集上跑全量预测并和真实标签比对，"
        "得到准确率、Top-K、AUC 等模型评估指标。这是模型上线前必跑的离线流程。"
    )
    logic = (
        "1) from_items 构造 N 条 (id, label) 样本；"
        "2) map_batches(predict) 批量打标，得到 pred 列；"
        "3) filter(pred == label) 筛选出预测正确的行；"
        "4) count() 触发实际计算（前面都是惰性算子）；"
        "5) accuracy = correct / total，返回最终指标。"
    )
    vs_spark = (
        "评估的核心仍是「模型批量预测」，与场景 1 同源：Spark 难以让模型常驻 + 难调度 GPU。"
        "另外 Ray Data 的算子链是 Python 原生函数对象，开发者可以直接复用训练代码里的 metric "
        "实现（如 sklearn / torchmetrics）；Spark 要把指标计算翻译成 SQL/UDAF 才能下推，\n"
        "对自定义复杂指标（如序列级 BLEU、Top-K 召回）落地成本更高。"
    )
    ray_apis = ["ray.data.from_items", "map_batches", "filter", "count"]
    params = [
        {"name": "num_rows", "label": "验证样本数", "type": "number", "default": 5000},
    ]

    def run(self, params, log, progress):
        import ray.data
        _ensure_ray()
        n = self.get_int(params, "num_rows", 5000)

        log(f"构造 {n} 条带标签验证样本")
        progress(15)
        ds = ray.data.from_items(
            [{"id": i, "label": _hash_int(f"y{i}") % 2} for i in range(n)]
        )

        def predict(batch):
            # 模拟模型预测：约 80% 与真实标签一致
            preds = []
            for i, y in zip(batch["id"], batch["label"]):
                flip = _hash_int(f"p{i}") % 100 < 20
                preds.append(1 - y if flip else y)
            batch["pred"] = preds
            return batch

        log("map_batches 批量预测")
        progress(45)
        ds = ds.map_batches(predict, batch_format="pandas")

        log("filter 统计预测正确的样本")
        progress(70)
        correct = ds.filter(lambda r: r["pred"] == r["label"]).count()
        total = n
        acc = round(correct / total, 4) if total else 0.0
        progress(90)
        return {
            "total": total,
            "correct": correct,
            "accuracy": acc,
        }
