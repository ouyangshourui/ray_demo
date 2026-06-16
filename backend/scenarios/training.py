"""
训练数据供给类场景：最后一公里预处理、多模态解码、数据增强。
"""
from .base import BaseScenario
from ._common import _ensure_ray


class LastMileScenario(BaseScenario):
    id = "last_mile"
    title = "5. 训练数据最后一公里预处理"
    category = "训练供给"
    summary = "训练时边读边解码打包成 batch 喂给 GPU，用 iter_batches 模拟多个 epoch 的迭代。"
    purpose = (
        "验证「数据加载 → 预处理 → 喂训练」最后一公里的流水线能力。"
        "训练 GPU 不能等数据：要求数据侧能持续以 batch 形式产出张量、内存占用恒定、"
        "且能与 PyTorch DataLoader / Ray Train 无缝对接。"
    )
    logic = (
        "1) ray.data.range(N) 构造惰性数据源；"
        "2) map(归一化) 链式接一个轻量预处理算子；"
        "3) 关键：iter_batches(batch_size=BS) 返回一个生成器，"
        "Ray 在后台流式产出 block 并切成训练 batch；"
        "4) 训练循环消费每个 batch（这里只是统计），"
        "实际场景下直接 yield 给 model.forward；"
        "5) 整个过程内存恒定，与数据集总规模无关。"
    )
    vs_spark = (
        "Spark 的设计目标是「批 ETL 出结果集」，不是「持续向训练 worker 喂 batch」。"
        "把 Spark 接到 PyTorch 训练循环要么 collect 到 driver、要么落盘再读，都不流式。"
        "Ray Data 的 iter_batches / iter_torch_batches 直接产出可被 DataLoader 消费的 PyTorch "
        "tensor，并能与 Ray Train 的多 worker 训练共享同一个 Ray 集群、同一份对象内存，"
        "省掉了「ETL 集群 → 训练集群」的数据搬运，端到端延迟更低。"
    )
    ray_apis = ["ray.data.range", "map", "iter_batches(batch_size=)"]
    params = [
        {"name": "num_rows", "label": "样本数", "type": "number", "default": 10000},
        {"name": "batch_size", "label": "训练批大小", "type": "number", "default": 256},
    ]

    def run(self, params, log, progress):
        import ray.data
        _ensure_ray()
        n = self.get_int(params, "num_rows", 10000)
        bs = self.get_int(params, "batch_size", 256, max_v=10000)

        log(f"构造 {n} 行训练数据并做归一化 map")
        progress(20)
        ds = ray.data.range(n).map(lambda r: {"x": r["id"] / n})

        log(f"iter_batches 迭代喂给训练（batch_size={bs}）")
        progress(50)
        n_batches = 0
        seen = 0
        for batch in ds.iter_batches(batch_size=bs):
            n_batches += 1
            seen += len(batch["x"])
        progress(90)
        return {
            "total_samples": seen,
            "batch_size": bs,
            "num_batches": n_batches,
            "note": "实际训练中此迭代器直接对接 Ray Train 的 per-worker 分片",
        }


class MultiModalDecodeScenario(BaseScenario):
    id = "multimodal_decode"
    title = "6. 多模态数据解码"
    category = "训练供给"
    summary = "从原始字节解码成张量（模拟图像解码 + resize），CPU 并行，报告张量形状。"
    purpose = (
        "验证「字节流 → 张量」的并行解码能力。多模态训练（图像/视频/音频）的瓶颈"
        "经常在解码而非模型，需要把解码任务并行化、与训练计算解耦。"
    )
    logic = (
        "1) from_items 构造 N 条「原始字节」样本（用 seed 模拟 JPEG bytes）；"
        "2) decode 函数把每个字节包解成 size×size×3 的张量（模拟 PIL/OpenCV 解码 + resize）；"
        "3) map_batches(decode) 在多个 CPU 进程中并行执行；"
        "4) 仅保留张量均值返回前端（避免传输巨大数组），实际场景输出张量本身；"
        "5) take + count 验证总数和形状一致。"
    )
    vs_spark = (
        "图像/视频/音频解码是 Python 库（PIL、OpenCV、torchvision、decord、ffmpeg-python）的"
        "天下，与 numpy/torch tensor 紧耦合。Spark 跑 Python UDF 必须走「JVM ↔ Python worker」"
        "的 socket 序列化，对二进制大对象（图像 bytes、视频帧）开销显著；"
        "Ray Data 全程 Python + Arrow zero-copy，解码后的张量可直接进入下游 GPU 推理或训练，"
        "不需要二次序列化。这也是各家多模态团队（Anyscale、Adobe、字节等公开案例）"
        "选择 Ray 替代 Spark 的主要原因。"
    )
    ray_apis = ["ray.data.from_items", "map_batches", "take"]
    params = [
        {"name": "num_rows", "label": "图片数", "type": "number", "default": 2000},
        {"name": "size", "label": "目标边长", "type": "number", "default": 64},
    ]

    def run(self, params, log, progress):
        import ray.data
        import numpy as np
        _ensure_ray()
        n = self.get_int(params, "num_rows", 2000)
        size = self.get_int(params, "size", 64, max_v=512)

        log(f"构造 {n} 张「原始字节」图片")
        progress(15)
        ds = ray.data.from_items([{"img_id": i, "raw": i} for i in range(n)])

        def decode(batch):
            tensors = []
            for seed in batch["raw"]:
                rng = np.random.RandomState(int(seed) % (2**32))
                # 模拟解码 + resize 成 size x size x 3
                arr = (rng.rand(size, size, 3) * 255).astype("uint8")
                tensors.append(arr.mean())  # 仅保留均值，避免结果过大
            batch["pixel_mean"] = tensors
            return batch

        log(f"map_batches 解码 + resize 到 {size}x{size}x3（CPU 并行）")
        progress(55)
        ds = ds.map_batches(decode, batch_format="pandas")
        rows = ds.take(3)
        total = ds.count()
        progress(90)
        return {
            "total": total,
            "tensor_shape": [size, size, 3],
            "samples": rows,
        }


class AugmentationScenario(BaseScenario):
    id = "augmentation"
    title = "7. 数据增强 (Augmentation)"
    category = "训练供给"
    summary = "对样本做随机翻转/缩放/抖动等在线增强，map 链式算子，每次执行结果不同。"
    purpose = (
        "验证「在线随机增强」能力：每个 epoch 看到的样本都是不同随机变换的结果，"
        "提升模型泛化。要求增强算子能在数据流水线中并行、轻量、可链式组合。"
    )
    logic = (
        "1) range(N).map 构造基础样本；"
        "2) augment 函数对每个 value 做随机缩放（0.8~1.2x）+ 高斯抖动；"
        "3) map_batches(augment) 在多 worker 并行执行，"
        "由于每次调用使用不同随机种子，结果天然每 epoch 不同；"
        "4) take 抽样观察增强效果。链式增强可以继续 .map(flip).map(crop) 串接。"
    )
    vs_spark = (
        "数据增强本质是训练时在线变换，每个 epoch 都要用不同随机种子重跑，强烈依赖与训练循环"
        "同进程的迭代器。Spark 是「批作业」语义，没有 per-epoch 流式接口，强行让 Spark 充当 "
        "DataLoader 角色会反复触发作业调度开销。Ray Data 的算子图可以被训练循环直接 "
        "iter_batches，每次迭代自然产生新的随机增强结果。"
    )
    ray_apis = ["ray.data.range", "map", "map_batches", "take"]
    params = [
        {"name": "num_rows", "label": "样本数", "type": "number", "default": 4000},
    ]

    def run(self, params, log, progress):
        import ray.data
        import numpy as np
        _ensure_ray()
        n = self.get_int(params, "num_rows", 4000)

        log(f"构造 {n} 个基础样本")
        progress(20)
        ds = ray.data.range(n).map(lambda r: {"id": r["id"], "value": float(r["id"])})

        def augment(batch):
            out = []
            for v in batch["value"]:
                # 随机缩放 + 抖动
                scale = 0.8 + np.random.rand() * 0.4
                jitter = np.random.randn() * 0.01
                out.append(v * scale + jitter)
            batch["augmented"] = out
            return batch

        log("map_batches 随机增强（缩放 + 抖动）")
        progress(55)
        ds = ds.map_batches(augment, batch_format="pandas")
        rows = ds.take(5)
        progress(90)
        return {
            "total": n,
            "note": "在线增强：每个 epoch / 每次执行结果都不同",
            "samples": rows,
        }
