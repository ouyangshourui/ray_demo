"""
流式 / 异构 / 集成类场景：流式加载、CPU+GPU 异构流水线、媒体批处理、训练框架集成。
"""
from .base import BaseScenario
from ._common import _ensure_ray


class StreamingScenario(BaseScenario):
    id = "streaming"
    title = "12. 流式加载超大数据集"
    category = "流式 / 异构 / 集成"
    summary = "惰性 + 流式执行，逐块处理不全量入内存，统计实际物化的 block 数。"
    purpose = (
        "验证「内存恒定的流式执行」能力：处理远超物理内存的数据集（TB / PB 级），"
        "任意时刻只在内存中保留少量 block，边读边算边丢弃。"
    )
    logic = (
        "1) range(N) + map_batches 定义惰性流水线（此时无任何计算）；"
        "2) iter_batches(batch_size=1000) 才真正触发执行，"
        "Ray 在后台调度多个 task 流式产出 block；"
        "3) for 循环逐块消费、累加 sum、统计 batch 数，"
        "处理过的 block 立即被释放，内存占用恒定；"
        "4) 演示了 Ray Data 的核心优势：数据集大小 ≠ 内存需求。"
    )
    vs_spark = (
        "Spark 也是惰性 + 流水线执行，单论「不全量入内存」两者都做得到。\n"
        "差别在执行模型：Spark 以「stage / task」为单位调度，stage 之间通常需要 shuffle 落盘\n"
        "构成屏障；Ray Data 是 streaming executor，算子之间通过 Ray Object Store 流式传递 block，\n"
        "在多段 map_batches 链中能更好地把 CPU/GPU 段流水线重叠（见场景 13）。\n"
        "对于「读 → 解码 → 推理 → 写」这类长链路 AI 流水线，Ray 的端到端吞吐通常更高。"
    )
    ray_apis = ["ray.data.range", "map_batches", "iter_batches", "num_blocks"]
    params = [
        {"name": "num_rows", "label": "行数", "type": "number", "default": 50000},
    ]

    def run(self, params, log, progress):
        import ray.data
        _ensure_ray()
        n = self.get_int(params, "num_rows", 50000)

        log(f"构造 {n} 行（惰性，不立即物化）")
        progress(20)
        ds = ray.data.range(n).map_batches(
            lambda b: {"value": [x * 2 for x in b["id"]]}, batch_format="numpy"
        )

        log("iter_batches 流式逐块消费（边读边算，内存恒定）")
        progress(50)
        total = 0
        blocks = 0
        for batch in ds.iter_batches(batch_size=1000):
            total += int(sum(batch["value"]))
            blocks += 1
        progress(90)
        return {
            "rows": n,
            "sum": total,
            "stream_batches_consumed": blocks,
            "note": "流式执行：任意时刻只驻留少量 block，可处理远超内存的数据集",
        }


class HeterogeneousScenario(BaseScenario):
    id = "heterogeneous"
    title = "13. CPU+GPU 异构流水线"
    category = "流式 / 异构 / 集成"
    summary = "CPU 段预处理 + 「GPU 段」推理两段 map_batches 混排，各自声明资源，调度器自动编排。"
    purpose = (
        "验证「CPU + GPU 异构资源混排」能力：预处理（CPU 密集）和推理（GPU 密集）"
        "分别由不同资源池承担，Ray 调度器自动让两段流水线重叠，避免 GPU 等 CPU。"
    )
    logic = (
        "1) 第 1 段 map_batches(preprocess, num_cpus=1)：声明只占用 CPU，"
        "Ray 把它调度到 CPU 节点；"
        "2) 第 2 段 map_batches(GpuModel, concurrency=2)：本地无 GPU 故用 Actor 池模拟，"
        "生产环境改成 num_gpus=1 即让 Ray 调度到 GPU 节点；"
        "3) 两段算子串接形成流水线，"
        "Ray 自动让 CPU 段提前预处理下一批数据，GPU 段处理当前批，吞吐重叠；"
        "4) take + count 触发执行并验证。"
    )
    vs_spark = (
        "这是 Ray vs Spark 最核心的差异点。Spark 的资源模型只有 executor（CPU + 内存），\n"
        "GPU 调度依赖 Spark 3.x 的 task resource hints + 集群管理器配合，且 GPU 与 CPU 算子\n"
        "无法在同一作业内自然混排——通常被迫拆成两个作业 + 中间落盘。\n"
        "Ray 的资源是一等公民：每个算子可独立声明 num_cpus / num_gpus / custom resources，\n"
        "调度器会把 CPU 段和 GPU 段分别放到对应节点，并让两段流水线时间重叠，\n"
        "GPU 不必等 CPU 段写完盘再读。这是 AI 流水线的「主场优势」。"
    )
    ray_apis = ["map_batches(num_cpus=)", "map_batches(Class, concurrency=)", "take"]
    params = [
        {"name": "num_rows", "label": "样本数", "type": "number", "default": 3000},
    ]

    def run(self, params, log, progress):
        import ray.data
        _ensure_ray()
        n = self.get_int(params, "num_rows", 3000)

        log(f"构造 {n} 条原始样本")
        progress(15)
        ds = ray.data.from_items([{"raw": i} for i in range(n)])

        log("第 1 段（CPU）：map_batches 预处理，num_cpus=1")

        def preprocess(batch):
            batch["feat"] = [x / n for x in batch["raw"]]
            return batch

        progress(40)
        ds = ds.map_batches(preprocess, num_cpus=1, batch_format="pandas")

        log("第 2 段（模拟 GPU）：Actor 池常驻模型推理，concurrency=2")

        class GpuModel:
            def __call__(self, batch):
                batch["score"] = [round(f * 0.7 + 0.1, 4) for f in batch["feat"]]
                return batch

        progress(70)
        # 本地无 GPU，用 concurrency 模拟 GPU Actor 池（生产环境改为 num_gpus=1）
        ds = ds.map_batches(GpuModel, concurrency=2, batch_format="pandas")
        rows = ds.take(5)
        total = ds.count()
        progress(90)
        return {
            "total": total,
            "pipeline": "CPU(num_cpus=1) → GPU(concurrency=2)",
            "note": "生产环境第 2 段改为 num_gpus=1，CPU/GPU 段自动流水线重叠",
            "samples": rows,
        }


class MediaProcessScenario(BaseScenario):
    id = "media_process"
    title = "14. 音频/视频媒体批处理"
    category = "流式 / 异构 / 集成"
    summary = "批量 ASR 转写（模拟）：解码段 + 模型段两阶段处理音频，输出转写文本长度。"
    purpose = (
        "验证「音视频两阶段批处理」流水线：解码（CPU 密集，FFmpeg/Decord）+ "
        "模型推理（GPU 密集，Whisper/ASR）。这是音视频内容理解的标准结构。"
    )
    logic = (
        "1) from_items 构造 N 条「音频」样本（dur 模拟时长）；"
        "2) 解码段 map_batches(decode)：把每条音频「解码」成帧序列（真实场景调 FFmpeg）；"
        "3) 模型段 map_batches(ASR, concurrency=2)：Actor 池常驻 ASR 模型，"
        "对帧序列产出 transcript_len（真实场景产出文本）；"
        "4) take + count 收尾。两段算子在 Ray 中自动流水线化。"
    )
    vs_spark = (
        "音视频处理几乎全是 Python C 扩展库（ffmpeg / decord / torchaudio / whisper），\n"
        "且单个样本是 MB~GB 级二进制对象。Spark JVM↔Python 的 socket 传输对这种大对象\n"
        "代价很高，且 Spark 不能让 ASR 模型常驻 GPU。Ray Data 全链路 Python，\n"
        "解码段（CPU）→ 模型段（GPU Actor 常驻）天然衔接，\n"
        "Anyscale / 字节等公开案例都展示了 Ray 在媒体批处理上数量级的吞吐提升。"
    )
    ray_apis = ["ray.data.from_items", "map_batches", "map_batches(Class)", "take"]
    params = [
        {"name": "num_rows", "label": "音频条数", "type": "number", "default": 1500},
    ]

    def run(self, params, log, progress):
        import ray.data
        _ensure_ray()
        n = self.get_int(params, "num_rows", 1500)

        log(f"构造 {n} 条「音频」样本")
        progress(15)
        ds = ray.data.from_items([{"audio_id": i, "dur": 1 + i % 30} for i in range(n)])

        log("解码段：map_batches 解码音频帧")

        def decode(batch):
            batch["frames"] = [d * 16 for d in batch["dur"]]  # 模拟 16 帧/秒
            return batch

        progress(45)
        ds = ds.map_batches(decode, batch_format="pandas")

        log("模型段：map_batches 模拟 ASR 转写")

        class ASR:
            def __call__(self, batch):
                batch["transcript_len"] = [int(f * 1.2) for f in batch["frames"]]
                return batch

        progress(70)
        ds = ds.map_batches(ASR, concurrency=2, batch_format="pandas")
        rows = ds.take(5)
        total = ds.count()
        progress(90)
        return {
            "total": total,
            "pipeline": "解码 → ASR 转写",
            "samples": rows,
        }


class TrainIntegrationScenario(BaseScenario):
    id = "train_integration"
    title = "15. 与训练框架集成（分片）"
    category = "流式 / 异构 / 集成"
    summary = "train_test_split + split 成多个 worker 分片，模拟 Ray Train 的 per-worker 数据供给。"
    purpose = (
        "验证 Ray Data 与 Ray Train 的「per-worker 分片供给」集成模式：N 张 GPU 训练时，"
        "每个 worker 只看到一份不重叠、大小均衡的数据切片，避免数据重复与丢失。"
    )
    logic = (
        "1) range(N) 构造完整数据集；"
        "2) train_test_split(test_size=0.2) 按比例切训练/验证集；"
        "3) train.split(num_workers, equal=True) 把训练集均匀切成 N 份，"
        "每份是一个独立 Dataset；"
        "4) shard_sizes 验证每个 shard 行数大致相等；"
        "5) 实际训练中每个 Ray Train worker 拿到 shards[i]，"
        "再 .iter_batches() 持续喂给本地 GPU。"
    )
    vs_spark = (
        "Spark 与 PyTorch 集成方案（Petastorm、Spark Torch Distributor 等）通常需要先把 \n"
        "Spark DataFrame 落成 Parquet/TFRecord，再由训练侧 DataLoader 重新读取，\n"
        "数据要跨集群、跨进程、跨格式搬运。\n"
        "Ray Data + Ray Train 在同一个 Ray 集群内通过 ds.split(num_workers, equal=True) \n"
        "直接给每个训练 worker 派发一个不重叠 shard，shard 通过 Ray Object Store 共享内存\n"
        "传递，训练 worker 用 iter_torch_batches 直接拿到 PyTorch tensor，\n"
        "整个链路不落盘、不跨集群，是 Ray 生态最经典的端到端优势场景。"
    )
    ray_apis = ["ray.data.range", "train_test_split", "split", "iter_batches"]
    params = [
        {"name": "num_rows", "label": "样本数", "type": "number", "default": 12000},
        {"name": "num_workers", "label": "训练 worker 数", "type": "number", "default": 4},
    ]

    def run(self, params, log, progress):
        import ray.data
        _ensure_ray()
        n = self.get_int(params, "num_rows", 12000)
        workers = self.get_int(params, "num_workers", 4, max_v=64)

        log(f"构造 {n} 行数据集")
        progress(20)
        ds = ray.data.range(n)

        log("train_test_split 切分训练/验证集 (test=0.2)")
        progress(45)
        train, test = ds.train_test_split(test_size=0.2)
        train_n, test_n = train.count(), test.count()

        log(f"split 成 {workers} 个 worker 分片")
        progress(70)
        shards = train.split(workers, equal=True)
        shard_sizes = [s.count() for s in shards]
        progress(90)
        return {
            "total": n,
            "train_rows": train_n,
            "test_rows": test_n,
            "num_workers": workers,
            "shard_sizes": shard_sizes,
            "note": "每个 shard 对接一个训练 worker，分片大小均衡",
        }
