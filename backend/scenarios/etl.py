"""
数据工程 / ETL 类场景：ETL 特征工程、格式转换、清洗去重、Shuffle 采样。
"""
import os
import tempfile

from .base import BaseScenario
from ._common import _ensure_ray


class ETLScenario(BaseScenario):
    id = "etl"
    title = "8. 大规模 ETL 与特征工程"
    category = "数据工程"
    summary = "清洗 + 字段派生 + 过滤，构造多列特征，演示 map/filter 链式 ETL。"
    purpose = (
        "验证 Ray Data 作为「分布式 ETL 引擎」的能力：典型的 Spark/Pandas 场景，"
        "用 map/filter/groupby 等算子对原始数据做清洗、特征派生、过滤、聚合。"
    )
    logic = (
        "1) range(N) 构造原始数据；"
        "2) map 派生多列特征（平方、奇偶、分桶）—— 一次 map 输出多个新列；"
        "3) filter 剔除 bucket=0 的样本（演示行级过滤）；"
        "4) count 触发实际计算并统计保留行数；"
        "5) take 抽 5 条返回前端预览。整个流程惰性 + 流式，"
        "中间结果不全量物化到内存。"
    )
    vs_spark = (
        "纯 ETL（结构化数据 + 关系算子）恰恰是 Spark 最成熟的领域，Catalyst 优化器、\n"
        "Tungsten 内存布局、SQL 下推都很强。这个场景下 Ray Data 并不一定胜过 Spark，\n"
        "但优势在「与 AI 流水线无缝衔接」：ETL 后的结果不用先写盘、再换集群，"
        "直接接 map_batches 跑模型推理或训练；省掉跨集群搬运是端到端最大的收益点。"
        "对于纯 SQL 重度的 ETL，建议仍用 Spark/Trino，把 Ray 留给下游 AI 段。"
    )
    ray_apis = ["ray.data.range", "map", "filter", "count"]
    params = [
        {"name": "num_rows", "label": "原始行数", "type": "number", "default": 20000},
    ]

    def run(self, params, log, progress):
        import ray.data
        _ensure_ray()
        n = self.get_int(params, "num_rows", 20000)

        log(f"读取 {n} 行原始数据")
        progress(20)
        ds = ray.data.range(n)

        log("map 派生特征：平方 / 奇偶 / 分桶")
        progress(45)
        ds = ds.map(lambda r: {
            "id": r["id"],
            "sq": r["id"] ** 2,
            "is_even": r["id"] % 2 == 0,
            "bucket": r["id"] % 10,
        })

        log("filter 剔除 bucket=0 的样本")
        progress(70)
        ds = ds.filter(lambda r: r["bucket"] != 0)
        kept = ds.count()
        rows = ds.take(5)
        progress(90)
        return {
            "input_rows": n,
            "output_rows": kept,
            "dropped": n - kept,
            "feature_columns": ["id", "sq", "is_even", "bucket"],
            "samples": rows,
        }


class FormatConvertScenario(BaseScenario):
    id = "format_convert"
    title = "9. 数据集格式转换"
    category = "数据工程"
    summary = "Range → 写 Parquet → 读回，演示 read/write connector 流式互转（含机器人数据集场景）。"
    purpose = (
        "验证 Ray Data 的多格式 connector 能力：在 Parquet / JSON / CSV / Lance / Iceberg 等"
        "存储格式之间互转。常见用途：把训练数据从原始 JSON 转成列存 Parquet 提速读取，"
        "或把机器人/RLHF 数据写入 Lance 支持向量检索。"
    )
    logic = (
        "1) range + map 构造源数据集；"
        "2) write_parquet(tmp_dir) 写出多个 Parquet 分片（每个 block 一个文件）；"
        "3) os.listdir 验证物理文件数；"
        "4) read_parquet(tmp_dir) 读回，"
        "重新构造 Dataset；"
        "5) count 校验读回行数 == 写入行数，take 抽样验证字段一致。"
    )
    vs_spark = (
        "Parquet/JSON/CSV 这种通用列存格式，Spark 与 Ray Data 性能在同一量级。\n"
        "差异点在 AI 原生格式：Lance（向量列存）官方主推 Ray Data connector，"
        "Spark 侧需要走第三方插件且生态远不如 Ray 成熟；"
        "Iceberg / Delta 也有 Ray 原生 reader（pyiceberg / deltalake），\n"
        "Ray Data 因此更适合做「AI 数据湖」（向量 + 结构化 + 多模态）的统一入口。"
    )
    ray_apis = ["ray.data.range", "write_parquet", "read_parquet", "count"]
    params = [
        {"name": "num_rows", "label": "行数", "type": "number", "default": 10000},
    ]

    def run(self, params, log, progress):
        import ray.data
        _ensure_ray()
        n = self.get_int(params, "num_rows", 10000)

        tmp = tempfile.mkdtemp(prefix="ray_fmt_")
        log(f"构造 {n} 行数据，写出 Parquet 到 {tmp}")
        progress(25)
        ds = ray.data.range(n).map(lambda r: {"id": r["id"], "value": r["id"] * 2})
        ds.write_parquet(tmp)

        files = [f for f in os.listdir(tmp) if f.endswith(".parquet")]
        log(f"写出 {len(files)} 个 Parquet 分片，读回校验")
        progress(65)
        ds2 = ray.data.read_parquet(tmp)
        read_count = ds2.count()
        rows = ds2.take(3)
        progress(90)
        return {
            "rows_written": n,
            "rows_read_back": read_count,
            "parquet_files": len(files),
            "consistent": read_count == n,
            "path": tmp,
            "samples": rows,
        }


class CleanDedupScenario(BaseScenario):
    id = "clean_dedup"
    title = "10. 数据清洗与去重"
    category = "数据工程"
    summary = "过滤空值/异常，按 key groupby 统计去重后的唯一值数量。"
    purpose = (
        "验证「数据质量治理」算子组合：剔除空值/异常、按业务 key 去重统计。"
        "训练数据的脏数据率会显著影响模型效果，这是上游必跑的预处理。"
    )
    logic = (
        "1) 构造 N 行数据：key 故意取 i%100 让大量重复，且每 50 行掺一个 None；"
        "2) filter(key is not None) 清洗空值脏数据；"
        "3) clean.count() 得到清洗后行数；"
        "4) groupby(\"key\").count() 按 key 分组，每组返回一行；"
        "5) 再 .count() 得到 unique_keys（即不同 key 的数量），实现去重统计。"
    )
    vs_spark = (
        "filter + groupby().count() 这种简单清洗 Spark 很强，Catalyst 能直接生成高效执行计划。\n"
        "Ray Data 在这里没有性能优势，写法上也没那么 SQL-friendly。\n"
        "Ray 的价值在于：清洗后紧接着是「调用模型做语义去重 / 嵌入相似度去重 / 内容审核」，\n"
        "这些 Python+模型逻辑 Spark 表达起来反而别扭。\n"
        "结论：纯结构化清洗用 Spark；模型驱动的清洗（如 LLM 判别脏数据）用 Ray Data。"
    )
    ray_apis = ["ray.data.from_items", "filter", "groupby().count()"]
    params = [
        {"name": "num_rows", "label": "原始行数(含重复)", "type": "number", "default": 10000},
    ]

    def run(self, params, log, progress):
        import ray.data
        _ensure_ray()
        n = self.get_int(params, "num_rows", 10000)

        log(f"构造 {n} 行（key 仅 0~99，故大量重复，并掺入空值）")
        progress(20)
        items = []
        for i in range(n):
            key = None if i % 50 == 0 else i % 100
            items.append({"id": i, "key": key})
        ds = ray.data.from_items(items)

        log("filter 剔除 key 为空的脏数据")
        progress(50)
        clean = ds.filter(lambda r: r["key"] is not None)
        clean_count = clean.count()

        log("groupby(key).count() 统计去重后的唯一 key 数")
        progress(75)
        grouped = clean.groupby("key").count()
        unique_keys = grouped.count()
        progress(90)
        return {
            "input_rows": n,
            "after_clean": clean_count,
            "removed_nulls": n - clean_count,
            "unique_keys": unique_keys,
        }


class ShuffleSampleScenario(BaseScenario):
    id = "shuffle_sample"
    title = "11. 大规模 Shuffle / 采样 / 重分区"
    category = "数据工程"
    summary = "全局随机打散 + 按比例采样 + 重分区，演示训练前的数据洗牌。"
    purpose = (
        "验证「全局 shuffle / 采样 / 重分区」三件套：训练前数据顺序随机化能避免"
        "模型偏置；按比例采样用于快速实验或数据均衡；repartition 控制并发粒度。"
    )
    logic = (
        "1) range(N) 构造顺序数据；"
        "2) random_shuffle() 触发全局 shuffle —— 这是 Ray Data 的重操作，"
        "需要全量物化和跨 block 数据交换；"
        "3) random_sample(frac) 按比例随机采样，比 take(frac*N) 更均匀；"
        "4) repartition(4) 强制重分区为 4 个 block，控制下游算子并发度；"
        "5) num_blocks 验证最终分块数。"
    )
    vs_spark = (
        "全局 Shuffle 是 Spark 的强项（成熟的外排/外存 shuffle、Tungsten 序列化）。"
        "Ray Data 的 shuffle 实现相对年轻，超大规模（>TB）下不一定追平 Spark。\n"
        "但对训练数据洗牌而言，重点是「能直接产出训练 batch」而非「shuffle 本身极致快」：\n"
        "Ray Data shuffle 后可直接 iter_batches 喂给 Ray Train worker，\n"
        "Spark shuffle 后通常还要落盘 Parquet 再读—多一次 I/O。\n"
        "结论：纯 shuffle 性能 Spark 优；端到端「shuffle→喂训练」吞吐 Ray 更顺畅。"
    )
    ray_apis = ["ray.data.range", "random_shuffle", "random_sample", "repartition"]
    params = [
        {"name": "num_rows", "label": "行数", "type": "number", "default": 20000},
        {"name": "fraction", "label": "采样比例(%)", "type": "number", "default": 10},
    ]

    def run(self, params, log, progress):
        import ray.data
        _ensure_ray()
        n = self.get_int(params, "num_rows", 20000)
        pct = self.get_int(params, "fraction", 10, max_v=100)
        frac = pct / 100.0

        log(f"构造 {n} 行")
        progress(20)
        ds = ray.data.range(n)

        log("random_shuffle 全局打散")
        progress(40)
        ds = ds.random_shuffle()
        head_before = [r["id"] for r in ds.take(5)]

        log(f"random_sample 采样 {pct}%")
        progress(60)
        sampled = ds.random_sample(frac)
        sampled_count = sampled.count()

        log("repartition 重分区为 4 个分片")
        progress(80)
        repart = sampled.repartition(4)
        num_blocks = repart.num_blocks()
        progress(90)
        return {
            "input_rows": n,
            "shuffled_head": head_before,
            "sample_fraction": frac,
            "sampled_rows": sampled_count,
            "num_blocks_after_repartition": num_blocks,
        }
