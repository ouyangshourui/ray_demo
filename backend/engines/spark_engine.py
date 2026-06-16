"""
Spark 引擎实现
使用本地 Spark 3.5.8 环境
"""
import time
from .base import BaseEngine
from config import (
    SPARK_HOME,
    SPARK_APP_NAME,
    SPARK_MASTER,
    SPARK_JAVA_OPTIONS,
    setup_spark_env,
)


class SparkEngine(BaseEngine):
    """Spark 数据处理引擎"""

    name = "spark"
    display_name = "Spark SQL"

    _spark = None

    def _get_session(self):
        """获取或创建 SparkSession（单例）"""
        if SparkEngine._spark is not None:
            return SparkEngine._spark

        setup_spark_env()
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder
            .appName(SPARK_APP_NAME)
            .master(SPARK_MASTER)
            .config("spark.driver.extraJavaOptions", SPARK_JAVA_OPTIONS)
            .config("spark.executor.extraJavaOptions", SPARK_JAVA_OPTIONS)
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        SparkEngine._spark = spark
        return spark

    def get_version(self) -> str:
        try:
            spark = self._get_session()
            return spark.version
        except Exception:
            import pyspark
            return pyspark.__version__

    def pipeline(self, num_rows: int, threshold: int, logger=None) -> dict:
        from pyspark.sql import functions as F

        def log(msg):
            if logger:
                logger(f"[Spark] {msg}")

        log("初始化 SparkSession...")
        spark = self._get_session()

        steps = []
        t0 = time.time()

        # 1. 创建数据集
        log(f"创建数据集 {num_rows} 行...")
        df = spark.range(0, num_rows).withColumnRenamed("id", "value")
        steps.append({"step": "创建数据集", "api": "spark.range(0, n)"})

        # 2. Map (用 SQL 表达式)
        log("执行 Map: value * 2 + 1")
        df = df.withColumn("value", F.col("value") * 2 + 1)
        steps.append({"step": "Map", "api": "df.withColumn('value', col*2+1)"})

        # 3. Filter
        log(f"执行 Filter: value > {threshold}")
        df = df.filter(F.col("value") > threshold)
        steps.append({"step": "Filter", "api": "df.filter(col('value') > t)"})

        # 4. 聚合
        log("执行聚合: count / sum / avg")
        agg = df.agg(
            F.count("value").alias("count"),
            F.sum("value").alias("sum"),
            F.avg("value").alias("avg"),
        ).collect()[0]
        steps.append({"step": "聚合", "api": "df.agg(count, sum, avg)"})

        elapsed_ms = round((time.time() - t0) * 1000, 1)
        log(f"完成，耗时 {elapsed_ms} ms")

        return {
            "count": int(agg["count"]),
            "sum": int(agg["sum"]),
            "avg": round(float(agg["avg"]), 2),
            "elapsed_ms": elapsed_ms,
            "steps": steps,
        }
