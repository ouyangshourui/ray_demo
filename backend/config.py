"""
配置中心
集中管理所有可配置项，方便环境切换和功能扩展
"""
import os
import sys

# ============ 服务配置 ============
HOST = "0.0.0.0"
PORT = 5050
DEBUG = True

# ============ Spark 配置 ============
SPARK_HOME = "/Users/ryanou/Documents/spark-3.5.8-bin-hadoop3"
SPARK_APP_NAME = "RayVsSparkDemo"
SPARK_MASTER = "local[*]"

# Spark on Java 17 需要的 JVM 参数（add-opens）
SPARK_JAVA_OPTIONS = (
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
)


def setup_spark_env():
    """设置 Spark 运行环境变量"""
    os.environ["SPARK_HOME"] = SPARK_HOME
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    # 将 Spark 自带的 pyspark 加入路径（确保版本一致）
    pyspark_path = os.path.join(SPARK_HOME, "python")
    if pyspark_path not in sys.path and os.path.exists(pyspark_path):
        sys.path.insert(0, pyspark_path)
