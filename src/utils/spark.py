import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

# Ensure Windows finds winutils/Hadoop if present
if "HADOOP_HOME" not in os.environ and os.path.exists(r"C:\hadoop"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    hadoop_bin = r"C:\hadoop\bin"
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

# Ensure PySpark workers on Windows invoke the active virtual environment python
if sys.executable:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def get_spark_session(app_name: str = "FootballDataLakehouse") -> SparkSession:
    """Create and return a SparkSession configured with Delta Lake support."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def stop_spark_session(spark: SparkSession) -> None:
    """Stop the active SparkSession."""
    if spark is not None:
        spark.stop()
