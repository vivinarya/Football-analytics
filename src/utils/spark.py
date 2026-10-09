from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


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
