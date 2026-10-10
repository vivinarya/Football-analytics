from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.utils.spark import get_spark_session, stop_spark_session


def compute_team_performance(matches_df: DataFrame) -> DataFrame:
    """Computes points and 5-game rolling averages for team performance."""
    points_df = matches_df.withColumn(
        "points",
        F.when(F.col("goals_for") > F.col("goals_against"), 3)
        .when(F.col("goals_for") == F.col("goals_against"), 1)
        .otherwise(0),
    )

    window_5g = (
        Window.partitionBy("season").orderBy("match_timestamp").rowsBetween(-4, 0)
    )

    return (
        points_df.withColumn("rolling_points_5g", F.sum("points").over(window_5g))
        .withColumn(
            "rolling_xg_for_5g",
            F.round(F.avg("xg_for").over(window_5g), 2),
        )
        .withColumn(
            "rolling_xg_against_5g",
            F.round(F.avg("xg_against").over(window_5g), 2),
        )
        .withColumn(
            "rolling_delta_xg_5g",
            F.round(F.avg("delta_xg").over(window_5g), 2),
        )
    )


def build_team_performance(spark, stage2_path: str, stage3_path: str) -> DataFrame:
    """Reads dim_matches Delta table, computes rolling stats, and writes table."""
    print("Converting stage 2 matches to stage 3 team performance...")
    matches_df = spark.read.format("delta").load(f"{stage2_path}/dim_matches")
    team_performance_df = compute_team_performance(matches_df)

    output_target = f"{stage3_path}/fct_rma_performance"
    team_performance_df.write.format("delta").mode("overwrite").partitionBy(
        "season"
    ).save(output_target)
    print(f"Successfully saved: {output_target}")
    return team_performance_df


def compute_player_efficiency(players_df: DataFrame) -> DataFrame:
    """Computes finishing differentials, conversion %, and involvement per 90."""
    return (
        players_df.withColumn(
            "xg_differential",
            F.round(F.col("goals") - F.col("xg"), 2),
        )
        .withColumn(
            "xa_differential",
            F.round(F.col("assists") - F.col("xa"), 2),
        )
        .withColumn(
            "shot_conversion_pct",
            F.when(
                F.col("shots") > 0,
                F.round((F.col("goals") / F.col("shots")) * 100, 1),
            ).otherwise(0.0),
        )
        .withColumn(
            "goal_involvement_per_90",
            F.round(F.col("goals_per_90") + F.col("xa_per_90"), 2),
        )
    )


def build_player_efficiency(spark, stage2_path: str, stage3_path: str) -> DataFrame:
    """Reads fct_player_stats Delta table, computes metrics, and writes table."""
    print("Building stage 3 player efficiency table...")
    players_df = spark.read.format("delta").load(f"{stage2_path}/fct_player_stats")
    player_efficiency_df = compute_player_efficiency(players_df)

    output_target = f"{stage3_path}/agg_rma_player_p90"
    player_efficiency_df.write.format("delta").mode("overwrite").partitionBy(
        "season"
    ).save(output_target)
    print(f"Successfully saved: {output_target}")
    return player_efficiency_df


def run_stage2_to_stage3_pipeline() -> None:
    """Runs the stage 2 to stage 3 transformation pipeline."""
    spark = get_spark_session("Stage2ToStage3Pipeline")
    try:
        stage2_root = "data/stage2"
        stage3_root = "data/stage3"

        build_team_performance(spark, stage2_root, stage3_root)
        build_player_efficiency(spark, stage2_root, stage3_root)
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    run_stage2_to_stage3_pipeline()
