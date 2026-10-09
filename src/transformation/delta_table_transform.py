from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
)

from src.utils.spark import get_spark_session, stop_spark_session


def build_clean_matches_df(raw_matches_df: DataFrame) -> DataFrame:
    """Transform raw matches payload into conformed match dimensions."""
    exploded_df = raw_matches_df.select(
        F.col("season"),
        F.col("team").alias("focus_team"),
        F.explode("matches").alias("match"),
    )

    matches_df = exploded_df.select(
        F.col("match.id").cast(StringType()).alias("match_id"),
        F.col("season"),
        F.to_timestamp(F.col("match.datetime")).alias("match_timestamp"),
        F.col("match.h.title").alias("home_team"),
        F.col("match.a.title").alias("away_team"),
        F.col("match.goals.h").cast(IntegerType()).alias("home_goals"),
        F.col("match.goals.a").cast(IntegerType()).alias("away_goals"),
        F.col("match.xG.h").cast(DoubleType()).alias("home_xg"),
        F.col("match.xG.a").cast(DoubleType()).alias("away_xg"),
        F.col("match.result").alias("match_result"),
    )

    return (
        matches_df.withColumn(
            "is_home",
            F.when(F.col("home_team") == "Real Madrid", F.lit(True)).otherwise(
                F.lit(False)
            ),
        )
        .withColumn(
            "opponent",
            F.when(F.col("is_home"), F.col("away_team")).otherwise(F.col("home_team")),
        )
        .withColumn(
            "goals_for",
            F.when(F.col("is_home"), F.col("home_goals")).otherwise(
                F.col("away_goals")
            ),
        )
        .withColumn(
            "goals_against",
            F.when(F.col("is_home"), F.col("away_goals")).otherwise(
                F.col("home_goals")
            ),
        )
        .withColumn(
            "xg_for",
            F.when(F.col("is_home"), F.col("home_xg")).otherwise(F.col("away_xg")),
        )
        .withColumn(
            "xg_against",
            F.when(F.col("is_home"), F.col("away_xg")).otherwise(F.col("home_xg")),
        )
        .withColumn(
            "delta_xg",
            F.round(F.col("xg_for") - F.col("xg_against"), 2),
        )
    )


def transform_matches(spark, stage1_path: str, stage2_path: str) -> DataFrame:
    """Read raw match JSON files, compute delta metrics, and write Delta format."""
    print("Processing converting raw team data to delta table.")

    raw_matches_df = spark.read.option("multiline", "true").json(
        f"{stage1_path}/*.json"
    )
    clean_matches_df = build_clean_matches_df(raw_matches_df)

    output_target = f"{stage2_path}/dim_matches"
    clean_matches_df.write.format("delta").mode("overwrite").partitionBy("season").save(
        output_target
    )

    print(f"Successfully written table to {output_target}")
    return clean_matches_df


def build_clean_players_df(raw_players_df: DataFrame) -> DataFrame:
    """Transform raw player payload into conformed stats with per-90 metrics."""
    exploded_df = raw_players_df.select(
        F.col("season"),
        F.col("team"),
        F.explode("players").alias("player"),
    )

    players_df = exploded_df.select(
        F.col("player.id").cast(StringType()).alias("player_id"),
        F.col("player.player_name").alias("player_name"),
        F.col("season"),
        F.col("player.games").cast(IntegerType()).alias("games_played"),
        F.col("player.time").cast(IntegerType()).alias("minutes_played"),
        F.col("player.goals").cast(IntegerType()).alias("goals"),
        F.col("player.xG").cast(DoubleType()).alias("xg"),
        F.col("player.assists").cast(IntegerType()).alias("assists"),
        F.col("player.xA").cast(DoubleType()).alias("xa"),
        F.col("player.shots").cast(IntegerType()).alias("shots"),
        F.col("player.key_passes").cast(IntegerType()).alias("key_passes"),
    )

    return (
        players_df.withColumn(
            "goals_per_90",
            F.when(
                F.col("minutes_played") > 0,
                F.round((F.col("goals") / F.col("minutes_played")) * 90, 2),
            ).otherwise(0.0),
        )
        .withColumn(
            "xg_per_90",
            F.when(
                F.col("minutes_played") > 0,
                F.round((F.col("xg") / F.col("minutes_played")) * 90, 2),
            ).otherwise(0.0),
        )
        .withColumn(
            "xa_per_90",
            F.when(
                F.col("minutes_played") > 0,
                F.round((F.col("xa") / F.col("minutes_played")) * 90, 2),
            ).otherwise(0.0),
        )
    )


def transform_players(spark, stage1_path: str, stage2_path: str) -> DataFrame:
    """Read raw player JSON files, compute per-90 metrics, and write Delta."""
    print("Processing player data to proper delta format")

    raw_players_df = spark.read.option("multiline", "true").json(
        f"{stage1_path}/*.json"
    )
    clean_players_df = build_clean_players_df(raw_players_df)

    output_target = f"{stage2_path}/fct_player_stats"
    clean_players_df.write.format("delta").mode("overwrite").partitionBy("season").save(
        output_target
    )

    print(f"Successfully written player stats to {output_target}")
    return clean_players_df


def run_stage1_to_stage2_pipeline():
    spark = get_spark_session("Stage1ToStage2Pipeline")
    try:
        stage1_matches = "data/stage1/matches"
        stage1_players = "data/stage1/players"
        stage2_root = "data/stage2"

        transform_matches(spark, stage1_matches, stage2_root)
        transform_players(spark, stage1_players, stage2_root)
    finally:
        stop_spark_session(spark)


if __name__ == "__main__":
    run_stage1_to_stage2_pipeline()
