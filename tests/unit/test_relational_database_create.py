import os
import sys
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Ensure PySpark workers on Windows invoke the correct virtual environment python
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from src.transformation.relational_database_create import (
    build_player_efficiency,
    build_team_performance,
    compute_player_efficiency,
    compute_team_performance,
    run_stage2_to_stage3_pipeline,
)


@pytest.fixture(scope="session")
def spark():
    """Provides a shared local SparkSession for unit tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("Stage3TransformationUnitTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_compute_team_performance_rolling_metrics(spark: SparkSession):
    schema = StructType(
        [
            StructField("match_id", StringType(), False),
            StructField("season", StringType(), False),
            StructField("match_timestamp", StringType(), False),
            StructField("goals_for", IntegerType(), False),
            StructField("goals_against", IntegerType(), False),
            StructField("xg_for", DoubleType(), False),
            StructField("xg_against", DoubleType(), False),
            StructField("delta_xg", DoubleType(), False),
        ]
    )

    data = [
        # Win: 3 pts
        ("1", "2023/2024", "2023-08-12 19:30:00", 2, 0, 1.80, 0.40, 1.40),
        # Win: 3 pts (rolling 6)
        ("2", "2023/2024", "2023-08-19 19:30:00", 3, 1, 2.20, 0.80, 1.40),
        # Draw: 1 pt (rolling 7)
        ("3", "2023/2024", "2023-08-25 19:30:00", 1, 1, 1.10, 1.10, 0.00),
        # Loss: 0 pts (rolling 7)
        ("4", "2023/2024", "2023-09-02 19:30:00", 0, 2, 0.90, 1.50, -0.60),
        # Win: 3 pts (rolling 10)
        ("5", "2023/2024", "2023-09-17 19:30:00", 2, 1, 1.60, 0.90, 0.70),
    ]

    matches_df = spark.createDataFrame(data, schema)
    result_df = compute_team_performance(matches_df)
    rows = result_df.orderBy("match_timestamp").collect()

    assert len(rows) == 5

    # Match 1: 1 game rolling sum
    assert rows[0]["points"] == 3
    assert rows[0]["rolling_points_5g"] == 3
    assert rows[0]["rolling_xg_for_5g"] == 1.80
    assert rows[0]["rolling_xg_against_5g"] == 0.40
    assert rows[0]["rolling_delta_xg_5g"] == 1.40

    # Match 3: Draw (1 pt)
    assert rows[2]["points"] == 1
    assert rows[2]["rolling_points_5g"] == 7

    # Match 4: Loss (0 pts)
    assert rows[3]["points"] == 0
    assert rows[3]["rolling_points_5g"] == 7

    # Match 5: Full 5-game window (3 + 3 + 1 + 0 + 3 = 10 pts)
    assert rows[4]["points"] == 3
    assert rows[4]["rolling_points_5g"] == 10
    # Avg xg_for over 5 games: (1.8 + 2.2 + 1.1 + 0.9 + 1.6) / 5 = 1.52
    assert rows[4]["rolling_xg_for_5g"] == 1.52
    # Avg xg_against: (0.4 + 0.8 + 1.1 + 1.5 + 0.9) / 5 = 0.94
    assert rows[4]["rolling_xg_against_5g"] == 0.94
    # Avg delta_xg: (1.4 + 1.4 + 0.0 + (-0.6) + 0.7) / 5 = 0.58
    assert rows[4]["rolling_delta_xg_5g"] == 0.58


def test_compute_player_efficiency_metrics(spark: SparkSession):
    schema = StructType(
        [
            StructField("player_id", StringType(), False),
            StructField("player_name", StringType(), False),
            StructField("season", StringType(), False),
            StructField("goals", IntegerType(), False),
            StructField("xg", DoubleType(), False),
            StructField("assists", IntegerType(), False),
            StructField("xa", DoubleType(), False),
            StructField("shots", IntegerType(), False),
            StructField("goals_per_90", DoubleType(), False),
            StructField("xa_per_90", DoubleType(), False),
        ]
    )

    data = [
        # Clinical finisher: 10 goals from 7.5 xG, 5 assists from 4.2 xA, 25 shots
        ("1", "Vinicius Junior", "2023/2024", 10, 7.50, 5, 4.20, 25, 0.65, 0.28),
        # Zero shots player (bench / defender)
        ("2", "Thibaut Courtois", "2023/2024", 0, 0.00, 0, 0.00, 0, 0.00, 0.00),
    ]

    players_df = spark.createDataFrame(data, schema)
    result_df = compute_player_efficiency(players_df)
    rows = result_df.orderBy("player_id").collect()

    assert len(rows) == 2

    vini = rows[0]
    assert vini["player_name"] == "Vinicius Junior"
    assert vini["xg_differential"] == 2.50  # 10 - 7.50
    assert vini["xa_differential"] == 0.80  # 5 - 4.20
    assert vini["shot_conversion_pct"] == 40.0  # (10 / 25) * 100
    assert vini["goal_involvement_per_90"] == 0.93  # 0.65 + 0.28

    courtois = rows[1]
    assert courtois["player_name"] == "Thibaut Courtois"
    assert courtois["xg_differential"] == 0.0
    assert courtois["xa_differential"] == 0.0
    assert courtois["shot_conversion_pct"] == 0.0
    assert courtois["goal_involvement_per_90"] == 0.0


def test_build_team_performance_writes_delta():
    mock_spark = MagicMock()
    mock_matches_df = MagicMock()
    mock_spark.read.format.return_value.load.return_value = mock_matches_df

    mock_perf_df = MagicMock()
    mock_writer = MagicMock()
    (
        mock_perf_df.write.format.return_value.mode.return_value.partitionBy.return_value
    ) = mock_writer

    with patch(
        "src.transformation.relational_database_create.compute_team_performance",
        return_value=mock_perf_df,
    ):
        result_df = build_team_performance(mock_spark, "data/stage2", "data/stage3")

        mock_spark.read.format.assert_called_once_with("delta")
        mock_spark.read.format().load.assert_called_once_with("data/stage2/dim_matches")
        mock_perf_df.write.format.assert_called_once_with("delta")
        mock_writer.save.assert_called_once_with("data/stage3/fct_rma_performance")
        assert result_df == mock_perf_df


def test_build_player_efficiency_writes_delta():
    mock_spark = MagicMock()
    mock_players_df = MagicMock()
    mock_spark.read.format.return_value.load.return_value = mock_players_df

    mock_eff_df = MagicMock()
    mock_writer = MagicMock()
    (
        mock_eff_df.write.format.return_value.mode.return_value.partitionBy.return_value
    ) = mock_writer

    with patch(
        "src.transformation.relational_database_create.compute_player_efficiency",
        return_value=mock_eff_df,
    ):
        result_df = build_player_efficiency(mock_spark, "data/stage2", "data/stage3")

        mock_spark.read.format.assert_called_once_with("delta")
        mock_spark.read.format().load.assert_called_once_with(
            "data/stage2/fct_player_stats"
        )
        mock_eff_df.write.format.assert_called_once_with("delta")
        mock_writer.save.assert_called_once_with("data/stage3/agg_rma_player_p90")
        assert result_df == mock_eff_df


@patch("src.transformation.relational_database_create.get_spark_session")
@patch("src.transformation.relational_database_create.stop_spark_session")
@patch("src.transformation.relational_database_create.build_team_performance")
@patch("src.transformation.relational_database_create.build_player_efficiency")
def test_run_stage2_to_stage3_pipeline(
    mock_player_eff, mock_team_perf, mock_stop, mock_get
):
    mock_spark = MagicMock()
    mock_get.return_value = mock_spark

    run_stage2_to_stage3_pipeline()

    mock_get.assert_called_once_with("Stage2ToStage3Pipeline")
    mock_team_perf.assert_called_once_with(mock_spark, "data/stage2", "data/stage3")
    mock_player_eff.assert_called_once_with(mock_spark, "data/stage2", "data/stage3")
    mock_stop.assert_called_once_with(mock_spark)