import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession

# Ensure PySpark workers on Windows invoke the correct
# virtual environment python executable
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from src.transformation.delta_table_transform import (
    build_clean_matches_df,
    build_clean_players_df,
    run_stage1_to_stage2_pipeline,
    transform_matches,
    transform_players,
)


@pytest.fixture(scope="session")
def spark():
    """Provides a shared local SparkSession for unit tests."""
    session = (
        SparkSession.builder.master("local[1]")
        .appName("TransformationUnitTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_build_clean_matches_df_home_and_away(spark: SparkSession, tmp_path: Path):
    sample_matches = {
        "team": "Real Madrid",
        "season": "2023/2024",
        "extracted_at": "2023-09-02T18:00:00+00:00",
        "matches": [
            {
                "id": "1",
                "datetime": "2023-09-02 16:15:00",
                "h": {"title": "Real Madrid"},
                "a": {"title": "Getafe"},
                "goals": {"h": 2, "a": 1},
                "xG": {"h": 2.10, "a": 0.40},
                "result": "w",
            },
            {
                "id": "2",
                "datetime": "2023-09-17 21:00:00",
                "h": {"title": "Atletico Madrid"},
                "a": {"title": "Real Madrid"},
                "goals": {"h": 3, "a": 1},
                "xG": {"h": 1.50, "a": 1.20},
                "result": "l",
            },
        ],
    }

    test_file = tmp_path / "matches_sample.json"
    with open(test_file, "w", encoding="utf-8") as f:
        json.dump(sample_matches, f)

    raw_df = spark.read.option("multiline", "true").json(str(test_file))
    clean_df = build_clean_matches_df(raw_df)

    rows = clean_df.orderBy("match_id").collect()
    assert len(rows) == 2

    # Match 1: Real Madrid at home
    home_match = rows[0]
    assert home_match["match_id"] == "1"
    assert home_match["is_home"] is True
    assert home_match["opponent"] == "Getafe"
    assert home_match["goals_for"] == 2
    assert home_match["goals_against"] == 1
    assert home_match["xg_for"] == 2.10
    assert home_match["xg_against"] == 0.40
    assert home_match["delta_xg"] == 1.70

    # Match 2: Real Madrid away
    away_match = rows[1]
    assert away_match["match_id"] == "2"
    assert away_match["is_home"] is False
    assert away_match["opponent"] == "Atletico Madrid"
    assert away_match["goals_for"] == 1
    assert away_match["goals_against"] == 3
    assert away_match["xg_for"] == 1.20
    assert away_match["xg_against"] == 1.50
    assert away_match["delta_xg"] == -0.30


def test_build_clean_players_df_per_90_metrics(spark: SparkSession, tmp_path: Path):
    sample_players = {
        "team": "Real Madrid",
        "season": "2023/2024",
        "extracted_at": "2023-09-02T18:00:00+00:00",
        "players": [
            {
                "id": "10",
                "player_name": "Vinicius Junior",
                "games": 10,
                "time": 900,
                "goals": 5,
                "xG": 4.50,
                "assists": 3,
                "xA": 2.70,
                "shots": 25,
                "key_passes": 15,
            },
            {
                "id": "11",
                "player_name": "Bench Player",
                "games": 0,
                "time": 0,
                "goals": 0,
                "xG": 0.00,
                "assists": 0,
                "xA": 0.00,
                "shots": 0,
                "key_passes": 0,
            },
        ],
    }

    test_file = tmp_path / "players_sample.json"
    with open(test_file, "w", encoding="utf-8") as f:
        json.dump(sample_players, f)

    raw_df = spark.read.option("multiline", "true").json(str(test_file))
    clean_df = build_clean_players_df(raw_df)

    rows = clean_df.orderBy("player_id").collect()
    assert len(rows) == 2

    # Active player: 900 minutes (10 90s), 5 goals -> 0.50 per 90
    vini = rows[0]
    assert vini["player_name"] == "Vinicius Junior"
    assert vini["goals_per_90"] == 0.50
    assert vini["xg_per_90"] == 0.45
    assert vini["xa_per_90"] == 0.27

    # Inactive player: 0 minutes -> should return 0.0 without ZeroDivisionError
    bench = rows[1]
    assert bench["player_name"] == "Bench Player"
    assert bench["goals_per_90"] == 0.0
    assert bench["xg_per_90"] == 0.0
    assert bench["xa_per_90"] == 0.0


def test_transform_matches_writes_delta():
    mock_spark = MagicMock()
    mock_raw_df = MagicMock()
    mock_spark.read.option.return_value.json.return_value = mock_raw_df

    mock_clean_df = MagicMock()
    mock_writer = MagicMock()
    (
        mock_clean_df.write.format.return_value.mode.return_value.partitionBy.return_value
    ) = mock_writer

    with patch(
        "src.transformation.delta_table_transform.build_clean_matches_df",
        return_value=mock_clean_df,
    ):
        result_df = transform_matches(mock_spark, "data/stage1/matches", "data/stage2")

        mock_spark.read.option.assert_called_once_with("multiline", "true")
        mock_spark.read.option().json.assert_called_once_with(
            "data/stage1/matches/*.json"
        )
        mock_clean_df.write.format.assert_called_once_with("delta")
        mock_writer.save.assert_called_once_with("data/stage2/dim_matches")
        assert result_df == mock_clean_df


def test_transform_players_writes_delta():
    mock_spark = MagicMock()
    mock_raw_df = MagicMock()
    mock_spark.read.option.return_value.json.return_value = mock_raw_df

    mock_clean_df = MagicMock()
    mock_writer = MagicMock()
    (
        mock_clean_df.write.format.return_value.mode.return_value.partitionBy.return_value
    ) = mock_writer

    with patch(
        "src.transformation.delta_table_transform.build_clean_players_df",
        return_value=mock_clean_df,
    ):
        result_df = transform_players(mock_spark, "data/stage1/players", "data/stage2")

        mock_spark.read.option.assert_called_once_with("multiline", "true")
        mock_spark.read.option().json.assert_called_once_with(
            "data/stage1/players/*.json"
        )
        mock_clean_df.write.format.assert_called_once_with("delta")
        mock_writer.save.assert_called_once_with("data/stage2/fct_player_stats")
        assert result_df == mock_clean_df


@patch("src.transformation.delta_table_transform.get_spark_session")
@patch("src.transformation.delta_table_transform.stop_spark_session")
@patch("src.transformation.delta_table_transform.transform_matches")
@patch("src.transformation.delta_table_transform.transform_players")
def test_run_stage1_to_stage2_pipeline(
    mock_trans_players, mock_trans_matches, mock_stop, mock_get
):
    mock_spark = MagicMock()
    mock_get.return_value = mock_spark

    run_stage1_to_stage2_pipeline()

    mock_get.assert_called_once_with("Stage1ToStage2Pipeline")
    mock_trans_matches.assert_called_once_with(
        mock_spark, "data/stage1/matches", "data/stage2"
    )
    mock_trans_players.assert_called_once_with(
        mock_spark, "data/stage1/players", "data/stage2"
    )
    mock_stop.assert_called_once_with(mock_spark)
