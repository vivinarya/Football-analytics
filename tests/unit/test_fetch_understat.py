import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.ingestion.fetch_understat import fetch_team_data


@pytest.fixture
def mock_understat_client():
    mock_client = MagicMock()
    mock_team_client = MagicMock()

    mock_team_client.get_match_data.return_value = [
        {
            "id": "1001",
            "datetime": "2023-08-12 21:30:00",
            "h": {"title": "Athletic Club"},
            "a": {"title": "Real Madrid"},
            "goals": {"h": 0, "a": 2},
            "xG": {"h": 0.45, "a": 1.52},
            "result": "w",
        }
    ]
    mock_team_client.get_player_data.return_value = [
        {
            "id": "2001",
            "player_name": "Jude Bellingham",
            "games": 1,
            "time": 90,
            "goals": 1,
            "xG": 0.65,
            "assists": 0,
            "xA": 0.12,
            "shots": 2,
            "key_passes": 1,
        }
    ]

    mock_client.team.return_value = mock_team_client
    mock_client.session = MagicMock()
    return mock_client


def test_fetch_team_data_saves_json_files(tmp_path: Path, mock_understat_client):
    seasons = ["2023/2024"]

    saved_files = fetch_team_data(
        seasons=seasons,
        base_dir=tmp_path,
        client=mock_understat_client,
    )

    mock_understat_client.team.assert_called_once_with(team="Real_Madrid")
    mock_understat_client.team().get_match_data.assert_called_once_with(
        season="2023/2024"
    )
    mock_understat_client.team().get_player_data.assert_called_once_with(
        season="2023/2024"
    )

    assert len(saved_files["matches"]) == 1
    assert len(saved_files["players"]) == 1

    matches_path = saved_files["matches"][0]
    players_path = saved_files["players"][0]

    assert matches_path.exists()
    assert players_path.exists()
    assert matches_path.name == "real_madrid_matches_2023_2024.json"
    assert players_path.name == "real_madrid_players_2023_2024.json"

    with open(matches_path, "r", encoding="utf-8") as f:
        matches_payload = json.load(f)

    assert matches_payload["team"] == "Real Madrid"
    assert matches_payload["season"] == "2023/2024"
    assert "extracted_at" in matches_payload
    assert len(matches_payload["matches"]) == 1
    assert matches_payload["matches"][0]["id"] == "1001"

    with open(players_path, "r", encoding="utf-8") as f:
        players_payload = json.load(f)

    assert players_payload["team"] == "Real Madrid"
    assert players_payload["season"] == "2023/2024"
    assert len(players_payload["players"]) == 1
    assert players_payload["players"][0]["player_name"] == "Jude Bellingham"


def test_fetch_team_data_multiple_seasons(tmp_path: Path, mock_understat_client):
    seasons = ["2022", "2023"]

    saved_files = fetch_team_data(
        seasons=seasons,
        base_dir=tmp_path,
        client=mock_understat_client,
    )

    assert len(saved_files["matches"]) == 2
    assert len(saved_files["players"]) == 2


@patch("src.ingestion.fetch_understat.UnderstatClient")
def test_fetch_team_data_closes_session_on_error(mock_client_cls, tmp_path: Path):
    mock_instance = MagicMock()
    mock_instance.team.side_effect = RuntimeError("Network error")
    mock_instance.session = MagicMock()
    mock_client_cls.return_value = mock_instance

    with pytest.raises(RuntimeError, match="Network error"):
        fetch_team_data(seasons=["2023"], base_dir=tmp_path)

    mock_instance.session.close.assert_called_once()
