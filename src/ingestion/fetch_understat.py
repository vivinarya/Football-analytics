from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests


class UnderstatDirectClient:
    """Direct HTTP client for Understat official REST endpoints."""

    BASE_URL = "https://understat.com"

    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "X-Requested-With": "XMLHttpRequest",
            }
        )

    def get_team_data(self, team: str, season: str) -> dict[str, Any]:
        """Fetch raw team payload containing dates, players, and statistics."""
        season_year = season.split("/")[0].strip()
        team_slug = team.replace(" ", "_")
        url = f"{self.BASE_URL}/getTeamData/{team_slug}/{season_year}"
        resp = self.session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def team(self, team: str):
        """Returns a team client compatible with understatapi interface."""
        parent = self

        class _TeamClient:
            def get_match_data(self, season: str) -> list[dict[str, Any]]:
                return parent.get_team_data(team, season).get("dates", [])

            def get_player_data(self, season: str) -> list[dict[str, Any]]:
                return parent.get_team_data(team, season).get("players", [])

        return _TeamClient()

    def close(self) -> None:
        self.session.close()


UnderstatClient = UnderstatDirectClient


def fetch_team_data(
    seasons: list[str],
    base_dir: Path | str = "data/stage1",
    client: Any | None = None,
) -> dict[str, list[Path]]:
    """Fetch match and player data for Real Madrid and save to stage 1 JSON files."""
    base_path = Path(base_dir)
    matches_dir = base_path / "matches"
    players_dir = base_path / "players"

    matches_dir.mkdir(parents=True, exist_ok=True)
    players_dir.mkdir(parents=True, exist_ok=True)

    saved_files: dict[str, list[Path]] = {"matches": [], "players": []}
    close_client = False

    if client is None:
        client = UnderstatClient()
        close_client = True

    try:
        team_client = client.team(team="Real_Madrid")

        for season in seasons:
            season_slug = season.replace("/", "_")
            print(f"Fetching Real Madrid data for season starting {season}...")

            team_matches = team_client.get_match_data(season=season)
            matches_file = matches_dir / f"real_madrid_matches_{season_slug}.json"
            with open(matches_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "team": "Real Madrid",
                        "season": season,
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                        "matches": team_matches,
                    },
                    f,
                    indent=2,
                )
            saved_files["matches"].append(matches_file)
            print(f"Saved {len(team_matches)} matches to Stage 1: {matches_file}")

            team_players = team_client.get_player_data(season=season)
            players_file = players_dir / f"real_madrid_players_{season_slug}.json"
            with open(players_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "team": "Real Madrid",
                        "season": season,
                        "extracted_at": datetime.now(timezone.utc).isoformat(),
                        "players": team_players,
                    },
                    f,
                    indent=2,
                )
            saved_files["players"].append(players_file)
            print(f"Saved {len(team_players)} players to Stage 1: {players_file}")
    finally:
        if close_client:
            if hasattr(client, "session") and client.session:
                client.session.close()
            elif hasattr(client, "close"):
                client.close()

    return saved_files


if __name__ == "__main__":
    seasons = ["2022/2023", "2023/2024", "2024/2025"]
    fetch_team_data(seasons)
