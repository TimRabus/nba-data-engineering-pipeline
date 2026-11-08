"""Fetch NBA games from balldontlie and export them as a JSON array.

The output can be uploaded to the Databricks Volume read by NBA_01_Bronze.py.
"""

import json
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

API_URL = "https://api.balldontlie.io/v1/games"
OUTPUT_PATH = Path("alle_spiele.json")
MAX_PAGES = 2 

def fetch_games(api_key: str, max_pages: int = MAX_PAGES) -> list[dict]:
    if max_pages < 1:
        raise ValueError("max_pages must be at least 1")

    session = requests.Session()
    session.headers.update({"Authorization": api_key})
    games: list[dict] = []
    cursor = None

    for page_number in range(1, max_pages + 1):
        params = {"per_page": 100}
        if cursor is not None:
            params["cursor"] = cursor

        response = session.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        if not isinstance(payload.get("data"), list):
            raise ValueError("Unexpected API response: 'data' is missing or not a list")

        games.extend(payload["data"])
        print(f"Page {page_number}: loaded {len(payload['data'])} games")

        cursor = payload.get("meta", {}).get("next_cursor")
        if not cursor:
            break

    return games


def main() -> None:
    load_dotenv()
    api_key = os.getenv("NBA_API_KEY")
    if not api_key:
        raise RuntimeError("NBA_API_KEY is missing. Add it to your local .env file.")

    try:
        games = fetch_games(api_key)
    except (requests.exceptions.RequestException, ValueError) as error:
        raise SystemExit(f"NBA API ingestion failed: {error}") from error

    if not games:
        raise SystemExit("NBA API ingestion returned no games; no file written.")

    with OUTPUT_PATH.open("w", encoding="utf-8") as file:
        json.dump(games, file, ensure_ascii=False, indent=2)

    print(f"Saved {len(games)} NBA games to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
