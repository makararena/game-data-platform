# Game Data Platform (pipeline)

Python pipeline for generating synthetic game analytics data and loading it into Snowflake. Produces raw CSVs (players, sessions, game events) and ingests them into `GAME_ANALYTICS.RAW` for use by the [game-dbt-project](../../game-dbt-project) transformations.

This README lives in `app/`. The main project readme (story + tasks) is in the [parent folder](../README.md).

## Structure (inside `app/`)

| Path | Description |
|------|-------------|
| `README.md` | This file. |
| `gen/` | Data generation scripts (players → sessions → events). |
| `ingest/` | Snowflake loader: reads CSVs from `data/` and writes to `RAW_*` tables. |
| `notebooks/` | Jupyter notebooks for inspecting and exploring the generated data. |
| `data/` | Output directory for raw CSVs (created by `gen/`, consumed by `ingest/`). Created at runtime. |

## Prerequisites

- Python 3.10+
- Snowflake account (for ingest)
- Environment variables or `.env` for Snowflake credentials (see below)

## Setup

```bash
# From this directory (app/)
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
pip install "snowflake-connector-python[pandas]"  # required for ingest
```

Create a `.env` in this directory (or set env vars) for the ingest step:

```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=GAME_ANALYTICS
SNOWFLAKE_SCHEMA=RAW
```

## Usage

### 1. Generate data

Runs in order: `players.py` → `sessions.py` → `events.py`. Outputs go to `data/` (in this folder).

From the parent folder (`game-data-platform/`):

```bash
python app/main.py
```

Or from this directory (`app/`):

```bash
python main.py
```

Config (edit in `main.py` or pass via env):

- `N_PLAYERS` – number of players (default: 2000)
- `MAX_SESSIONS_PER_PLAYER` – max sessions per player (default: 25)
- `GAME_VERSION` – version string for events (default: `1.0.3`)
- `GAME_DATA_SEED` – random seed (default: `42`). Same seed ensures **every run produces the same data** for all users, so everyone can compare dbt results on identical inputs.

### 2. Load into Snowflake

Creates or replaces `RAW_PLAYERS`, `RAW_SESSIONS`, and `RAW_GAME_EVENTS` in the configured database/schema and loads from `data/raw_players.csv`, `data/raw_sessions.csv`, `data/raw_game_events.csv`.

```bash
# From this directory (app/)
python ingest/load_to_snowflake.py
```

Run from this directory so paths to `data/` resolve correctly. The script expects the Snowflake `[pandas]` extra for `write_pandas()`.

### 3. Transform with dbt

After data is in Snowflake, run the dbt project (sibling of `game-data-platform/`) to build staging and marts:

```bash
cd ../../game-dbt-project   # from app/
dbt build
```

## Output tables (Snowflake)

| Table | Source file (in this folder) | Description |
|-------|-----------------------------|-------------|
| `RAW_PLAYERS` | `data/raw_players.csv` | Player id, first seen, country, language, difficulty |
| `RAW_SESSIONS` | `data/raw_sessions.csv` | Session id, player id, start/end, platform |
| `RAW_GAME_EVENTS` | `data/raw_game_events.csv` | Event id, time, player, event name, platform, version, properties (VARIANT) |

## License

Internal / project-specific.
