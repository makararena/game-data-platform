# Game Data Platform (pipeline)

Python pipeline for generating synthetic game analytics data and loading it into Snowflake. Produces raw CSVs (players, sessions, game events) and ingests them into `GAME_ANALYTICS.RAW` for use by your dbt project (sibling of `game-data-platform/`; see [dbt setup](../instructions/dbt-setup.md)).

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
- `LOAD_BATCH_ID` – batch ID (default: 1). Use `--batch 2` or higher for incremental: **new users, sessions, events** with unique IDs.

### 2. Load into Snowflake

Loads CSVs into three Snowflake tables:

- `RAW_PLAYERS`
- `RAW_SESSIONS`
- `RAW_GAME_EVENTS`

There are **two load modes**:

- **RECREATE** – create or replace the RAW tables and load data (fresh start).
- **APPEND** – keep existing RAW tables and **append only new rows**.

Interactive usage (recommended for students):

```bash
# From this directory (app/)
python ingest/load_to_snowflake.py
```

You will be asked whether you want to **recreate** the tables (first run,
or when you want a full reset) or **append** new data (e.g. later in the
course when testing incremental models).

Non-interactive usage (CI / automation):

```bash
# RECREATE mode: drop & reload RAW tables
python ingest/load_to_snowflake.py --mode recreate

# APPEND mode: keep existing RAW tables and add new rows
python ingest/load_to_snowflake.py --mode append
```

Run from this directory so paths to `data/` resolve correctly. The script
expects the Snowflake `[pandas]` extra for `write_pandas()`.

### 3. Test incremental load (new users, sessions, events)

For incremental dbt testing, generate a **new batch** of users with unique IDs:

```bash
# 1. Initial full load: generate + load (RECREATE)
python app/main.py

# 2. Generate an incremental batch: new users, sessions, events
#    Use --batch 2+ and a later date range; no overlap with previous data
python app/main.py --batch 2 --start 2011-02-13 --end 2011-03-15 --no-ingest

# 3. Append the new batch into Snowflake
python app/ingest/load_to_snowflake.py --mode append
```

For batch 2+, IDs are namespaced: `player_2_1`, `session_2_1`, `event_2_0`, etc., so no duplicates with batch 1.

### 4. Transform with dbt

After data is in Snowflake, run the dbt project (in the parent directory; see [dbt setup](../instructions/dbt-setup.md)) to build staging and marts:

```bash
cd ..   # from game-data-platform/ to parent
cd your_dbt_project   # the folder created by dbt init
source ../venv/bin/activate   # if needed
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
