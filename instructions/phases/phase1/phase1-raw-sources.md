# Phase 1: Raw layer (sources) — Task

This phase defines dbt **sources** for the three raw Snowflake tables loaded by the game-data-platform ingest: `raw_players`, `raw_sessions`, `raw_game_events`. You will use a **separate folder for sources** and **one YAML file per table**.

**Prerequisites:** dbt project created (see [dbt setup](../../dbt-setup.md)), raw tables loaded into Snowflake. There is one schema for raw data; all sources point to it using `target.schema`.

---

## Goals

- **1.1** Create a source named `raw` that points to the three raw tables: `raw_players`, `raw_sessions`, `raw_game_events`. Set database and schema from `target`. Add a short description per table.
- **1.2** Add column definitions for each source table and source tests: `unique` and `not_null` on primary keys, `accepted_values` on difficulty and platform using the [reference data](#reference-data-for-accepted_values) below.

---

## 1. Folder structure

In your dbt project, create a `sources` folder under `models/` and add three YAML files:

```
models/
  sources/
    raw_players.yml
    raw_sessions.yml
    raw_game_events.yml
```

dbt will merge all sources with the same name (`raw`) from these files into one source with three tables.

---

## 2. Source configuration (all three files)

In each file, define the **same** source block so dbt can merge them:

- **name:** `raw`
- **description:** e.g. *Raw game data loaded from CSVs (players, sessions, game events).*
- **database:** `"{{ target.database }}"`
- **schema:** `"{{ target.schema }}"` — one schema holds all raw tables; use the target schema so sources resolve correctly.

---

## 3. Tables and columns to define

**raw_players**

- **Table:** `raw_players`
- **Description:** One row per player; id, first seen, locale, and selected difficulty.
- **Columns:** `player_id`, `first_seen_at`, `country`, `language`, `difficulty_selected`
- **Tests:** `player_id` — `unique`, `not_null`. `difficulty_selected` — `accepted_values` (see reference data).

**raw_sessions**

- **Table:** `raw_sessions`
- **Description:** One row per session; links to player, start/end times, platform.
- **Columns:** `session_id`, `player_id`, `session_start`, `session_end`, `platform`
- **Tests:** `session_id` — `unique`, `not_null`. `player_id` — `not_null`. `platform` — `accepted_values` (see reference data).

**raw_game_events**

- **Table:** `raw_game_events`
- **Description:** One row per game event; event type, time, player, platform, version, optional properties.
- **Columns:** `event_id`, `event_time`, `player_id`, `event_name`, `platform`, `game_version`, `properties`
- **Tests:** `event_id` — `unique`, `not_null`. `player_id` — `not_null`. `platform` — `accepted_values` (see reference data).

---

## Reference data for `accepted_values`

Use these values so your tests match the synthetic pipeline and the main [README](../../../README.md) reference table:

| Field                 | Table(s)                    | Unique values                              |
|-----------------------|-----------------------------|--------------------------------------------|
| **difficulty_selected** | `raw_players`             | `easy`, `normal`, `hard`, `grounded`       |
| **platform**          | `raw_sessions`, `raw_game_events` | `ps5`, `pc`                          |

---

## 4. Verify

From your dbt project root (with venv activated):

```bash
dbt parse
dbt ls --resource-type source
dbt test --select source:raw
```

- **`dbt parse`** — checks that all YAML and SQL parse correctly.
- **`dbt ls --resource-type source`** — lists the `raw` source and its three tables.
- **`dbt test --select source:raw`** — runs all source tests (unique, not_null, accepted_values) on the raw source.

---

## Why this phase matters

- Defining sources with columns and tests turns your raw Snowflake tables into a documented contract for the rest of the project.
- Catching issues (bad values, missing keys) at the source layer keeps downstream models simpler and failures easier to debug.

---

**When you’re done,** see [Phase 1 — Check yourself](phase1-raw-sources-check-yourself.md) for a full solution.
