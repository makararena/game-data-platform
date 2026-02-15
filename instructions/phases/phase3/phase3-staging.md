# Phase 3: Staging layer — Task

This phase creates **staging models** that clean and standardize the raw data. Each staging model is a view that selects from a source, applies renames, type casts, and computed columns. You will use a **staging folder** under `models/` and **one SQL file plus schema YAML per model**.

**Prerequisites:** Phase 1 complete (sources defined and tested). Phase 2 complete (macros and multi-schema: vars, `generate_schema_name`, source schema, staging `+schema`). Raw tables loaded in Snowflake. Raw timestamp columns (`first_seen_at`, `session_start`, `session_end`, `event_time`) are stored as `STRING` in Snowflake; use `try_to_timestamp` to cast them.

---

## Why staging and this structure?

- **Staging vs raw:** Raw tables keep the ingest schema (string timestamps, mixed-case column names, quirks). Staging models produce a **clean, typed surface** with consistent naming (`country_code`, `session_start_at`, `event_at`), proper timestamps, and computed fields. Downstream models (dim_players, fct_sessions, etc.) reference staging, not raw—so they never deal with raw quirks.
- **Staging folder under models/:** Staging models are dbt models (SQL + schema YAML). They must live in a directory listed in `model-paths` (e.g. `models/`). A `models/staging/` subfolder groups them logically and lets you run `dbt run --select staging` or `dbt test --select staging` as a unit. With Phase 2 in place, these models are built in the STAGING schema.
- **One SQL + schema per model:** Keeps each model self-contained. The schema YAML documents columns and tests; dbt runs tests when you `dbt test`.

---

## Goals

- **3.1** Create **stg_players**: a view from `source('raw', 'raw_players')`. Output: `player_id`, `first_seen_at` (cast to timestamp), `country_code` (rename from `country`, uppercase), `language_code` (rename from `language`, lowercase), `difficulty_selected` (lowercase). Materialize as view.
- **3.2** Create **stg_sessions**: a view from `source('raw', 'raw_sessions')`. Output: `session_id`, `player_id`, `session_start_at`, `session_end_at` (cast to timestamp; raw columns are `session_start` / `session_end`), `platform` (lowercase), `session_duration_minutes` (computed). Materialize as view.
- **3.3** Create **stg_game_events**: a view from `source('raw', 'raw_game_events')`. Output: `event_id`, `event_at` (cast to timestamp; raw is `event_time`), `player_id`, `event_name` (lowercase), `platform` (lowercase), `game_version`, `properties`. Materialize as view.
- **3.4** For each staging model, add a schema YAML with model description, column descriptions, and tests: `unique` and `not_null` on primary keys, `not_null` on important fields; for `stg_sessions`, add a `relationships` test from `player_id` to `ref('stg_players')`.

---

## 1. Folder structure

Create a `staging` folder under `models/` and add three SQL files plus one schema YAML:

```
models/
  staging/
    stg_players.sql
    stg_sessions.sql
    stg_game_events.sql
    schema.yml
```

You can use one `schema.yml` for all three models (grouped under `models:`) or split into `stg_players.yml`, `stg_sessions.yml`, `stg_game_events.yml`—your choice. The important part is that each model has a `description`, column `description`s, and the required tests.

---

## 2. Model configuration (all three SQL files)

In each model file, set materialization to view:

```sql
{{ config(materialized='view') }}
```

**Why views:** Staging models are lightweight transformations. Views avoid storing duplicate data; they recompute on each query. For small-to-medium datasets this is fine. If you later need tables for performance, you can change to `materialized='table'` per model.

---

## 3. Models to build

### stg_players

- **Source:** `{{ source('raw', 'raw_players') }}`
- **Output columns:** `player_id`, `first_seen_at`, `country_code`, `language_code`, `difficulty_selected`
- **Transformations:**
  - `first_seen_at` — `try_to_timestamp(first_seen_at)` (raw is STRING)
  - `country_code` — `upper(country)` (rename and standardize)
  - `language_code` — `lower(language)` (rename and standardize)
  - `difficulty_selected` — `lower(difficulty_selected)`

**Why these renames:** `country_code` and `language_code` make the semantics clear (codes, not free text). Uppercase for country matches ISO (e.g. `BY`, `DE`); lowercase for language and difficulty matches common conventions and keeps values consistent for grouping/filtering.

### stg_sessions

- **Source:** `{{ source('raw', 'raw_sessions') }}`
- **Output columns:** `session_id`, `player_id`, `session_start_at`, `session_end_at`, `platform`, `session_duration_minutes`
- **Transformations:**
  - `session_start_at` — `try_to_timestamp(session_start)` (raw is STRING)
  - `session_end_at` — `try_to_timestamp(session_end)`
  - `platform` — `lower(platform)`
  - `session_duration_minutes` — `datediff('minute', try_to_timestamp(session_start), try_to_timestamp(session_end))`

**Why session_duration_minutes:** Duration is a core metric for playtime analysis. Computing it once in staging avoids repeating the logic in every downstream model.

### stg_game_events

- **Source:** `{{ source('raw', 'raw_game_events') }}`
- **Output columns:** `event_id`, `event_at`, `player_id`, `event_name`, `platform`, `game_version`, `properties`
- **Transformations:**
  - `event_at` — `try_to_timestamp(event_time)` (raw is STRING)
  - `event_name` — `lower(event_name)`
  - `platform` — `lower(platform)`

**Why keep properties as-is:** `properties` is a VARIANT/JSON payload. Staging passes it through; downstream models can extract specific keys (e.g. `chapter_name`) when needed.

---

## 4. Schema and tests

For each staging model, add to the schema YAML:

- **Model:** `description` (what the model does, grain)
- **Columns:** `description` for each column
- **Tests:**
  - Primary keys (`player_id`, `session_id`, `event_id`): `unique`, `not_null`
  - Foreign keys (`player_id` in sessions/events): `not_null`
  - **stg_sessions only:** `relationships` from `player_id` to `ref('stg_players')` (ensures every session belongs to a known player)

**Why relationships on stg_sessions:** Sessions reference players. The `relationships` test fails if a session has a `player_id` that doesn’t exist in `stg_players`—catching orphaned or bad data early. We test sessions→players because that’s the direct FK; events→players could be added too, but sessions→players is the main referential chain.

---

## 5. Verify

From your dbt project root (with venv activated):

```bash
dbt parse
dbt run --select staging
dbt test --select staging
```

- **`dbt parse`** — checks SQL and YAML parse correctly.
- **`dbt run --select staging`** — builds all three staging views (in the STAGING schema, per Phase 2).
- **`dbt test --select staging`** — runs unique, not_null, and relationships tests on staging models.

---

## Why this phase matters

- Staging creates a **single source of truth** for cleaned data. All downstream models (`dim_players`, `fct_sessions`, etc.) reference `ref('stg_*')`, not raw. Renames and type casts happen once; changes propagate automatically.
- Tests at staging catch **referential and data quality issues** before they reach marts. A failing `relationships` test on `stg_sessions` tells you exactly which rows violate the player→session link.

---

**When you’re done,** see [Phase 3 — Check yourself](phase3-staging-check-yourself.md) for a full solution.
