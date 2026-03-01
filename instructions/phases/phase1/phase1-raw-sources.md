# Phase 1: Raw layer (sources) — Task

This phase defines dbt **sources** for the three raw Snowflake tables loaded by the game-data-platform ingest: `raw_players`, `raw_sessions`, `raw_game_events`. You will use a **separate folder for sources** and **one YAML file per table**.

**Prerequisites:** dbt project created (see [dbt setup](../../dbt-setup.md)), raw tables loaded into Snowflake. There is one schema for raw data; all sources point to it using `target.schema`.

---

## Goals

- **1.1** Create a source named `raw` that points to the three raw tables: `raw_players`, `raw_sessions`, `raw_game_events`. Set database and schema from `target`. Add a short description per table.
- **1.2** Add column definitions for each source table and source tests: `unique` and `not_null` on primary keys, `accepted_values` on difficulty and platform using the [reference data](#reference-data-for-accepted_values) below.

---

## 1. Folder structure

In your dbt project, create a `sources` folder at the **project root** (sibling to `models/`) and add three YAML files:

```
game_dbt_project/
  dbt_project.yml
  models/
    ...
  sources/
    raw_players.yml
    raw_sessions.yml
    raw_game_events.yml
```

dbt will merge all sources with the same name (`raw`) from these files into one source with three tables.

**Important:** dbt only parses YAML (including source definitions) in directories listed in `model-paths` in `dbt_project.yml`. So add `sources` to `model-paths`:

```yaml
model-paths: ["models", "sources"]
```

Otherwise dbt will not see the source definitions in `sources/`.

---

## 2. Source configuration (all three files)

In each file, define the **same** source block so dbt can merge them:

- **name:** `raw`
- **description:** e.g. *Raw game data loaded from CSVs (players, sessions, game events).*
- **database:** `"{{ target.database }}"`
- **schema:** `"{{ target.schema }}"`

**Why the same source block in every file:** dbt treats sources with the same `name` (and same `database`/`schema` when set) as one logical source. Each file adds its *tables* to that source; the source-level keys (name, description, database, schema) must match so dbt doesn’t create duplicate sources.

**Why `target.database` and `target.schema`:** The ingest loads raw tables into the same database/schema as the one you use for dbt (e.g. dev vs prod). Using `target` keeps sources correct when you run `dbt run --target dev` or `--target prod` without hardcoding names. When you have multiple schemas (e.g. RAW for sources, STAGING for staging), use `schema: "{{ var('raw_schema', 'RAW') }}"` in sources and define `raw_schema` in `dbt_project.yml`. Otherwise one schema for all raw tables keeps the setup simple and matches the typical “one raw schema” pattern.

---

## 3. Tables and columns to define

**Why define columns and tests here:** Listing columns documents the contract of each raw table and enables column-level tests. Catching bad data (duplicate keys, nulls where they shouldn’t be, invalid enums) at the source layer means downstream models can assume clean input; when something breaks, the failing test points to the source, not to a complex join.

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

**Why these tests:**

- **`unique` + `not_null` on primary keys:** The primary key (e.g. `player_id`, `session_id`, `event_id`) is the single identifier for a row. If it’s null or duplicated, joins and aggregations give wrong results. Enforcing this at the source prevents subtle bugs downstream.
- **`not_null` on foreign keys (e.g. `player_id` in sessions/events):** Sessions and events must belong to a player. Null `player_id` would break joins and make “per player” metrics incorrect.
- **`accepted_values` on difficulty and platform:** These fields are controlled enums produced by the game and the ingest. Restricting values to the known set (e.g. `easy`, `normal`, `hard`, `grounded` and `ps3`, `xbox360`, `pc`) catches typos, bad CSV data, or schema drift early.

---

## Reference data for `accepted_values`

Use these values so your tests match the synthetic pipeline and the main [README](../../../README.md) reference table:

| Field                 | Table(s)                    | Unique values                              |
|-----------------------|-----------------------------|--------------------------------------------|
| **difficulty_selected** | `raw_players`             | `easy`, `normal`, `hard`, `grounded`       |
| **platform**          | `raw_sessions`, `raw_game_events` | `ps3`, `xbox360`, `pc`               |

**Why use this reference:** The ingest and the main [README](../../../README.md) use the same canonical values. Aligning `accepted_values` with them ensures source tests pass for data produced by the platform and keeps documentation and tests in sync.

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

- Defining sources with columns and tests turns your raw Snowflake tables into a **documented contract** for the rest of the project: anyone can see what raw tables exist, what columns they have, and what guarantees (tests) they satisfy.
- Catching issues (bad values, missing keys) **at the source layer** keeps downstream models simpler (no defensive null checks or enum handling everywhere) and makes failures easier to debug—the test name and column tell you exactly what broke.
- Once this phase is done, later phases can safely reference `source('raw', 'raw_players')` (and the other tables) knowing that the schema and tests are in place.

---

**When you’re done,** see [Phase 1 — Check yourself](phase1-raw-sources-check-yourself.md) for a full solution.
