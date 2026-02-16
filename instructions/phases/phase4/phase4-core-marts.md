# Phase 4: Core marts — Task

This phase creates **core mart models**: a dimension (`dim_players`) and two fact tables (`fct_sessions`, `fct_game_events`). These are materialized tables that join staging data, add session and event aggregates, and become the main interface for analytics. You will use a **marts folder** under `models/` and **one SQL file plus schema YAML per model**.

**Prerequisites:** Phase 1 complete (sources defined). Phase 2 complete (macros and multi-schema). Phase 3 complete (staging models: stg_players, stg_sessions, stg_game_events). Raw tables loaded in Snowflake.

---

## Why core marts and this structure?

- **Dimensions vs facts:** A **dimension** (e.g. `dim_players`) describes entities—one row per player with attributes and aggregated session metrics. **Fact tables** (e.g. `fct_sessions`, `fct_game_events`) describe events or transactions—one row per session or per event, with foreign keys to dimensions and measures.
- **Marts folder:** Core marts live in `models/marts/` (or `models/marts/core/`). With Phase 2, add a `marts` var and `+schema: marts` so these models build in the MARTS schema.
- **Materialized tables:** Marts are materialized as tables (not views) because they are heavier joins and aggregations; tables give faster query performance for dashboards and reports.

---

## Goals

- **4.1** Create **dim_players** as a materialized table. Base: select all columns from `ref('stg_players')` (one row per player).
- **4.2** Add a CTE that aggregates **stg_sessions** per player: `total_sessions`, `total_playtime_minutes`, `avg_session_duration_minutes`, `first_session_at`, `last_session_at`, `active_days`. Group by `player_id`.
- **4.3** Join players to the session aggregates (left join). Output: all player attributes, session aggregate columns (use `coalesce(..., 0)` for counts and playtime), plus **days_since_first_seen** and **days_since_last_session** (null if no sessions). One row per player.
- **4.4** Create **fct_sessions** as a materialized table. Base: all columns from `ref('stg_sessions')` (one row per session).
- **4.5** Join each session to **stg_players** to add `country_code`, `language_code`, `difficulty_selected`.
- **4.6** Match **stg_game_events** to sessions by `player_id` and event time within `session_start_at` and `session_end_at`. Aggregate per session: `total_events`, `unique_event_types`, `deaths_count`, `enemies_killed`, `chapters_completed`, `first_event_at`, `last_event_at`.
- **4.7** Join sessions to the event aggregates (left join). Output: all session and player columns, event aggregate columns (coalesce for counts), and **events_per_minute** = total_events / session_duration_minutes (0 if duration is 0). One row per session.
- **4.8** Create **fct_game_events** as a materialized table. Base: from `ref('stg_game_events')` (one row per event).
- **4.9** Match each event to a session: same `player_id` and `event_at` between `session_start_at` and `session_end_at`. If an event falls in multiple sessions, pick one (e.g. earliest). Add `session_id`, `session_start_at`, `session_end_at`. Events outside any session keep these null.
- **4.10** Join events to **stg_players** for `country_code`, `language_code`, `difficulty_selected`. Add **seconds_since_session_start** (null when session_id is null). One row per event.
- **4.11** Add schema YAML for `dim_players`, `fct_sessions`, and `fct_game_events`: column descriptions and tests. Include `unique` + `not_null` on primary keys, `relationships`: `fct_sessions.player_id` → `dim_players.player_id`, `fct_game_events.session_id` → `fct_sessions.session_id`.

---

## 1. Folder structure

Create a `marts` folder under `models/` (and optionally `marts/core/`) and add SQL files plus schema YAML:

```
models/
  marts/
    core/
      dim_players.sql
      fct_sessions.sql
      fct_game_events.sql
      schema.yml
```

Or a flatter structure: `models/marts/dim_players.sql`, etc. Ensure the marts folder has `+schema: marts` in `dbt_project.yml` (add a `marts_schema` var and extend `generate_schema_name` if needed, similar to staging).

---

## 2. Model configuration (all three SQL files)

In each model file, set materialization to table:

```sql
{{ config(materialized='table') }}
```

---

## 3. Models to build

### dim_players

- **Base:** `{{ ref('stg_players') }}` — one row per player.
- **sessions_agg CTE:** From `ref('stg_sessions')`, group by `player_id`. Output: `total_sessions`, `total_playtime_minutes`, `avg_session_duration_minutes` (avg of `session_duration_minutes`), `first_session_at`, `last_session_at`, `active_days` (count distinct date of `session_start_at`).
- **Final:** Left join players to sessions_agg. Select all player columns, coalesce session aggregates to 0 where null, `days_since_first_seen` = `datediff('day', first_seen_at, current_timestamp())`, `days_since_last_session` = `datediff('day', last_session_at, current_timestamp())` (null when no sessions).

### fct_sessions

- **Base:** `{{ ref('stg_sessions') }}` — one row per session.
- **players CTE:** Select `player_id`, `country_code`, `language_code`, `difficulty_selected` from `ref('stg_players')`.
- **events_agg CTE:** Match events to sessions: join `stg_game_events` to `stg_sessions` on `player_id` and `event_at` between `session_start_at` and `session_end_at`. Group by `session_id`. Output: `total_events`, `unique_event_types` (count distinct event_name), `deaths_count` (event_name = 'player_died'), `enemies_killed` ('enemy_killed'), `chapters_completed` ('chapter_completed'), `first_event_at`, `last_event_at`.
- **Final:** Left join sessions to players and to events_agg. Add `events_per_minute` = `total_events / nullif(session_duration_minutes, 0)` or 0 when duration is 0.

### fct_game_events

- **Base:** `{{ ref('stg_game_events') }}` — one row per event.
- **sessions CTE:** Select `session_id`, `player_id`, `session_start_at`, `session_end_at` from `ref('stg_sessions')`.
- **events_with_sessions CTE:** Left join events to sessions on `player_id` and `event_at` between `session_start_at` and `session_end_at`. Use `row_number() over (partition by event_id order by session_start_at)` to pick one session per event when multiple match.
- **players CTE:** Select `player_id`, `country_code`, `language_code`, `difficulty_selected` from `ref('stg_players')`.
- **Final:** Join events_with_sessions to players. Add `seconds_since_session_start` = `datediff('second', session_start_at, event_at)` (null when session_id is null).

---

## 4. Schema and tests

For each mart model, add to the schema YAML:

- **Model:** `description` (what the model does, grain)
- **Columns:** `description` for each column
- **Tests:**
  - Primary keys: `unique`, `not_null`
  - **fct_sessions:** `relationships` from `player_id` to `ref('dim_players')`
  - **fct_game_events:** `relationships` from `session_id` to `ref('fct_sessions')` (or `stg_sessions` if you prefer; fct_sessions is the mart layer)

---

## 5. Verify

From your dbt project root (with venv activated):

```bash
dbt parse
dbt run --select dim_players fct_sessions fct_game_events
dbt test --select dim_players fct_sessions fct_game_events
```

---

## Why this phase matters

- Core marts (dimensions and facts) are the main interface between raw data and analytics: almost every metric is built on them.
- Getting join keys, grain, and aggregations right here prevents subtle bugs in every dashboard and analysis that follows.

---

**When you're done,** see [Phase 4 — Check yourself](phase4-core-marts-check-yourself.md) for a full solution. Then proceed to [Phase 5: Analytics marts](../phase5/phase5-analytics-marts.md).
