# Phase 5: Analytics marts — Task

This phase creates **analytics mart models**: `daily_active_players`, `funnel_sessions`, and `retention`. These are reporting-ready aggregates that translate raw behavioral data into KPIs (DAU, funnel conversion, retention curves). You will add them under `models/marts/` (e.g. `marts/analytics/`) so they build in the MARTS schema alongside core marts.

**Prerequisites:** Phase 1–4 complete (sources, staging, core marts). Raw tables loaded in Snowflake.

---

## Why analytics marts?

- **Reporting layer:** Core marts (dim_players, fct_sessions, fct_game_events) are the interface for analysis. Analytics marts pre-aggregate by date, platform, country, and difficulty so dashboards and reports can query simple metrics without complex joins.
- **KPIs:** `daily_active_players` = DAU by segment; `funnel_sessions` = conversion from game_started → chapter_started → checkpoint → chapter_completed → game_closed; `retention` = cohort retention curves.
- **Materialized tables:** These are heavier aggregations; materialize as tables for fast dashboard queries.

---

## Goals

### daily_active_players

- **5.1** From **stg_sessions**, compute per (player_id, session_date, platform): `session_date`, `platform`, `sessions_count` (count of sessions), `total_playtime_minutes` (sum of duration). Join to **stg_players** for `country_code`, `difficulty_selected`.
- **5.2** Aggregate by (session_date, platform, country_code, difficulty_selected). Output: `session_date`, `platform`, `country_code`, `difficulty_selected`, `active_players` (count distinct player_id), `total_sessions`, `total_playtime_minutes`, `avg_sessions_per_player`, `avg_playtime_minutes_per_player`. Order by session_date desc, platform, country_code, difficulty_selected.

### funnel_sessions

- **5.3** Match **stg_game_events** to **stg_sessions** by player_id and event time within session window. Per session compute flags: `has_game_started`, `has_chapter_started`, `has_checkpoint_reached`, `has_chapter_completed`, `has_game_closed` (1 if any such event, else 0), and counts: `game_started_count`, `chapters_started_count`, `checkpoints_reached_count`, `chapters_completed_count`. Join to **stg_players** for `country_code`, `difficulty_selected`.
- **5.4** Aggregate by (session_date, platform, country_code, difficulty_selected). Output: `total_sessions`, `sessions_with_game_started`, `sessions_with_chapter_started`, `sessions_with_checkpoint_reached`, `sessions_with_chapter_completed`, `sessions_with_game_closed`; conversion rates as percentage of total_sessions (e.g. `game_started_rate_pct`); `avg_chapters_started`, `avg_checkpoints_reached`, `avg_chapters_completed`, `avg_session_duration_minutes`. Order by session_date desc, platform, country_code, difficulty_selected.

### retention

- **5.5** Define cohorts from **stg_players**: `cohort_date` = date(first_seen_at), plus `country_code`, `difficulty_selected`. From **stg_sessions** take (player_id, session_date).
- **5.6** Join so each (player, cohort_date, session_date) has session_date >= cohort_date. Compute `days_since_cohort` = datediff(day, cohort_date, session_date). Aggregate by (cohort_date, country_code, difficulty_selected, days_since_cohort): `active_players` (count distinct player_id), `cohort_size` (same for all days in that cohort). Add `retention_rate_pct` = (active_players / cohort_size) * 100. Order by cohort_date desc, days_since_cohort, country_code, difficulty_selected.
- **5.7** Add schema YAML for the three analytics models: descriptions and, where useful, tests (e.g. not_null on key dimensions, non-negative numeric columns).

---

## 1. Folder structure

Add analytics marts under `models/marts/`:

```
models/
  marts/
    core/           # Phase 4
      ...
    analytics/
      daily_active_players.sql
      funnel_sessions.sql
      retention.sql
      schema.yml
```

The `marts` folder already has `+schema: marts` from Phase 2, so analytics models build in MARTS.

---

## 2. Event names (reference)

For funnel flags and counts, use these event names (lowercase, from stg_game_events):

| Event              | event_name          |
|--------------------|---------------------|
| Game started       | `game_started`      |
| Chapter started    | `chapter_started`   |
| Checkpoint reached | `checkpoint_reached`|
| Chapter completed  | `chapter_completed` |
| Game closed        | `game_closed`       |

---

## 3. Verify

From your dbt project root (with venv activated):

```bash
dbt parse
dbt run --select daily_active_players funnel_sessions retention
dbt test --select daily_active_players funnel_sessions retention
```

---

## Why this phase matters

- Analytics marts (DAU, funnel, retention) translate raw behavioral data into the KPIs your team actually discusses.
- By keeping these as dbt models, you can iterate on definitions (e.g. what counts as active) with version control and tests instead of ad hoc SQL.

---

**When you're done,** see [Phase 5 — Check yourself](phase5-analytics-marts-check-yourself.md) for a full solution. Then proceed to [Phase 6: Macros and project config](../../../README.md#phase-6-macros-and-project-config).
