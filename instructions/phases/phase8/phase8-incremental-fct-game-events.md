# Phase 8: Incremental `fct_game_events` and `fct_sessions` — Task

This phase converts large fact tables to incremental materialization so repeated runs process only new data.

**Prerequisites:** Phase 0–7 complete; `fct_game_events` and `fct_sessions` currently build successfully as non-incremental models.

---

## Goals

- **8.1** Convert `fct_game_events` to incremental using `unique_key='event_id'`.
- **8.2** Convert `fct_sessions` to incremental using `unique_key='session_id'`.
- **8.3** Add `is_incremental()` filters with timestamp cutoffs.
- **8.4** Validate full-refresh vs incremental behavior.
- **8.5** Explain tradeoffs (late-arriving data, idempotency, key choice) in PR description.

---

## 1. Update `fct_game_events`

In `models/marts/core/fct_game_events.sql`:

- set `materialized='incremental'`
- set `unique_key='event_id'`
- set `on_schema_change='ignore'` (or your team standard)
- in `is_incremental()` branch, filter by `event_at > max(event_at)` from `{{ this }}`

Example pattern:

```sql
{{ config(materialized='incremental', unique_key='event_id', on_schema_change='ignore') }}

with base as (
    select *
    from {{ ref('stg_game_events') }}
    {% if is_incremental() %}
      where event_at > (
          select coalesce(max(event_at), '1900-01-01'::timestamp)
          from {{ this }}
      )
    {% endif %}
)

select * from base
```

## 2. Update `fct_sessions`

In `models/marts/core/fct_sessions.sql`:

- set `materialized='incremental'`
- set `unique_key='session_id'`
- set `on_schema_change='ignore'` (or your team standard)
- in `is_incremental()` branch, filter by `session_start_at > max(session_start_at)` from `{{ this }}`

## 3. Validate behavior

Run:

```bash
dbt run --full-refresh --select fct_game_events fct_sessions
dbt run --select fct_game_events fct_sessions
dbt test --select fct_game_events fct_sessions
```

What to confirm:

- second run is faster than full-refresh
- no duplicates by `event_id` and `session_id`
- tests still pass

## 4. Explain in PR

In your PR description, include:

- why `event_id` and `session_id` are valid `unique_key` values
- why timestamp filters are safe for your generated data
- how you would handle late-arriving data in production

---

When done, return to the main [README](../../../README.md) and complete the Final Boss analytics questions.
