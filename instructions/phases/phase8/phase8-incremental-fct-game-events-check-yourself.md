# Phase 8: Incremental `fct_game_events` and `fct_sessions` — Check yourself

Use this reference to compare your implementation.

---

## `fct_game_events` pattern

```sql
{{ config(materialized='incremental', unique_key='event_id', on_schema_change='ignore') }}

with base as (
    select * from {{ ref('stg_game_events') }}
    {% if is_incremental() %}
      where event_at > (
          select coalesce(max(event_at), '1900-01-01'::timestamp)
          from {{ this }}
      )
    {% endif %}
)

select * from base
```

## `fct_sessions` pattern

```sql
{{ config(materialized='incremental', unique_key='session_id', on_schema_change='ignore') }}

with base as (
    select * from {{ ref('stg_sessions') }}
    {% if is_incremental() %}
      where session_start_at > (
          select coalesce(max(session_start_at), '1900-01-01'::timestamp)
          from {{ this }}
      )
    {% endif %}
)

select * from base
```

---

## Validation checklist

1. Both models use `materialized='incremental'`.
2. Both models have correct `unique_key`.
3. Both models contain `is_incremental()` filtering.
4. Re-running `dbt run --select fct_game_events fct_sessions` does not create duplicates.
5. `dbt test --select fct_game_events fct_sessions` passes.

## Engineering note

This basic strategy works for append-only event streams. For late-arriving rows in production, consider:

- a lookback window (for example, reprocess last N days), or
- merge strategy keyed by ID with updated timestamp logic.
