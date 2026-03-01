# Phase 8: Incremental `fct_game_events` and `fct_sessions` — Check yourself

Use this reference to compare your implementation.

---

## `fct_game_events` full code

```sql
{{ config(
    materialized='incremental',
    unique_key='event_id',
    on_schema_change='ignore'
) }}

with events as (

    select *
    from {{ ref('stg_game_events') }}

    {% if is_incremental() %}
        where event_at > (
            select coalesce(max(event_at), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}

),

sessions as (
    select session_id, player_id, session_start_at, session_end_at
    from {{ ref('stg_sessions') }}
),

events_with_sessions as (
    select
        e.*,
        s.session_id,
        s.session_start_at,
        s.session_end_at,
        row_number() over (
            partition by e.event_id
            order by s.session_start_at
        ) as rn
    from events e
    left join sessions s
        on e.player_id = s.player_id
        and e.event_at >= s.session_start_at
        and e.event_at <= s.session_end_at
),

events_with_one_session as (
    select
        event_id,
        event_at,
        player_id,
        event_name,
        platform,
        game_version,
        properties,
        session_id,
        session_start_at,
        session_end_at
    from events_with_sessions
    where rn = 1
),

players as (
    select player_id, country_code, language_code, difficulty_selected
    from {{ ref('stg_players') }}
),

final as (
    select
        e.event_id,
        e.event_at,
        e.player_id,
        e.event_name,
        e.platform,
        e.game_version,
        e.properties,
        e.session_id,
        e.session_start_at,
        e.session_end_at,
        p.country_code,
        p.language_code,
        p.difficulty_selected,
        case
            when e.session_id is not null
                then datediff('second', e.session_start_at, e.event_at)
            else null
        end as seconds_since_session_start
    from events_with_one_session e
    left join players p
        on e.player_id = p.player_id
)

select * from final
```

Use as full file content for `models/marts/core/fct_game_events.sql`.

## `fct_sessions` full code

```sql
{{ config(
    materialized='incremental',
    unique_key='session_id',
    on_schema_change='ignore'
) }}

with sessions as (

    select *
    from {{ ref('stg_sessions') }}

    {% if is_incremental() %}
        where session_start_at > (
            select coalesce(max(session_start_at), '1900-01-01'::timestamp)
            from {{ this }}
        )
    {% endif %}

),

players as (
    select player_id, country_code, language_code, difficulty_selected
    from {{ ref('stg_players') }}
),

events_matched as (
    select
        s.session_id,
        e.event_id,
        e.event_at,
        e.event_name
    from sessions s
    inner join {{ ref('stg_game_events') }} e
        on e.player_id = s.player_id
        and e.event_at >= s.session_start_at
        and e.event_at <= s.session_end_at
),

events_agg as (
    select
        session_id,
        count(*) as total_events,
        count(distinct event_name) as unique_event_types,
        count_if(event_name = 'player_died') as deaths_count,
        count_if(event_name = 'enemy_killed') as enemies_killed,
        count_if(event_name = 'chapter_completed') as chapters_completed,
        min(event_at) as first_event_at,
        max(event_at) as last_event_at
    from events_matched
    group by session_id
),

final as (
    select
        s.*,
        p.country_code,
        p.language_code,
        p.difficulty_selected,
        coalesce(e.total_events, 0) as total_events,
        coalesce(e.unique_event_types, 0) as unique_event_types,
        coalesce(e.deaths_count, 0) as deaths_count,
        coalesce(e.enemies_killed, 0) as enemies_killed,
        coalesce(e.chapters_completed, 0) as chapters_completed,
        e.first_event_at,
        e.last_event_at,
        case
            when s.session_duration_minutes > 0
                then coalesce(e.total_events, 0)::float / s.session_duration_minutes
            else 0
        end as events_per_minute
    from sessions s
    left join players p
        on s.player_id = p.player_id
    left join events_agg e
        on s.session_id = e.session_id
)

select * from final
```

Use as full file content for `models/marts/core/fct_sessions.sql`.

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
