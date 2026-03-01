# Phase 4: Core marts — Check yourself

This file gives the full SQL and schema solution for Phase 4.
Try the [task](phase4-core-marts.md) first, then use this to compare.

---

## File 1: `models/marts/core/dim_players.sql`

```sql
{{ config(materialized='table') }}

with players as (
    select * from {{ ref('stg_players') }}
),

sessions_agg as (
    select
        player_id,
        count(*) as total_sessions,
        sum(session_duration_minutes) as total_playtime_minutes,
        avg(session_duration_minutes) as avg_session_duration_minutes,
        min(session_start_at) as first_session_at,
        max(session_start_at) as last_session_at,
        count(distinct date(session_start_at)) as active_days
    from {{ ref('stg_sessions') }}
    group by player_id
),

final as (
    select
        p.*,
        coalesce(s.total_sessions, 0) as total_sessions,
        coalesce(s.total_playtime_minutes, 0) as total_playtime_minutes,
        coalesce(s.avg_session_duration_minutes, 0) as avg_session_duration_minutes,
        s.first_session_at,
        s.last_session_at,
        coalesce(s.active_days, 0) as active_days,
        datediff('day', p.first_seen_at, current_timestamp()) as days_since_first_seen,
        case
            when s.last_session_at is not null then datediff('day', s.last_session_at, current_timestamp())
            else null
        end as days_since_last_session
    from players p
    left join sessions_agg s on p.player_id = s.player_id
)

select * from final
```

---

## File 2: `models/marts/core/fct_sessions.sql`

```sql
{{ config(materialized='table') }}

with sessions as (
    select * from {{ ref('stg_sessions') }}
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
    from {{ ref('stg_sessions') }} s
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
            when s.session_duration_minutes > 0 then coalesce(e.total_events, 0)::float / s.session_duration_minutes
            else 0
        end as events_per_minute
    from sessions s
    left join players p on s.player_id = p.player_id
    left join events_agg e on s.session_id = e.session_id
)

select * from final
```

---

## File 3: `models/marts/core/fct_game_events.sql`

```sql
{{ config(materialized='table') }}

with events as (
    select * from {{ ref('stg_game_events') }}
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
            when e.session_id is not null then datediff('second', e.session_start_at, e.event_at)
            else null
        end as seconds_since_session_start
    from events_with_one_session e
    left join players p on e.player_id = p.player_id
)

select * from final
```

---

## File 4: `models/marts/core/schema.yml`

```yaml
version: 2

models:
  - name: dim_players
    description: Player dimension with session aggregates; one row per player.
    columns:
      - name: player_id
        description: Unique player identifier (primary key).
        tests:
          - unique
          - not_null
      - name: first_seen_at
        description: Timestamp when the player was first seen.
      - name: country_code
        description: Country code (e.g. BY, DE, ES).
      - name: language_code
        description: Language code (e.g. en, de, es).
      - name: difficulty_selected
        description: Player-selected difficulty (easy, normal, hard, grounded).
      - name: total_sessions
        description: Total number of sessions for this player.
      - name: total_playtime_minutes
        description: Sum of session durations in minutes.
      - name: avg_session_duration_minutes
        description: Average session duration in minutes.
      - name: first_session_at
        description: Timestamp of first session.
      - name: last_session_at
        description: Timestamp of most recent session.
      - name: active_days
        description: Count of distinct days with at least one session.
      - name: days_since_first_seen
        description: Days from first_seen_at to current date.
      - name: days_since_last_session
        description: Days from last_session_at to current date; null if no sessions.

  - name: fct_sessions
    description: Session fact table with player attributes and event aggregates; one row per session.
    columns:
      - name: session_id
        description: Unique session identifier (primary key).
        tests:
          - unique
          - not_null
      - name: player_id
        description: Player who owns this session (foreign key to dim_players).
        tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_players')
                field: player_id
      - name: session_start_at
        description: Session start timestamp.
      - name: session_end_at
        description: Session end timestamp.
      - name: platform
        description: Platform where the session was played.
      - name: session_duration_minutes
        description: Duration in minutes.
      - name: country_code
        description: Player's country code.
      - name: language_code
        description: Player's language code.
      - name: difficulty_selected
        description: Player's difficulty.
      - name: total_events
        description: Total events in this session.
      - name: unique_event_types
        description: Count of distinct event types.
      - name: deaths_count
        description: Count of player_died events.
      - name: enemies_killed
        description: Count of enemy_killed events.
      - name: chapters_completed
        description: Count of chapter_completed events.
      - name: first_event_at
        description: First event timestamp in session.
      - name: last_event_at
        description: Last event timestamp in session.
      - name: events_per_minute
        description: total_events / session_duration_minutes.

  - name: fct_game_events
    description: Game events fact table with session and player context; one row per event.
    columns:
      - name: event_id
        description: Unique event identifier (primary key).
        tests:
          - unique
          - not_null
      - name: session_id
        description: Session this event belongs to (foreign key to fct_sessions); null if outside any session.
        tests:
          - relationships:
              arguments:
                to: ref('fct_sessions')
                field: session_id
      - name: event_at
        description: Event occurrence timestamp.
      - name: player_id
        description: Player who generated the event.
      - name: event_name
        description: Name of the event.
      - name: platform
        description: Platform where the event occurred.
      - name: game_version
        description: Game version at time of event.
      - name: properties
        description: Optional JSON/VARIANT payload.
      - name: session_start_at
        description: Session start timestamp; null if no session.
      - name: session_end_at
        description: Session end timestamp; null if no session.
      - name: country_code
        description: Player's country code.
      - name: language_code
        description: Player's language code.
      - name: difficulty_selected
        description: Player's difficulty.
      - name: seconds_since_session_start
        description: Seconds from session start to event; null if no session.
```

---

## Verify

From your dbt project root (with venv activated):

```bash
dbt parse
dbt run --select dim_players fct_sessions fct_game_events
dbt test --select dim_players fct_sessions fct_game_events
```

---

## Note on fct_game_events

The `relationships` test on `session_id` may fail if `session_id` can be null (events outside any session). The `relationships` test in dbt typically skips nulls by default. If your dbt version requires it, you can make the relationship test conditional or exclude the test for null session_id. Check your dbt docs for `relationships` behavior with nulls.

---

Then proceed to [Phase 5: Analytics marts](../phase5/phase5-analytics-marts.md).
