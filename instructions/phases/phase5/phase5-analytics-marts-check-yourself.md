# Phase 5: Analytics marts — Check yourself

This file gives the full SQL and schema solution for Phase 5. Try the [task](phase5-analytics-marts.md) first, then use this to compare.

---

## File 1: `models/marts/analytics/daily_active_players.sql`

```sql
{{ config(materialized='table') }}

with sessions as (
    select
        player_id,
        platform,
        date(session_start_at) as session_date,
        count(*) as sessions_count,
        sum(session_duration_minutes) as total_playtime_minutes
    from {{ ref('stg_sessions') }}
    group by 1, 2, 3
),

players as (
    select player_id, country_code, difficulty_selected
    from {{ ref('stg_players') }}
),

sessions_with_players as (
    select
        s.session_date,
        s.platform,
        p.country_code,
        p.difficulty_selected,
        s.player_id,
        s.sessions_count,
        s.total_playtime_minutes
    from sessions s
    left join players p on s.player_id = p.player_id
),

final as (
    select
        session_date,
        platform,
        country_code,
        difficulty_selected,
        count(distinct player_id) as active_players,
        sum(sessions_count) as total_sessions,
        sum(total_playtime_minutes) as total_playtime_minutes,
        sum(sessions_count)::float / nullif(count(distinct player_id), 0) as avg_sessions_per_player,
        sum(total_playtime_minutes)::float / nullif(count(distinct player_id), 0) as avg_playtime_minutes_per_player
    from sessions_with_players
    group by 1, 2, 3, 4
)

select * from final
order by session_date desc, platform, country_code, difficulty_selected
```

---

## File 2: `models/marts/analytics/funnel_sessions.sql`

```sql
{{ config(materialized='table') }}

with sessions as (
    select
        session_id,
        player_id,
        session_start_at,
        session_end_at,
        platform,
        session_duration_minutes,
        date(session_start_at) as session_date
    from {{ ref('stg_sessions') }}
),

events as (
    select player_id, event_name, event_at
    from {{ ref('stg_game_events') }}
),

session_events as (
    select
        s.session_id,
        s.player_id,
        s.session_date,
        s.platform,
        s.session_duration_minutes,
        e.event_name
    from sessions s
    left join events e
        on s.player_id = e.player_id
        and e.event_at >= s.session_start_at
        and e.event_at <= s.session_end_at
),

session_funnel as (
    select
        session_id,
        player_id,
        session_date,
        platform,
        session_duration_minutes,
        max(case when event_name = 'game_started' then 1 else 0 end) as has_game_started,
        max(case when event_name = 'chapter_started' then 1 else 0 end) as has_chapter_started,
        max(case when event_name = 'checkpoint_reached' then 1 else 0 end) as has_checkpoint_reached,
        max(case when event_name = 'chapter_completed' then 1 else 0 end) as has_chapter_completed,
        max(case when event_name = 'game_closed' then 1 else 0 end) as has_game_closed,
        count_if(event_name = 'game_started') as game_started_count,
        count_if(event_name = 'chapter_started') as chapters_started_count,
        count_if(event_name = 'checkpoint_reached') as checkpoints_reached_count,
        count_if(event_name = 'chapter_completed') as chapters_completed_count
    from session_events
    group by 1, 2, 3, 4, 5
),

players as (
    select player_id, country_code, difficulty_selected
    from {{ ref('stg_players') }}
),

session_funnel_with_players as (
    select
        f.*,
        p.country_code,
        p.difficulty_selected
    from session_funnel f
    left join players p on f.player_id = p.player_id
),

final as (
    select
        session_date,
        platform,
        country_code,
        difficulty_selected,
        count(*) as total_sessions,
        sum(has_game_started) as sessions_with_game_started,
        sum(has_chapter_started) as sessions_with_chapter_started,
        sum(has_checkpoint_reached) as sessions_with_checkpoint_reached,
        sum(has_chapter_completed) as sessions_with_chapter_completed,
        sum(has_game_closed) as sessions_with_game_closed,
        sum(has_game_started)::float / nullif(count(*), 0) * 100 as game_started_rate_pct,
        sum(has_chapter_started)::float / nullif(count(*), 0) * 100 as chapter_started_rate_pct,
        sum(has_checkpoint_reached)::float / nullif(count(*), 0) * 100 as checkpoint_reached_rate_pct,
        sum(has_chapter_completed)::float / nullif(count(*), 0) * 100 as chapter_completed_rate_pct,
        sum(has_game_closed)::float / nullif(count(*), 0) * 100 as game_closed_rate_pct,
        sum(chapters_started_count)::float / nullif(count(*), 0) as avg_chapters_started,
        sum(checkpoints_reached_count)::float / nullif(count(*), 0) as avg_checkpoints_reached,
        sum(chapters_completed_count)::float / nullif(count(*), 0) as avg_chapters_completed,
        avg(session_duration_minutes) as avg_session_duration_minutes
    from session_funnel_with_players
    group by 1, 2, 3, 4
)

select * from final
order by session_date desc, platform, country_code, difficulty_selected
```

---

## File 3: `models/marts/analytics/retention.sql`

```sql
{{ config(materialized='table') }}

with cohorts as (
    select
        player_id,
        date(first_seen_at) as cohort_date,
        country_code,
        difficulty_selected
    from {{ ref('stg_players') }}
),

session_dates as (
    select distinct
        player_id,
        date(session_start_at) as session_date
    from {{ ref('stg_sessions') }}
),

cohort_sizes as (
    select
        cohort_date,
        country_code,
        difficulty_selected,
        count(distinct player_id) as cohort_size
    from cohorts
    group by 1, 2, 3
),

retention_raw as (
    select
        c.player_id,
        c.cohort_date,
        c.country_code,
        c.difficulty_selected,
        s.session_date,
        datediff('day', c.cohort_date, s.session_date) as days_since_cohort
    from cohorts c
    inner join session_dates s
        on c.player_id = s.player_id
        and s.session_date >= c.cohort_date
),

retention_agg as (
    select
        cohort_date,
        country_code,
        difficulty_selected,
        days_since_cohort,
        count(distinct player_id) as active_players
    from retention_raw
    group by 1, 2, 3, 4
),

final as (
    select
        r.cohort_date,
        r.country_code,
        r.difficulty_selected,
        r.days_since_cohort,
        r.active_players,
        cs.cohort_size,
        (r.active_players::float / nullif(cs.cohort_size, 0) * 100) as retention_rate_pct
    from retention_agg r
    left join cohort_sizes cs
        on r.cohort_date = cs.cohort_date
        and r.country_code = cs.country_code
        and r.difficulty_selected = cs.difficulty_selected
)

select * from final
order by cohort_date desc, days_since_cohort, country_code, difficulty_selected
```

---

## File 4: `models/marts/analytics/schema.yml`

```yaml
version: 2

models:
  - name: daily_active_players
    description: Daily active players by session date, platform, country, and difficulty; aggregated metrics.
    columns:
      - name: session_date
        description: Date of the session.
        tests:
          - not_null
      - name: platform
        description: Platform (e.g. ps5, pc).
      - name: country_code
        description: Player country code.
      - name: difficulty_selected
        description: Player difficulty.
      - name: active_players
        description: Count of distinct players active on this date/segment.
      - name: total_sessions
        description: Total sessions in this segment.
      - name: total_playtime_minutes
        description: Sum of session durations.
      - name: avg_sessions_per_player
        description: Average sessions per active player.
      - name: avg_playtime_minutes_per_player
        description: Average playtime per active player.

  - name: funnel_sessions
    description: Funnel conversion metrics by session date, platform, country, and difficulty.
    columns:
      - name: session_date
        description: Date of the session.
        tests:
          - not_null
      - name: platform
        description: Platform.
      - name: country_code
        description: Player country code.
      - name: difficulty_selected
        description: Player difficulty.
      - name: total_sessions
        description: Total sessions in segment.
      - name: sessions_with_game_started
        description: Sessions with at least one game_started event.
      - name: sessions_with_chapter_started
        description: Sessions with at least one chapter_started event.
      - name: sessions_with_checkpoint_reached
        description: Sessions with at least one checkpoint_reached event.
      - name: sessions_with_chapter_completed
        description: Sessions with at least one chapter_completed event.
      - name: sessions_with_game_closed
        description: Sessions with at least one game_closed event.
      - name: game_started_rate_pct
        description: Percentage of sessions with game started.
      - name: chapter_started_rate_pct
        description: Percentage of sessions with chapter started.
      - name: checkpoint_reached_rate_pct
        description: Percentage of sessions with checkpoint reached.
      - name: chapter_completed_rate_pct
        description: Percentage of sessions with chapter completed.
      - name: game_closed_rate_pct
        description: Percentage of sessions with game closed.
      - name: avg_chapters_started
        description: Average chapters started per session.
      - name: avg_checkpoints_reached
        description: Average checkpoints reached per session.
      - name: avg_chapters_completed
        description: Average chapters completed per session.
      - name: avg_session_duration_minutes
        description: Average session duration in minutes.

  - name: retention
    description: Cohort retention by cohort date, country, difficulty, and days since cohort.
    columns:
      - name: cohort_date
        description: Date when the cohort was first seen.
        tests:
          - not_null
      - name: country_code
        description: Player country code.
      - name: difficulty_selected
        description: Player difficulty.
      - name: days_since_cohort
        description: Days since cohort date (0 = cohort day).
      - name: active_players
        description: Count of distinct players from cohort active on this day.
      - name: cohort_size
        description: Total players in this cohort.
      - name: retention_rate_pct
        description: active_players / cohort_size * 100.
```

---

## Verify

From your dbt project root (with venv activated):

```bash
dbt parse
dbt run --select daily_active_players funnel_sessions retention
dbt test --select daily_active_players funnel_sessions retention
```

---

Then proceed to [Phase 6: Tests and quality](../phase6/phase6-tests-quality.md).
