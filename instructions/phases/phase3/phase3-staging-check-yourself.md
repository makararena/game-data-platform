# Phase 3: Staging layer — Check yourself

This file gives the full SQL and schema solution for Phase 3. Try the [task](phase3-staging.md) first, then use this to compare.

---

## File 1: `models/staging/stg_players.sql`

```sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'raw_players') }}
),

renamed as (
    select
        player_id,
        try_to_timestamp(first_seen_at) as first_seen_at,
        upper(country) as country_code,
        lower(language) as language_code,
        lower(difficulty_selected) as difficulty_selected
    from source
)

select * from renamed
```

---

## File 2: `models/staging/stg_sessions.sql`

```sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'raw_sessions') }}
),

renamed as (
    select
        session_id,
        player_id,
        try_to_timestamp(session_start) as session_start_at,
        try_to_timestamp(session_end) as session_end_at,
        lower(platform) as platform,
        datediff(
            'minute',
            try_to_timestamp(session_start),
            try_to_timestamp(session_end)
        ) as session_duration_minutes
    from source
)

select * from renamed
```

---

## File 3: `models/staging/stg_game_events.sql`

```sql
{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'raw_game_events') }}
),

renamed as (
    select
        event_id,
        try_to_timestamp(event_time) as event_at,
        player_id,
        lower(event_name) as event_name,
        lower(platform) as platform,
        game_version,
        properties
    from source
)

select * from renamed
```

---

## File 4: `models/staging/schema.yml`

```yaml
version: 2

models:
  - name: stg_players
    description: Staging view of players; cleans raw_players with typed timestamps and standardized codes.
    columns:
      - name: player_id
        description: Unique player identifier (primary key).
        tests:
          - unique
          - not_null
      - name: first_seen_at
        description: Timestamp when the player was first seen.
        tests:
          - not_null
      - name: country_code
        description: Country code, uppercase (e.g. BY, DE, ES).
      - name: language_code
        description: Language code, lowercase (e.g. en, de, es).
      - name: difficulty_selected
        description: Player-selected difficulty, lowercase (easy, normal, hard, grounded).

  - name: stg_sessions
    description: Staging view of sessions; cleans raw_sessions with typed timestamps and computed duration.
    columns:
      - name: session_id
        description: Unique session identifier (primary key).
        tests:
          - unique
          - not_null
      - name: player_id
        description: Player who owns this session (foreign key to stg_players).
        tests:
          - not_null
          - relationships:
              arguments:
                to: ref('stg_players')
                field: player_id
      - name: session_start_at
        description: Session start timestamp.
        tests:
          - not_null
      - name: session_end_at
        description: Session end timestamp.
        tests:
          - not_null
      - name: platform
        description: Platform where the session was played (lowercase).
      - name: session_duration_minutes
        description: Duration in minutes between session start and end.

  - name: stg_game_events
    description: Staging view of game events; cleans raw_game_events with typed timestamps.
    columns:
      - name: event_id
        description: Unique event identifier (primary key).
        tests:
          - unique
          - not_null
      - name: event_at
        description: Event occurrence timestamp.
        tests:
          - not_null
      - name: player_id
        description: Player who generated the event.
        tests:
          - not_null
      - name: event_name
        description: Name of the event (lowercase, e.g. game_started, chapter_started).
      - name: platform
        description: Platform where the event occurred (lowercase).
      - name: game_version
        description: Game version at time of event.
      - name: properties
        description: Optional JSON/VARIANT payload with event-specific attributes.
```

---

## Verify

From your dbt project root (with venv activated):

```bash
dbt parse
dbt run --select staging
dbt test --select staging
```
