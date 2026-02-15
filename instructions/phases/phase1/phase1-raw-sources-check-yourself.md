# Phase 1: Raw layer (sources) — Check yourself

This file gives the full YAML solution for Phase 1. Try the [task](phase1-raw-sources.md) first, then use this to compare.

---

## File 1: `models/sources/raw_players.yml`

```yaml
# Raw layer: players table (loaded by game-data-platform ingest).
version: 2

sources:
  - name: raw
    description: Raw game data loaded from CSVs (players, sessions, game events).
    database: "{{ target.database }}"
    schema: "{{ target.schema }}"
    tables:
      - name: raw_players
        description: One row per player; id, first seen, locale, and selected difficulty.
        columns:
          - name: player_id
            description: Unique player identifier (primary key).
            tests:
              - unique
              - not_null
          - name: first_seen_at
            description: Timestamp when the player was first seen.
          - name: country
            description: Country code (e.g. BY, DE, ES).
          - name: language
            description: Language code (e.g. be, de, es).
          - name: difficulty_selected
            description: Player-selected difficulty level.
            tests:
              - accepted_values:
                  arguments:
                    values:
                      - "easy"
                      - "normal"
                      - "hard"
                      - "grounded"
```

---

## File 2: `models/sources/raw_sessions.yml`

```yaml
# Raw layer: sessions table (loaded by game-data-platform ingest).
version: 2

sources:
  - name: raw
    description: Raw game data loaded from CSVs (players, sessions, game events).
    database: "{{ target.database }}"
    schema: "{{ target.schema }}"
    tables:
      - name: raw_sessions
        description: One row per session; links to player, start/end times, platform.
        columns:
          - name: session_id
            description: Unique session identifier (primary key).
            tests:
              - unique
              - not_null
          - name: player_id
            description: Player who owns this session (foreign key to raw_players).
            tests:
              - not_null
          - name: session_start
            description: Session start timestamp (string in raw).
          - name: session_end
            description: Session end timestamp (string in raw).
          - name: platform
            description: Platform where the session was played.
            tests:
              - accepted_values:
                  arguments:
                    values:
                      - "ps5"
                      - "pc"
```

---

## File 3: `models/sources/raw_game_events.yml`

```yaml
# Raw layer: game_events table (loaded by game-data-platform ingest).
version: 2

sources:
  - name: raw
    description: Raw game data loaded from CSVs (players, sessions, game events).
    database: "{{ target.database }}"
    schema: "{{ target.schema }}"
    tables:
      - name: raw_game_events
        description: One row per game event; event type, time, player, platform, version, optional properties.
        columns:
          - name: event_id
            description: Unique event identifier (primary key).
            tests:
              - unique
              - not_null
          - name: event_time
            description: Event occurrence timestamp (string in raw).
          - name: player_id
            description: Player who generated the event.
            tests:
              - not_null
          - name: event_name
            description: Name of the event (e.g. game_started, chapter_started).
          - name: platform
            description: Platform where the event occurred.
            tests:
              - accepted_values:
                  arguments:
                    values:
                      - "ps5"
                      - "pc"
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
dbt ls --resource-type source
dbt test --select source:raw
```
