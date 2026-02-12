# Game Analytics: Build Your Own Warehouse

A hands-on project to build an analytics warehouse for a narrative game. You run the **game data platform** to generate and load raw data into Snowflake, then build the **dbt project** step by step: raw → staging → marts, macros, and tests. By the end, you’ll have a working pipeline and a clear path from raw events to reporting-ready tables.

---

## The Story

![Joel and Ellie in the post-apocalyptic world of The Last of Us](images/img.png)

It’s **2012** in Santa Monica. You’re on the team building **The Last of Us** — the story of Joel and Ellie is on the page, the levels are greyboxed, and combat is being tuned week by week. Design keeps asking: *Where do players die the most? Do they come back after the first session? Which chapters get abandoned?* Right now the answer is usually “we’ll check the build and the forums.” You’ve already wired the game to emit events and dump them into pipelines, but there’s no single place to go for “how many players, how many sessions, where they drop.” Someone has to turn that raw firehose into tables the team can actually use. That someone is you.

You’re in a lucky position: unlike the real 2012 stack, you have **modern tools**. **Snowflake** holds the raw data; **dbt** and a proper data platform are on the table. So instead of one-off scripts and spreadsheets, you get to build an **analytics warehouse** the right way — clean, tested, and ready for the questions that will only get louder as the game nears ship.

---

**What you have to work with**

The game is at **MVP**: a few key flows are playable end-to-end, and telemetry is already flowing. You have **three core data frames** (and the pipelines that fill them):

- **Players** — one row per player: country, language, difficulty, first seen, and other attributes. Who they are and how they chose to play.
- **Sessions** — one row per play session. Each player has many sessions; each session has a start time, end time, platform, and duration. *When* and *how long* they play.
- **Events** — one row per in-game event. Events belong to sessions (and thus to players): *game started*, *chapter started*, *checkpoint reached*, *enemy killed*, *player died*, *item crafted*, *chapter completed*, *game closed*. Payloads can include chapter names, locations, weather, enemy names, weapon names, crafting materials — the stuff design will use to balance difficulty and fix friction.

The hierarchy is simple: **player → sessions → events**. Your job is to turn that into a warehouse: define sources, stage and clean the data, build dimensions and facts (dim_players, fct_sessions, fct_game_events), add analytics marts (DAU, funnel, retention), and harden everything with tests and CI. When you’re done, the team can stop guessing and start answering — and you’ll have done it step by step, by following the tasks below. No prior dbt experience required.

---

## How It Works

1. **Game Data Platform** (`game-data-platform/`)  
   A Python pipeline that:
   - **Generates** synthetic players, sessions, and game events (CSVs).
   - **Loads** them into Snowflake as `RAW_PLAYERS`, `RAW_SESSIONS`, `RAW_GAME_EVENTS`.

2. **dbt Project** (`game-dbt-project/`)  
   You (or the reference solution) build:
   - **Raw** — source definitions pointing at those tables.
   - **Staging** — views that clean, rename, and type the raw data.
   - **Marts** — core (dimensions + facts) and analytics (DAU, funnel, retention).
   - **Macros** — e.g. schema naming for dev/ci/prod.
   - **Tests** — not null, unique, relationships, and a custom business rule (e.g. no overlapping sessions).

You run the platform once to get data, then work through the dbt tasks. Every step is something you can do yourself; the repos provide the structure and (optionally) the reference implementation.

---

## Project Outline

| Layer / Area   | What you build |
|----------------|----------------|
| **Data in**    | Run `game-data-platform`: generate data → load to Snowflake. |
| **Raw**        | dbt **sources**: `raw_players`, `raw_sessions`, `raw_game_events` (schema, columns, optional source tests). |
| **Staging**    | dbt **models**: `stg_players`, `stg_sessions`, `stg_game_events` — clean, standardize names and types. |
| **Core marts** | `dim_players`, `fct_sessions`, `fct_game_events` — dimensions and facts for analysis. |
| **Analytics marts** | `daily_active_players`, `funnel_sessions`, `retention` — reporting-ready aggregates. |
| **Macros**     | e.g. `generate_schema_name` for environment-specific schemas. |
| **Tests**      | Column tests (not null, unique, relationships) + at least one singular test (e.g. sessions don’t overlap). |
| **CI**         | GitHub Action: `dbt compile` and optionally `dbt build` so broken contracts fail the pipeline. |

---

## Tasks (Do It Yourself)

Do the tasks in order. Each task is a single, clear step from start to finish.

---

### Phase 0: Get data and set up dbt

- [ ] **0.1** Clone the repo. In `app/` (this folder’s pipeline code), create a venv, install `requirements.txt`, and add a `.env` with Snowflake credentials (user, password, account, warehouse, database, schema). See [app/README.md](app/README.md) for setup.
- [ ] **0.2** From this folder (`game-data-platform/`), run `python app/main.py` (or `cd app && python main.py`) to generate CSVs and load them into Snowflake as `RAW_PLAYERS`, `RAW_SESSIONS`, `RAW_GAME_EVENTS`.
- [ ] **0.3** In the sibling `game-dbt-project/` folder, install dbt (e.g. `pip install dbt-snowflake`), create `~/.dbt/profiles.yml` with profile `game_analytics` pointing at your Snowflake database and schema, and run `dbt deps`.

<details>
<summary>Answer — Phase 0</summary>

*(No code — follow the steps above. You can add notes or links here later.)*

</details>

---

### Phase 1: Raw layer (sources)

- [ ] **1.1** Create a source named `raw` that points to the three raw tables: `raw_players`, `raw_sessions`, `raw_game_events`. Set database and schema (e.g. from `target`). Add a short description per table.
- [ ] **1.2** In the same YAML, add column definitions for each source table (player_id, first_seen_at, country, language, difficulty_selected for players; session_id, player_id, session start/end, platform for sessions; event_id, event_time, player_id, event_name, platform, game_version, properties for events). Add source tests where useful: `unique` and `not_null` on primary keys, `accepted_values` on fields like difficulty or platform.

<details>
<summary>Answer — Phase 1 (sources)</summary>

```yaml
version: 2
sources:
  - name: raw
    database: "{{ target.database }}"
    schema: "{{ target.schema }}"
    tables:
      - name: raw_players
      # raw_sessions, raw_game_events + columns & tests
```

</details>

---

### Phase 2: Staging layer

- [ ] **2.1** Create **stg_players**: a view that selects from `source('raw', 'raw_players')`. Output columns: `player_id`, `first_seen_at` (cast to timestamp, e.g. `try_to_timestamp`), `country_code` (rename from `country`, uppercase), `language_code` (rename from `language`, lowercase), `difficulty_selected` (lowercase). Materialize as view.
- [ ] **2.2** Create **stg_sessions**: a view from `source('raw', 'raw_sessions')`. Output: `session_id`, `player_id`, `session_start_at`, `session_end_at` (cast to timestamp; raw columns may be named e.g. `session_start` / `session_end`), `platform` (lowercase), `session_duration_minutes` (computed as minutes between start and end). Materialize as view.
- [ ] **2.3** Create **stg_game_events**: a view from `source('raw', 'raw_game_events')`. Output: `event_id`, `event_at` (cast to timestamp; raw may be `event_time`), `player_id`, `event_name` (lowercase), `platform` (lowercase), `game_version`, `properties`. Materialize as view.
- [ ] **2.4** For each staging model, add a schema YAML with model description, column descriptions, and tests: `unique` and `not_null` on primary keys (player_id, session_id, event_id), `not_null` on important fields; for `stg_sessions`, add a `relationships` test from `player_id` to `ref('stg_players')`.

<details>
<summary>Answer — Phase 2 (staging models)</summary>

```sql
-- stg_players.sql
{{ config(materialized='view') }}
with source as ( select * from {{ source('raw', 'raw_players') }} )
select * from source  -- add renames, try_to_timestamp, upper/lower
```

```sql
-- stg_sessions.sql
{{ config(materialized='view') }}
with source as ( select * from {{ source('raw', 'raw_sessions') }} )
select * from source  -- add session_start_at, session_end_at, session_duration_minutes
```

```sql
-- stg_game_events.sql
{{ config(materialized='view') }}
with source as ( select * from {{ source('raw', 'raw_game_events') }} )
select * from source  -- add event_at, lower(event_name), lower(platform)
```

</details>

---

### Phase 3: Core marts

**dim_players**

- [ ] **3.1** Create model **dim_players** as a **materialized table**. Base: select all columns from `ref('stg_players')` (one row per player).
- [ ] **3.2** Add a CTE that aggregates **stg_sessions** per player: `total_sessions` (count), `total_playtime_minutes` (sum of `session_duration_minutes`), `avg_session_duration_minutes`, `first_session_at` (min of `session_start_at`), `last_session_at` (max of `session_start_at`), `active_days` (count distinct date of `session_start_at`). Group by `player_id`.
- [ ] **3.3** Join players to the session aggregates (left join). In the final select output: all player attributes, the session aggregate columns (use `coalesce(..., 0)` for counts and playtime so players with no sessions get 0), plus **days_since_first_seen** (days from `first_seen_at` to current date) and **days_since_last_session** (days from `last_session_at` to current date; null if no sessions). One row per player.

<details>
<summary>Answer — dim_players (3.1–3.3)</summary>

```sql
{{ config(materialized='table') }}
with players as ( select * from {{ ref('stg_players') }} ),
sessions_agg as (
    select player_id, count(*) as total_sessions, sum(session_duration_minutes) as total_playtime_minutes,
           min(session_start_at) as first_session_at, max(session_start_at) as last_session_at,
           count(distinct date(session_start_at)) as active_days
    from {{ ref('stg_sessions') }} group by player_id
),
final as (
    select p.*, coalesce(s.total_sessions,0) as total_sessions, coalesce(s.total_playtime_minutes,0) as total_playtime_minutes,
           coalesce(s.active_days,0) as active_days, datediff('day', p.first_seen_at, current_timestamp()) as days_since_first_seen
    from players p left join sessions_agg s on p.player_id = s.player_id
)
select * from final
```

</details>

**fct_sessions**

- [ ] **3.4** Create model **fct_sessions** as a **materialized table**. Base: select all columns from `ref('stg_sessions')` (one row per session).
- [ ] **3.5** Join each session to **stg_players** to add `country_code`, `language_code`, `difficulty_selected` to each session row.
- [ ] **3.6** Match **stg_game_events** to sessions by `player_id` and event time within `session_start_at` and `session_end_at`. Aggregate per session: `total_events`, `unique_event_types` (count distinct event_name), `deaths_count` (count where event_name = 'player_died'), `enemies_killed` (event_name = 'enemy_killed'), `chapters_completed` (event_name = 'chapter_completed'), `first_event_at`, `last_event_at`.
- [ ] **3.7** Join sessions to the event aggregates (left join). In the final select: all session and player columns, the event aggregate columns (use `coalesce(..., 0)` for counts), and **events_per_minute** = total_events / session_duration_minutes (0 if duration is 0). One row per session.

<details>
<summary>Answer — fct_sessions (3.4–3.7)</summary>

```sql
{{ config(materialized='table') }}
with sessions as ( select * from {{ ref('stg_sessions') }} ),
players as ( select player_id, country_code, language_code, difficulty_selected from {{ ref('stg_players') }} ),
events_agg as ( /* match events to sessions by player_id + time window, then group by session_id */ ),
final as ( select s.*, p.country_code, p.language_code, p.difficulty_selected, e.total_events, e.deaths_count, ... from sessions s left join players p ... left join events_agg e ... )
select * from final
```

</details>

**fct_game_events**

- [ ] **3.8** Create model **fct_game_events** as a **materialized table**. Base: select from `ref('stg_game_events')` (one row per event).
- [ ] **3.9** Match each event to a session: same `player_id` and `event_at` between `session_start_at` and `session_end_at`. If an event falls in multiple sessions, pick one (e.g. earliest session). Add columns: `session_id`, `session_start_at`, `session_end_at`. Events outside any session keep `session_id` (and session timestamps) null.
- [ ] **3.10** Join events to **stg_players** to add `country_code`, `language_code`, `difficulty_selected`. In the final select add **seconds_since_session_start** = seconds from `session_start_at` to `event_at` (null when session_id is null). One row per event.

<details>
<summary>Answer — fct_game_events (3.8–3.10)</summary>

```sql
{{ config(materialized='table') }}
with events as ( select * from {{ ref('stg_game_events') }} ),
sessions as ( select session_id, player_id, session_start_at, session_end_at from {{ ref('stg_sessions') }} ),
players as ( select player_id, country_code, language_code, difficulty_selected from {{ ref('stg_players') }} ),
events_with_sessions as ( /* left join events to sessions on player_id and event_at between start/end; row_number() to pick one session per event */ ),
final as ( select *, datediff('second', session_start_at, event_at) as seconds_since_session_start from ... )
select * from final
```

</details>

- [ ] **3.11** Add schema YAML for `dim_players`, `fct_sessions`, and `fct_game_events`: column descriptions and tests. Include `unique` + `not_null` on primary keys, `relationships`: `fct_sessions.player_id` → `dim_players.player_id`, `fct_game_events.session_id` → `fct_sessions.session_id`.

<details>
<summary>Answer — Core marts schema (3.11)</summary>

```yaml
# dim_players.yml / fct_sessions.yml / fct_game_events.yml
models:
  - name: dim_players
    columns:
      - name: player_id
        tests: [unique, not_null]
# + relationships on fct_sessions.player_id, fct_game_events.session_id
```

</details>

### Phase 4: Analytics marts

**daily_active_players**

- [ ] **4.1** Create **daily_active_players** as a materialized table. From **stg_sessions**, compute per (player_id, session_date, platform): session date, platform, sessions_count (count distinct session_id), total_playtime_minutes (sum duration). Join to **stg_players** for `country_code`, `difficulty_selected`.
- [ ] **4.2** In **daily_active_players**, aggregate by (session_date, platform, country_code, difficulty_selected). Output: `session_date`, `platform`, `country_code`, `difficulty_selected`, `active_players` (count distinct player_id), `total_sessions`, `total_playtime_minutes`, `avg_sessions_per_player`, `avg_playtime_minutes_per_player`. Order by session_date desc, platform, country_code, difficulty_selected.

<details>
<summary>Answer — daily_active_players (4.1–4.2)</summary>

```sql
{{ config(materialized='table') }}
with sessions as ( select player_id, platform, date(session_start_at) as session_date, count(*) as sessions_count, sum(session_duration_minutes) as total_playtime_minutes from {{ ref('stg_sessions') }} group by 1,2,3 ),
players as ( select player_id, country_code, difficulty_selected from {{ ref('stg_players') }} ),
final as ( select s.session_date, s.platform, p.country_code, p.difficulty_selected, count(distinct s.player_id) as active_players, sum(s.sessions_count) as total_sessions, ... from sessions s left join players p ... group by ... )
select * from final order by session_date desc, platform, country_code, difficulty_selected
```

</details>

**funnel_sessions**

- [ ] **4.3** Create **funnel_sessions** as a materialized table. Match **stg_game_events** to **stg_sessions** by player_id and event time within session window. Per session compute flags: has_game_started, has_chapter_started, has_checkpoint_reached, has_chapter_completed, has_game_closed (1 if any such event, else 0), and counts: game_started_count, chapters_started_count, checkpoints_reached_count, chapters_completed_count. Join to **stg_players** for country_code, difficulty_selected.
- [ ] **4.4** In **funnel_sessions**, aggregate by (session_date, platform, country_code, difficulty_selected). Output: total_sessions, sessions_with_game_started, sessions_with_chapter_started, sessions_with_checkpoint_reached, sessions_with_chapter_completed, sessions_with_game_closed; conversion rates as percentage of total_sessions (e.g. game_started_rate_pct); avg_chapters_started, avg_checkpoints_reached, avg_chapters_completed, avg_session_duration_minutes. Order by session_date desc, platform, country_code, difficulty_selected.

<details>
<summary>Answer — funnel_sessions (4.3–4.4)</summary>

```sql
{{ config(materialized='table') }}
with sessions as ( select ... from {{ ref('stg_sessions') }} ),
events as ( select player_id, event_name, event_at from {{ ref('stg_game_events') }} ),
session_events as ( /* join sessions + events by player_id and time window; max(case when event_name=...) as has_* */ ),
final as ( select session_date, platform, country_code, difficulty_selected, count(*) as total_sessions, sum(has_game_started), ... from session_events ... )
select * from final order by ...
```

</details>

**retention**

- [ ] **4.5** Create **retention** as a materialized table. Define cohorts from **stg_players**: cohort_date = date(first_seen_at), plus country_code, difficulty_selected. From **stg_sessions** take (player_id, session_date).
- [ ] **4.6** In **retention**: join so each (player, cohort_date, session_date) has session_date >= cohort_date. Compute days_since_cohort = datediff(day, cohort_date, session_date). Aggregate by (cohort_date, country_code, difficulty_selected, days_since_cohort): active_players (count distinct player_id), cohort_size (same for all days in that cohort). Add retention_rate_pct = (active_players / cohort_size) * 100. Order by cohort_date desc, days_since_cohort, country_code, difficulty_selected.
- [ ] **4.7** Add schema YAML for the three analytics models: descriptions and, where useful, tests (e.g. not_null on key dimensions, non-negative numeric columns).

<details>
<summary>Answer — retention (4.5–4.6) + analytics schema (4.7)</summary>

```sql
-- retention.sql: cohorts from stg_players (cohort_date=date(first_seen_at)); join to session dates; aggregate by cohort_date, country_code, difficulty_selected, days_since_cohort; retention_rate_pct = active_players/cohort_size*100
{{ config(materialized='table') }}
with players as ( select player_id, country_code, difficulty_selected, date(first_seen_at) as cohort_date from {{ ref('stg_players') }} ),
...
```
```yaml
# analytics model .yml: not_null on session_date, platform, country_code, etc.
```

</details>

### Phase 5: Macros and project config

- [ ] **5.1** Add macro **generate_schema_name(custom_schema_name, node)** so that when `custom_schema_name` is set it is used, otherwise use `target.schema`. (This lets staging and marts build into different schemas when you set `+schema` in dbt_project.)
- [ ] **5.2** In `dbt_project.yml`, under `models.game_analytics`, define **staging** with `+materialized: view` and `+schema: staging`, and **marts** with `+materialized: table` and `+schema: marts`.

<details>
<summary>Answer — Phase 5 (macro + dbt_project)</summary>

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name is not none else target.schema }}
{%- endmacro %}
```
```yaml
# dbt_project.yml
models:
  game_analytics:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
```

</details>

### Phase 6: Tests and quality

- [ ] **6.1** In schema YAMLs, ensure primary key columns have `unique` and `not_null`; foreign keys have `relationships` to the referenced model. Add `not_null` (or accepted_values) on other critical columns.
- [ ] **6.2** Add a **singular test** (e.g. `tests/sessions_no_overlap.sql`) that returns rows where the same player has two sessions with overlapping [session_start_at, session_end_at]. The test should fail if any such rows exist. Register it in `tests/schema.yml` and run `dbt test`.
- [ ] **6.3** Run `dbt build` locally and fix any failing models or tests until everything passes.

<details>
<summary>Answer — Phase 6 (tests)</summary>

```sql
-- tests/sessions_no_overlap.sql: return rows where same player has two sessions with overlapping [start,end]
select * from ( ... s1 join s2 on player_id and s1.session_start < s2.session_end and s1.session_end > s2.session_start )
```
```yaml
# tests/schema.yml
tests:
  - name: sessions_no_overlap
    config: { severity: error, store_failures: true }
```

</details>

### Phase 7: CI

- [ ] **7.1** Add a GitHub Actions workflow (e.g. `.github/workflows/dbt.yml`) that on push/PR to `main`: checks out repo, sets up Python, installs dbt-snowflake, runs `dbt deps`, loads Snowflake profile from a secret (e.g. `SNOWFLAKE_CI_PROFILE`), runs `dbt compile --target ci`, and runs `dbt build --target ci`. Ensure the CI target in the profile uses a dedicated schema so broken contracts or failing tests block the merge.

<details>
<summary>Answer — Phase 7 (CI)</summary>

```yaml
# .github/workflows/dbt.yml
name: dbt
on:
  push:    { branches: [main] }
  pull_request: { branches: [main] }
jobs:
  dbt-compile:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
      - run: pip install dbt-snowflake && dbt deps
      - run: dbt compile --target ci
      - run: dbt build --target ci
# profile from secrets.SNOWFLAKE_CI_PROFILE
```

</details>

---

## 🏁 Final Boss: Product Questions You Must Be Able to Answer

![Bloater](images/bloater.png)

You didn't build a warehouse to admire clean models. You built it to answer hard product questions.

After completing all phases (sources → staging → marts → tests → CI), your final task is to answer the questions below **using only your dbt models**:

- `dim_players`
- `fct_sessions`
- `fct_game_events`
- `daily_active_players`
- `funnel_sessions`
- `retention`

If you can answer these — your warehouse is working.

### 🎮 1. Retention & Player Return

1. What are **D1, D3, and D7 retention rates**?
2. Which **countries** have the lowest retention?
3. Does retention differ by **difficulty_selected**?
4. What % of players have **only one session**?
5. What % of players return for a **second session**?

Goal: understand whether players come back — and who does not.

### 💀 2. Drop-Off & Friction

6. At which step of the session funnel do players drop the most?
7. What % of sessions never reach a **checkpoint**?
8. What % of sessions start a chapter but never complete one?
9. Are there sessions with **high deaths but low progression**?

Goal: identify friction points in the core loop.

### ⚔️ 3. Difficulty & Balance

10. Which difficulty has the highest **death rate per session**?
11. Do players on higher difficulty churn faster?
12. Which chapters have the lowest completion rates?
13. Is there a relationship between **session duration** and **chapter completion**?

Goal: detect balance issues before ship.

### ⏱ 4. Session Behavior

14. What is the **median session duration**?
15. Are longer sessions correlated with higher retention?
16. What is the average **events_per_minute**?
17. Are there sessions with zero or unusually low event activity?

Goal: understand how players actually play.

### 🌍 5. Segmentation

18. Define a "core player" (e.g. ≥5 sessions, ≥120 minutes playtime, ≥1 chapter completed).

- How many core players exist?
- From which countries?
- On which difficulty?

19. Define "one-and-done" players (1 session, no chapter completed).

- What % of total players are they?
- Do they cluster by country or difficulty?

Goal: separate your engaged audience from early churn.

### 📉 6. Data Quality & Edge Cases

20. Are there **events outside session windows**?
21. Are there overlapping sessions for the same player?
22. Are there sessions with negative or zero duration?
23. Are any core foreign key relationships broken?

Goal: validate the integrity of your warehouse.

### 🚀 7. Strategic Thinking

24. If D1 retention improved by 5%, how would that affect DAU over 30 days?
25. If one chapter's completion rate increased by 10%, what downstream metrics would likely change?
26. If you could monitor only **one metric before launch**, what would it be — and why?

Goal: move from reporting to decision-making.

### 🏆 Completion Criteria

You have successfully completed the project if:

- You can answer these questions using only warehouse models.
- You can explain *how* each answer is derived.
- Your models pass all tests.
- Your CI pipeline fails when contracts break.
- You can identify at least one actionable product insight.

At that point, you are no longer "building tables." You are running a game analytics platform.

---

## Repos at a Glance

| Path | Purpose |
|------|--------|
| **This folder** (`game-data-platform/`) | Story and task list (this README). Run `python app/main.py` from here to generate and load data. Pipeline details in [app/README.md](app/README.md). |
| `app/` | Pipeline code: gen, ingest, main. See [app/README.md](app/README.md). |
| `game-dbt-project/` (sibling) | dbt project: sources → staging → marts, macros, tests. Run `dbt build` after data is loaded. |

Start with the platform to get data; then work through the dbt tasks in order. When you’re done, you’ll have a full story: from “raw events in Snowflake” to “tables ready for DAU, funnel, and retention.”
