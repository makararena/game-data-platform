# Game Analytics: Build Your Own Warehouse

A hands-on project to build an analytics warehouse for a narrative game. You run the **game data platform** to generate and load raw data into Snowflake, then build the **dbt project** step by step: raw → staging → marts, macros, and tests. By the end, you’ll have a working pipeline and a clear path from raw events to reporting-ready tables.

---

## The Story

![Joel and Ellie in the post-apocalyptic world of The Last of Us](images/last-of-us/img.png)

It’s **2012** in Santa Monica. You’re on the team building **The Last of Us** — the story of Joel and Ellie is on the page, the levels are greyboxed, and combat is being tuned week by week. Design keeps asking: *Where do players die the most? Do they come back after the first session? Which chapters get abandoned?* Right now the answer is usually “we’ll check the build and the forums.” You’ve already wired the game to emit events and dump them into pipelines, but there’s no single place to go for “how many players, how many sessions, where they drop.” Someone has to turn that raw firehose into tables the team can actually use. That someone is you.

You’re in a lucky position: unlike the real 2012 stack, you have **modern tools**. **Snowflake** holds the raw data; **dbt** and a proper data platform are on the table. So instead of one-off scripts and spreadsheets, you get to build an **analytics warehouse** the right way — clean, tested, and ready for the questions that will only get louder as the game nears ship.

---

**What you have to work with**

The game is at **MVP**: a few key flows are playable end-to-end, and telemetry is already flowing. You have **three core data frames** (and the pipelines that fill them):

- **Players** — one row per player: country, language, difficulty, first seen, and other attributes. Who they are and how they chose to play.
- **Sessions** — one row per play session. Each player has many sessions; each session has a start time, end time, platform, and duration. *When* and *how long* they play.
- **Events** — one row per in-game event. Events belong to sessions (and thus to players): *game started*, *chapter started*, *checkpoint reached*, *enemy killed*, *player died*, *item crafted*, *chapter completed*, *game closed*. Payloads can include chapter names, locations, weather, enemy names, weapon names, crafting materials — the stuff design will use to balance difficulty and fix friction.

The hierarchy is simple: **player → sessions → events**. Your job is to turn that into a warehouse: define sources, stage and clean the data, build dimensions and facts (dim_players, fct_sessions, fct_game_events), add analytics marts (DAU, funnel, retention), and harden everything with tests and CI. When you’re done, the team can stop guessing and start answering — and you’ll have done it step by step, by following the tasks below. No prior dbt experience required.

### Schema hierarchy (entities and relationships)

There are **three entities**; all relationships are **one-to-many** (no many-to-many in the core model):

| Entity   | Raw table        | Grain              | Relationship                          |
|----------|------------------|--------------------|---------------------------------------|
| **Player**  | `RAW_PLAYERS`    | One row per player | One player has **many** sessions      |
| **Session** | `RAW_SESSIONS`   | One row per session| One session has **many** events       |
| **Event**   | `RAW_GAME_EVENTS`| One row per event  | Belongs to exactly one session (and thus one player) |

```
┌─────────────┐      1:N      ┌─────────────┐      1:N      ┌─────────────┐
│   PLAYER    │──────────────>│   SESSION   │──────────────>│    EVENT    │
├─────────────┤               ├─────────────┤               ├─────────────┤
│ player_id   │               │ session_id  │               │ event_id    │
│ first_seen  │               │ player_id   │               │ session_id  │
│ country     │               │ start/end   │               │ player_id   │
│ language    │               │ platform    │               │ event_name  │
│ difficulty  │               │             │               │ event_at    │
└─────────────┘               └─────────────┘               │ properties  │
                                                            └─────────────┘
```

In short: **Player 1 → N Sessions → N Events**. Events are tied to a session via `session_id` (and to a player via `player_id`); sessions are tied to a player via `player_id`.

### Unique values (reference data)

The synthetic pipeline uses fixed sets of values. Useful for dbt `accepted_values` tests and for understanding the data:

| Field          | Table(s)        | Unique values |
|----------------|-----------------|---------------|
| **country**    | `RAW_PLAYERS`   | `US`, `PL`, `DE`, `FR`, `ES`, `BY` |
| **language**   | `RAW_PLAYERS`   | `en`, `pl`, `de`, `fr`, `es`, `be` |
| **difficulty_selected** | `RAW_PLAYERS` | `easy`, `normal`, `hard`, `grounded` |
| **platform**   | `RAW_SESSIONS`, `RAW_GAME_EVENTS` | `ps5`, `pc` |
| **event_name** | `RAW_GAME_EVENTS` | `game_started`, `chapter_started`, `checkpoint_reached`, `enemy_killed`, `player_died`, `item_crafted`, `chapter_completed`, `game_closed` |

**properties** (in `RAW_GAME_EVENTS`) is a JSON/VARIANT; not every event has every key. Possible keys and example values:

- **chapter_name**: e.g. `The Outskirts`, `The Quarantine Zone`, `Downtown`, `The Suburbs`, `The University`, `The Hospital`, `The Financial District`, `The Docks`, `The Bridge`, `The Firefly Lab`
- **location**: e.g. `abandoned_building`, `street`, `sewer`, `rooftop`, `warehouse`, `park`, `subway`, `apartment`, `mall`, `school`
- **weather**: `clear`, `rain`, `fog`, `snow`
- **enemy_name**: e.g. `runner`, `clicker`, `bloater`, `stalker`, `shambler`, `hunter`, `soldier`, `scavenger`, `bandit`, `merchant`
- **weapon_name**: e.g. `9mm_pistol`, `revolver`, `hunting_rifle`, `assault_rifle`, `hunting_bow`, `compound_bow`
- **crafting_materials**: e.g. `alcohol`, `rag`, `scissors`, `bottle`, `tape`, `blade` (used for medkit, molotov, shiv)

---

## Project Outline

| Layer / Area   | What you build |
|----------------|----------------|
| **Data in**    | Run `game-data-platform`: generate data → load to Snowflake. |
| **Raw**        | dbt **sources**: `raw_players`, `raw_sessions`, `raw_game_events` (schema, columns, optional source tests). |
| **Staging**    | dbt **models**: `stg_players`, `stg_sessions`, `stg_game_events` — clean, standardize names and types. |
| **Core marts** | `dim_players`, `fct_sessions`, `fct_game_events` — dimensions and facts for analysis. |
| **Analytics marts** | `daily_active_players`, `funnel_sessions`, `retention` — reporting-ready aggregates. |
| **Macros**     | `generate_schema_name` for environment-specific schemas. |
| **Tests**      | Column tests (not null, unique, relationships) + at least one singular test (e.g. sessions don’t overlap). |
| **CI**         | GitHub Action: `dbt compile` and optionally `dbt build` so broken contracts fail the pipeline. |

 ---
 
 ## Tasks

Do the tasks in order. Each task is a single, clear step from start to finish.

---

### Phase 0: Get data and set up dbt

- [ ] **0.1** **Snowflake account setup** — follow [Snowflake account setup](instructions/snowflake-account-setup.md).
- [ ] **0.2** **Pre-launch setup** — create database and schemas in Snowflake. See [Pre-launch setup](instructions/pre-launch-setup.md): choose **Path 1** (minimal: database + schemas only) or **Path 2** (production-style: warehouse + database + role + schemas, including `ALTER USER ... SET DEFAULT_ROLE = DBT_ROLE`).
- [ ] **0.3** **Snowflake credentials** — see [Snowflake credentials](instructions/snowflake-credentials.md) for how to find your account identifier, user, warehouse, database, and schema.
- [ ] **0.4** **Run the platform** — from the repo root run `chmod +x run_platform.sh` (first time only), then `./run_platform.sh` to generate synthetic data and load `RAW_PLAYERS`, `RAW_SESSIONS`, `RAW_GAME_EVENTS` into Snowflake.
- [ ] **0.5** **Set up dbt** — follow [dbt setup](instructions/dbt-setup.md): create your dbt project, `~/.dbt/profiles.yml` (profile `game_analytics` or as in the doc), and run `dbt deps`.

<details>
<summary>Check yourself (Phase 0)</summary>

Verify each of the following:

1. **Snowflake account access** — You can log in and see your account (e.g. trial credits, ACCOUNTADMIN role).  
   ![Snowflake account access](images/check-yourself-0/account.png)

2. **Database, schemas, and tables in the catalog** — In Snowflake’s Database Explorer (Horizon Catalog), the `GAME_ANALYTICS` database and its schemas are visible; under the `RAW` schema you see the three tables: `RAW_GAME_EVENTS`, `RAW_PLAYERS`, `RAW_SESSIONS`.  
   ![Database Explorer — GAME_ANALYTICS / RAW tables](images/check-yourself-0/db-schema-tables.png)

3. **Data preview** — In Snowflake's Database Explorer, open each raw table and go to the **Data Preview** tab. You should see sample rows in each table. Use this to confirm data was loaded correctly. Reference screenshots:
   - **RAW_PLAYERS** (e.g. ~2K rows: player_id, first_seen_at, country, language, difficulty_selected)  
     ![RAW_PLAYERS data preview](images/check-yourself-0/raw_players.png)
   - **RAW_SESSIONS** (e.g. ~6K rows: session_id, player_id, session_start, session_end, platform)  
     ![RAW_SESSIONS data preview](images/check-yourself-0/raw_sessions.png)
   - **RAW_GAME_EVENTS** (e.g. ~231K rows: event_id, event_time, player_id, event_name, platform)  
     ![RAW_GAME_EVENTS data preview](images/check-yourself-0/raw_game_events.png)

4. **dbt project and venv in parent directory** — One level up from `game-data-platform` you have your dbt project folder (created by `dbt init`) and a `venv` for dbt. You run dbt from the project folder with the venv activated.

5. **dbt debug passes** — From your dbt project directory, run `dbt debug`. The output shows all checks passed (connection, profile, and project config).  
   ![dbt debug — all checks passed](images/check-yourself-0/dbt-debug.png)

</details>

### Phase 1: Raw layer (sources)

**Instructions:** [Phase 1 — Task](instructions/phases/phase1/phase1-raw-sources.md) · [Phase 1 — Check yourself](instructions/phases/phase1/phase1-raw-sources-check-yourself.md)

---

### Phase 2: Macros and multi-schema support

**Instructions:** [Phase 2 — Task](instructions/phases/phase2/phase2-macros-schemas.md) · [Phase 2 — Check yourself](instructions/phases/phase2/phase2-macros-schemas-check-yourself.md)

- [ ] **2.1** Add **vars** `raw_schema`, `staging_schema`, and `marts_schema` in `dbt_project.yml`.
- [ ] **2.2** Set each **source**’s `schema` to `"{{ var('raw_schema', 'RAW') }}"`.
- [ ] **2.3** Create **`generate_schema_name`** macro so models with `+schema: staging` build in `var('staging_schema', 'STAGING')` and `+schema: marts` in `var('marts_schema', 'MARTS')`.
- [ ] **2.4** Under `models`, set **staging** folder to `+schema: staging`.
- [ ] **2.5** Under `models`, set **marts** folder to `+schema: marts`.

---

### Phase 3: Staging layer

**Instructions:** [Phase 3 — Task](instructions/phases/phase3/phase3-staging.md) · [Phase 3 — Check yourself](instructions/phases/phase3/phase3-staging-check-yourself.md)

- [ ] **3.1** Create **stg_players**: a view from `source('raw', 'raw_players')`. Output: `player_id`, `first_seen_at` (cast to timestamp), `country_code`, `language_code`, `difficulty_selected`. Materialize as view.
- [ ] **3.2** Create **stg_sessions**: a view from `source('raw', 'raw_sessions')`. Output: `session_id`, `player_id`, `session_start_at`, `session_end_at`, `platform`, `session_duration_minutes`. Materialize as view.
- [ ] **3.3** Create **stg_game_events**: a view from `source('raw', 'raw_game_events')`. Output: `event_id`, `event_at`, `player_id`, `event_name`, `platform`, `game_version`, `properties`. Materialize as view.
- [ ] **3.4** Add schema YAML for each staging model: descriptions, `unique`/`not_null` on primary keys, `relationships` from `stg_sessions.player_id` to `stg_players`.

---

### Phase 4: Core marts

**Instructions:** [Phase 4 — Task](instructions/phases/phase4/phase4-core-marts.md) · [Phase 4 — Check yourself](instructions/phases/phase4/phase4-core-marts-check-yourself.md)

- [ ] **4.1** Create model **dim_players** as a **materialized table**. Base: select all columns from `ref('stg_players')` (one row per player).
- [ ] **4.2** Add a CTE that aggregates **stg_sessions** per player: `total_sessions` (count), `total_playtime_minutes` (sum of `session_duration_minutes`), `avg_session_duration_minutes`, `first_session_at` (min of `session_start_at`), `last_session_at` (max of `session_start_at`), `active_days` (count distinct date of `session_start_at`). Group by `player_id`.
- [ ] **4.3** Join players to the session aggregates (left join). In the final select output: all player attributes, the session aggregate columns (use `coalesce(..., 0)` for counts and playtime so players with no sessions get 0), plus **days_since_first_seen** (days from `first_seen_at` to current date) and **days_since_last_session** (days from `last_session_at` to current date; null if no sessions). One row per player.
- [ ] **4.4** Create model **fct_sessions** as a **materialized table**. Base: select all columns from `ref('stg_sessions')` (one row per session).
- [ ] **4.5** Join each session to **stg_players** to add `country_code`, `language_code`, `difficulty_selected` to each session row.
- [ ] **4.6** Match **stg_game_events** to sessions by `player_id` and event time within `session_start_at` and `session_end_at`. Aggregate per session: `total_events`, `unique_event_types` (count distinct event_name), `deaths_count` (count where event_name = 'player_died'), `enemies_killed` (event_name = 'enemy_killed'), `chapters_completed` (event_name = 'chapter_completed'), `first_event_at`, `last_event_at`.
- [ ] **4.7** Join sessions to the event aggregates (left join). In the final select: all session and player columns, the event aggregate columns (use `coalesce(..., 0)` for counts), and **events_per_minute** = total_events / session_duration_minutes (0 if duration is 0). One row per session.
- [ ] **4.8** Create model **fct_game_events** as a **materialized table**. Base: select from `ref('stg_game_events')` (one row per event).
- [ ] **4.9** Match each event to a session: same `player_id` and `event_at` between `session_start_at` and `session_end_at`. If an event falls in multiple sessions, pick one (e.g. earliest session). Add columns: `session_id`, `session_start_at`, `session_end_at`. Events outside any session keep `session_id` (and session timestamps) null.
- [ ] **4.10** Join events to **stg_players** to add `country_code`, `language_code`, `difficulty_selected`. In the final select add **seconds_since_session_start** = seconds from `session_start_at` to `event_at` (null when session_id is null). One row per event.
- [ ] **4.11** Add schema YAML for `dim_players`, `fct_sessions`, and `fct_game_events`: column descriptions and tests. Include `unique` + `not_null` on primary keys, `relationships`: `fct_sessions.player_id` → `dim_players.player_id`, `fct_game_events.session_id` → `fct_sessions.session_id`.

---

### Phase 5: Analytics marts

**daily_active_players**

- [ ] **5.1** Create **daily_active_players** as a materialized table. From **stg_sessions**, compute per (player_id, session_date, platform): session date, platform, sessions_count (count distinct session_id), total_playtime_minutes (sum duration). Join to **stg_players** for `country_code`, `difficulty_selected`.
- [ ] **5.2** In **daily_active_players**, aggregate by (session_date, platform, country_code, difficulty_selected). Output: `session_date`, `platform`, `country_code`, `difficulty_selected`, `active_players` (count distinct player_id), `total_sessions`, `total_playtime_minutes`, `avg_sessions_per_player`, `avg_playtime_minutes_per_player`. Order by session_date desc, platform, country_code, difficulty_selected.

<details>
<summary>Check yourself — daily_active_players (4.1–4.2)</summary>

```sql
{{ config(materialized='table') }}
with sessions as ( select player_id, platform, date(session_start_at) as session_date, count(*) as sessions_count, sum(session_duration_minutes) as total_playtime_minutes from {{ ref('stg_sessions') }} group by 1,2,3 ),
players as ( select player_id, country_code, difficulty_selected from {{ ref('stg_players') }} ),
final as ( select s.session_date, s.platform, p.country_code, p.difficulty_selected, count(distinct s.player_id) as active_players, sum(s.sessions_count) as total_sessions, ... from sessions s left join players p ... group by ... )
select * from final order by session_date desc, platform, country_code, difficulty_selected
```

</details>

**funnel_sessions**

- [ ] **5.3** Create **funnel_sessions** as a materialized table. Match **stg_game_events** to **stg_sessions** by player_id and event time within session window. Per session compute flags: has_game_started, has_chapter_started, has_checkpoint_reached, has_chapter_completed, has_game_closed (1 if any such event, else 0), and counts: game_started_count, chapters_started_count, checkpoints_reached_count, chapters_completed_count. Join to **stg_players** for country_code, difficulty_selected.
- [ ] **5.4** In **funnel_sessions**, aggregate by (session_date, platform, country_code, difficulty_selected). Output: total_sessions, sessions_with_game_started, sessions_with_chapter_started, sessions_with_checkpoint_reached, sessions_with_chapter_completed, sessions_with_game_closed; conversion rates as percentage of total_sessions (e.g. game_started_rate_pct); avg_chapters_started, avg_checkpoints_reached, avg_chapters_completed, avg_session_duration_minutes. Order by session_date desc, platform, country_code, difficulty_selected.

<details>
<summary>Check yourself — funnel_sessions (4.3–4.4)</summary>

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

- [ ] **5.5** Create **retention** as a materialized table. Define cohorts from **stg_players**: cohort_date = date(first_seen_at), plus country_code, difficulty_selected. From **stg_sessions** take (player_id, session_date).
- [ ] **5.6** In **retention**: join so each (player, cohort_date, session_date) has session_date >= cohort_date. Compute days_since_cohort = datediff(day, cohort_date, session_date). Aggregate by (cohort_date, country_code, difficulty_selected, days_since_cohort): active_players (count distinct player_id), cohort_size (same for all days in that cohort). Add retention_rate_pct = (active_players / cohort_size) * 100. Order by cohort_date desc, days_since_cohort, country_code, difficulty_selected.
- [ ] **5.7** Add schema YAML for the three analytics models: descriptions and, where useful, tests (e.g. not_null on key dimensions, non-negative numeric columns).

<details>
<summary>Check yourself — retention (4.5–4.6) + analytics schema (4.7)</summary>

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

#### Why this phase matters

- Analytics marts (DAU, funnel, retention) translate raw behavioral data into the KPIs your team actually discusses.
- By keeping these as dbt models, you can iterate on definitions (e.g. what counts as active) with version control and tests instead of ad hoc SQL.

### Phase 6: Macros and project config

- [ ] **6.1** Add macro **generate_schema_name(custom_schema_name, node)** so that when `custom_schema_name` is set it is used, otherwise use `target.schema`. (Phase 2 already covers this; use this task if you add more layers, e.g. marts schema.)
- [ ] **6.2** In `dbt_project.yml`, under `models.game_analytics`, define **staging** with `+materialized: view` and `+schema: staging`, and **marts** with `+materialized: table` and `+schema: marts`.

<details>
<summary>Check yourself (Phase 5 — macro + dbt_project)</summary>

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

#### Why this phase matters

- Centralizing environment settings (schemas, materializations) keeps dev, CI, and prod aligned without copying SQL.
- Small macros like `generate_schema_name` make it easy to add more environments later without rewriting models.

### Phase 7: Tests and quality

- [ ] **7.1** In schema YAMLs, ensure primary key columns have `unique` and `not_null`; foreign keys have `relationships` to the referenced model. Add `not_null` (or accepted_values) on other critical columns.
- [ ] **7.2** Add a **singular test** (e.g. `tests/sessions_no_overlap.sql`) that returns rows where the same player has two sessions with overlapping [session_start_at, session_end_at]. The test should fail if any such rows exist. Register it in `tests/schema.yml` and run `dbt test`.
- [ ] **7.3** Run `dbt build` locally and fix any failing models or tests until everything passes.

<details>
<summary>Check yourself (Phase 6 — tests)</summary>

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

#### Why this phase matters

- Tests turn your warehouse into something you can trust: they catch broken assumptions as soon as data or code changes.
- Encoding business rules (like no overlapping sessions) in tests prevents silent data drift that would invalidate product decisions.

### Phase 8: CI

- [ ] **8.1** Add a GitHub Actions workflow (e.g. `.github/workflows/dbt.yml`) that on push/PR to `main`: checks out repo, sets up Python, installs dbt-snowflake, runs `dbt deps`, loads Snowflake profile from a secret (e.g. `SNOWFLAKE_CI_PROFILE`), runs `dbt compile --target ci`, and runs `dbt build --target ci`. Ensure the CI target in the profile uses a dedicated schema so broken contracts or failing tests block the merge.

<details>
<summary>Check yourself (Phase 7 — CI)</summary>

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

#### Why this phase matters

- CI ensures every change to models or tests is exercised automatically before it reaches main.
- When CI fails on bad data or broken contracts, you find issues in pull requests instead of in production dashboards.

---

### Phase 9 (Advanced): Incremental `fct_game_events`

- [ ] **9.1** Update `fct_game_events` to use `materialized='incremental'` with a stable `unique_key` (e.g. `event_id`) and a sensible `on_schema_change` strategy.
- [ ] **9.2** Implement an incremental filter with `is_incremental()` so that incremental runs only process **new** events (e.g. `event_at > max(event_at) in {{ this }}`).
- [ ] **9.3** Run a full-refresh and an incremental run, compare row counts and tests, and make sure incremental behavior matches the full build.

<details>
<summary>Check yourself (Phase 9 — incremental fct_game_events)</summary>

```sql
{{ config(
  materialized = 'incremental',
  unique_key   = 'event_id',
  on_schema_change = 'ignore',  -- or 'append_new_columns' / 'sync_all_columns'
) }}

with events as (
  select * from {{ ref('stg_game_events') }}
  -- joins and enrichments here
)

{% if is_incremental() %}
  -- Only process events later than what we already have
  , filtered as (
      select *
      from events
      where event_at > (select coalesce(max(event_at), '1900-01-01') from {{ this }})
    )
  select * from filtered
{% else %}
  select * from events
{% endif %}
```

</details>

#### Why this phase matters

- Real production warehouses rarely rebuild large event tables from scratch — **incremental models** keep compute + cost under control.
- Designing a correct incremental filter (grain, unique key, late-arriving data) forces you to think carefully about **time, idempotency, and data correctness**.


Congratulations — you’ve completed all 8 core phases. The *Final Boss* section below is the bonus challenge.

---

## 🏁 Final Boss: Product Questions You Must Be Able to Answer

![Bloater](images/last-of-us/bloater.png)

You didn't build a warehouse to admire clean models. You built it to answer hard product questions.

After completing all phases (sources → macros/schemas → staging → marts → tests → CI), your final task is to answer the questions below **using only your dbt models**:

- `dim_players`
- `fct_sessions`
- `fct_game_events`
- `daily_active_players`
- `funnel_sessions`
- `retention`

If you can answer these — your warehouse is working.

### 1. Retention & Player Return

1. What are **D1, D3, and D7 retention rates**?
2. Which **countries** have the lowest retention?
3. Does retention differ by **difficulty_selected**?
4. What % of players have **only one session**?
5. What % of players return for a **second session**?

Goal: understand whether players come back — and who does not.

<details>
<summary>Check yourself — 1. Retention & Player Return</summary>

- **Q1 – D1/D3/D7 retention**: use the `retention` model; filter `days_since_cohort IN (1, 3, 7)` and read `retention_rate_pct` for those days (optionally averaging across cohorts).
- **Q2 – Countries with lowest retention**: from `retention`, group by `country_code` and a chosen `days_since_cohort` (e.g. 1 or 7), order by `retention_rate_pct` ascending.
- **Q3 – Retention by difficulty**: same `retention` model, grouped by `difficulty_selected` and `days_since_cohort`.
- **Q4 – % of players with only one session**: from `dim_players`, compute `count(*) WHERE total_sessions = 1` divided by `count(*)` overall.
- **Q5 – % of players returning for a second session**: from `dim_players`, compute `count(*) WHERE total_sessions >= 2` divided by `count(*)` overall.

</details>

### 2. Drop-Off & Friction

6. At which step of the session funnel do players drop the most?
7. What % of sessions never reach a **checkpoint**?
8. What % of sessions start a chapter but never complete one?
9. Are there sessions with **high deaths but low progression**?

Goal: identify friction points in the core loop.

<details>
<summary>Check yourself — 2. Drop-Off & Friction</summary>

- **Q6 – Funnel step with biggest drop**: use `funnel_sessions`; compare `sessions_with_*` columns vs `total_sessions` for each step to see where conversion rate is lowest.
- **Q7 – % of sessions without a checkpoint**: in `funnel_sessions`, compute `1 - sessions_with_checkpoint_reached / total_sessions` for your chosen date range.
- **Q8 – Sessions that start a chapter but never complete one**: from `funnel_sessions`, compare `sessions_with_chapter_started` vs `sessions_with_chapter_completed` and compute the difference / `total_sessions`.
- **Q9 – High deaths, low progression sessions**: query `fct_sessions` filtering for `deaths_count` above a threshold and `chapters_completed = 0` (or very low), optionally grouping by chapter or difficulty to locate problem areas.

</details>

### 3. Difficulty & Balance

10. Which difficulty has the highest **death rate per session**?
11. Do players on higher difficulty churn faster?
12. Which chapters have the lowest completion rates?
13. Is there a relationship between **session duration** and **chapter completion**?

Goal: detect balance issues before ship.

<details>
<summary>Check yourself — 3. Difficulty & Balance</summary>

- **Q10 – Difficulty with highest death rate per session**: from `fct_sessions`, group by `difficulty_selected` and compute `sum(deaths_count) / count(*)` (or average `deaths_count`).
- **Q11 – Do higher difficulties churn faster?**: from `retention`, group by `difficulty_selected` and `days_since_cohort` (e.g. day 1 / 7) and compare `retention_rate_pct` across difficulties.
- **Q12 – Chapters with lowest completion**: use `fct_game_events` (or staging events) to compare counts of `chapter_started` vs `chapter_completed` by `chapter_id` / `chapter_name` and compute completion rate.
- **Q13 – Session duration vs chapter completion**: from `fct_sessions`, bucket `session_duration_minutes` (short/medium/long) and compute `chapters_completed` or completion rate per bucket to see the relationship.

</details>

### 4. Session Behavior

14. What is the **median session duration**?
15. Are longer sessions correlated with higher retention?
16. What is the average **events_per_minute**?
17. Are there sessions with zero or unusually low event activity?

Goal: understand how players actually play.

<details>
<summary>Check yourself — 4. Session Behavior</summary>

- **Q14 – Median session duration**: read `median(session_duration_minutes)` (or approximate with `percentile_cont(0.5)`) from `fct_sessions`.
- **Q15 – Longer sessions vs retention**: aggregate `fct_sessions` to player level (e.g. average `session_duration_minutes` per player), join to `retention` or cohort info, and compare retention metrics across duration buckets.
- **Q16 – Average events_per_minute**: from `fct_sessions`, take `avg(events_per_minute)` (optionally by difficulty, platform, or country).
- **Q17 – Low-activity sessions**: filter `fct_sessions` where `events_per_minute` is near zero or `total_events = 0` to find idle/buggy sessions.

</details>

### 5. Segmentation

18. Define a "core player" (e.g. ≥5 sessions, ≥120 minutes playtime, ≥1 chapter completed).

- How many core players exist?
- From which countries?
- On which difficulty?

19. Define "one-and-done" players (1 session, no chapter completed).

- What % of total players are they?
- Do they cluster by country or difficulty?

Goal: separate your engaged audience from early churn.

<details>
<summary>Check yourself — 5. Segmentation</summary>

- **Q18 – Core players**: in `dim_players`, define a core player as `total_sessions >= 5`, `total_playtime_minutes >= 120`, and `chapters_completed >= 1` (if you add that metric); count them and slice by `country_code` and `difficulty_selected`.
- **Q19 – One-and-done players**: from `dim_players`, filter `total_sessions = 1` and (optionally) zero progression; compute their share of all players and break down by country and difficulty to see where early churn concentrates.

</details>

### Completion Criteria

You have successfully completed the project if:

- You can answer these questions using only warehouse models.
- You can explain *how* each answer is derived.
- Your models pass all tests.
- Your CI pipeline fails when contracts break.
- You can identify at least one actionable product insight.

At that point, you are no longer "building tables." You are running a game analytics platform.