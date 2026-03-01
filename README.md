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

There are **three entities**; all relationships are **one-to-many**:

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
| **platform**   | `RAW_SESSIONS`, `RAW_GAME_EVENTS` | `ps3`, `xbox360`, `pc` |
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
 
## dbt commands we are gonna use and what they mean

This project uses a small set of `dbt` commands during development and in CI. Below is what each command does, when to run it, and a short example.

- **`dbt deps`**: Installs packages listed in `packages.yml` into the local `dbt_modules/` folder. Run this after cloning the repo or when `packages.yml` changes.

```bash
dbt deps
```

- **`dbt compile`**: Renders Jinja and SQL for all models, macros, and tests, and writes the compiled SQL to `target/compiled/`. Useful to quickly validate templating and macros without executing queries. In CI we run `dbt compile --target ci` to ensure the project compiles under the CI profile.

```bash
dbt compile
dbt compile --target ci
```

- **`dbt build`**: The all-in-one command that runs `dbt run` (models), `dbt test` (schema & data tests), `dbt seed`, and `dbt snapshot` in the proper dependency order. Use this to execute models and tests together; CI uses `dbt build --target ci` so failing tests or models break the pipeline.

```bash
dbt build
dbt build --target ci
```

- **`dbt run`**: Executes model SQL to create/update objects in the target database/schema. Use this locally when you only want to materialize models (not tests).

```bash
dbt run
```

- **`dbt test`**: Runs schema tests and any singular tests defined in `tests/` (e.g. `sessions_no_overlap.sql`). Use this to validate data quality after model runs, or run tests independently in CI if preferred.

```bash
dbt test
```

- **`dbt debug`**: Checks the active `profiles.yml`, confirms connection to the data warehouse, and validates the environment. Useful when setting up a new profile or troubleshooting CI credentials.

```bash
dbt debug
```

- **`dbt docs generate` / `dbt docs serve`**: Builds the documentation site (`target/catalog.json`, `manifest.json`) and serves it locally. Helpful for exploring model lineage and column descriptions.

```bash
dbt docs generate
dbt docs serve
```

Notes:
- In CI we prefer `dbt compile --target ci` to detect compilation issues early and `dbt build --target ci` to run models + tests and fail the job on error.
- Use `--select` / `--exclude` flags to target subsets of models (e.g., `dbt build --select marts+`).
- Keep secrets out of source: the workflow expects a `SNOWFLAKE_CI_PROFILE` secret that writes `~/.dbt/profiles.yml` during the run.

 
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

⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜ 0%

**Instructions:** [Phase 1 — Task](instructions/phases/phase1/phase1-raw-sources.md) · [Phase 1 — Check yourself](instructions/phases/phase1/phase1-raw-sources-check-yourself.md)

---

### Phase 2: Macros and multi-schema support

🟩⬜⬜⬜⬜⬜⬜⬜⬜⬜ 10%

**Instructions:** [Phase 2 — Task](instructions/phases/phase2/phase2-macros-schemas.md) · [Phase 2 — Check yourself](instructions/phases/phase2/phase2-macros-schemas-check-yourself.md)

---

### Phase 3: Staging layer

🟩🟩⬜⬜⬜⬜⬜⬜⬜⬜ 20%

**Instructions:** [Phase 3 — Task](instructions/phases/phase3/phase3-staging.md) · [Phase 3 — Check yourself](instructions/phases/phase3/phase3-staging-check-yourself.md)

---

### Phase 4: Core marts

🟩🟩🟩⬜⬜⬜⬜⬜⬜⬜ 30%

**Instructions:** [Phase 4 — Task](instructions/phases/phase4/phase4-core-marts.md) · [Phase 4 — Check yourself](instructions/phases/phase4/phase4-core-marts-check-yourself.md)

---

### Phase 5: Analytics marts

🟩🟩🟩🟩⬜⬜⬜⬜⬜⬜ 40%

**Instructions:** [Phase 5 — Task](instructions/phases/phase5/phase5-analytics-marts.md) · [Phase 5 — Check yourself](instructions/phases/phase5/phase5-analytics-marts-check-yourself.md)

---

### Phase 6: Tests and quality

🟩🟩🟩🟩🟩⬜⬜⬜⬜⬜ 50%

**Instructions:** [Phase 6 — Task](instructions/phases/phase6/phase6-tests-quality.md) · [Phase 6 — Check yourself](instructions/phases/phase6/phase6-tests-quality-check-yourself.md)

---

### Phase 7: CI

🟩🟩🟩🟩🟩🟩⬜⬜⬜⬜ 60%

**Instructions:** [Phase 7 — Task](instructions/phases/phase7/phase7-ci.md) · [Phase 7 — Check yourself](instructions/phases/phase7/phase7-ci-check-yourself.md)

---

### Phase 8 (Advanced): Incremental `fct_game_events` + `fct_sessions`

🟩🟩🟩🟩🟩🟩🟩⬜⬜⬜ 70%

**Instructions:** [Phase 8 — Task](instructions/phases/phase8/phase8-incremental-fct-game-events.md) · [Phase 8 — Check yourself](instructions/phases/phase8/phase8-incremental-fct-game-events-check-yourself.md)

---

## 🏁 Final Boss: Product Questions You Must Be Able to Answer

🟩🟩🟩🟩🟩🟩🟩🟩🟩🟩 100%

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

---

### 1. Retention & Player Return

**Models you’ll use:** `retention`, `dim_players` (and optionally staging for max date).

**Goal:** Understand whether players come back — and who does not.

Work through each question below: read the task, solve it using your warehouse, then use **Check your answer** to verify your approach and **Reference solution** only if you’re stuck.

---

#### Task 1: D1, D3, and D7 retention rates

Compute D1, D3, and D7 retention rates (one rate per day) from your retention mart.

**Your solution should:**

- Use **weighted retention**: `sum(active_players) / sum(cohort_size)` (not a plain average of per-cohort percentages).
- Include **only mature cohorts** for each day (e.g. for D7, only cohorts that are at least 7 days old).
- Include cohorts with **zero** retained players (e.g. `LEFT JOIN` + `COALESCE(active_players, 0)`), so retention isn't biased upward.

**The output should look like:** three rows with `retention_day` (1, 3, 7) and `retention_rate_pct`.

<details>
<summary>Check Solution</summary>

```sql
with cohort_base as (
    select
        cohort_date,
        country_code,
        difficulty_selected,
        max(cohort_size) as cohort_size
    from GAME_ANALYTICS.MARTS.RETENTION
    where days_since_cohort = 0
    group by 1, 2, 3
),

max_date as (
    select max(date(session_end_at)) as max_data_date
    from GAME_ANALYTICS.STAGING.STG_SESSIONS
),

target_days as (
    select days_since_cohort
    from (
        select 1 as days_since_cohort
        union all
        select 3 as days_since_cohort
        union all
        select 7 as days_since_cohort
    ) t
),

cohort_day_grid as (
    select
        c.cohort_date,
        c.country_code,
        c.difficulty_selected,
        d.days_since_cohort,
        c.cohort_size
    from cohort_base c
    cross join target_days d
    cross join max_date m
    where c.cohort_date <= dateadd(day, -d.days_since_cohort, m.max_data_date)
),

retention_filled as (
    select
        g.days_since_cohort,
        coalesce(r.active_players, 0) as active_players,
        g.cohort_size
    from cohort_day_grid g
    left join GAME_ANALYTICS.MARTS.RETENTION r
        on r.cohort_date = g.cohort_date
        and r.country_code = g.country_code
        and r.difficulty_selected = g.difficulty_selected
        and r.days_since_cohort = g.days_since_cohort
)

select
    days_since_cohort as retention_day,
    round(sum(active_players) * 100.0 / nullif(sum(cohort_size), 0), 2) as retention_rate_pct
from retention_filled
group by 1
order by 1;
```

This query avoids bias by: (1) including cohorts with zero retained players, (2) weighting by cohort size, (3) excluding immature cohorts per target day.
</details>

---

#### Task 2: Countries with the lowest retention

Find which **countries** have the **lowest retention**. Use one fixed day (e.g. D7).

**Your solution should:**

- Use **one fixed day** (commonly D7) and **only mature cohorts** for that day.
- Apply a **minimum sample size** (e.g. `HAVING sum(cohort_size) >= 50`) so tiny countries don't dominate the bottom list by noise.
- Use **weighted** retention by cohort size, not a simple average of per-country percentages.

**The output should look like:** a short list (e.g. top 10) of countries with `country_code`, D7 retention rate, and sample size (`total_players_in_scope`).

<details>
<summary>Check Solution</summary>

```sql
with cohort_base as (
    select
        cohort_date,
        country_code,
        difficulty_selected,
        max(cohort_size) as cohort_size
    from GAME_ANALYTICS.MARTS.RETENTION
    where days_since_cohort = 0
    group by 1, 2, 3
),

max_date as (
    select max(date(session_end_at)) as max_data_date
    from GAME_ANALYTICS.STAGING.STG_SESSIONS
),

cohort_day_grid as (
    select
        cohort_date,
        country_code,
        difficulty_selected,
        7 as days_since_cohort,
        cohort_size
    from cohort_base
    cross join max_date
    where cohort_date <= dateadd(day, -7, max_data_date)
),

retention_filled as (
    select
        g.country_code,
        coalesce(r.active_players, 0) as active_players,
        g.cohort_size
    from cohort_day_grid g
    left join GAME_ANALYTICS.MARTS.RETENTION r
        on r.cohort_date = g.cohort_date
        and r.country_code = g.country_code
        and r.difficulty_selected = g.difficulty_selected
        and r.days_since_cohort = g.days_since_cohort
)
select
    country_code,
    sum(cohort_size) as total_players_in_scope,
    round(sum(active_players) * 100.0 / nullif(sum(cohort_size), 0), 2) as d7_retention_rate_pct
from retention_filled
group by 1
having sum(cohort_size) >= 50
order by d7_retention_rate_pct asc, total_players_in_scope desc
limit 10;
```

Produces a stable ranking by country using weighted D7 retention and a minimum volume threshold.
</details>

---

#### Task 3: Retention by difficulty

Determine whether retention differs by **difficulty_selected** (e.g. easy vs normal vs hard).

**Your solution should:**

- Compare retention at the **same day(s)** (e.g. D1 and/or D7) across difficulties.
- Use **weighted rates** and **mature cohorts only**.
- Optionally apply a minimum sample size per difficulty so small groups don't skew the comparison.

**The output should look like:** rows with `difficulty_selected`, sample size (`total_players_in_scope`), and D7 retention rate.

<details>
<summary>Check Solution</summary>

```sql
with cohort_base as (
    select
        cohort_date,
        country_code,
        difficulty_selected,
        max(cohort_size) as cohort_size
    from GAME_ANALYTICS.MARTS.RETENTION
    where days_since_cohort = 0
    group by 1, 2, 3
),

max_date as (
    select max(date(session_end_at)) as max_data_date
    from GAME_ANALYTICS.STAGING.STG_SESSIONS
),

cohort_day_grid as (
    select
        cohort_date,
        country_code,
        difficulty_selected,
        7 as days_since_cohort,
        cohort_size
    from cohort_base
    cross join max_date
    where cohort_date <= dateadd(day, -7, max_data_date)
),

retention_filled as (
    select
        g.difficulty_selected,
        coalesce(r.active_players, 0) as active_players,
        g.cohort_size
    from cohort_day_grid g
    left join GAME_ANALYTICS.MARTS.RETENTION r
        on r.cohort_date = g.cohort_date
        and r.country_code = g.country_code
        and r.difficulty_selected = g.difficulty_selected
        and r.days_since_cohort = g.days_since_cohort
)
select
    difficulty_selected,
    sum(cohort_size) as total_players_in_scope,
    round(sum(active_players) * 100.0 / nullif(sum(cohort_size), 0), 2) as d7_retention_rate_pct
from retention_filled
group by 1
having sum(cohort_size) >= 50
order by d7_retention_rate_pct asc, total_players_in_scope desc
limit 10;
```

Lower retention for higher difficulty at D1/D7 indicates faster churn for harder modes.
</details>

---

#### Task 4: Share of players with only one session

What **% of players** have **only one session**?

**Your solution should:**

- Use **`dim_players`**, which already has `total_sessions` per player (avoids re-aggregating from session facts).
- Compute: `count(players where total_sessions = 1) * 100 / count(*)`.

**The output should look like:** a single percentage (e.g. `pct_players_only_one_session`).

<details>
<summary>Check Solution</summary>

```sql
select
    round(count_if(total_sessions = 1) * 100.0 / nullif(count(*), 0), 2) as pct_players_only_one_session
from GAME_ANALYTICS.MARTS.DIM_PLAYERS;
```

This is the direct churn-risk share for players who never progressed beyond their first session.
</details>

---

#### Task 5: Share of players who return for a second session

What **% of players** return for a **second session** (i.e. have at least 2 sessions)? This is the complement of Task 4.

**Your solution should:**

- Use the **same denominator** as Task 4 (all players in `dim_players`) for consistency.
- Compute: `count(players where total_sessions >= 2) * 100 / count(*)`.

**The output should look like:** a single percentage (e.g. `pct_players_with_second_session`).

<details>
<summary>Check Solution</summary>

```sql
select
    round(count_if(total_sessions >= 2) * 100.0 / nullif(count(*), 0), 2) as pct_players_with_second_session
from GAME_ANALYTICS.MARTS.DIM_PLAYERS;
```

Together with Q4, this gives a clear first-return health check.
</details>

### 2. Drop-Off & Friction

6. At which step of the session funnel do players drop the most?

<details>
<summary>Check Answer in details</summary>

Aggregate funnel counts first, then compute drop from the previous step.  
Do not compare raw percentages across different denominators.

</details>

<details>
<summary>SQL code with explanation</summary>

```sql
-- Q6: Biggest drop between consecutive funnel steps.
with totals as (
    select
        sum(total_sessions) as total_sessions,
        sum(sessions_with_game_started) as game_started_sessions,
        sum(sessions_with_chapter_started) as chapter_started_sessions,
        sum(sessions_with_checkpoint_reached) as checkpoint_sessions,
        sum(sessions_with_chapter_completed) as chapter_completed_sessions,
        sum(sessions_with_game_closed) as game_closed_sessions
    from funnel_sessions
),
steps as (
    select 1 as step_order, 'total_sessions' as step_name, total_sessions as sessions_reached from totals
    union all
    select 2, 'game_started', game_started_sessions from totals
    union all
    select 3, 'chapter_started', chapter_started_sessions from totals
    union all
    select 4, 'checkpoint_reached', checkpoint_sessions from totals
    union all
    select 5, 'chapter_completed', chapter_completed_sessions from totals
    union all
    select 6, 'game_closed', game_closed_sessions from totals
),
step_metrics as (
    select
        step_order,
        step_name,
        sessions_reached,
        lag(sessions_reached) over (order by step_order) as prev_sessions
    from steps
)
select
    step_name,
    sessions_reached,
    round((1 - sessions_reached::float / nullif(prev_sessions, 0)) * 100, 2) as drop_from_previous_step_pct
from step_metrics
where prev_sessions is not null
qualify row_number() over (order by drop_from_previous_step_pct desc) = 1;
```

This returns the exact funnel stage with the worst step-to-step conversion loss.

</details>

7. What % of sessions never reach a **checkpoint**?

<details>
<summary>Check Answer in details</summary>

Use total sessions as denominator and checkpoint-reached sessions as numerator.  
This is `1 - checkpoint reach rate`.

</details>

<details>
<summary>SQL code with explanation</summary>

```sql
-- Q7: Sessions that never reach a checkpoint.
select
    round(
        (1 - sum(sessions_with_checkpoint_reached)::float / nullif(sum(total_sessions), 0)) * 100,
        2
    ) as pct_sessions_without_checkpoint
from funnel_sessions;
```

This is the cleanest top-level checkpoint friction KPI.

</details>

8. What % of sessions start a chapter but never complete one?

<details>
<summary>Check Answer in details</summary>

Use sessions with chapter start as denominator; measure how many of those fail to complete any chapter.

</details>

<details>
<summary>SQL code with explanation</summary>

```sql
-- Q8: Among sessions that started a chapter, percent that completed none.
with totals as (
    select
        sum(sessions_with_chapter_started) as started_sessions,
        sum(sessions_with_chapter_completed) as completed_sessions
    from funnel_sessions
)
select
    round(
        greatest(started_sessions - completed_sessions, 0) * 100.0 / nullif(started_sessions, 0),
        2
    ) as pct_started_sessions_without_completion
from totals;
```

This isolates failure after entry into chapter gameplay, which is where level design friction often appears.

</details>

9. Are there sessions with **high deaths but low progression**?

<details>
<summary>Check Answer in details</summary>

Define "high deaths" relative to each difficulty (p90 threshold) and "low progression" as zero chapter completions.  
Difficulty-aware thresholds avoid false positives on naturally harder modes.

</details>

<details>
<summary>SQL code with explanation</summary>

```sql
-- Q9: Sessions with high deaths (difficulty-specific p90) and zero chapter completion.
with death_thresholds as (
    select
        difficulty_selected,
        percentile_cont(0.9) within group (order by deaths_count) as p90_deaths
    from fct_sessions
    group by 1
)
select
    s.session_id,
    s.player_id,
    s.difficulty_selected,
    s.deaths_count,
    s.chapters_completed,
    s.session_duration_minutes,
    s.events_per_minute
from fct_sessions s
inner join death_thresholds t
    on s.difficulty_selected = t.difficulty_selected
where s.deaths_count >= t.p90_deaths
  and s.chapters_completed = 0
order by s.deaths_count desc, s.session_duration_minutes desc;
```

The result is an actionable session list for difficulty tuning and encounter pacing review.

</details>

Goal: identify friction points in the core loop.

### 3. Difficulty & Balance

10. Which difficulty has the highest **death rate per session**?

<details>
<summary>Check Answer in details</summary>

Compute deaths per session as `sum(deaths_count) / count(*)` by difficulty.  
This keeps the metric aligned with session-level gameplay experience.

</details>

<details>
<summary>SQL code with explanation</summary>

```sql
-- Q10: Difficulty with the highest deaths per session.
select
    difficulty_selected,
    count(*) as sessions,
    sum(deaths_count) as total_deaths,
    round(sum(deaths_count)::float / nullif(count(*), 0), 2) as deaths_per_session
from fct_sessions
group by 1
order by deaths_per_session desc;
```

Top row is the difficulty with the highest death burden per play session.

</details>

11. Do players on higher difficulty churn faster?

<details>
<summary>Check Answer in details</summary>

Compare weighted D1 and D7 retention by difficulty and derive churn as `100 - retention`.  
Use mature cohorts and zero-fill to avoid survivorship bias.

</details>

<details>
<summary>SQL code with explanation</summary>

```sql
-- Q11: Churn pace by difficulty from D1 and D7 retention.
with cohort_base as (
    select
        cohort_date,
        country_code,
        difficulty_selected,
        max(cohort_size) as cohort_size
    from retention
    where days_since_cohort = 0
    group by 1, 2, 3
),
target_days as (
    select column1::int as days_since_cohort
    from values (1), (7)
),
cohort_day_grid as (
    select
        c.cohort_date,
        c.country_code,
        c.difficulty_selected,
        d.days_since_cohort,
        c.cohort_size
    from cohort_base c
    cross join target_days d
    where c.cohort_date <= dateadd(day, -d.days_since_cohort, current_date)
),
retention_filled as (
    select
        g.difficulty_selected,
        g.days_since_cohort,
        coalesce(r.active_players, 0) as active_players,
        g.cohort_size
    from cohort_day_grid g
    left join retention r
        on r.cohort_date = g.cohort_date
        and r.country_code = g.country_code
        and r.difficulty_selected = g.difficulty_selected
        and r.days_since_cohort = g.days_since_cohort
)
select
    difficulty_selected,
    days_since_cohort as retention_day,
    round(sum(active_players) * 100.0 / nullif(sum(cohort_size), 0), 2) as retention_rate_pct,
    round(100 - (sum(active_players) * 100.0 / nullif(sum(cohort_size), 0)), 2) as churn_rate_pct
from retention_filled
group by 1, 2
order by retention_day, difficulty_selected;
```

If harder difficulties show systematically lower D1/D7 retention and higher churn, they churn faster.

</details>

12. Which chapters have the lowest completion rates?

<details>
<summary>Check Answer in details</summary>

Use chapter-level event properties from `fct_game_events.properties` to compare `chapter_started` vs `chapter_completed`.  
Apply a minimum starts threshold to avoid noisy chapters.

</details>

<details>
<summary>SQL code with explanation</summary>

```sql
-- Q12: Chapter completion rate from chapter_started vs chapter_completed.
with chapter_events as (
    select
        properties:chapter_id::int as chapter_id,
        properties:chapter_name::string as chapter_name,
        event_name
    from fct_game_events
    where event_name in ('chapter_started', 'chapter_completed')
),
chapter_totals as (
    select
        chapter_id,
        chapter_name,
        count_if(event_name = 'chapter_started') as starts,
        count_if(event_name = 'chapter_completed') as completions
    from chapter_events
    where chapter_id is not null
    group by 1, 2
)
select
    chapter_id,
    chapter_name,
    starts,
    completions,
    round(completions * 100.0 / nullif(starts, 0), 2) as completion_rate_pct
from chapter_totals
where starts >= 30
order by completion_rate_pct asc, starts desc;
```

Lowest completion-rate chapters are likely balance or pacing problem areas.

</details>

13. Is there a relationship between **session duration** and **chapter completion**?

<details>
<summary>Check Answer in details</summary>

Bucket sessions by duration and compare chapter completion metrics per bucket.  
Both "% with any completion" and average completions per session are useful.

</details>

<details>
<summary>SQL code with explanation</summary>

```sql
-- Q13: Session duration bucket vs chapter completion outcomes.
with session_buckets as (
    select
        case
            when session_duration_minutes < 20 then 'short (<20m)'
            when session_duration_minutes < 45 then 'medium (20-44m)'
            else 'long (45m+)'
        end as duration_bucket,
        case when chapters_completed > 0 then 1 else 0 end as has_any_chapter_completion,
        chapters_completed
    from fct_sessions
)
select
    duration_bucket,
    count(*) as sessions,
    round(avg(has_any_chapter_completion) * 100, 2) as pct_sessions_with_any_completion,
    round(avg(chapters_completed), 2) as avg_chapters_completed_per_session
from session_buckets
group by 1
order by
    case duration_bucket
        when 'short (<20m)' then 1
        when 'medium (20-44m)' then 2
        else 3
    end;
```

If longer buckets show higher completion metrics, longer sessions correlate with progression.

</details>

Goal: detect balance issues before ship.

### 4. Session Behavior

14. What is the **median session duration**?

<details>
<summary>Check Answer in details</summary>

Use percentile, not average. Median is robust to long-session outliers.

</details>

<details>
<summary>SQL code with explanation</summary>

```sql
-- Q14: Median session duration.
select
    round(percentile_cont(0.5) within group (order by session_duration_minutes), 2)
        as median_session_duration_minutes
from fct_sessions;
```

This is the central tendency of playtime per session.

</details>

15. Are longer sessions correlated with higher retention?

<details>
<summary>Check Answer in details</summary>

Build a per-player D7 return flag and compare D7 retention across player-duration quartiles.  
Quartiles avoid arbitrary bucket cutoffs and reveal monotonic trends.

</details>

<details>
<summary>SQL code with explanation</summary>

```sql
-- Q15: Correlation check between average player session duration and D7 retention.
with session_dates as (
    select
        player_id,
        date(session_start_at) as session_date,
        session_duration_minutes
    from fct_sessions
),
player_base as (
    select
        player_id,
        min(session_date) as cohort_date,
        avg(session_duration_minutes) as avg_session_duration_minutes
    from session_dates
    group by 1
),
d7_flags as (
    select
        p.player_id,
        p.avg_session_duration_minutes,
        case when d7.player_id is not null then 1 else 0 end as returned_d7
    from player_base p
    left join (
        select distinct player_id, session_date
        from session_dates
    ) d7
        on d7.player_id = p.player_id
        and d7.session_date = dateadd(day, 7, p.cohort_date)
    where p.cohort_date <= dateadd(day, -7, current_date)
),
ranked as (
    select
        player_id,
        avg_session_duration_minutes,
        returned_d7,
        ntile(4) over (order by avg_session_duration_minutes) as duration_quartile
    from d7_flags
)
select
    duration_quartile,
    count(*) as players,
    round(avg(avg_session_duration_minutes), 2) as avg_duration_minutes,
    round(avg(returned_d7) * 100, 2) as d7_retention_rate_pct
from ranked
group by 1
order by 1;
```

If D7 retention rises with quartile number, longer sessions are positively correlated with retention.

</details>

16. What is the average **events_per_minute**?

<details>
<summary>Check Answer in details</summary>

Use `fct_sessions.events_per_minute` directly and report overall average (optionally segmented).

</details>

<details>
<summary>SQL code with explanation</summary>

```sql
-- Q16: Overall average event intensity.
select
    round(avg(events_per_minute), 2) as avg_events_per_minute
from fct_sessions;
```

This is the baseline interaction intensity for the session population.

</details>

17. Are there sessions with zero or unusually low event activity?

<details>
<summary>Check Answer in details</summary>

Use two flags:
1. hard zero (`total_events = 0`)
2. very low activity (bottom 5% `events_per_minute`).

</details>

<details>
<summary>SQL code with explanation</summary>

```sql
-- Q17: Zero-event and very-low-activity session shares.
with thresholds as (
    select percentile_cont(0.05) within group (order by events_per_minute) as p05_events_per_minute
    from fct_sessions
),
flagged as (
    select
        case
            when total_events = 0 then 'zero_events'
            when events_per_minute <= (select p05_events_per_minute from thresholds) then 'very_low_events_per_minute'
            else null
        end as issue_type
    from fct_sessions
)
select
    issue_type,
    count(*) as sessions,
    round(count(*) * 100.0 / nullif((select count(*) from fct_sessions), 0), 2) as pct_of_sessions
from flagged
where issue_type is not null
group by 1
order by sessions desc;
```

This separates true no-activity sessions from low-intensity but nonzero sessions.

</details>

Goal: understand how players actually play.

### 5. Segmentation

18. Define a "core player" (e.g. ≥5 sessions, ≥120 minutes playtime, ≥1 chapter completed).

- How many core players exist?
- From which countries?
- On which difficulty?

<details>
<summary>Check Answer in details</summary>

`dim_players` gives session/playtime thresholds.  
Use `fct_sessions` to compute chapter completion count per player, then segment core users by country and difficulty.

</details>

<details>
<summary>SQL code with explanation</summary>

```sql
-- Q18: Core players and their country/difficulty distribution.
with player_progress as (
    select
        player_id,
        sum(chapters_completed) as chapters_completed_total
    from fct_sessions
    group by 1
),
core_players as (
    select
        p.player_id,
        p.country_code,
        p.difficulty_selected
    from dim_players p
    left join player_progress pr
        on p.player_id = pr.player_id
    where p.total_sessions >= 5
      and p.total_playtime_minutes >= 120
      and coalesce(pr.chapters_completed_total, 0) >= 1
)
select
    country_code,
    difficulty_selected,
    count(*) as core_players,
    sum(count(*)) over () as total_core_players
from core_players
group by 1, 2
order by core_players desc;
```

`total_core_players` answers "how many"; grouped rows answer where they come from and what they play.

</details>

19. Define "one-and-done" players (1 session, no chapter completed).

- What % of total players are they?
- Do they cluster by country or difficulty?

<details>
<summary>Check Answer in details</summary>

Join `dim_players` with per-player chapter completion totals from `fct_sessions`, then compare one-and-done volume against all players.

</details>

<details>
<summary>SQL code with explanation</summary>

```sql
-- Q19: One-and-done share and clustering by country/difficulty.
with player_progress as (
    select
        player_id,
        sum(chapters_completed) as chapters_completed_total
    from fct_sessions
    group by 1
),
one_and_done as (
    select
        p.player_id,
        p.country_code,
        p.difficulty_selected
    from dim_players p
    left join player_progress pr
        on p.player_id = pr.player_id
    where p.total_sessions = 1
      and coalesce(pr.chapters_completed_total, 0) = 0
),
totals as (
    select count(*) as total_players from dim_players
)
select
    country_code,
    difficulty_selected,
    count(*) as one_and_done_players,
    round(count(*) * 100.0 / nullif((select total_players from totals), 0), 2) as pct_of_all_players
from one_and_done
group by 1, 2
order by one_and_done_players desc;
```

Summing `one_and_done_players` gives the absolute count; `pct_of_all_players` gives population share.

</details>

Goal: separate your engaged audience from early churn.

### Completion Criteria

You have successfully completed the project if:

- You can answer these questions using only warehouse models.
- You can explain *how* each answer is derived.
- Your models pass all tests.
- Your CI pipeline fails when contracts break.
- You can identify at least one actionable product insight.

At that point, you are no longer "building tables." You are running a game analytics platform.
