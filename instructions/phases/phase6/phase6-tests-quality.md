# Phase 6: Tests and quality — Task

This phase hardens your warehouse with **schema tests** (unique, not_null, relationships) and a **singular test** that encodes a business rule: sessions for the same player must not overlap in time. You will verify schema YAMLs have the right tests, add `tests/sessions_no_overlap.sql`, register it in `tests/schema.yml`, and run `dbt build` until everything passes.

**Prerequisites:** Phase 1–5 complete (sources, staging, core marts, analytics marts). Raw tables loaded in Snowflake.

---

## Why this phase?

- **Schema tests** (unique, not_null, relationships) catch data quality issues: duplicate keys, nulls where they shouldn't be, broken foreign keys. Phases 3–5 already added many of these; this phase ensures they're complete and consistent.
- **Singular tests** encode custom business rules. The rule "a player cannot have two sessions with overlapping [session_start_at, session_end_at]" is critical: if sessions overlap, funnel and retention logic breaks. A singular test that returns rows **fails** — so we write SQL that returns overlapping pairs.
- **dbt build** runs models and tests together; fixing failures before merge prevents bad data from reaching dashboards.

---

## Goals

- **6.1** In schema YAMLs, ensure primary key columns have `unique` and `not_null`; foreign keys have `relationships` to the referenced model. Add `not_null` (or accepted_values) on other critical columns where missing.
- **6.2** Add a **singular test** (e.g. `tests/sessions_no_overlap.sql`) that returns rows where the same player has two sessions with overlapping [session_start_at, session_end_at]. The test fails if any such rows exist. Register it in `tests/schema.yml` and run `dbt test`.
- **6.3** Run `dbt build` locally and fix any failing models or tests until everything passes.

---

## 1. Schema tests (review)

Review your schema YAMLs in:

- `sources/` — raw_players, raw_sessions, raw_game_events
- `models/staging/schema.yml`
- `models/marts/core/schema.yml`
- `models/marts/analytics/schema.yml`

Ensure:

- **Primary keys:** `unique`, `not_null`
- **Foreign keys:** `not_null`, `relationships` to the parent model
- **Critical dimensions:** `not_null` on session_date, cohort_date, etc. where appropriate

Phases 3–5 should have most of these; add any missing tests.

---

## 2. Singular test: sessions_no_overlap

Create **`tests/sessions_no_overlap.sql`**.

**Logic:** Two sessions overlap if `s1.session_start_at < s2.session_end_at` AND `s1.session_end_at > s2.session_start_at`. We want to find pairs where the same player has two *different* sessions that overlap. The SQL should return those rows; dbt fails the test when the query returns any rows.

**Structure:**

- Self-join `ref('stg_sessions')` on `player_id`
- Exclude self-joins: `s1.session_id < s2.session_id` (or `!=`) so we don't match a session to itself
- Add overlap condition: `s1.session_start_at < s2.session_end_at AND s1.session_end_at > s2.session_start_at`
- Select enough columns to debug failures (e.g. both session_ids, player_id, timestamps)

---

## 3. Register the test

Create or update **`tests/schema.yml`** to configure the singular test:

```yaml
version: 2

tests:
  - name: sessions_no_overlap
    config:
      severity: error
      store_failures: true
```

- **severity: error** — Fails the run (default for tests).
- **store_failures: true** — dbt materializes failing rows so you can query them in Snowflake to debug.

---

## 4. Verify

From your dbt project root (with venv activated):

```bash
dbt test
dbt build
```

- **`dbt test`** — Runs all schema tests and singular tests.
- **`dbt build`** — Runs models (in dependency order) then tests; fails if any test fails.

Fix any failing models or tests until `dbt build` completes successfully.

---

## Why this phase matters

- Tests turn your warehouse into something you can trust: they catch broken assumptions as soon as data or code changes.
- Encoding business rules (like no overlapping sessions) in tests prevents silent data drift that would invalidate product decisions.

---

**When you're done,** see [Phase 6 — Check yourself](phase6-tests-quality-check-yourself.md) for a full solution. Then proceed to [Phase 7: CI](../../../README.md#phase-7-ci).
