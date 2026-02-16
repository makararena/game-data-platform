# Phase 6: Tests and quality — Check yourself

This file gives the full solution for Phase 6. Try the [task](phase6-tests-quality.md) first, then use this to compare.

---

## File 1: `tests/sessions_no_overlap.sql`

```sql
-- Singular test: returns rows where the same player has two sessions with overlapping time.
-- The test fails if any rows are returned.
-- Overlap: [s1_start, s1_end] and [s2_start, s2_end] overlap when s1_start < s2_end AND s1_end > s2_start.

select
    s1.session_id as session_id_1,
    s2.session_id as session_id_2,
    s1.player_id,
    s1.session_start_at as session_1_start_at,
    s1.session_end_at as session_1_end_at,
    s2.session_start_at as session_2_start_at,
    s2.session_end_at as session_2_end_at
from {{ ref('stg_sessions') }} s1
inner join {{ ref('stg_sessions') }} s2
    on s1.player_id = s2.player_id
    and s1.session_id < s2.session_id
    and s1.session_start_at < s2.session_end_at
    and s1.session_end_at > s2.session_start_at
```

---

## File 2: `tests/schema.yml`

```yaml
version: 2

tests:
  - name: sessions_no_overlap
    config:
      severity: error
      store_failures: true
```

---

## Schema tests (review)

Your schema YAMLs from Phases 3–5 should already include:

- **Staging:** `unique`, `not_null` on player_id, session_id, event_id; `relationships` from stg_sessions.player_id to stg_players
- **Core marts:** `unique`, `not_null` on primary keys; `relationships` from fct_sessions.player_id to dim_players, fct_game_events.session_id to fct_sessions
- **Analytics marts:** `not_null` on session_date, cohort_date where applicable

If any are missing, add them. The exact structure depends on your schema files; refer to phase3, phase4, and phase5 check-yourself files for the full YAML.

---

## Verify

From your dbt project root (with venv activated):

```bash
dbt test
dbt build
```

- **`dbt test`** — Runs all schema tests and singular tests.
- **`dbt build`** — Runs models then tests; fails if any test fails.

---

Then proceed to [Phase 7: CI](../../../README.md#phase-7-ci).
