# Phase 7: CI — Task

This phase adds Continuous Integration (CI) for your dbt project using GitHub Actions. CI ensures model compilation, builds, and tests run on every PR or push to `main`, preventing broken contracts or failing tests from reaching production.

**Prerequisites:** Phase 0–6 complete; dbt project exists and `dbt build` runs locally; Snowflake account/credentials available for CI.

---

## Goals

- **7.1** Create a GitHub Actions workflow `.github/workflows/dbt.yml` to run `dbt deps`, `dbt compile --target ci`, and `dbt build --target ci` on push/PR to `main`.
- Use a `ci` target in `profiles.yml` that points at dedicated CI schemas to isolate test runs.
- Store the CI `profiles.yml` (or its credentials) as a GitHub secret (recommended: `SNOWFLAKE_CI_PROFILE`).

---

## Steps

1. Add the workflow file `.github/workflows/dbt.yml` (example below).
2. Add a GitHub secret `SNOWFLAKE_CI_PROFILE` containing a `profiles.yml` snippet (or provide individual secrets and write `~/.dbt/profiles.yml` in the workflow).
3. Ensure the `ci` target's `schema` values point to isolated schemas (e.g. `GAME_ANALYTICS_CI_STAGING`, `GAME_ANALYTICS_CI_MARTS`) so CI runs do not impact prod data.
4. Push a test branch and open a PR to confirm the Actions workflow executes and fails when tests/models break.

---