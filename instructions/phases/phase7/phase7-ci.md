# Phase 7: CI — Task

This phase adds Continuous Integration (CI) for your dbt project using GitHub Actions and formalizes a safe git workflow for merging changes into `main`.

**Prerequisites:** Phase 0–6 complete; dbt project exists and `dbt build` runs locally; Snowflake credentials are available for CI.

---

## Goals

- **7.1** Add a GitHub Actions workflow `.github/workflows/dbt.yml` to run `dbt deps`, `dbt compile --target ci`, and `dbt build --target ci` on pushes/PRs to `main`.
- **7.2** Configure a `ci` target profile and isolated CI schema (`ci_schema`) so CI builds do not affect dev/prod schemas.
- **7.3** Use a branch-based workflow for changes: feature branch -> commit -> push -> PR -> merge to `main`.
- **7.4** Protect `main` with required PRs and required dbt CI checks.

---

## 1. Add CI workflow

Create `.github/workflows/dbt.yml`:

```yaml
name: dbt

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  dbt:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dbt-snowflake
        run: pip install dbt-snowflake

      - name: dbt deps
        run: dbt deps

      - name: Generate profiles.yml from secret
        run: |
          mkdir -p ~/.dbt
          printf '%s' "$PROFILE_CONTENT" > ~/.dbt/profiles.yml
        env:
          PROFILE_CONTENT: ${{ secrets.SNOWFLAKE_CI_PROFILE }}

      - name: dbt compile (CI)
        run: dbt compile --target ci

      - name: dbt build (CI)
        run: dbt build --target ci
```

Add repository secret `SNOWFLAKE_CI_PROFILE` with a valid `profiles.yml` that contains a `ci` target.

---

## 2. Branch workflow and commits

Do not commit directly to `main`. Use this flow for every change:

```bash
git checkout -b feature/phase7-ci
# edit files
git add .
git commit -m "phase7: add dbt CI workflow and docs"
git push -u origin feature/phase7-ci
```

Then in GitHub:

1. Open a Pull Request from `feature/phase7-ci` into `main`.
2. Wait for CI job `dbt` to pass.
3. Merge PR into `main`.
4. Optionally delete the feature branch.

---

## 2 Настраиваем защиту main

В GitHub:

Repo -> Settings -> Branches -> Add branch protection rule

Выбираешь branch: `main`

И включаешь:

✔ Require a pull request before merging  
✔ Require status checks to pass before merging  
✔ Выбери job `dbt`

Сохраняешь.

---

## 4. Verify

From your dbt project root (with CI profile ready):

```bash
dbt deps
dbt compile --target ci
dbt build --target ci
```

Then create a test PR and confirm the GitHub Action runs and reports status on the PR.

---

When done, continue to [Phase 8](../phase8/phase8-incremental-fct-game-events.md).
