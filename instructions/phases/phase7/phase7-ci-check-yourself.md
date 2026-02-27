# Phase 7: CI — Check yourself

Verify the CI workflow and profile are configured correctly. The checklist below helps you confirm the CI pipeline runs and fails on broken models/tests.

1. GitHub Actions workflow present
   - File: `.github/workflows/dbt.yml`
   - Trigger: `push` and `pull_request` on branch `main` (or your repo's main branch)

2. Workflow steps
   - `actions/checkout@v4`
   - `actions/setup-python@v5` (или аналог)
   - `pip install dbt-snowflake`
   - `dbt deps`
   - `dbt compile --target ci`
   - `dbt build --target ci`

### Explanations of the actions used

- **`actions/checkout@v4`**
  : this is the official GitHub action that clones the repository onto the runner. without it the subsequent steps have no project code. version `v4` includes the latest security updates and fixes.

- **`actions/setup-python@v5`**
  : configures the specified Python version (in the example `3.11`) on the GitHub Actions virtual machine. after this step, the `python` and `pip` commands will be available at the desired version. version `v5` brings improvements such as package caching and more flexible version specification.

The remaining commands install the dbt package, fetch dependencies, and perform project compilation/build.

- **`pip install dbt-snowflake`**
  : installs the dbt CLI plus the Snowflake adapter. this allows subsequent `dbt` commands to run and talk to Snowflake.

- **`dbt deps`**
  : downloads any packages declared in `packages.yml` (e.g. dbt-utils) into the `dbt_modules` directory. ensures macros and shared logic are available.

- **`dbt compile --target ci`**
  : parses and renders all models, macros and SQL into compiled SQL files in `target/` using the `ci` profile. it checks for syntax errors and unresolved references without touching the warehouse.

- **`dbt build --target ci`**
  : runs the full build (models, seeds, tests) against Snowflake using the `ci` target. this step exercises tests and fails the job on broken code or failing assertions.

- **Profile generation step**
  : the `mkdir`/`printf` block writes a `profiles.yml` file from the secret `SNOWFLAKE_CI_PROFILE`. it keeps credentials out of source control and adapts to GitHub Actions' environment.

3. CI profile loaded from secret
   - GitHub secret `SNOWFLAKE_CI_PROFILE` exists and contains a valid `profiles.yml` snippet for a `ci` target (or use individual secrets and write `~/.dbt/profiles.yml` in the workflow).

4. Isolation
   - The `ci` target uses dedicated schemas (or a dedicated database) so CI runs won't collide with production data.

5. Secrets/security
   - Secrets are added via the repository settings (not checked into source).

6. Failure behaviour
   - Intentionally break a model or a test (locally or via a throwaway PR) and verify the workflow fails.
   - Check the Actions log — dbt errors should be visible and non-zero exit codes returned.

7. Stored failures (optional)
   - If you use `store_failures: true` for singular tests, failing rows will be persisted in Snowflake for debugging.

---

Example minimal `profiles.yml` you can store in `SNOWFLAKE_CI_PROFILE` (replace placeholders):

```yaml
game_analytics:
  target: ci
  outputs:
    ci:
      type: snowflake
      account: <ACCOUNT>
      user: <USER>
      password: <PASSWORD>
      role: <ROLE>
      database: <DATABASE>
      warehouse: <WAREHOUSE>
      schema: GAME_ANALYTICS_CI
      threads: 1
```

## Example workflow file

Add the following as `.github/workflows/dbt.yml` in your repo (adapt as needed):

```yaml
name: dbt

on:
   push:
      branches: [main]
   pull_request:
      branches: [main]

jobs:
   dbt-compile:
      runs-on: ubuntu-latest
      steps:
         - name: Checkout repository
            uses: actions/checkout@v4

         - name: Set up Python
            uses: actions/setup-python@v5
            with:
               python-version: "3.11"

         - name: Install dbt-snowflake
            run: pip install dbt-snowflake

         - name: dbt deps
            run: dbt deps

         - name: Generate profiles.yml from Secrets
            run: |
               mkdir -p ~/.dbt
               printf '%s' "$PROFILE_CONTENT" > ~/.dbt/profiles.yml
            env:
               PROFILE_CONTENT: ${{ secrets.SNOWFLAKE_CI_PROFILE }}

         - name: dbt compile (CI)
            run: dbt compile --target ci

         - name: dbt build (run + test — catches broken contracts)
            run: dbt build --target ci

```

To validate: open a PR and observe the Actions run; permission issues or wrong credentials will appear in the logs when dbt attempts to connect.

When everything passes, your CI is set up and will protect `main` from broken changes.

