# dbt setup

Create your own dbt project (none in this repo). You need Python 3.10+, pip, and Snowflake raw tables + `app/.env` credentials.

**Docs:** [Get started](https://docs.getdbt.com/docs/get-started-dbt) · [Install with pip](https://docs.getdbt.com/docs/core/pip-install)

---

## 1. Project folder and venv

Create a folder for your dbt project (e.g. for GitHub) and a virtual environment inside it; install dbt into the venv so dependencies are managed in one place.

```bash
mkdir dbt-project
cd dbt-project
python -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip
python -m pip install dbt-core dbt-snowflake
dbt --version              # check dbt-core + snowflake
```

---

## 2. Initialize project

From `dbt-project/` (venv activated), run:

```bash
dbt init game_dbt_project
cd game_dbt_project
```

dbt creates the project and may prompt to set up your profile:

- **Overwrite profile?** If it says "The profile game_dbt_project already exists... Continue and overwrite?" choose `y` to use the wizard or `n` to keep existing `~/.dbt/profiles.yml` and configure it manually (step 3).
- **If you choose `y`:** pick database `1` (snowflake), then enter account (e.g. `xxxxx-xxxxxxx`), user, authentication `1` (password), password, role (e.g. `ACCOUNTADMIN`), warehouse (e.g. `COMPUTE_WH`), database (e.g. `GAME_ANALYTICS`), schema (e.g. `DEV`), and threads. The profile is written to `~/.dbt/profiles.yml` with the same name as the project (`game_dbt_project`).

**Important:** Run all dbt commands from this project folder (the one that contains `dbt_project.yml`), with the venv activated.

---

## 3. Snowflake profile

dbt uses `~/.dbt/profiles.yml`. Use the same values as in `game-data-platform/app/.env`.

```yaml
game_analytics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "YOUR_ACCOUNT"      # SNOWFLAKE_ACCOUNT
      user: "YOUR_USER"
      password: "YOUR_PASSWORD"
      role: "ACCOUNTADMIN"
      warehouse: "COMPUTE_WH"
      database: "GAME_ANALYTICS"
      schema: "DEV"                 # where dbt builds models (raw stays in RAW)
      threads: 4
```

In your project’s `dbt_project.yml`, set `profile: 'game_analytics'` (if you configured profiles manually) or `profile: 'game_dbt_project'` (if you used the init wizard). The value must match a profile name in `~/.dbt/profiles.yml`.

---

## 4. Verify

From the dbt project folder that contains `dbt_project.yml` (e.g. `dbt-project/game_dbt_project/`), with venv activated:

```bash
dbt debug
```

Then follow the main [README](../README.md): sources → staging → marts.
