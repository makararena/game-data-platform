# dbt setup

You will need to **create your own dbt project** — there is no `game-dbt-project` folder in this repo. This document explains how to get started with dbt and connect it to the raw data loaded by the game data platform.

---

## How to start with dbt

*(Add your content here: quick start link and steps.)*

- **dbt quick start:** *(paste your link here)*

After you have run `run_platform.sh` and have `RAW_PLAYERS`, `RAW_SESSIONS`, `RAW_GAME_EVENTS` in Snowflake, create a dbt project, point it at your Snowflake database and `RAW` schema, then follow the tasks in the main [README](../README.md) (sources → staging → marts).
