# Pre-launch setup: database and schema in Snowflake

Before you run `run_platform.sh`, you need a **database** and a **schema** in Snowflake where the raw tables will live. Use the **Compute** page (or any warehouse you already have) — you don't need to create or choose a warehouse; just use the one Snowflake gives you and write its name in your `.env` later.

<details>
<summary><strong>What is a warehouse?</strong></summary>

A **warehouse** in Snowflake is the compute resource that runs your queries and loads. It's like a "server" that Snowflake spins up when you run SQL or load data. You don't create tables inside it — you just pick one (e.g. `COMPUTE_WH`) and Snowflake uses it to do the work. Trial accounts usually have one already; you only need its name for `.env`.
</details>

<details>
<summary><strong>What is a database?</strong></summary>

A **database** is a top-level container for your data. Think of it as a folder that holds schemas and tables. For this course we use one database (e.g. `GAME_ANALYTICS`) to keep all game analytics objects in one place.
</details>

<details>
<summary><strong>What is a schema?</strong></summary>

A **schema** lives inside a database and groups related tables. It's like a subfolder. We use several schemas: `RAW` (where the platform loads CSV data), `STAGING` and `MARTS` (where dbt builds staging views and mart tables), and `CI` (where dbt runs in the CI pipeline). Each keeps a layer of the warehouse separate so you can manage permissions and environments cleanly.
</details>

<details>
<summary><strong>What are the tables?</strong></summary>

**Tables** are where the actual rows of data live. In this project, the pipeline creates three raw tables: `RAW_PLAYERS`, `RAW_SESSIONS`, and `RAW_GAME_EVENTS`. They sit in your database and schema (e.g. `GAME_ANALYTICS.RAW.RAW_PLAYERS`). You then use dbt to build more tables (staging, marts) on top of these.
</details>

<details>
<summary><strong>What is the <code>run_platform.sh</code> script?</strong></summary>

**run_platform.sh** is a bootstrap script in the project root. When you run it, it asks for your Snowflake credentials (or reuses them from `.env`), writes them into `app/.env`, sets up a Python environment, generates synthetic game data (players, sessions, events), and loads that data into your Snowflake raw tables. It's the main way to get data into Snowflake for this course.
</details>

<details>
<summary><strong>What is a role?</strong></summary>

A **role** in Snowflake controls what you can do: which warehouses you can use, which databases and schemas you can read or write, and which objects you can create or alter. Snowflake has built-in roles (e.g. `ACCOUNTADMIN`, the highest privilege) and you can create custom roles (e.g. `dbt_role`) and grant them only the permissions they need. Using a dedicated role for dbt instead of running as admin follows the **principle of least privilege**: dbt gets only warehouse usage and database access, not full account control. That keeps analytics work separate from administration and limits blast radius if something goes wrong.
</details>

<details>
<summary><strong>What are grants (permissions)?</strong></summary>

**Grants** are how you give a role permission to use Snowflake objects. You don't put data inside a role — you *grant* the role the right to use a warehouse (`grant usage on warehouse … to role …`), the right to use a database (`grant all on database … to role …`), and you grant the role itself to a user (`grant role … to user …`). Until you grant those, the role can't run queries or create tables. A clean setup chains them explicitly: user → has role → role has warehouse + database → so dbt (running as that user) can do its job without needing admin.
</details>

<details>
<summary><strong>What is "principle of least privilege"?</strong></summary>

**Principle of least privilege** means giving each user or process only the permissions it needs, and no more. Instead of running dbt as `ACCOUNTADMIN` (who can do anything), you create a role like `dbt_role` that can use one warehouse and one database. If dbt (or its credentials) is ever misused, the damage is limited to that warehouse and database, not the whole account. It's a security and governance best practice and is how production-ready Snowflake environments are usually set up.
</details>

<details>
<summary><strong>What is the permission chain (user → role → warehouse/database)?</strong></summary>

The **permission chain** is how access flows in Snowflake. A **user** is granted one or more **roles**. Each **role** is granted **usage** on warehouses (so it can run queries) and **privileges** on databases/schemas (so it can create and query tables). For dbt: your Snowflake user gets a role (e.g. `dbt_role`); that role is granted usage on a warehouse and access to a database; so when dbt connects as that user, it can run models and write to that database — without needing account admin. Understanding this chain helps you set up `profiles.yml` and troubleshoot "permission denied" errors.
</details>

---

## Where to run SQL in Snowflake

1. In the Snowflake web UI, open the left-hand menu and click **SQL File** (under the + icon) to create a new SQL worksheet.

![Where to create a new SQL file](../images/snowflake/sql-file-button-ui.png)

2. You'll get a worksheet where you can paste and run the commands below. Use the **Run** button (play icon) to execute them. At the top you'll see your current context: role, warehouse (e.g. `COMPUTE_WH`), database, and schema — that's the warehouse name you'll put in `.env`.

![Where to paste and run the SQL commands](../images/snowflake/sql-execution-ui.png)

---

## SQL to run

Choose **one** path below and run that block **once**, in order. Do not mix paths or run the same block twice.

### Path 1: Minimal (database + schemas only)

Use this if you're using an existing warehouse (e.g. trial default) and don't need a separate role. Paste and run in order:

```sql
CREATE DATABASE IF NOT EXISTS GAME_ANALYTICS;
USE DATABASE GAME_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;
CREATE SCHEMA IF NOT EXISTS CI;
```

### Path 2: Production-style (warehouse + database + role + schemas)

Use this if you want a dedicated warehouse and role for dbt. Replace `your_username` with your Snowflake user, then paste and run **in this order**:

```sql
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE DBT_WH WITH WAREHOUSE_SIZE = 'X-SMALL';
CREATE DATABASE GAME_ANALYTICS;
CREATE ROLE DBT_ROLE;
GRANT USAGE ON WAREHOUSE DBT_WH TO ROLE DBT_ROLE;
GRANT ALL ON DATABASE GAME_ANALYTICS TO ROLE DBT_ROLE;
GRANT ROLE DBT_ROLE TO USER your_username;
USE ROLE DBT_ROLE;
USE DATABASE GAME_ANALYTICS;
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS MARTS;
CREATE SCHEMA IF NOT EXISTS CI;
```

**Why we need these schemas (both paths)**

- **RAW** — The platform loads CSV data here. dbt only reads from RAW; it never writes to it, so raw data stays stable.
- **STAGING** — dbt builds staging models (cleaned views) here. Separating staging from raw and marts keeps layers clear and lets you refresh or permission them independently.
- **MARTS** — dbt builds dimensions, facts, and analytics tables here. This is the reporting-ready layer; keeping it in its own schema lets analysts and tools use only marts.
- **CI** — The CI pipeline (e.g. GitHub Actions) runs dbt in this schema so runs don't overwrite dev or marts and you can validate changes before merging.

---

## What the production path does (Path 2 explained)

If you chose **Path 2**, here's what each part of that script does. You're preparing a **clean Snowflake environment for dbt to work properly**.

1. **`USE ROLE ACCOUNTADMIN`** — Creating warehouses, databases, roles, and grants requires admin rights.
2. **`CREATE WAREHOUSE DBT_WH`** — A warehouse is compute power. dbt needs it to run models and build tables. `X-SMALL` is cheap and enough for development.
3. **`CREATE DATABASE GAME_ANALYTICS`** — The database is created **once** here. dbt models and schemas live inside it.
4. **`CREATE ROLE DBT_ROLE`** — A separate, limited role (not admin) follows least privilege and keeps analytics separate from administration.
5. **`GRANT ... TO ROLE DBT_ROLE`** and **`GRANT ROLE ... TO USER`** — The role gets warehouse usage and full access to the database; your user gets the role. So: user → `DBT_ROLE` → warehouse + database.
6. **`USE ROLE DBT_ROLE`** — Confirms everything works without admin rights.
7. **`USE DATABASE GAME_ANALYTICS`** and **`CREATE SCHEMA ... RAW, STAGING, MARTS, CI`** — Puts you in the database and creates the four schemas this project uses. dbt will build models here.

**Big picture:** Separate warehouse, database, and role with explicit permissions — production-ready. For how this connects to `profiles.yml` and running dbt, see [dbt setup](dbt-setup.md).