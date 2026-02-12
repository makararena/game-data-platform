## What to do before running `run_platform.sh`

Before you run the Game Data Platform bootstrap script, you should have a **minimal Snowflake setup** ready:

- A **warehouse** you can use for compute.
- A **database** for this course (e.g. `GAME_ANALYTICS`).
- A **schema** for raw tables (e.g. `RAW`).

This file explains what to create and how it will be used.

---

### 1. Choose (or create) a warehouse

You need a **virtual warehouse** for compute. It can be:

- A shared course warehouse (e.g. `TRANSFORMING`, `COMPUTE_WH`), or
- Your own small warehouse (e.g. `COMPUTE_WH`, `ANALYTICS_WH`).

If you have permission to create one, in Snowflake you can run:

```sql
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WITH WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

In your `.env`:

```env
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
```

---

### 2. Create a database for the course

To keep things clean, use a dedicated database, for example `GAME_ANALYTICS`:

```sql
CREATE DATABASE IF NOT EXISTS GAME_ANALYTICS;
```

Then set in your `.env`:

```env
SNOWFLAKE_DATABASE=GAME_ANALYTICS
```

---

### 3. Create a schema for raw tables

Inside your course database, create a **RAW** schema for the ingested CSVs:

```sql
USE DATABASE GAME_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS RAW;
```

Then set in your `.env`:

```env
SNOWFLAKE_SCHEMA=RAW
```

After you run `run_platform.sh`, the pipeline will create or replace:

- `GAME_ANALYTICS.RAW.RAW_PLAYERS`
- `GAME_ANALYTICS.RAW.RAW_SESSIONS`
- `GAME_ANALYTICS.RAW.RAW_GAME_EVENTS`

> If you use different names, just update `SNOWFLAKE_DATABASE` and `SNOWFLAKE_SCHEMA` accordingly.

---

### 4. (Optional) Create a dev schema for dbt

For the dbt project, it’s common to use a **separate schema** for models (e.g. `DEV` or `MARTS`):

```sql
USE DATABASE GAME_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS DEV;
```

Then in your dbt profile (not in `.env`) you would use:

```yaml
database: GAME_ANALYTICS
schema: DEV
```

dbt will read sources from `GAME_ANALYTICS.RAW` and build models into `GAME_ANALYTICS.DEV`.

---

### 5. Checklist before first run

Before you run:

```bash
./run_platform.sh
```

make sure:

1. You can log into Snowflake with your **user + password**.
2. You have:
   - A **warehouse** (name noted).
   - A **database** (e.g. `GAME_ANALYTICS`).
   - A **schema** for raw tables (e.g. `RAW`).
3. Your `.env` (either `game-data-platform/.env` or `game-data-platform/app/.env`) contains:

   ```env
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_WAREHOUSE=COMPUTE_WH
   SNOWFLAKE_DATABASE=GAME_ANALYTICS
   SNOWFLAKE_SCHEMA=RAW
   ```

Once this is in place, `run_platform.sh` will:

- Generate CSVs.
- Load them into your `RAW` schema.
- Tell you exactly where the tables live so you can start your dbt project.

