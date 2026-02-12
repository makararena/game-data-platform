## Snowflake credentials: what you need and where to put them

This project uses **Snowflake** to store the raw game telemetry tables:

- `RAW_PLAYERS`
- `RAW_SESSIONS`
- `RAW_GAME_EVENTS`

To connect, you need a small set of **Snowflake credentials** and a place to store them so that the Python pipeline (and your dbt project) can read them.

---

### 1. Which credentials you need

You need the following values:

- **SNOWFLAKE_USER**: your Snowflake login (e.g. `joel.miller`).
- **SNOWFLAKE_PASSWORD**: your Snowflake password.
- **SNOWFLAKE_ACCOUNT**: your Snowflake account identifier  
  - Often looks like `xy12345.eu-central-1` or similar.
  - You can usually see it in the Snowflake URL:  
    `https://xy12345.eu-central-1.snowflakecomputing.com`
- **SNOWFLAKE_WAREHOUSE**: the virtual warehouse to use for compute (e.g. `COMPUTE_WH` or `TRANSFORMING`).
- **SNOWFLAKE_DATABASE**: the database where this course will create raw tables (e.g. `GAME_ANALYTICS`).
- **SNOWFLAKE_SCHEMA**: the schema for raw tables (e.g. `RAW`).

If you are doing this course with a provided Snowflake account, your **instructor or course materials** should give you these values directly.

---

### 2. Where to put the credentials (.env file)

The Python pipeline (`game-data-platform/app`) expects credentials in an `.env` file:

- Path: `game-data-platform/app/.env`
- Format:

```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=GAME_ANALYTICS
SNOWFLAKE_SCHEMA=RAW
```

> Note: values should be **plain**, without quotes.  
> `SNOWFLAKE_USER=joel` ✅  
> `SNOWFLAKE_USER="joel"` ✅ (also works)  
> `SNOWFLAKE_USER = joel` ❌ (spaces around `=` can be problematic in some tools)

You can also keep a **root `.env`** at:

- Path: `game-data-platform/.env`

with the same variable names. The bootstrap script (see below) will read from it as defaults, so you don’t have to type everything repeatedly.

---

### 3. Using the bootstrap script (`run_platform.sh`)

Instead of manually creating `app/.env`, you can let the helper script do it:

From `game-data-platform/`:

```bash
chmod +x run_platform.sh   # one time
./run_platform.sh
```

The script will:

1. **Read defaults** from:
   - `game-data-platform/.env` (if it exists)
   - `game-data-platform/app/.env` (if it exists)
2. **Ask you** for each Snowflake value (pre-filling with existing defaults when available).
3. **Write** the final values into `game-data-platform/app/.env`.
4. Create/activate a virtualenv, install dependencies, generate data, and load it into Snowflake.

After it finishes, you will have:

- `GAME_ANALYTICS.RAW.RAW_PLAYERS`
- `GAME_ANALYTICS.RAW.RAW_SESSIONS`
- `GAME_ANALYTICS.RAW.RAW_GAME_EVENTS`

…or the equivalent, based on your `SNOWFLAKE_DATABASE` and `SNOWFLAKE_SCHEMA`.

---

### 4. How this relates to your dbt project

Your **dbt profile** (usually in `~/.dbt/profiles.yml`) also needs Snowflake connection info.  
The minimal mapping looks like:

- `account`  ← `SNOWFLAKE_ACCOUNT`
- `user`     ← `SNOWFLAKE_USER`
- `password` ← `SNOWFLAKE_PASSWORD`
- `warehouse`← `SNOWFLAKE_WAREHOUSE`
- `database` ← `SNOWFLAKE_DATABASE`
- `schema`   ← your dev schema (often **not** `RAW`; e.g. `DEV`)

The idea is:

- The **pipeline** writes **raw** tables into `SNOWFLAKE_DATABASE.SNOWFLAKE_SCHEMA`.
- The **dbt project** reads from those as **sources** and builds staging/marts into a **separate schema** (e.g. `DEV` or `MARTS`).

---

### 5. Security notes

- Never commit `.env` files to Git (they contain passwords).
  - This repo’s `.gitignore` already ignores common env files, but double-check before committing.
- Treat the `.env` file like any other secret: do not paste it into screenshots, gists, or public repos.
- If you accidentally leak your credentials, rotate your password (and, if needed, regenerate keys) in Snowflake.

