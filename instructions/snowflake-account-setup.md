## Creating a Snowflake account (for this project)

This project assumes you have access to a **Snowflake account**.  
There are two common paths:

1. **Course-provided account** (recommended for workshops / classes)
2. **Free Snowflake trial** (if you are doing this on your own)

---

### 1. Course-provided account

If you are doing this as part of a **course, bootcamp, or workshop**, your instructor may give you:

- A Snowflake **URL** (e.g. `https://cwrlboz-pz37526.snowflakecomputing.com`)
- A **username** and temporary **password**
- The **role**, **warehouse**, and **database/schema** you should use

In that case:

1. Follow the instructor’s onboarding docs to log in the first time and change your password.
2. Keep the following values handy (you’ll need them for `.env` and your dbt profile):
   - Account identifier (e.g. `cwrlboz-pz37526`)
   - Username and password
   - Warehouse name (e.g. `COMPUTE_WH`, `TRANSFORMING`)
   - Database and schema names where you are allowed to create tables
3. Put these values into:
   - `game-data-platform/app/.env` (or `game-data-platform/.env`)  
   - Your `~/.dbt/profiles.yml` (for the dbt project)

> See `snowflake-credentials.md` in this folder for exact variable names and file formats.

---

### 2. Free Snowflake trial (self‑study)

If you are doing this independently, you can sign up for a **free Snowflake trial**:

1. Go to the official Snowflake site and start a **free trial**:
   - Search for “Snowflake free trial” in your browser and follow the official link.
2. During signup, pick:
   - A **cloud provider** and **region** close to you.
   - An account name (Snowflake will generate an identifier like `xy12345.eu-central-1`).
3. After signup:
   - Log in to the Snowflake web UI.
   - Note your **account identifier** from the URL.
   - Create (or identify) a **warehouse** you can use.

You will then:

- Create a **database** for this course (see `pre-launch-setup.md`).
- Create a **schema** for raw tables (e.g. `RAW`).
- Optionally create a **dev schema** for dbt models (e.g. `DEV`).

---

### 3. Mapping Snowflake UI to the values we use

In the Snowflake UI you will see:

- **Account URL**  
  - Example: `https://xy12345.eu-central-1.snowflakecomputing.com`  
  - We use: `SNOWFLAKE_ACCOUNT=xy12345.eu-central-1`

- **User**  
  - Example: `JOEL`  
  - We use: `SNOWFLAKE_USER=JOEL`

- **Warehouse**  
  - Example: `COMPUTE_WH`  
  - We use: `SNOWFLAKE_WAREHOUSE=COMPUTE_WH`

- **Database**  
  - Example: `GAME_ANALYTICS`  
  - We use: `SNOWFLAKE_DATABASE=GAME_ANALYTICS`

- **Schema**  
  - Example: `RAW`  
  - We use: `SNOWFLAKE_SCHEMA=RAW`

> Again, see `snowflake-credentials.md` for the `.env` snippet you can copy‑paste.

