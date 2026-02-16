# Phase 2: Macros and multi-schema support — Task

This phase sets up **schema separation**: raw tables stay in one schema (e.g. **RAW**), and dbt-built models go into dedicated schemas (e.g. **STAGING**, **MARTS**). You will add **vars**, a **macro** that controls where each model is built, **source schema** wiring, and **folder config** so staging uses the staging schema and marts use the marts schema. Do this before building staging models so that when you run staging in Phase 3, views are created in the right place, and so Phase 4 core marts build in MARTS.

**Prerequisites:** Phase 1 complete (sources defined). You have multiple schemas in Snowflake (e.g. RAW, STAGING, MARTS) and want sources to read from RAW while staging models are built in STAGING and marts in MARTS.

---

## Why this phase (internals)

- **Problem:** If everything uses `target.schema`, then sources and all models point at the same schema. When your profile uses `schema: RAW`, dbt would build staging views in RAW too, mixing raw and transformed objects. You want **raw data in RAW**, **staging in STAGING**, and **marts in MARTS**.
- **Approach:** (1) **Sources** use a variable `raw_schema` so they always read from the schema where ingest wrote the tables. (2) **Models** get their schema from a macro `generate_schema_name`: when a model (or its folder) sets a custom schema name (e.g. `staging` or `marts`), the macro returns the corresponding schema (e.g. `STAGING` or `MARTS`). So one macro + vars + config drive the whole layout.

---

## Goals

- **2.1** Add **vars** `raw_schema`, `staging_schema`, and `marts_schema` in `dbt_project.yml` so schema names are configurable.
- **2.2** Set each **source**’s `schema` to `"{{ var('raw_schema', 'RAW') }}"` so sources point at the raw schema regardless of `target.schema`.
- **2.3** Create the **`generate_schema_name`** macro so that models with custom schema `staging` are built in `var('staging_schema', 'STAGING')`, models with `marts` in `var('marts_schema', 'MARTS')`; other models use `target.schema` (or default concatenation).
- **2.4** In `dbt_project.yml`, under `models`, set **staging** folder to `+schema: staging` so staging models get the custom schema and the macro applies.
- **2.5** In `dbt_project.yml`, under `models`, set **marts** folder to `+schema: marts` so mart models (Phase 4) are built in the MARTS schema.

---

## 1. Vars in `dbt_project.yml`

Add three variables at the top level (e.g. after `profile:`):

```yaml
vars:
  raw_schema: 'RAW'
  staging_schema: 'STAGING'
  marts_schema: 'MARTS'
```

- **`raw_schema`** — The Snowflake schema where raw tables live (where the ingest wrote `RAW_PLAYERS`, etc.). Sources will use this so they never depend on `target.schema` for reading raw data.
- **`staging_schema`** — The schema where staging models (Phase 3) will be built. The macro will return this when the model’s custom schema is `staging`.
- **`marts_schema`** — The schema where mart models (Phase 4 and beyond) will be built. The macro will return this when the model's custom schema is `marts`.

**Why vars:** Different environments (dev/prod) or tenants might use different schema names. Centralizing them in vars gives one place to change and allows overrides via CLI: `dbt run --vars '{"raw_schema":"RAW","staging_schema":"STAGING","marts_schema":"MARTS"}'` without editing YAML or the macro.

---

## 2. Source schema (update Phase 1 sources)

In each source definition (in `sources/raw_players.yml`, `raw_sessions.yml`, `raw_game_events.yml`), set:

- **database:** `"{{ target.database }}"`
- **schema:** `"{{ var('raw_schema', 'RAW') }}"`

So sources always read from the raw schema. If you previously had `schema: "{{ target.schema }}"`, replace it with the var.

**Why not target.schema for sources:** When you use multiple schemas, `target.schema` is often the “default” schema for dbt (e.g. where you run). Raw tables are in RAW; making sources use `var('raw_schema')` keeps that contract explicit and independent of the target.

---

## 3. Override `generate_schema_name` macro

dbt resolves the Snowflake schema for each model by calling `generate_schema_name(custom_schema_name, node)`. By default, dbt often uses `target.schema` or `target.schema + "_" + custom_schema`, which can put everything in one schema. To send staging to a dedicated schema, override the macro.

Create **`macros/generate_schema_name.sql`**:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif custom_schema_name | trim | lower == 'staging' -%}
        {{ var('staging_schema', 'STAGING') }}
    {%- elif custom_schema_name | trim | lower == 'marts' -%}
        {{ var('marts_schema', 'MARTS') }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

**Why it works:**

- When **no custom schema** is set (`custom_schema_name is none`), the model uses `target.schema` (default behavior).
- When the custom schema is **`staging`** (set for the staging folder), the macro returns `var('staging_schema', 'STAGING')`, so staging views are created in that schema.
- When the custom schema is **`marts`** (set for the marts folder), the macro returns `var('marts_schema', 'MARTS')`, so mart models (Phase 4) are built in that schema.
- For **any other** custom schema, the macro uses `target.schema + "_" + custom_schema` so you can add more layers without changing the macro each time.

**Internals:** dbt passes the model’s `schema` config (or the folder’s `+schema`) as `custom_schema_name`. The macro’s return value is the actual Snowflake schema name used when creating the relation.

---

## 4. Staging and marts folder config: `+schema: staging` and `+schema: marts`

In `dbt_project.yml`, under your project’s `models:` block, give the staging and marts folders a custom schema name:

```yaml
models:
  game_dbt_project:
    staging:
      +schema: staging
    marts:
      +schema: marts
```

(Use your actual project name if it’s not `game_dbt_project`.)

**Why this is needed:** The macro only receives a non-null `custom_schema_name` when a model (or its folder) sets `+schema`. So `+schema: staging` makes dbt pass `"staging"` into `generate_schema_name` and staging models are built in the STAGING schema; `+schema: marts` does the same for marts (Phase 4). Without this config, those models would use `target.schema` like any other model.

---

## 5. Verify

From your dbt project root:

```bash
dbt parse
```

Then inspect compiled schema names (no Snowflake run required):

- Ensure sources in the manifest use `var('raw_schema')` (you can check the resolved source schema in the docs or manifest).
- After you add staging models in Phase 3, `dbt run --select staging` will create views in the schema returned by the macro (e.g. STAGING). After you add marts in Phase 4, `dbt run --select marts` will create tables in MARTS.

You can also run `dbt debug` to confirm project and profile load; schema resolution is applied at compile/run time.

---

## Why this phase matters

- **Separation of concerns:** Raw data stays in RAW; staging (and later marts) live in their own schemas. Easier to grant permissions, reason about lineage, and avoid mixing raw and refined objects.
- **One place to control layout:** Vars + one macro (+ folder config) define the mapping. Staging and marts each have a var and a branch in the macro, so RAW, STAGING, and MARTS stay clearly separated.

---

**When you’re done,** see [Phase 2 — Check yourself](phase2-macros-schemas-check-yourself.md) for the full solution. Then proceed to [Phase 3: Staging layer](../phase3/phase3-staging.md).
