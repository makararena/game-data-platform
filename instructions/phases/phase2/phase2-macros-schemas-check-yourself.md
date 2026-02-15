# Phase 2: Macros and multi-schema support — Check yourself

This file gives the full solution for Phase 2. Try the [task](phase2-macros-schemas.md) first, then use this to compare.

---

## 1. Vars and staging config in `dbt_project.yml`

Add (or merge) the following. Vars at top level; `staging` under your project’s `models` block:

```yaml
# Top level, e.g. after profile: 'game_dbt_project'
vars:
  raw_schema: 'RAW'
  staging_schema: 'STAGING'

# Under models: → game_dbt_project: (or your project name)
models:
  game_dbt_project:
    staging:
      +schema: staging
```

---

## 2. Macro: `macros/generate_schema_name.sql`

Create the file with this content:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif custom_schema_name | trim | lower == 'staging' -%}
        {{ var('staging_schema', 'STAGING') }}
    {%- else -%}
        {{ target.schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

---

## 3. Source schema in each source YAML

In **each** of `sources/raw_players.yml`, `sources/raw_sessions.yml`, `sources/raw_game_events.yml`, the source block should have:

```yaml
sources:
  - name: raw
    description: ...
    database: "{{ target.database }}"
    schema: "{{ var('raw_schema', 'RAW') }}"
    tables:
      - name: raw_players   # or raw_sessions / raw_game_events
        ...
```

So: **schema** is `"{{ var('raw_schema', 'RAW') }}"`, not `target.schema`.

---

## Verify

```bash
dbt parse
```

Then proceed to Phase 3 to add staging models; `dbt run --select staging` will build them in the STAGING schema.
