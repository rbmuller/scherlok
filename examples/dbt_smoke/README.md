# dbt Smoke Test

A minimal dbt project used to validate `scherlok dbt` end-to-end against a real Postgres + real dbt manifest. Use it as a starting point if you're contributing or just want to see the integration in action.

## What's here

- `dbt_project.yml` / `profiles.yml` — points at the local Postgres seeded by [`../docker-compose.yml`](../docker-compose.yml)
- `models/staging/stg_users.sql` — view (smoke-tests view discovery, see issue #7)
- `models/marts/fct_orders.sql` — table (smoke-tests volume drop detection)
- `models/marts/dim_products.sql` — table

## Run it

dbt-core requires Python 3.10–3.12 (not 3.13/3.14), so use a separate venv if your main scherlok venv is on a newer version.

```bash
# 1. Boot the seeded Postgres
cd examples
docker compose up -d

# 2. Install dbt-postgres in a 3.11 venv
cd dbt_smoke
pyenv shell 3.11.8     # or any 3.10–3.12
python -m venv .venv-dbt
source .venv-dbt/bin/activate
pip install "dbt-postgres>=1.8"

# 3. Generate the manifest
DBT_PROFILES_DIR=. dbt run

# 4. Run scherlok against the manifest (use your scherlok venv here)
deactivate
scherlok dbt --project-dir . --profiles-dir .
```

You should see all 3 models profiled with `✓` on the first run.

## Force an anomaly

```bash
docker exec examples-postgres-1 \
    psql -U scherlok -d demo -c "DELETE FROM orders WHERE id > 5;"

DBT_PROFILES_DIR=. dbt run --select fct_orders
scherlok dbt --project-dir . --profiles-dir . --fail-on critical
```

Expected: `fct_orders` shows `✗ CRITICAL: Row count dropped 60.0% (10 -> 4)` and the command exits with status 1.
