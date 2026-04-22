# Scherlok Quick Demo

Try Scherlok in 2 minutes with a local PostgreSQL database.

## Setup

```bash
# Start PostgreSQL with sample data
cd examples
docker compose up -d

# Install Scherlok
pip install scherlok

# Connect
scherlok connect postgres://scherlok:scherlok@localhost:5433/demo
```

## Try it

```bash
# 1. Profile your data
scherlok investigate

# 2. Check health
scherlok status

# 3. See the full report
scherlok report

# 4. Watch for anomalies (should be clean on first run)
scherlok watch
```

## Simulate an anomaly

```bash
# Delete half the orders (simulates a data pipeline failure)
docker exec -i examples-postgres-1 psql -U scherlok demo -c "DELETE FROM orders WHERE id > 7"

# Run watch again — Scherlok will detect the volume drop
scherlok watch
```

## Cleanup

```bash
docker compose down -v
```
