# Reconcile

Comparing a table with its own history can't see a join that fans out on the first build, or a total that changes while the row count stays the same. `scherlok reconcile` compares a pipeline's output with its input instead, and fails when they don't add up. This is where wrong numbers cost real money: revenue, royalties, ledgers.

```bash
scherlok reconcile --source raw.orders --target mart.orders \
  --key order_id --sum amount --tolerance 0.001
```

Both tables live in the database of the configured connection (`SCHERLOK_CONNECTION` or `scherlok connect`). No baseline is needed: it works on the first build.

## What it checks

| Finding | Needs | Finds |
|---|---|---|
| `row_count_mismatch` | nothing | The target has more or fewer rows than the source |
| `duplicate_keys` | `--key` | A key value on more than one row of the target: a join that fans out |
| `missing_keys` | `--key` | Keys of the source that the target doesn't have: rows dropped by a join or a filter |
| `extra_keys` | `--key` | Keys of the target that the source doesn't have |
| `sum_mismatch` | `--sum` | A column's total differs by more than `--tolerance` |

Every finding is CRITICAL, and any finding exits 1: an output that doesn't add up is wrong, not suspicious.

- **`--key`** names the column the target is unique on. Repeat it for a composite key: `--key order_id --key line_no`. Rows with a NULL in any key column are counted apart (`null_keys`) and never matched, since NULL equals nothing. A source that repeats a key too is reported in the finding's `baseline`, but the target is still expected to be unique.
- **`--sum`** names a column whose total must match. Repeat it for several. A total with no non-NULL value counts as 0.
- **`--tolerance`** is the largest absolute difference allowed between two totals, inclusive; the default 0 means exact. Totals are compared as decimals: a `DECIMAL` column is exact, and a floating-point total is taken at the value it prints as, so `100.01` against `100.0` differs by exactly `0.01`. Floating-point sums depend on the order rows are added, so give a `DOUBLE` or `FLOAT64` column a small tolerance such as `1e-6`. A negative, NaN or infinite tolerance is rejected.
- **Table names** may be qualified: `orders` (the connection's default schema or dataset), `raw.orders`, or `db.raw.orders` (`project.dataset.table` on BigQuery; MySQL takes `database.table` at most). A dot always separates parts. Every name is quoted by the connector, never pasted into SQL as typed.

The source and the target must be the same database in this version. Comparing across connections, partition filters and a config file of many table pairs are follow-ups.

## Output

The text output puts the two tables side by side (rows, NULL keys, distinct keys, repeated keys, keys the other table lacks, totals), lists the findings and ends with a one-line verdict. `--output json` prints one object on stdout, with Rich output, including a failed verdict, on stderr:

```json
{
  "passed": false,
  "source": {"table": "raw.orders", "row_count": 1000, "null_keys": 0, "distinct_keys": 1000,
             "duplicate_keys": 0, "duplicate_rows": 0, "sums": {"amount": 625625}},
  "target": {"table": "mart.orders", "row_count": 1100, "null_keys": 0, "distinct_keys": 1000,
             "duplicate_keys": 100, "duplicate_rows": 100, "sums": {"amount": 687875}},
  "key": ["order_id"],
  "sum": ["amount"],
  "tolerance": 0.001,
  "missing_keys": 0,
  "extra_keys": 0,
  "anomaly_count": 3,
  "info": 0,
  "warning": 0,
  "critical": 3,
  "anomalies": [
    {
      "table": "mart.orders",
      "type": "duplicate_keys",
      "severity": "CRITICAL",
      "message": "100 order_id values appear more than once (100 extra rows); raw.orders has 0",
      "column": "order_id",
      "metric": "duplicate_keys",
      "baseline": 0,
      "current": 100,
      "change_pct": null,
      "hint": "A join fans out; check that every table joined into the target is unique on its join key."
    }
  ]
}
```

The `anomalies` above are cut to one; the run also reports `row_count_mismatch` and `sum_mismatch`. Each finding carries the [structured fields](ci-cd.md#structured-anomaly-fields) every anomaly has. `baseline` is the source's value and `current` the target's, except for `missing_keys` and `extra_keys`, where `current` is the number of keys without a match and `baseline` is the 0 expected. `duplicate_rows` counts the rows beyond the first for each key; with `--key` absent, the key fields are `null`.

**Exit code:** 1 on any finding, or on an error such as a missing connection, an unknown table or column, or an invalid flag; 0 otherwise.

## What it reads and saves

- **Counts and totals only.** Every query is an aggregate (`COUNT`, `SUM`, a `GROUP BY` on the key, an anti-join counted with `COUNT(*)`); no key value is read back, so none can reach a CI log, a PR comment or an agent. A database error is reported by its type, such as `ConversionException`, and never by its text, because a conversion error quotes the value it could not convert.
- **Nothing is saved.** No profile, anomaly, run record or config is written, and no remote store is touched.

## Cost on BigQuery and Snowflake

Reconcile scans both tables in full on every run. Per run, it issues:

| Query | Reads |
|---|---|
| Row count and key statistics, once per table | the key columns |
| Totals, once per table, with `--sum` | the sum columns |
| Anti-join, twice (missing and extra keys), with `--key` | the key columns of both tables |

On BigQuery, which bills bytes scanned, that is about three reads of each table's key columns plus one of each sum column. On Snowflake there is no per-byte charge, but the warehouse runs for the duration of the two anti-joins, which dominate on large tables. Run it on the tables a build just wrote rather than on a schedule over everything, and try it on a small pair first. There is no partition filter yet, so a reconcile of a partitioned table reads every partition.

## From an agent

The MCP server exposes the same check as the read-only tool `reconcile(source, target, key_columns, sum_columns, tolerance)`, with the same result. Both tables must be listed by `list_tables`, and every key and sum column must exist in both; see [MCP server](mcp.md).
