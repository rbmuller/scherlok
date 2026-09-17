# How Scherlok compares

Every claim below about another tool links to that tool's own public documentation or pricing page, as read on 2026-09-17. If something is out of date, [open an issue](https://github.com/rbmuller/scherlok/issues) and it will be corrected.

## At a glance

| | **Scherlok** | Elementary | Soda | Great Expectations | Monte Carlo |
|---|---|---|---|---|---|
| Open-source core | MIT: CLI + dbt package | Apache-2.0 dbt package + CLI | Apache-2.0 Soda Core | Apache-2.0 GX Core | No (SaaS) |
| Config before detection starts | None | YAML per anomaly test | SodaCL YAML checks | Expectations you declare | Monitors configured in the product |
| Learns baselines in the free tier | Yes, automatically | Yes, with per-test config | No: anomaly checks need Soda Library + Soda Cloud | No: validates declared expectations | n/a |
| Works without dbt | Yes | No | Yes | Yes | Yes |
| Lineage | From `manifest.json`, in alerts | OSS report; column-level in Cloud | Cloud | Cloud | Yes |
| Self-hosted | Yes | OSS yes; Cloud is managed | Core yes; Cloud is managed | Core yes; Cloud is managed | No |
| Pricing | Free | OSS free; Cloud by seats and environments | Core free; Cloud has a free plan | Core free; Cloud has a free Developer option | Quote-based |

## Elementary

Elementary OSS is a dbt package plus a CLI. The package provides anomaly detection tests and metadata tables; the CLI sends alerts and generates a self-hosted observability report that "help[s] you track data lineage, test coverage, and overall pipeline health" ([Elementary OSS introduction](https://docs.elementary-data.com/oss/oss-introduction)). Each anomaly test is configured in YAML per model, with parameters such as `timestamp_column`, `time_bucket`, `where_expression` and the training period ([volume_anomalies](https://docs.elementary-data.com/data-tests/anomaly-detection-tests/volume-anomalies)). Elementary Cloud adds ML-powered anomaly detection, automated monitors and column-level lineage; plans are priced by seats and environments and quoted on request ([pricing](https://www.elementary-data.com/pricing)). Elementary requires dbt.

**Pick Elementary** when your team lives in dbt and wants fine-grained control over each test. **Pick Scherlok** when you want detection running in minutes with nothing to configure, need it outside dbt, or want the detectors on tables dbt does not own.

## Soda

Soda Core v3 is a free, open-source library and CLI that turns SodaCL checks (YAML) into SQL ([Soda Core overview](https://docs.soda.io/soda-core/overview-main.html)). Anomaly detection checks are marked "not supported in Soda Core": they require Soda Library with Soda Cloud and the Soda Scientific package ([anomaly detection checks](https://docs.soda.io/soda-cl/anomaly-detection.html)). Soda Cloud has a free plan billed in Soda Processing Units, with paid tiers above it ([pricing](https://www.soda.io/pricing)).

**Pick Soda** for declarative data contracts with a large check language. **Pick Scherlok** when the goal is catching the failures you did not write a check for.

## Great Expectations

GX Core is an Apache-2.0 Python library: you declare Expectations about your data and GX validates them ([GX Core introduction](https://docs.greatexpectations.io/docs/core/introduction/)). Learning a baseline over time is not part of GX Core; GX Cloud offers a free Developer option and Team/Enterprise plans ([GX Cloud pricing](https://greatexpectations.io/pricing)).

**Pick Great Expectations** when you need a rich vocabulary of explicit assertions and documentation generated from them. **Pick Scherlok** when nobody has time to write the assertions first.

## Monte Carlo

Monte Carlo is a managed data observability platform with out-of-the-box monitors for table freshness, volume and schema change, plus metric and validation monitors ([monitors overview](https://docs.getmontecarlo.com/docs/monitors-overview)). Pricing is not published; the pricing page routes to a sales conversation ([pricing](https://www.montecarlodata.com/pricing/)).

**Pick Monte Carlo** when you want a managed platform with incident management across the whole stack and have the budget for it. **Pick Scherlok** when you want the freshness/volume/schema/NULL/distribution detectors as a free, self-hosted CLI that fits in a CI step.

## What Scherlok does not do (yet)

- No hosted UI: the [HTML dashboard](dashboard.md) is a single file you generate and share.
- No column-level lineage: lineage comes from dbt's `manifest.json`, so it is model-level and dbt-only.
- Detectors are statistical (robust median/MAD bands and fixed cold-start thresholds), not ML models.
- Connectors: PostgreSQL, BigQuery, Snowflake, MySQL, DuckDB. Databricks is [in progress](https://github.com/rbmuller/scherlok/issues/37).
