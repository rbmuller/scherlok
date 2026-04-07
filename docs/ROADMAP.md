# Scherlok — 6-Month Roadmap

**Timeline:** April 2026 — September 2026
**Constraint:** Part-time (~10h/week). Robson maintains 3 US clients ($220k/yr) in parallel.

---

## Month 1 (April 2026) — Foundation

**Goal:** Working prototype that profiles a Postgres database.

### Week 1-2: Project setup
- [x] Create repo `github.com/rbmuller/scherlok`
- [x] Python project structure (src/scherlok/, tests/, pyproject.toml)
- [x] CLI with Typer (`scherlok connect`, `scherlok investigate`, `scherlok watch`, `scherlok config`)
- [x] CI/CD with GitHub Actions (pytest + ruff linting)
- [x] README with install instructions and product positioning

### Week 3-4: Postgres profiler + remote storage
- [x] `scherlok connect postgres://...` — validate connection, discover tables
- [x] `scherlok investigate` — profile all tables:
  - Row count per table
  - Column types, nullability rates
  - Basic stats per numeric column (mean, median, min, max, stddev)
  - Cardinality per column (distinct count)
  - Top 10 values per categorical column
  - Last modified timestamp (freshness)
- [x] Store profiles in local SQLite (`.scherlok/profiles.db`)
- [x] `scherlok report` — print profile summary to terminal
- [x] Remote storage: S3, GCS, Azure Blob (`scherlok config --store s3://...`)
- [x] Sync context: auto download/upload profiles for CI/CD environments
- [x] 37 tests passing, CI green

**Deliverable:** `pip install scherlok` works. Can profile a Postgres database. Profiles persist in cloud storage.

---

## Month 2 (May 2026) — Detection

**Goal:** Detect anomalies by comparing current state vs stored profile.

### Week 5-6: Anomaly detection engine
- [x] `scherlok watch` — compare current state vs last profile:
  - [x] Volume drops (>20% WARNING, >50% CRITICAL)
  - [x] Volume spikes (>100% WARNING, >300% CRITICAL)
  - [x] Schema drift: column added, removed, or type changed
  - [x] Freshness: table not updated within expected cadence
  - [x] Nullability: NULL rate changed significantly
  - [x] Distribution: mean/stddev shifted beyond threshold
  - [x] Cardinality: number of distinct values changed unexpectedly
- [x] Severity scoring: INFO / WARNING / CRITICAL
- [x] Auto-calibrated thresholds (no manual config needed)
- [x] `scherlok status` — real-time health check with anomaly detection
- [x] `scherlok history` — timeline of all past anomalies
- [x] `scherlok report` — shows anomaly count per table with link to history
- [x] Anomaly persistence — anomalies saved to SQLite for history tracking

### Week 7-8: Alerting
- [ ] `scherlok watch --slack https://hooks.slack.com/...` — Slack notifications
- [x] `scherlok watch --exit-code` — exit 1 on CRITICAL (for CI/CD)
- [x] Alert format: what changed, when, how much
- [ ] `scherlok watch --schedule "0 6 * * *"` — built-in cron scheduler

**Deliverable:** End-to-end flow works: connect → investigate → watch → alert on Slack.

---

## Month 3 (June 2026) — Polish & Launch

**Goal:** Public launch. First 100 stars.

### Week 9-10: UX polish
- [ ] Beautiful terminal output (Rich library for colored tables, progress bars)
- [ ] `scherlok status` — dashboard showing all monitored tables and their health
- [ ] `scherlok history` — show anomaly timeline
- [ ] Improve error messages (connection failures, permission issues)
- [ ] Add `--verbose` and `--quiet` flags

### Week 11-12: Launch
- [ ] Write README with GIF/screenshot of terminal output
- [ ] Create demo video (60s, showing connect → investigate → detect anomaly)
- [ ] Publish on PyPI: `pip install scherlok`
- [ ] Blog post on dev.to: "I Built a Zero-Config Data Quality Tool"
- [ ] LinkedIn post from Robson's account
- [ ] Reddit: r/dataengineering, r/python
- [ ] Hacker News: Show HN
- [ ] Submit to awesome-python list
- [ ] Product Hunt launch

**Deliverable:** Public repo, PyPI package, 100+ stars.

---

## Month 4 (July 2026) — BigQuery + Snowflake

**Goal:** Support the 3 most popular data warehouses. First external contributors.

### Week 13-14: BigQuery connector
- [ ] `scherlok connect bigquery://project.dataset`
- [ ] Profile via INFORMATION_SCHEMA (fast, no full table scan)
- [ ] Cost-aware profiling (estimate query cost before running)
- [ ] Test with real BigQuery data (Fevo client data as dogfood)

### Week 15-16: Snowflake connector + community
- [ ] `scherlok connect snowflake://account/db/schema`
- [ ] Create 10 "good first issue" labels on GitHub
- [ ] Write CONTRIBUTING.md
- [ ] Respond to all issues/PRs within 24h
- [ ] Target: first external PR merged

**Deliverable:** 3 connectors (Postgres, BigQuery, Snowflake). 300+ stars.

---

## Month 5 (August 2026) — Lineage + CI/CD Integration

**Goal:** Become the go-to CI/CD data quality gate.

### Week 17-18: Basic lineage
- [ ] `scherlok lineage` — detect table dependencies (via foreign keys, column name matching)
- [ ] When anomaly detected, show downstream impact: "Table X is affected, used by dashboard Y"
- [ ] dbt integration: read dbt manifest.json for lineage
- [ ] Display lineage as ASCII tree in terminal

### Week 19-20: CI/CD integration
- [ ] `scherlok check --fail-on critical` — designed for CI/CD pipelines
- [ ] GitHub Action: `uses: rbmuller/scherlok-action@v1`
- [ ] Pre-built Docker image for easy CI integration
- [ ] Documentation: "How to add Scherlok to your CI/CD pipeline"
- [ ] Example: GitHub Actions workflow that blocks deploy on data quality failure

**Deliverable:** CI/CD ready. GitHub Action published. 500+ stars.

---

## Month 6 (September 2026) — MySQL, DuckDB + Scherlok Cloud planning

**Goal:** 5 connectors, 1000 stars, decision on SaaS.

### Week 21-22: MySQL + DuckDB connectors
- [ ] `scherlok connect mysql://...`
- [ ] `scherlok connect duckdb:///path/to/file.db`
- [ ] Connector plugin system (make it easy for community to add new connectors)

### Week 23-24: Evaluate + plan next phase
- [ ] Analyze metrics: stars, downloads, issues, community size
- [ ] Survey users: what's missing? what would they pay for?
- [ ] Decision point: continue as OSS or build Scherlok Cloud (SaaS dashboard)?
- [ ] If SaaS: wireframe dashboard, define pricing tiers, plan MVP
- [ ] If OSS only: plan v1.0 with advanced features (ML-based detection, custom checks)
- [ ] Write "6-month retrospective" blog post

**Deliverable:** 5 connectors, CI/CD integration, 1000+ stars, clear next-phase plan.

---

## Summary

| Month | Focus | Stars target | Key deliverable |
|-------|-------|-------------|-----------------|
| 1 | Foundation + Postgres profiler | — | `pip install` works |
| 2 | Anomaly detection + Slack alerts | — | End-to-end flow |
| 3 | Polish + public launch | 100 | PyPI + HN + Product Hunt |
| 4 | BigQuery + Snowflake | 300 | 3 connectors |
| 5 | Lineage + CI/CD | 500 | GitHub Action |
| 6 | MySQL + DuckDB + evaluate | 1000 | 5 connectors + decision |

## Time Investment

| Activity | Hours/week |
|----------|-----------|
| Coding | 6-8h |
| Community (issues, PRs, discussions) | 1-2h |
| Content (blog, LinkedIn, demos) | 1-2h |
| **Total** | **~10h/week** |

Compatible with maintaining 3 US clients ($220k/yr). Weekend + evening work.

## Decision Gates

| Gate | When | Question | If No |
|------|------|----------|-------|
| Gate 1 | Month 3 (launch) | Did we get 50+ stars in first week? | Reassess positioning, not product |
| Gate 2 | Month 4 | Are people opening issues and asking for features? | Project may not have PMF — consider pivoting |
| Gate 3 | Month 6 | 500+ stars and growing? | Keep as portfolio piece, don't invest in SaaS |

## What Success Looks Like

**Minimum (portfolio value):** 500 stars, 3 connectors, used by Robson in own clients. Strengthens Georgia Tech application and LinkedIn positioning.

**Good (community tool):** 1000+ stars, external contributors, mentioned in data engineering blogs/podcasts. Establishes Robson as thought leader.

**Great (business opportunity):** 5000+ stars, companies asking for enterprise features, clear path to SaaS revenue. Consider full-time or funding.

---

*"The game is afoot."*
