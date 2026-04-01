# Scherlok — Vision Document

**"A detective for your data."**

## The Problem

Every data team has the same nightmare: bad data reaching production undetected.

A source API silently changes from dollars to cents. A column starts returning NULLs. Row counts drop 40% on a Tuesday. A new enum value appears that breaks downstream models. Revenue dashboards show wrong numbers for 3 weeks before anyone notices.

Current solutions require you to **define what "correct" looks like** before you can detect what's wrong. You write hundreds of rules, tests, and assertions. And you still miss things — because you can't write rules for problems you haven't imagined yet.

## The Solution

Scherlok takes the opposite approach: **learn first, then detect.**

```bash
pip install scherlok
scherlok connect postgres://user:pass@host/db
scherlok investigate    # profiles your data, learns the patterns
scherlok watch          # continuously monitors, alerts on drift
```

**Zero config. Zero rules to write. Works in 5 minutes.**

Scherlok builds a statistical profile of every table and column — distributions, nullability patterns, cardinality, freshness cadence, volume trends, value ranges, correlations. It learns what "normal" looks like from historical data.

Then it watches. When something drifts from normal, it alerts with context: what changed, when it changed, how much it deviated, and which downstream tables/dashboards are affected.

## How It Works

### Phase 1: Investigate

```bash
scherlok investigate --lookback 30d
```

Scherlok connects to your data warehouse and builds profiles:

- **Volume profile**: daily row counts, growth rate, seasonal patterns
- **Schema profile**: column names, types, nullability
- **Distribution profile**: per-column stats (mean, median, stddev, percentiles, cardinality, top values)
- **Freshness profile**: how often each table updates, expected cadence
- **Relationship profile**: foreign key consistency, referential integrity

This runs once and takes minutes to hours depending on data size. Profiles are stored locally (SQLite) or in a shared store (PostgreSQL).

### Phase 2: Watch

```bash
scherlok watch --schedule "0 6 * * *"   # daily at 6 AM
scherlok watch --continuous              # real-time via CDC
```

Every run, Scherlok compares current state against learned profiles:

| Check | What It Detects |
|-------|----------------|
| **Volume anomaly** | Row count dropped 40% vs 7-day avg |
| **Freshness alert** | Table hasn't updated in 12h (normally updates every 2h) |
| **Schema drift** | Column added, removed, or type changed |
| **Distribution shift** | NULL rate jumped from 2% to 45% |
| **Value range breach** | Amount column has negative values (never seen before) |
| **Cardinality change** | Status column went from 5 values to 500 |
| **Referential break** | 15% of user_ids in orders don't exist in users |

### Phase 3: Alert

```bash
scherlok alert --slack https://hooks.slack.com/...
scherlok alert --email data-team@company.com
scherlok alert --pagerduty KEY
scherlok alert --exit-code    # for CI/CD: exits 1 if critical
```

Alerts include:
- **What** changed (which table, which column, which metric)
- **When** it changed (timestamp of first deviation)
- **How much** it deviated (statistical significance score)
- **Impact** (which downstream tables/dashboards are affected, if lineage is available)
- **Suggested action** ("Check source API for schema changes" or "Upstream table X also shows anomaly")

## Differentiators vs Competition

### vs Great Expectations

| | Great Expectations | Scherlok |
|---|---|---|
| Setup time | Hours (config, expectations, stores) | 5 minutes |
| Rules | You write every rule manually | Learns automatically |
| Config | 50+ lines of YAML | Zero |
| Learning curve | Steep | `pip install && investigate && watch` |
| Anomaly detection | Manual thresholds | Statistical (auto-calibrated) |
| Price | Free (OSS) | Free (OSS) |

Great Expectations is powerful but complex. It's designed for data engineers who want full control. Scherlok is for teams who want protection without the overhead.

### vs Soda

| | Soda | Scherlok |
|---|---|---|
| Config | YAML checks files | Zero config |
| Anomaly detection | Paid feature (Soda Cloud) | Free, built-in |
| Self-hosted | Limited | Fully self-hosted |
| Pricing | Free tier + paid | 100% free |

Soda's free tier requires writing checks in YAML. Anomaly detection is paywalled behind Soda Cloud ($$$).

### vs Monte Carlo / Bigeye / Anomalo

| | Monte Carlo | Scherlok |
|---|---|---|
| Deployment | SaaS only | Self-hosted or SaaS |
| Pricing | $50K-200K/year | Free (OSS) |
| Zero-config | Yes | Yes |
| Data leaves your infra | Yes | No (self-hosted) |

Monte Carlo pioneered "data observability" but costs $50K+/year and requires sending metadata to their cloud. Scherlok provides 80% of the value for $0, running entirely in your infrastructure.

### Why Scherlok wins against Monte Carlo

Monte Carlo has more features. That's not the point. Scherlok wins on 3 things Monte Carlo can't fix:

**1. Price — $0 vs $50-200K/year**

95% of companies can't afford Monte Carlo. Startups, mid-market, LATAM/European companies, bootstrapped teams — all priced out. Scherlok is free. The market isn't people who already use Monte Carlo. The market is everyone who **needs** Monte Carlo but will **never pay** for it.

**2. Data sovereignty — self-hosted vs SaaS-only**

Monte Carlo is SaaS only — your metadata goes to their cloud. Healthcare, fintech, government, and any company in regulated industries can't or won't send data outside their infrastructure. Scherlok runs entirely self-hosted. Data never leaves.

**3. Simplicity — 5 minutes vs weeks of onboarding**

Monte Carlo requires onboarding, integration meetings, training, account managers. Scherlok: `pip install scherlok && scherlok investigate` — a single data engineer can set it up in 5 minutes without asking anyone for permission.

**The Metabase parallel:** Metabase doesn't do everything Tableau does. But it's free, self-hosted, and good enough. Today it's valued at $500M. Scherlok is the Metabase of data observability.

### The positioning

**"Monte Carlo for the rest of us."**

Or: **"Why pay $50K/year for data observability when `pip install scherlok` does 80% of the work?"**

Or: **"What if Great Expectations didn't need expectations?"**

## Target Users

### Primary: Data Engineers at startups (10-200 employees)

- Can't afford Monte Carlo ($50K+/year)
- Don't have time to set up Great Expectations properly
- Need something that works TODAY, not next quarter
- Already use dbt, Airflow, or similar tools

### Secondary: Data teams at mid-market companies

- Have some data quality tools but coverage is spotty
- Want automatic anomaly detection without writing rules for everything
- Need to justify ROI to management (Scherlok is free)

### Tertiary: Solo data engineers / consultants

- Managing multiple clients' data stacks
- Need a tool they can deploy quickly at each client
- Want something they can recommend and look good

## Market Size

- **Data Quality market:** $2.3B in 2025, growing to $5.7B by 2030 (CAGR 20%)
- **Data Observability market:** $1.8B in 2025, growing to $4.2B by 2030
- **Open source data tools market:** dbt (valued at $4.2B), Airbyte ($1.5B), Metabase ($500M)
- **Key competitors' funding:** Monte Carlo ($236M), Bigeye ($70M), Anomalo ($66M), Soda ($40M)

The market is massive and growing. Competitors are well-funded but charge enterprise prices. There's a wide-open gap for a free, open-source, zero-config tool.

## Real Use Cases

### Case 1: The Silent API Change

A payment processor silently changes their API to return amounts in cents instead of dollars. Revenue dashboard shows 100x spike. Nobody notices for 3 weeks until the board meeting.

**With Scherlok:** Distribution shift detected on `amount` column within 1 hour. Alert: "Mean value of `payments.amount` increased 100x (from $45.20 to $4,520.00). Check source API."

### Case 2: The Missing Data

A third-party data provider has an outage. Their API returns empty responses. ETL pipeline runs "successfully" with 0 rows.

**With Scherlok:** Volume anomaly detected. Alert: "Table `vendor_data` received 0 rows today (7-day avg: 45,000). Freshness: last real data was 26 hours ago."

### Case 3: The Schema Surprise

Upstream team adds a new column to a shared table. Downstream dbt models break because of SELECT * usage.

**With Scherlok:** Schema drift detected. Alert: "Column `user_status_v2` added to `users` table. 3 downstream tables reference this table via SELECT *."

### Case 4: The Slow Degradation

Data quality degrades gradually — NULL rate in `email` column creeps from 1% to 2% to 5% to 15% over 3 months. Nobody notices because there's no threshold set.

**With Scherlok:** Trend anomaly detected. Alert: "NULL rate in `users.email` has increased 15x over 90 days (1% → 15%). Trend is accelerating."

### Case 5: CI/CD Pipeline Guard

Before deploying a dbt model change, run Scherlok to verify data integrity:

```yaml
# GitHub Actions
- name: Check data quality
  run: |
    scherlok investigate --table modified_tables
    scherlok check --fail-on critical
```

If Scherlok detects a problem, the deploy fails. Data quality as a CI/CD gate.

## Technical Architecture (Proposed)

```
┌─────────────────────────────────────────────┐
│                  scherlok                     │
├──────────┬──────────┬──────────┬────────────┤
│ Connectors│ Profiler │ Detector │  Alerter   │
│          │          │          │            │
│ Postgres │ Volume   │ Z-score  │ Slack      │
│ BigQuery │ Schema   │ IQR      │ Email      │
│ Snowflake│ Distrib  │ Prophet  │ PagerDuty  │
│ Redshift │ Freshness│ Custom   │ Webhook    │
│ MySQL    │ Referent │          │ CI/CD exit │
│ DuckDB   │          │          │            │
└──────────┴──────────┴──────────┴────────────┘
         │              │              │
         ▼              ▼              ▼
    ┌─────────┐   ┌──────────┐  ┌──────────┐
    │ Your DB  │   │ Profile  │  │ Slack /  │
    │ (read    │   │ Store    │  │ Email /  │
    │  only)   │   │ (SQLite) │  │ CI/CD    │
    └─────────┘   └──────────┘  └──────────┘
```

## Language: Python

- Target audience (data engineers) works in Python
- Integration with ecosystem (SQLAlchemy, pandas, dbt)
- Distribution: `pip install scherlok`
- Also: standalone binary via PyInstaller for CI/CD

## Monetization (Future — if it grows)

1. **Scherlok Cloud** (SaaS) — hosted version with dashboard, team management, historical trends. $99-499/month.
2. **Enterprise features** — SSO, audit logs, role-based access. Custom pricing.
3. **Connectors marketplace** — community + premium connectors for exotic data sources.

Core tool stays free forever. This is the dbt/Airbyte model.

## Success Metrics

| Milestone | Target | Timeline |
|-----------|--------|----------|
| Working prototype (Postgres + Slack) | v0.1 | Month 2 |
| BigQuery + Snowflake connectors | v0.3 | Month 4 |
| 100 GitHub stars | Community traction | Month 4 |
| 1,000 GitHub stars | Validation | Month 8 |
| First external contributor | Community | Month 3 |
| Product Hunt launch | Awareness | Month 5 |
| 10,000 stars | Mainstream | Month 12 |
| YC application | Funding option | Month 6 |

## Risks

| Risk | Mitigation |
|------|-----------|
| Monte Carlo releases free tier | Scherlok is self-hosted (data never leaves your infra) |
| dbt adds native anomaly detection | Scherlok is DB-agnostic, not tied to dbt |
| Nobody cares about another data tool | Start with killer zero-config UX, grow from there |
| Too hard to build alone | Start with 1 connector (Postgres), prove concept, then expand |
| No time (3 clients + other projects) | Build MVP in weekends, validate before investing more |

## The Ask

Should we build this? The market is there. The pain is real. The competition is expensive. The gap exists.

What it needs: 2-3 months of part-time work for a working MVP with Postgres + Slack. Then validate with 10 real users before going further.

---

*"The game is afoot." — Sherlock Holmes*
