# Security Policy

## Supported versions

Scherlok ships from a single `main` branch; only the latest release receives security fixes.

| Version | Supported          |
|---------|--------------------|
| 0.5.x   | ✅                 |
| < 0.5.0 | ❌ (please upgrade) |

## Reporting a vulnerability

**Do not open a public GitHub issue for security reports.** Public reports for unpatched issues give attackers a head start.

Send security reports privately to:

📧 **rbmuller91@gmail.com**

Please include:

- A brief description of the issue and the impact you observed
- Steps to reproduce, or a minimal proof-of-concept
- The Scherlok version and connector adapter (Postgres / BigQuery / Snowflake) where you saw it
- Whether the issue affects user data, credentials, or only the local profile store

You'll get a confirmation reply within 72 hours. Most reports are resolved within 7 days; complex ones may take longer and we'll keep you posted.

## Scope

Scherlok is a read-only CLI that connects to databases the user controls. The most relevant security boundaries are:

- **Connection strings stored in `~/.scherlok/config.json`** — these can include passwords. We mask passwords on display (dashboard, error messages) but the file on disk is plaintext. Treat it like a `~/.pgpass`.
- **SQL execution.** All SQL is parameterized via the underlying drivers (psycopg2, google-cloud-bigquery, snowflake-connector-python). String concatenation in the few unavoidable cases (table names) goes through identifier quoting; if you find a path that doesn't, that's a security report.
- **The HTML dashboard renders user-controlled values** (table names, column names, anomaly messages). Jinja2 autoescape is on. If you find a way to inject HTML/JS through manifest data, that's a security report.
- **Optional remote stores** (S3 / GCS / Azure Blob) use the cloud provider's standard SDK auth. Scherlok does not handle the credentials directly.

Out of scope:

- Vulnerabilities in dependencies (report to the dependency)
- Self-XSS in your own profile store viewed in your own browser
- Issues that require privileged access to the host running Scherlok

## Disclosure timeline

We follow a coordinated-disclosure approach. Once a fix is ready, we publish a CVE-style advisory in the GitHub Security tab, release a patched version on PyPI, and credit the reporter (unless you ask to remain anonymous).
