# Alerts

`scherlok watch`, `ci`, `check`, `dbt` and `dbt-run-and-watch` all take the same alert flags.

## Webhooks

```bash
scherlok watch --webhook https://hooks.slack.com/services/...     # Slack
scherlok watch --webhook https://discord.com/api/webhooks/...     # Discord
scherlok watch --webhook https://outlook.office.com/webhook/...   # Microsoft Teams
scherlok watch --webhook https://my-api.com/alerts                # anything else: generic JSON
```

Slack, Discord and Teams are detected from the URL and get a formatted payload; any other URL receives a generic JSON document with the anomaly list.

## E-mail

```bash
export SCHERLOK_SMTP_HOST=smtp.gmail.com
export SCHERLOK_SMTP_USER=alerts@company.com
export SCHERLOK_SMTP_PASSWORD=app-specific-password

scherlok watch --email team@company.com --email cto@company.com
```

Multipart HTML and plain text.

## Exit codes

```bash
scherlok watch --exit-code --fail-on critical
```

Exit 1 when an anomaly at or above the chosen severity fired; see [CI/CD gate](ci-cd.md).

## Add a root-cause hypothesis

`--explain` sends the anomaly batch (never rows or credentials) to Claude and injects a short hypothesis into the same alert. Opt-in; see [AI-explained alerts](explain.md).

## Alert history

Every fired anomaly is persisted next to the profiles. `scherlok history --days 30` lists them; the [HTML dashboard](dashboard.md) groups them per table.
