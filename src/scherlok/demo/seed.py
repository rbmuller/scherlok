"""Deterministic sample warehouse for `scherlok demo`.

Three tables shaped like a small SaaS backend, generated from a fixed random
seed so every run (and every test) sees identical data, plus the "bad deploy"
mutations the demo asks Scherlok to catch. DuckDB is imported lazily so the
rest of the package never depends on it.
"""

from __future__ import annotations

import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 1907
DATABASE_FILENAME = "demo.duckdb"

TABLE_USERS = "users"
TABLE_ORDERS = "orders"
TABLE_PRODUCTS = "products"

USERS_ROWS = 2_000
ORDERS_ROWS = 20_000
PRODUCTS_ROWS = 500

# Every timestamp is derived from this anchor so the data is reproducible.
ANCHOR = datetime(2026, 9, 1, 12, 0, 0)
USERS_HISTORY_DAYS = 90
ORDERS_HISTORY_DAYS = 30

# Rows are generated in Python (deterministic, version-independent) and loaded
# through a temporary CSV + COPY: `executemany` took ~17 s for this dataset and
# multi-row VALUES with thousands of placeholders ~10 s; COPY loads it in < 0.2 s.
CSV_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

PLANS = ("free", "starter", "pro", "business", "enterprise")
PLAN_WEIGHTS = (55, 20, 15, 7, 3)
COUNTRIES = ("US", "BR", "DE", "GB", "FR", "CA", "AU", "IN", "JP", "MX")
ORDER_STATUSES = ("pending", "paid", "shipped", "delivered", "refunded")
ORDER_STATUS_WEIGHTS = (10, 30, 20, 35, 5)
CATEGORIES = ("books", "electronics", "home", "garden", "toys", "sports", "beauty", "grocery")
FIRST_NAMES = (
    "Alice", "Bruno", "Carla", "Diego", "Elena", "Farid", "Grace", "Hiro", "Iris", "Joao",
)
LAST_NAMES = ("Silva", "Müller", "Okafor", "Rossi", "Tanaka", "Nguyen", "Kowalski", "Haddad")

# The bad deploy. Each fraction is chosen to trip one detector at a known
# severity against the fixed (cold-start) thresholds.
ORDERS_DROP_MODULUS = 5
ORDERS_DROP_REMAINDERS = (0, 1, 2)   # 3/5 of orders vanish -> volume_drop CRITICAL (>= 50%)
EMAIL_NULL_MODULUS = 5               # 1/5 of e-mails nulled -> null_rate_change WARNING (>= 10pp)
PLAN_FREE_TEXT_MODULUS = 10
PLAN_FREE_TEXT_BELOW = 3             # 3/10 of plans become free text -> cardinality CRITICAL
DROPPED_COLUMN = "category"          # products loses a column -> column_removed CRITICAL


def database_url(path: Path) -> str:
    """Connection string for a DuckDB file, in the form the connector expects."""
    return f"duckdb://{path.resolve()}"


def _csv_value(value):
    if isinstance(value, datetime):
        return value.strftime(CSV_TIMESTAMP_FORMAT)
    if isinstance(value, date):
        return value.isoformat()
    return value


def _load_rows(conn, table: str, rows: list[tuple], csv_path: Path) -> None:
    """Load ``rows`` into ``table`` through a temporary CSV and DuckDB's COPY."""
    with csv_path.open("w", newline="") as fh:
        writer = csv.writer(fh)
        for row in rows:
            writer.writerow(
                _csv_value(value) for value in row
            )
    try:
        conn.execute(f"COPY {table} FROM '{csv_path}' (FORMAT CSV, HEADER FALSE)")
    finally:
        csv_path.unlink(missing_ok=True)


def _connect(path: Path):
    import duckdb  # lazy: optional dependency

    return duckdb.connect(str(path))


def _users(rng: random.Random) -> list[tuple]:
    rows = []
    for user_id in range(1, USERS_ROWS + 1):
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        created = ANCHOR - timedelta(
            days=rng.uniform(0, USERS_HISTORY_DAYS), seconds=rng.randint(0, 86_399)
        )
        rows.append(
            (
                user_id,
                f"{first} {last}",
                f"{first.lower()}.{last.lower()}.{user_id}@example.com",
                rng.choices(PLANS, weights=PLAN_WEIGHTS)[0],
                rng.choice(COUNTRIES),
                created,
            )
        )
    return rows


def _orders(rng: random.Random, products: list[tuple]) -> list[tuple]:
    """Fact rows: (user_id, product_id, amount, status, order_date).

    Deliberately no surrogate key or timestamp: every column is low-cardinality
    relative to the row count, so the bad deploy's volume drop shows up as one
    anomaly instead of also dragging the distinct counts of unique columns.
    """
    rows = []
    for _ in range(ORDERS_ROWS):
        product = rng.choice(products)
        order_date = (ANCHOR - timedelta(days=rng.randint(0, ORDERS_HISTORY_DAYS - 1))).date()
        rows.append(
            (
                rng.randint(1, USERS_ROWS),
                product[0],
                product[3],
                rng.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS)[0],
                order_date,
            )
        )
    return rows


def _products(rng: random.Random) -> list[tuple]:
    rows = []
    for product_id in range(1, PRODUCTS_ROWS + 1):
        category = rng.choice(CATEGORIES)
        rows.append(
            (
                product_id,
                f"{category.title()} item {product_id}",
                category,
                round(rng.uniform(5, 500), 2),
                rng.randint(0, 1_000),
                ANCHOR - timedelta(hours=rng.uniform(0, 72)),
            )
        )
    return rows


def seed_database(path: Path) -> dict[str, int]:
    """Create the three demo tables at ``path``. Returns row counts per table."""
    rng = random.Random(SEED)
    users, products = _users(rng), _products(rng)
    orders = _orders(rng, products)
    conn = _connect(path)
    try:
        conn.execute(
            f"CREATE TABLE {TABLE_USERS} (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, "
            "email VARCHAR, plan VARCHAR NOT NULL, country VARCHAR, created_at TIMESTAMP NOT NULL)"
        )
        conn.execute(
            f"CREATE TABLE {TABLE_ORDERS} (user_id INTEGER NOT NULL, product_id INTEGER NOT NULL, "
            "amount DECIMAL(10, 2) NOT NULL, status VARCHAR NOT NULL, order_date DATE NOT NULL)"
        )
        conn.execute(
            f"CREATE TABLE {TABLE_PRODUCTS} (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, "
            f"{DROPPED_COLUMN} VARCHAR, price DECIMAL(10, 2) NOT NULL, stock INTEGER NOT NULL, "
            "updated_at TIMESTAMP NOT NULL)"
        )
        tables = ((TABLE_USERS, users), (TABLE_ORDERS, orders), (TABLE_PRODUCTS, products))
        for table, rows in tables:
            _load_rows(conn, table, rows, path.with_name(f"{table}.csv"))
    finally:
        conn.close()
    return {TABLE_USERS: len(users), TABLE_ORDERS: len(orders), TABLE_PRODUCTS: len(products)}


def apply_bad_deploy(path: Path) -> None:
    """Mutate the seeded database the way a broken 2 AM release would."""
    conn = _connect(path)
    try:
        remainders = ", ".join(str(r) for r in ORDERS_DROP_REMAINDERS)
        conn.execute(
            f"DELETE FROM {TABLE_ORDERS} WHERE rowid % {ORDERS_DROP_MODULUS} IN ({remainders})"
        )
        conn.execute(
            f"UPDATE {TABLE_USERS} SET email = NULL WHERE id % {EMAIL_NULL_MODULUS} = 0"
        )
        conn.execute(
            f"UPDATE {TABLE_USERS} SET plan = 'plan-' || CAST(id AS VARCHAR) "
            f"WHERE id % {PLAN_FREE_TEXT_MODULUS} < {PLAN_FREE_TEXT_BELOW}"
        )
        conn.execute(f"ALTER TABLE {TABLE_PRODUCTS} DROP COLUMN {DROPPED_COLUMN}")
    finally:
        conn.close()
