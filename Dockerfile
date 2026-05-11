# syntax=docker/dockerfile:1.7

# Stage 1: builder
# Installs scherlok with all source-warehouse extras (dbt, bigquery, snowflake)
# into a virtual environment. Building in a separate stage keeps build-time
# dependencies (compilers, dev headers) out of the runtime image.
FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /build

# Copy only the metadata first so the dependency install layer caches when
# only source files change.
COPY pyproject.toml README.md ./
COPY src ./src

# Install into a self-contained venv we can copy wholesale into the runtime
# stage. Includes every optional dependency group so users do not have to
# rebuild for a different warehouse.
RUN python -m venv /opt/venv \
    && /opt/venv/bin/pip install --upgrade pip \
    && /opt/venv/bin/pip install ".[bigquery,snowflake,dbt]"

# Stage 2: runtime
# Slim Python base + the installed venv. No build toolchain, no source tree.
FROM python:3.12-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Copy the prepared virtual environment from the builder.
COPY --from=builder /opt/venv /opt/venv

# Run as a non-root user. Most CI runners (GitHub Actions, GitLab CI) honor
# the image's USER directive when the user has not been overridden.
RUN groupadd --system --gid 1000 scherlok \
    && useradd --system --uid 1000 --gid scherlok --shell /bin/bash --create-home scherlok
USER scherlok
WORKDIR /home/scherlok

ENTRYPOINT ["scherlok"]
CMD ["--help"]
