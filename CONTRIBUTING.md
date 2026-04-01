# Contributing to Scherlok

Thanks for your interest in contributing to Scherlok!

## Development Setup

```bash
# Clone the repo
git clone https://github.com/rbmuller/scherlok.git
cd scherlok

# Create virtual environment
python -m venv .venv
source .venv/bin/activate

# Install in development mode
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run linter
ruff check src/ tests/
```

## Code Standards

- Python 3.10+ with type hints on all function signatures
- Docstrings on all public functions
- Use `pathlib` for file paths
- Use `rich` for terminal output
- Follow conventional commits for commit messages

## Adding a New Connector

1. Create a new module in `src/scherlok/connectors/`
2. Extend `BaseConnector` from `connectors/base.py`
3. Register the scheme in `connectors/__init__.py`
4. Add tests in `tests/test_connector.py`

## Adding a New Alerter

1. Create a new module in `src/scherlok/alerter/`
2. Follow the pattern from `alerter/slack.py` or `alerter/console.py`
3. Add tests

## Pull Requests

- Create a branch from `main`
- Write tests for new functionality
- Make sure `pytest` and `ruff check` pass
- Open a PR with a clear description
