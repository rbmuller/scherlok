"""Verbosity-aware print helpers.

Layered on top of Rich's Console. Set verbosity once via `set_verbosity()`,
then call `info()` / `verbose_info()` / `error()` from anywhere in the CLI.

- `info(msg)` — printed unless `--quiet`. Default user-facing output.
- `verbose_info(msg)` — printed only when `--verbose`. Diagnostics, timings.
- `error(msg)` — always printed, regardless of `--quiet`. Failures and warnings.
"""

from rich.console import Console

_console = Console()


class _Verbosity:
    verbose: bool = False
    quiet: bool = False


def set_verbosity(verbose: bool = False, quiet: bool = False) -> None:
    """Configure the global verbosity. Mutually exclusive flags.

    `--verbose` wins over `--quiet` if somehow both are set.
    """
    _Verbosity.verbose = verbose
    _Verbosity.quiet = quiet and not verbose


def is_verbose() -> bool:
    return _Verbosity.verbose


def is_quiet() -> bool:
    return _Verbosity.quiet


def info(msg: str) -> None:
    """Default user-facing message. Suppressed by `--quiet`."""
    if _Verbosity.quiet:
        return
    _console.print(msg)


def verbose_info(msg: str) -> None:
    """Diagnostic detail. Visible only with `--verbose`."""
    if _Verbosity.verbose:
        _console.print(f"[dim]· {msg}[/dim]")


def error(msg: str) -> None:
    """Error/warning message. Always printed, even with `--quiet`."""
    _console.print(msg)


def get_console() -> Console:
    """Return the underlying Rich Console (for advanced cases like Tables)."""
    return _console
