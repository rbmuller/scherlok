"""Tests for verbosity helpers in scherlok.output."""

from io import StringIO

from rich.console import Console

import scherlok.output as out


def _capture():
    """Replace the module-level console with one that writes to StringIO."""
    buf = StringIO()
    out._console = Console(file=buf, force_terminal=False, width=200)
    return buf


def setup_function():
    """Reset verbosity between tests."""
    out.set_verbosity(verbose=False, quiet=False)


def test_info_visible_by_default():
    buf = _capture()
    out.info("hello")
    assert "hello" in buf.getvalue()


def test_info_hidden_with_quiet():
    out.set_verbosity(quiet=True)
    buf = _capture()
    out.info("hello")
    assert buf.getvalue() == ""


def test_verbose_info_hidden_by_default():
    buf = _capture()
    out.verbose_info("only-when-verbose")
    assert "only-when-verbose" not in buf.getvalue()


def test_verbose_info_visible_with_verbose():
    out.set_verbosity(verbose=True)
    buf = _capture()
    out.verbose_info("only-when-verbose")
    assert "only-when-verbose" in buf.getvalue()


def test_error_visible_even_with_quiet():
    """Errors must always print, even under --quiet."""
    out.set_verbosity(quiet=True)
    buf = _capture()
    out.error("[red]boom[/red]")
    assert "boom" in buf.getvalue()


def test_verbose_overrides_quiet_when_both_set():
    """--verbose wins over --quiet to keep the user's intent unambiguous."""
    out.set_verbosity(verbose=True, quiet=True)
    assert out.is_verbose()
    assert not out.is_quiet()
