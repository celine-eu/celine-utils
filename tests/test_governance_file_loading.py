"""Which absences are answers and which are mistakes.

`auto_discover` finding nothing is an answer: discovery by convention that finds
nothing has found nothing, and an empty config states that truthfully. `from_file`
finding nothing is a mistake: a caller handing over a path has already said which
file it means.

`from_file` used to answer both the same way — a warning in a log nobody reads and
an empty config. `ds` renamed a `governance/` directory without updating the
connector's default path, and the provider then started clean, served an empty
dataset list and empty sharing offers, and said nothing at all while the files sat
on disk. The first symptom was an unrelated end-to-end test failing on a fixture
that was right there in the file.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from celine.governance import GovernanceResolver, load_pipelines

BASE = """
defaults:
  expose: true
sources:
  db.schema.t: {}
"""


def write(path: Path, body: str = BASE) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)
    return path


def test_from_file_raises_on_a_path_that_is_not_there(tmp_path: Path):
    missing = tmp_path / "governance.yaml"

    with pytest.raises(FileNotFoundError, match=str(missing)):
        GovernanceResolver.from_file(missing)


def test_from_file_raises_on_a_directory(tmp_path: Path):
    """The near-miss that produced the outage: the directory, not the file in it."""
    directory = tmp_path / "governance"
    directory.mkdir()

    with pytest.raises(FileNotFoundError):
        GovernanceResolver.from_file(directory)


def test_from_file_still_loads_a_file_that_is_there(tmp_path: Path):
    path = write(tmp_path / "governance.yaml")

    assert GovernanceResolver.from_file(path).resolve("db.schema.t").expose is True


def test_a_missing_overlay_is_not_a_missing_file(tmp_path: Path):
    """The overlay is found by convention, so its absence is "nothing was asked for"."""
    path = write(tmp_path / "governance.yaml")

    resolver = GovernanceResolver.from_file_with_override(path, overlay_name="nowhere")

    assert resolver.resolve("db.schema.t").expose is True


def test_a_missing_base_is_a_missing_file(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        GovernanceResolver.from_file_with_override(
            tmp_path / "governance.yaml", overlay_name="nowhere"
        )


def test_auto_discover_keeps_its_empty_fallback(monkeypatch, tmp_path: Path):
    """Step 4 is unchanged, and is the reason `from_file` can afford to raise."""
    monkeypatch.delenv("GOVERNANCE_CONFIG_PATH", raising=False)
    monkeypatch.setenv("PIPELINES_ROOT", str(tmp_path))

    resolver = GovernanceResolver.auto_discover(app_name="nothing-here")

    assert resolver.config.sources == {}
    assert resolver.config.depends_on is None


def test_auto_discover_does_not_raise_on_a_configured_path_that_is_gone(
    monkeypatch, tmp_path: Path
):
    """It checks `is_file()` first and warns, which is its documented step 1."""
    monkeypatch.setenv("GOVERNANCE_CONFIG_PATH", str(tmp_path / "gone.yaml"))
    monkeypatch.delenv("PIPELINES_ROOT", raising=False)

    assert GovernanceResolver.auto_discover().config.sources == {}


def test_load_pipelines_propagates_rather_than_surveying_one_short(tmp_path: Path):
    """`load_pipelines` is fed by `discover`, which globs — the files exist.

    A caller handing it a path that is not there is making exactly the mistake
    `from_file` now names, and a dependency graph silently one pipeline short is
    the same quiet wrong answer in a different shape.
    """
    present = write(tmp_path / "app" / "governance.yaml")

    assert len(load_pipelines([present])) == 1

    with pytest.raises(FileNotFoundError):
        load_pipelines([present, tmp_path / "gone" / "governance.yaml"])
