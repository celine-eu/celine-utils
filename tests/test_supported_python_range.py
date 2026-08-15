"""The supported Python range is declared once, and this holds it to that.

`.github/workflows/test.yaml` builds its matrix from the
``Programming Language :: Python :: 3.x`` classifiers in ``pyproject.toml``, so the
classifiers are the single declaration of what is supported and the workflow is what
makes them true rather than aspirational.

That leaves one thing CI cannot check about itself: whether ``requires-python`` and
the classifiers still agree. They are two statements of one fact, and the failure is
silent in both directions — a floor higher than the classifiers makes CI test a
version nobody can install, and a floor lower than them lets an install succeed on a
version nothing tests.

This package once declared ``>=3.12`` while its core resolved as low as 3.9, and the
floor came in entirely through an extras-only dependency. `celine-superset` ships
inside ``apache/superset:6.0.0`` on Python 3.10 and could not adopt the package at
all.
"""

import re
import sys
from pathlib import Path

import pytest

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"

CLASSIFIER = re.compile(r"^Programming Language :: Python :: 3\.(\d+)$")


def _load_pyproject() -> dict:
    if not PYPROJECT.is_file():
        pytest.skip("running outside a source checkout — no pyproject.toml")
    try:
        import tomllib
    except ImportError:  # Python 3.10 has no tomllib and this package supports it.
        pytest.skip("tomllib is unavailable below Python 3.11")
    return tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))


def _declared_minors() -> list[int]:
    project = _load_pyproject()["project"]
    minors = sorted(
        int(m.group(1))
        for c in project["classifiers"]
        if (m := CLASSIFIER.match(c)) is not None
    )
    if not minors:
        pytest.fail(
            "no `Programming Language :: Python :: 3.x` classifiers — "
            "the CI matrix would be empty"
        )
    return minors


# @verifies REQ-0003
def test_requires_python_floor_matches_the_lowest_classifier():
    """Two statements of one fact must not drift.

    `requires-python` decides what a consumer may install; the classifiers decide
    what CI tests. A gap between them is a version that is either untested or
    uninstallable, and nothing else reports it.
    """
    project = _load_pyproject()["project"]
    requires = project["requires-python"]

    match = re.match(r">=\s*3\.(\d+)$", requires.strip())
    assert match, (
        f"requires-python is {requires!r}; this test understands `>=3.x` and needs "
        "updating if the constraint form changed deliberately"
    )

    floor = int(match.group(1))
    assert floor == _declared_minors()[0], (
        f"requires-python declares >=3.{floor} but the lowest tested classifier is "
        f"3.{_declared_minors()[0]}"
    )


# @verifies REQ-0003
def test_supported_versions_are_contiguous():
    """A gap in the matrix is a version that installs and is never tested."""
    minors = _declared_minors()
    assert minors == list(range(minors[0], minors[-1] + 1)), (
        f"classifiers declare 3.{minors} — a missing version between the floor and "
        "the ceiling is installable and untested"
    )


# @verifies REQ-0003
def test_the_running_interpreter_is_one_this_package_claims():
    """If the suite passes on a version the package does not claim, the claim is
    what is wrong — the evidence is being produced somewhere the declaration does
    not cover."""
    minors = _declared_minors()
    assert sys.version_info[0] == 3
    assert minors[0] <= sys.version_info[1] <= minors[-1], (
        f"running on 3.{sys.version_info[1]}, which is outside the declared range "
        f"3.{minors[0]}–3.{minors[-1]}"
    )
