"""Overlaying a dataset's governance block onto the file's defaults.

The rule is narrow and was violated for `dcat`: a dataset stating *one* DCAT
field must keep the file's defaults for the rest. Whole-object replacement made

    defaults:
      dcat:
        themes: [.../data-theme/ENER]
    sources:
      datasets.x:
        dcat:
          conforms_to: http://www.w3.org/ns/sosa/

mean *"and no themes"*, silently — the defaults still in the file, looking like
they applied. It survived because `conforms_to` is the first field anybody set
on its own; a governance file that restates the whole block per dataset never
sees it.

The same defect existed in the two other copies of this parser (`dataset-api`
`cli/export_governance.py`, `ds` `libs/governance`) and was fixed in all three
together.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

from celine.governance import GovernanceResolver

THEME = "http://publications.europa.eu/resource/authority/data-theme/ENER"
SOSA = "http://www.w3.org/ns/sosa/"


def _resolve(tmp_path: Path, body: str, dataset: str):
    path = tmp_path / "governance.yaml"
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    return GovernanceResolver.from_file(path).resolve(dataset)


def test_declaring_conforms_to_keeps_the_default_dcat_metadata(tmp_path: Path):
    rule = _resolve(
        tmp_path,
        f"""
        defaults:
          access_level: internal
          dcat:
            themes: [{THEME}]
            accrual_periodicity: http://example.org/freq/IRREG
        sources:
          datasets.gold.measurements:
            dcat:
              conforms_to: {SOSA}
        """,
        "datasets.gold.measurements",
    )

    assert rule.dcat is not None
    assert rule.dcat.conforms_to == SOSA
    # The half that regressed: stating one field must not erase the others.
    assert rule.dcat.themes == [THEME]
    assert rule.dcat.accrual_periodicity == "http://example.org/freq/IRREG"


def test_a_dataset_overrides_the_default_it_restates(tmp_path: Path):
    """Merging is not union — a value the dataset states still wins."""
    other = "http://publications.europa.eu/resource/authority/data-theme/ENVI"
    rule = _resolve(
        tmp_path,
        f"""
        defaults:
          dcat:
            themes: [{THEME}]
            conforms_to: {SOSA}
        sources:
          datasets.gold.grid:
            dcat:
              themes: [{other}]
              conforms_to: http://iec.ch/TC57/CIM100#
        """,
        "datasets.gold.grid",
    )

    assert rule.dcat.themes == [other]
    assert rule.dcat.conforms_to == "http://iec.ch/TC57/CIM100#"


def test_an_explicit_null_overrides_an_inherited_default(tmp_path: Path):
    """"Silent" and "said no" are different claims and merge differently.

    A dataset writing `conforms_to: null` states it has **no** payload model —
    the distinction the semantic seam is built on, and the reason the catalogue
    emits `dct:conformsTo` absent rather than null. A truthiness-based overlay
    cannot see it: `None or <default>` inherits, so the dataset silently
    advertises a model it explicitly disclaimed.
    """
    rule = _resolve(
        tmp_path,
        f"""
        defaults:
          dcat:
            themes: [{THEME}]
            conforms_to: {SOSA}
        sources:
          datasets.gold.unmodelled:
            dcat:
              conforms_to: null
        """,
        "datasets.gold.unmodelled",
    )

    assert rule.dcat.conforms_to is None
    # ...while everything it stayed silent about is still inherited.
    assert rule.dcat.themes == [THEME]


def test_defaults_apply_to_a_dataset_that_declares_no_dcat(tmp_path: Path):
    rule = _resolve(
        tmp_path,
        f"""
        defaults:
          dcat:
            themes: [{THEME}]
        sources:
          datasets.gold.plain:
            tags: [gold]
        """,
        "datasets.gold.plain",
    )

    assert rule.dcat is not None
    assert rule.dcat.themes == [THEME]
    assert rule.dcat.conforms_to is None


def test_ontology_is_replaced_whole_not_merged(tmp_path: Path):
    """`spec` and `spec_file` are alternatives, so field-wise overlay is wrong.

    Merging them the way `dcat` is merged would let a default `spec` survive
    beside a dataset's `spec_file` and produce a rule declaring both — which the
    schema forbids and the mapping resolver rejects with "two answers to what one
    column means". The exclusivity is the reason this one field stays a straight
    replacement.
    """
    rule = _resolve(
        tmp_path,
        """
        defaults:
          ontology:
            spec: obs_energy_measurement
        sources:
          datasets.gold.bespoke:
            ontology:
              spec_file: ./mappings/custom.yaml
        """,
        "datasets.gold.bespoke",
    )

    assert rule.ontology.spec_file == "./mappings/custom.yaml"
    assert rule.ontology.spec is None
