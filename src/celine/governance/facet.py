"""Project a :class:`GovernanceRule` onto the OpenLineage governance facet.

One function, because there were four hand-written copies of this projection:
three in ``celine-utils``' lineage extractors and one in ``dataset-api``'s
governance exporter — the last built as a raw ``dict`` with camelCase keys typed
by eye, matching a class in a package it does not depend on.

Returns a plain ``dict`` and imports nothing from OpenLineage, so a consumer gets
the projection without the dependency.
"""

from __future__ import annotations

from typing import Any, Dict

from celine.governance.models import GovernanceRule

#: Published location of the facet's JSON Schema.
#:
#: **This string is a contract, not a configuration.** It is embedded in every
#: OpenLineage event already emitted and sitting in Marquez, so changing it does
#: not break a build — it silently invalidates historical lineage. The file may
#: move between repositories; this URL may not move with it.
SCHEMA_URL = "https://celine-eu.github.io/schema/GovernanceDatasetFacet.schema.json"


def build_facet(
    rule: GovernanceRule,
    producer: str,
    *,
    include_dataspace: bool = True,
) -> Dict[str, Any]:
    """Build the ``governance`` dataset facet for ``rule``.

    Absent values are **omitted**, never emitted as ``null``. A facet that says
    ``"license": null`` claims the dataset has no license; one that omits the key
    says nothing about it, and only the second is what silence means.

    ``include_dataspace`` exists because the two pre-existing implementations
    disagreed, and the disagreement is worth keeping visible rather than
    resolving by accident:

    - ``dataset-api``'s exporter **projects** ``dataspace`` into the facet
      (``medallion``, ``contractRequired``, ``consentRequired``, ``odrlAction``,
      ``purpose``), because its DCAT formatter reads them back out.
    - the lineage extractors **do not**, so events already in Marquez carry no
      dataspace fields.

    Defaulting to ``True`` matches the catalogue, which is the consumer that
    reads these fields. A lineage extractor adopting this function should pass
    ``False`` to keep emitting what it emits today; widening it is a deliberate
    change to the lineage payload, not a side effect of deduplication.
    """
    facet: Dict[str, Any] = {"_producer": producer, "_schemaURL": SCHEMA_URL}

    if rule.title:
        facet["title"] = rule.title
    if rule.description:
        facet["description"] = rule.description
    if rule.license:
        facet["license"] = rule.license
    if rule.attribution:
        facet["attribution"] = rule.attribution
    if rule.ownership:
        facet["owners"] = [o.name for o in rule.ownership]
    if rule.access_level:
        facet["accessLevel"] = rule.access_level
    if rule.access_requirements:
        facet["accessRequirements"] = rule.access_requirements
    if rule.classification:
        facet["classification"] = rule.classification
    if rule.tags:
        facet["tags"] = rule.tags
    if rule.retention_days is not None:
        facet["retentionDays"] = rule.retention_days
    if rule.documentation_url:
        facet["documentationUrl"] = rule.documentation_url
    if rule.source_system:
        facet["sourceSystem"] = rule.source_system
    if rule.row_filters:
        facet["rowFilters"] = rule.row_filters

    if include_dataspace and rule.dataspace:
        ds = rule.dataspace
        if ds.medallion:
            facet["medallion"] = ds.medallion
        if ds.contract_required:
            facet["contractRequired"] = True
        if ds.consent_required:
            facet["consentRequired"] = True
        if ds.odrl_action != "use":
            facet["odrlAction"] = ds.odrl_action
        if ds.purpose:
            facet["purpose"] = ds.purpose

    return facet


def is_empty(rule: GovernanceRule) -> bool:
    """True when a rule carries nothing worth emitting a facet for.

    Mirrors the guard the dbt extractor already applied inline, so the "should we
    emit at all" decision stops being restated per extractor.
    """
    return not any(
        (
            rule.title,
            rule.description,
            rule.license,
            rule.attribution,
            rule.ownership,
            rule.access_level,
            rule.access_requirements,
            rule.classification,
            rule.tags,
            rule.retention_days is not None,
            rule.documentation_url,
            rule.source_system,
            rule.row_filters,
        )
    )
