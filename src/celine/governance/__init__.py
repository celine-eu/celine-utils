"""Canonical parsing, merging and validation of ``governance.yaml``.

This is the one implementation. Before it there were four — in ``celine-utils``,
``dataset-api``, ``ds`` and ``celine-superset`` — because ``celine-utils`` pulled
dbt, Meltano, Prefect and Keycloak in order to parse a YAML file, and no API
service could take that dependency. The copies were dependency avoidance, not
carelessness, and they disagreed about what the same file meant.

**Runtime dependencies are ``pydantic``, ``pyyaml`` and ``jsonschema``. Nothing
else, ever.** ``celine.governance`` must not import ``celine.utils``, sqlalchemy,
OpenLineage or anything from an optional extra; that constraint is the reason
this package can be shared, and a CI job asserts it rather than trusting review.

The package is shipped from the ``celine-utils`` wheel today and is expected to
become its own distribution. The import path ``celine.governance`` is chosen so
that move is a directory rename and no consumer edits an import.

Usage::

    from celine.governance import GovernanceResolver, validate_file

    validate_file(Path("governance.yaml"))
    rule = GovernanceResolver.from_file(Path("governance.yaml")).resolve(
        "datasets.ds_dev_gold.grid_substations"
    )
    rule.dataspace.expose  # -> True
"""

from celine.governance.facet import SCHEMA_URL, build_facet, is_empty
from celine.governance.levels import (
    AccessRequirement,
    DataClassification,
    GovernanceAccessLevel,
    normalize_access_level,
    normalize_classification,
)
from celine.governance.merge import merge_dataspace, merge_models, merge_rules
from celine.governance.models import (
    KNOWN_KEYS,
    DataspaceConfig,
    DcatConfig,
    GovernanceConfig,
    GovernanceOwner,
    GovernanceRule,
    OntologyConfig,
    TemporalCoverage,
)
from celine.governance.owners import OwnerEntry, OwnersRegistry, load_owners_yaml
from celine.governance.resolver import GovernanceResolver, parse_rule
from celine.governance.validation import (
    GovernanceValidationError,
    load_schema,
    schema_errors,
    unknown_keys,
    validate,
    validate_file,
)

__all__ = [
    # models
    "GovernanceRule",
    "GovernanceConfig",
    "GovernanceOwner",
    "DcatConfig",
    "OntologyConfig",
    "DataspaceConfig",
    "TemporalCoverage",
    "KNOWN_KEYS",
    # resolving
    "GovernanceResolver",
    "parse_rule",
    # merging
    "merge_rules",
    "merge_models",
    "merge_dataspace",
    # validation
    "validate",
    "validate_file",
    "schema_errors",
    "unknown_keys",
    "load_schema",
    "GovernanceValidationError",
    # owners
    "OwnersRegistry",
    "OwnerEntry",
    "load_owners_yaml",
    # levels
    "AccessRequirement",
    "GovernanceAccessLevel",
    "DataClassification",
    "normalize_access_level",
    "normalize_classification",
    # facet
    "build_facet",
    "is_empty",
    "SCHEMA_URL",
]
