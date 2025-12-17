from enum import Enum


class AccessRequirement(str, Enum):
    ALL = "all"  # no precondition
    PARTNER = "partner"  # ecosystem participant
    CONTRACT = "contract"  # explicit legal agreement


# Who is allowed to see this dataset?
class GovernanceAccessLevel(str, Enum):
    OPEN = "open"  # publicly shareable
    INTERNAL = "internal"  # org-internal only
    RESTRICTED = "restricted"  # explicit authorization required


# How sensitive is this data?
class DataClassification(str, Enum):
    GREEN = "green"  # non-sensitive
    YELLOW = "yellow"  # potentially sensitive
    RED = "red"  # sensitive / regulated
    PII = "pii"  # personal data (special handling)


def normalize_classification(value: str | None) -> str | None:
    if value is None:
        return None
    return DataClassification(value.lower()).value


def normalize_access_level(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.lower()
    if value == "secret":
        return GovernanceAccessLevel.RESTRICTED.value
    return GovernanceAccessLevel(value).value
