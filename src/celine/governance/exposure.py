"""Which channel a dataset is exposed through — resolved in one place.

There are two questions, and until this module they shared one boolean:

``expose``
    Is the dataset listed in the catalogue and served by the API? Gates
    ``/catalogue*`` and ``/query``.
``dataspace.expose``
    Is it *offered into the dataspace*? Gates requests arriving with EDR
    context — a negotiated contract, a third party, a different legal footing.

They were conflated because only the second existed, and the exporter copied it
onto the catalogue flag. So a dataset that had to appear in the catalogue was
thereby offered into the dataspace, and one withheld from the dataspace was also
unqueryable through the API. There was no third option, and the live case is
`grid_substations`: grid topology carries `dataspace.expose: true` not because
anyone decided it belongs in a dataspace, but because that was the only way the
dashboard could see it.

**The two gates are AND, not OR.** Dataspace access requires both. A dataset
absent from the catalogue cannot be discovered, negotiated or described, so
``expose: false`` with ``dataspace.expose: true`` is not a narrower grant — it is
a contradiction, and :func:`exposure_conflict` reports it rather than letting it
resolve silently. Silently picking a direction there is a security-relevant
surprise whichever way it goes.
"""

from __future__ import annotations

from typing import Optional

from celine.governance.models import GovernanceRule


def effective_expose(rule: GovernanceRule) -> bool:
    """Is the dataset listed in the catalogue and served by the API?

    Falls back to ``dataspace.expose`` when ``expose`` is unstated, which is what
    makes this change deployable before the governance files are migrated: every
    file written against the old grammar keeps its current catalogue behaviour
    exactly. Once a file states ``expose`` the fallback stops applying to it.
    """
    if rule.expose is not None:
        return rule.expose
    return bool(rule.dataspace and rule.dataspace.expose)


def dataspace_expose(rule: GovernanceRule) -> bool:
    """Is the dataset offered into the dataspace?

    No fallback in either direction. This reads only what the ``dataspace`` block
    says, so a dataset is offered only where something actually said so — and
    absent a ``dataspace`` block the answer is no.
    """
    return bool(rule.dataspace and rule.dataspace.expose)


def exposure_conflict(rule: GovernanceRule) -> Optional[str]:
    """Return why the two gates contradict each other, or ``None``.

    The only contradiction is *offered but unlisted*. A consumer reaches a
    dataspace asset through the catalogue entry that describes it, so offering
    something that is not listed cannot be honoured — and resolving it either way
    without saying so is worse than refusing: granting would publish data the
    catalogue never advertised, withholding would silently drop an offer someone
    deliberately made.

    Reported rather than raised so a caller can collect every conflict in a run
    instead of failing on the first.
    """
    if rule.expose is False and dataspace_expose(rule):
        return (
            "dataspace.expose is true but expose is false — a dataset that is not "
            "in the catalogue cannot be discovered or negotiated in the dataspace. "
            "Set expose: true to offer it, or dataspace.expose: false to withdraw it."
        )
    return None
