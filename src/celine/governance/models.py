"""Canonical governance models.

The grammar of ``governance.yaml``, in one place. Four independently-written
parsers existed before this module — in ``celine-utils``, ``dataset-api``, ``ds``
and ``celine-superset`` — and they disagreed. The models here are the union of
what all four read, so adopting this module cannot silently drop a field only one
of them knew about.

**Nothing in ``celine.governance`` may import ``celine.utils``.** The whole point
of this package is that a consumer can parse governance without installing dbt,
Meltano, Prefect or Keycloak; the moment a convenience import creeps back the
consumers fork again. Its runtime dependencies are ``pydantic`` and ``pyyaml``
(plus ``jsonschema`` for :mod:`celine.governance.validation`) and must stay that
way.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class GovernanceOwner(BaseModel):
    name: str
    type: str = Field(default="OWNER")  # semantic, not enforced by spec


class TemporalCoverage(BaseModel):
    model_config = ConfigDict(extra="ignore")

    start: Optional[str] = None
    end: Optional[str] = None


class DcatConfig(BaseModel):
    """DCAT-AP metadata for catalogue exposition.

    Consumed by ``dataset-api`` to build ``dcat:Dataset`` nodes.
    """

    model_config = ConfigDict(extra="ignore")

    publisher_uri: Optional[str] = None
    themes: List[str] = Field(default_factory=list)
    language_uris: List[str] = Field(default_factory=list)
    spatial_uris: List[str] = Field(default_factory=list)
    accrual_periodicity: Optional[str] = None  # URI from EU Authority Table
    conforms_to: Optional[str] = None  # URI of a standard the payload conforms to
    temporal: Optional[TemporalCoverage] = None


class OntologyConfig(BaseModel):
    """Semantic model binding — which mapping spec says what the columns *mean*.

    Two ways to locate one, because mappings divide into two kinds:

    ``spec``
        A **shared** mapping published in ``celine-ontologies``. Meter readings
        and forecasts recur across producers, and restating that mapping per
        dataset would make one fact many.
    ``spec_file``
        A path **relative to this governance.yaml**, for a dataset whose shape is
        its own. Locality is the point: a spec names source *columns*, and those
        columns are what the pipeline emits, so a spec living in another
        repository goes stale on a rename with nothing to detect it.

    Exactly one, enforced by the schema — two bindings for one dataset is two
    answers to "what does this column mean".

    Distinct from :attr:`DcatConfig.conforms_to`, deliberately: ``conforms_to``
    names the model (an IRI a consumer can compare across datasets), while this
    names the mapping *onto these columns*. Several datasets can conform to one
    model through different mappings, and one shared mapping can serve datasets
    that declare different models.

    Resolution lives in the consumer (``dataset-api``), not here: resolving
    ``spec`` means importing ``celine-ontologies``, and nothing in this package
    should drag in the ontology stack.
    """

    model_config = ConfigDict(extra="ignore")

    spec: Optional[str] = None  # shared mapping name, e.g. "obs_rec_energy"
    spec_file: Optional[str] = None  # path relative to the governance.yaml dir


class DataspaceConfig(BaseModel):
    """Dataspace exposure and ODRL policy hints.

    Simple fields only. The EDC-specific sub-objects (``asset``,
    ``data_address``, ``contract``) are ``ds``'s concern and are carried in its
    own ``DataspaceSpec`` subclass; ``extra="ignore"`` lets a file declare them
    without this model rejecting it.
    """

    model_config = ConfigDict(extra="ignore")

    medallion: Optional[str] = None  # bronze | silver | gold
    contract_required: bool = False
    consent_required: bool = False
    odrl_action: str = "use"
    purpose: List[str] = Field(default_factory=list)
    expose: bool = False  # offered into the dataspace


class GovernanceRule(BaseModel):
    """One resolved governance block.

    ``ds`` extends this with ``policy`` (ODRL/EDC) and its richer
    ``DataspaceSpec``; the fields here are what every consumer shares.
    """

    model_config = ConfigDict(extra="ignore")

    title: Optional[str] = None
    description: Optional[str] = None

    # Tri-state, and the `None` is the whole point.
    #
    # Until this field existed the only exposure gate was `dataspace.expose`, and
    # the exporter copied it straight onto the catalogue's flag — so one boolean
    # answered two different questions: "is this listed and queryable" and "is
    # this offered into the dataspace". Every `dataspace.expose: true` in the
    # deployed files is therefore a statement about the *catalogue*; it is the
    # only way a dataset could appear there.
    #
    # `None` means "not stated", and the resolved gate falls back to
    # `dataspace.expose` — see `celine.governance.exposure.effective_expose`. That
    # keeps every existing file behaving exactly as it does today while the two
    # questions are separated, which is what makes the split shippable ahead of
    # the file migration rather than in lockstep with it.
    expose: Optional[bool] = None

    license: Optional[str] = None
    attribution: Optional[str] = None
    ownership: List[GovernanceOwner] = Field(default_factory=list)
    access_level: Optional[str] = None  # open | internal | restricted | secret
    access_requirements: Optional[str] = None  # all | partner | contract
    classification: Optional[str] = None  # pii | red | yellow | green
    tags: List[str] = Field(default_factory=list)
    retention_days: Optional[int] = None
    documentation_url: Optional[str] = None
    source_system: Optional[str] = None
    row_filters: List[dict] = Field(default_factory=list)  # [{handler, args}]

    # Legacy, carried for `ds` backward compatibility with deployed files.
    # `row_filters` supersedes it: a filter names its handler, and a bare column
    # cannot say how a subject maps to values in it. No CELINE governance file
    # uses it. Canonical wins where both appear (ds `GOV-05`).
    user_filter_column: Optional[str] = None

    dcat: Optional[DcatConfig] = None
    ontology: Optional[OntologyConfig] = None
    dataspace: Optional[DataspaceConfig] = None
    extra: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("ownership", mode="before")
    @classmethod
    def _normalise_ownership(cls, v: Any) -> Any:
        """Accept ``[{name, type}]`` or a bare ``[name]`` list.

        Runs in ``before`` mode so parsing can go through ``model_validate`` —
        which is what keeps ``model_fields_set`` honest. See
        :func:`celine.governance.merge.merge_rules`.

        ``BaseModel`` instances pass through untouched. Running in ``before``
        mode means this sees whatever the caller passed, including an already
        built :class:`GovernanceOwner` — which pydantic would otherwise accept
        for a ``List[GovernanceOwner]`` field. Coercing it as if it were a bare
        label produced ``name="name='rec' type='OWNER'"``: a valid model
        carrying the repr of another model, which then travelled all the way to
        a published ``urn:owner:name='rec' type='OWNER'``. The YAML path is
        unaffected — it deals in dicts — so nothing detected it.
        """
        if not isinstance(v, list):
            return v
        return [
            item if isinstance(item, (dict, BaseModel)) else {"name": str(item)}
            for item in v
        ]


class Dependency(BaseModel):
    """One dataset a pipeline consumes.

    Names a **dataset**, never a pipeline. Several producers can satisfy one
    dataset through a shared alias — ``mt`` and ``owm`` both publish
    ``weather__forecast_hourly``, and the whole point of that contract is that
    ``weather`` does not know which one ran — and a deployment may substitute its
    own producer entirely. The producing pipeline is *resolved*, by matching this
    pattern against the ``sources`` keys of every governance file in the scanned
    set (:mod:`celine.governance.graph`), which is also why nothing here ever
    names a repository.

    Prefer a glob over the schema segment. ``ds_dev_*`` is the platform default
    and stays the default everywhere — environments are separated by
    infrastructure, not by renaming schemas — but a consumer resolves the schema
    through ``CELINE_SILVER_SCHEMA`` / ``CELINE_GOLD_SCHEMA``, which a deployment
    may point anywhere. ``datasets.*_gold.pv_overture_buildings`` is therefore the
    form that does not depend on that choice.
    """

    model_config = ConfigDict(extra="ignore")

    dataset: str

    # Distinguishes a genuine outside producer from a typo. Both resolve to
    # nothing in a single-repository scan and nothing else can tell them apart:
    # unresolved *and* unmarked is a mistake, unresolved *and* marked is the
    # documented case. Marked-but-resolved is reported too — as information, since
    # it means a wider scan closed the graph.
    external: bool = False

    # A soft edge: order after the producer where it exists, do not require it.
    # `pv_estimation` degrades rather than fails when `pv_detection` has not run,
    # and today that fact lives in `flows/config.yaml` as `require_detection`.
    optional: bool = False

    description: Optional[str] = None


class GovernanceConfig(BaseModel):
    defaults: GovernanceRule = Field(default_factory=GovernanceRule)

    # Whether this pipeline is on a schedule anywhere. `False` covers two cases
    # and does not distinguish them, because for the graph they are the same:
    # *retired* — `copernicus`, `dwd` and `owm`, each superseded by another
    # pipeline — and *local only* — the PV apps and `overture`, run on demand
    # against a local database. Either way nothing arrives unless someone acts,
    # and until this field existed nothing in the files said so.
    #
    # **App-level, and that is a real limit.** One app can host several flows on
    # independent schedules, and a deployment can pause one of them: `om` runs
    # four flows and its observations flow is paused while the other three run.
    # This field cannot express that, and must not be read as if it could —
    # per-flow state lives in the deployment, which is also the only place that
    # knows it. What it does express honestly is an app with no deployed flow at
    # all, which is the case that leaves a whole subtree of the graph dormant
    # with nothing saying why.
    active: bool = True

    # `None`, not `[]`, and the difference is load-bearing: absent means the
    # pipeline has not declared its inputs, `[]` means it declares it has none.
    # Collapsing the two would make a tier-0 pipeline and an un-migrated file
    # indistinguishable, which is the one thing an adoption checker needs to see.
    depends_on: Optional[List[Dependency]] = None

    sources: Dict[str, GovernanceRule] = Field(default_factory=dict)


#: Keys the grammar defines. Anything else in a block lands in ``extra``.
#:
#: Keeping this beside the model matters: when a field is added to
#: :class:`GovernanceRule` and *not* added here, the key parses into ``extra``
#: and the field reads as absent — silently, with the schema still validating it.
#: That is exactly how the ``ontology`` block failed on introduction.
#:
#: **``depends_on`` is deliberately not here, and adding it would be a bug.** This
#: set splits one governance *block* — ``defaults``, or one entry under
#: ``sources`` — and ``depends_on`` is a root key of the document, a sibling of
#: both. Listing it would make ``depends_on:`` inside a dataset block parse as a
#: rule field that no model declares. Root keys are checked by
#: :func:`celine.governance.validation.unknown_keys` against
#: :data:`KNOWN_ROOT_KEYS` instead.
KNOWN_KEYS: frozenset[str] = frozenset(
    {
        "title",
        "description",
        "expose",
        "license",
        "attribution",
        "ownership",
        "access_level",
        "access_requirements",
        "classification",
        "tags",
        "retention_days",
        "documentation_url",
        "source_system",
        "row_filters",
        "user_filter_column",
        "dcat",
        "ontology",
        "dataspace",
    }
)

#: Keys the grammar defines at the **root** of a governance document.
#:
#: Separate from :data:`KNOWN_KEYS` because the two are checked against different
#: things: that one splits a block, this one guards the document. A misspelled
#: ``depends-on:`` at the root is invisible to a schema that permits additional
#: root properties and to a parser that reads three keys by name — the file
#: validates, the graph is empty, and nothing says why.
KNOWN_ROOT_KEYS: frozenset[str] = frozenset(
    {
        "active",
        "defaults",
        "depends_on",
        "sources",
    }
)
