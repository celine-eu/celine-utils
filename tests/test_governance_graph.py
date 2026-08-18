"""The inter-pipeline dependency graph."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest

from celine.governance import (
    CYCLE,
    EXTERNAL_SATISFIED,
    INACTIVE_PRODUCER,
    NOT_DEPLOYED,
    SCHEDULE_COLLISION,
    SCHEDULE_INVERSION,
    MULTIPLE_PRODUCERS,
    SELF_DEPENDENCY,
    UNRESOLVED,
    GovernanceResolver,
    build_graph,
    check_schedules,
    discover,
    load_flows,
    load_pipelines,
    matches,
    validate,
)
from celine.governance.graph import SCHEDULE_UNVERIFIED, _minutes
from celine.governance.merge import merge_configs


def write_app(root: Path, name: str, body: str) -> Path:
    app = root / "apps" / name
    app.mkdir(parents=True, exist_ok=True)
    path = app / "governance.yaml"
    path.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    return path


def graph_of(root: Path, pattern: str = "apps/*"):
    found = discover([str(root / pattern)])
    return build_graph(load_pipelines(found.files))


def kinds(graph) -> list[str]:
    return [f.kind for f in graph.findings]


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_absent_and_empty_depends_on_are_different_statements(tmp_path: Path):
    """`None` is "has not declared inputs"; `[]` is "declares it has none".

    @verifies REQ-0006
    """
    absent = write_app(
        tmp_path,
        "absent",
        """
        defaults: {}
        sources: {}
        """,
    )
    empty = write_app(
        tmp_path,
        "empty",
        """
        defaults: {}
        depends_on: []
        sources: {}
        """,
    )

    assert GovernanceResolver.from_file(absent).config.depends_on is None
    assert GovernanceResolver.from_file(empty).config.depends_on == []

    pipelines = {p.name: p for p in load_pipelines([absent, empty])}
    assert pipelines["absent"].declared is False
    assert pipelines["empty"].declared is True


def test_dependency_defaults_and_fields(tmp_path: Path):
    path = write_app(
        tmp_path,
        "consumer",
        """
        defaults: {}
        depends_on:
          - dataset: datasets.*_gold.a
          - dataset: datasets.*_gold.b
            external: true
            optional: true
            description: why
        sources: {}
        """,
    )
    deps = GovernanceResolver.from_file(path).config.depends_on
    assert deps is not None

    assert deps[0].dataset == "datasets.*_gold.a"
    assert deps[0].external is False
    assert deps[0].optional is False
    assert deps[0].description is None

    assert deps[1].external is True
    assert deps[1].optional is True
    assert deps[1].description == "why"


def test_entry_requires_dataset_and_rejects_unknown_fields():
    """The block is new, so it is strict from the start.

    `governanceBlock` cannot be tightened without breaking files that predate
    validation; this one has no such history and takes the strictness now.
    """
    with pytest.raises(Exception):
        validate({"defaults": {}, "sources": {}, "depends_on": [{"external": True}]})

    with pytest.raises(Exception):
        validate(
            {
                "defaults": {},
                "sources": {},
                "depends_on": [{"dataset": "datasets.raw.x", "extrnal": True}],
            }
        )


def test_a_misspelled_root_key_is_reported(tmp_path: Path):
    """The root permits additional properties and the parser reads three keys by
    name, so nothing else would ever mention `depends-on:`.

    @verifies REQ-0006
    """
    unknown = validate({"defaults": {}, "sources": {}, "depends-on": []})
    assert "<root>: depends-on" in unknown

    assert validate({"defaults": {}, "sources": {}, "depends_on": []}) == []


# ---------------------------------------------------------------------------
# Matching
# ---------------------------------------------------------------------------


def test_matching_is_two_sided():
    """Either side may be the glob — the schema segment differs per deployment.

    @verifies REQ-0006
    """
    assert matches("datasets.*_gold.x", "datasets.ds_dev_gold.x")
    assert matches("datasets.*_gold.x", "datasets.ds_prod_gold.x")
    assert matches("datasets.ds_dev_gold.x", "datasets.ds_dev_gold.x")
    # producer declares the family, consumer names one member
    assert matches("datasets.ds_dev_gold.x", "datasets.ds_dev_gold.*")

    assert not matches("datasets.*_gold.x", "datasets.ds_dev_silver.x")
    assert not matches("datasets.*_gold.x", "datasets.ds_dev_gold.y")


def test_matching_is_case_sensitive_regardless_of_host_os():
    assert not matches("datasets.*_gold.x", "DATASETS.ds_dev_gold.X")


# ---------------------------------------------------------------------------
# Building
# ---------------------------------------------------------------------------


def test_an_edge_resolves_across_files_and_survives_the_schema_prefix(tmp_path: Path):
    """The consumer globs the schema; the producer spells it `ds_dev_`.

    @verifies REQ-0006
    """
    write_app(
        tmp_path,
        "producer",
        """
        defaults: {}
        sources:
          datasets.ds_dev_gold.weather__forecast_hourly: {}
        """,
    )
    write_app(
        tmp_path,
        "consumer",
        """
        defaults: {}
        depends_on:
          - dataset: datasets.*_gold.weather__forecast_hourly
        sources: {}
        """,
    )

    graph = graph_of(tmp_path)

    assert [(e.producer, e.consumer) for e in graph.edges] == [("producer", "consumer")]
    assert graph.edges[0].dataset == "datasets.ds_dev_gold.weather__forecast_hourly"
    assert graph.tiers() == [["producer"], ["consumer"]]
    assert kinds(graph) == []


def test_one_dataset_two_producers_is_reported(tmp_path: Path):
    """Two governance files claiming one dataset is two answers to who owns it.

    @verifies REQ-0006
    """
    write_app(
        tmp_path, "a", "defaults: {}\nsources:\n  datasets.ds_dev_gold.shared: {}\n"
    )
    write_app(
        tmp_path, "b", "defaults: {}\nsources:\n  datasets.ds_dev_gold.shared: {}\n"
    )

    graph = graph_of(tmp_path)

    assert kinds(graph) == [MULTIPLE_PRODUCERS]
    assert "a, b" in graph.findings[0].detail


def test_unresolved_is_reported_unless_marked_external(tmp_path: Path):
    """The flag is what separates a typo from a genuine outside producer.

    @verifies REQ-0006
    """
    write_app(
        tmp_path,
        "typo",
        """
        defaults: {}
        depends_on:
          - dataset: datasets.*_silver.mteers_data_normalized
        sources: {}
        """,
    )
    write_app(
        tmp_path,
        "honest",
        """
        defaults: {}
        depends_on:
          - dataset: datasets.*_silver.meters_data_normalized
            external: true
        sources: {}
        """,
    )

    graph = graph_of(tmp_path)

    assert kinds(graph) == [UNRESOLVED]
    assert graph.findings[0].pipeline == "typo"


def test_external_that_resolves_is_reported_as_information(tmp_path: Path):
    """Scanning the deployment tree too closes the graph. That is not a fault.

    @verifies REQ-0006
    """
    write_app(
        tmp_path,
        "deployment",
        """
        defaults: {}
        sources:
          datasets.ds_dev_silver.meters_data_normalized: {}
        """,
    )
    write_app(
        tmp_path,
        "oss",
        """
        defaults: {}
        depends_on:
          - dataset: datasets.*_silver.meters_data_normalized
            external: true
        sources: {}
        """,
    )

    graph = graph_of(tmp_path)

    assert kinds(graph) == [EXTERNAL_SATISFIED]
    assert [(e.producer, e.consumer) for e in graph.edges] == [("deployment", "oss")]


def test_self_dependency_is_dropped_and_reported(tmp_path: Path):
    write_app(
        tmp_path,
        "solo",
        """
        defaults: {}
        depends_on:
          - dataset: datasets.*_gold.mine
        sources:
          datasets.ds_dev_gold.mine: {}
        """,
    )

    graph = graph_of(tmp_path)

    assert kinds(graph) == [SELF_DEPENDENCY]
    assert graph.edges == ()
    assert graph.tiers() == [["solo"]]


def test_a_cycle_is_reported_and_omitted_from_the_tiers(tmp_path: Path):
    """A tier table is read as an instruction, so a made-up order is worse than none.

    @verifies REQ-0006
    """
    write_app(
        tmp_path,
        "a",
        """
        defaults: {}
        depends_on:
          - dataset: datasets.ds_dev_gold.b_out
        sources:
          datasets.ds_dev_gold.a_out: {}
        """,
    )
    write_app(
        tmp_path,
        "b",
        """
        defaults: {}
        depends_on:
          - dataset: datasets.ds_dev_gold.a_out
        sources:
          datasets.ds_dev_gold.b_out: {}
        """,
    )

    graph = graph_of(tmp_path)

    assert CYCLE in kinds(graph)
    assert graph.tiers() == []


def test_optional_edges_still_order_but_are_marked(tmp_path: Path):
    write_app(
        tmp_path, "det", "defaults: {}\nsources:\n  datasets.ds_dev_gold.det: {}\n"
    )
    write_app(
        tmp_path,
        "est",
        """
        defaults: {}
        depends_on:
          - dataset: datasets.*_gold.det
            optional: true
        sources: {}
        """,
    )

    graph = graph_of(tmp_path)

    assert graph.edges[0].optional is True
    assert graph.tiers() == [["det"], ["est"]]


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def test_discovery_accepts_directories_and_files_and_skips_overlays(tmp_path: Path):
    write_app(tmp_path, "one", "defaults: {}\nsources: {}\n")
    overlay = tmp_path / "apps" / "one" / "governance.demo.yaml"
    overlay.write_text("defaults: {}\n", encoding="utf-8")

    by_dir = discover([str(tmp_path / "apps" / "*")])
    by_file = discover([str(tmp_path / "apps" / "*" / "governance.yaml")])
    everything = discover([str(tmp_path / "apps" / "*" / "*.yaml")])

    assert [p.name for p in by_dir.files] == ["governance.yaml"]
    assert by_dir.files == by_file.files
    assert everything.files == by_file.files
    assert [p.name for p in everything.skipped_overlays] == ["governance.demo.yaml"]


def test_same_named_apps_in_two_trees_stay_distinguishable(tmp_path: Path):
    """An open-source app and a deployment's copy of it must not both print `grid`.

    One extra segment is not enough — the real case is `apps/grid` against another
    checkout's `apps/grid`, where the parent is `apps` on both sides.

    @verifies REQ-0006
    """
    a = write_app(tmp_path / "oss", "grid", "defaults: {}\nsources: {}\n")
    b = write_app(tmp_path / "deployment", "grid", "defaults: {}\nsources: {}\n")

    names = sorted(p.name for p in load_pipelines([a, b]))
    assert names == ["deployment/apps/grid", "oss/apps/grid"]


def test_only_colliding_names_get_lengthened(tmp_path: Path):
    a = write_app(tmp_path / "oss", "grid", "defaults: {}\nsources: {}\n")
    b = write_app(tmp_path / "deployment", "grid", "defaults: {}\nsources: {}\n")
    c = write_app(tmp_path / "oss", "weather", "defaults: {}\nsources: {}\n")

    names = sorted(p.name for p in load_pipelines([a, b, c]))
    assert names == ["deployment/apps/grid", "oss/apps/grid", "weather"]


def test_missing_file_yields_an_empty_config_not_a_crash(tmp_path: Path):
    assert discover([str(tmp_path / "nothing" / "*")]).files == ()


# ---------------------------------------------------------------------------
# Overlays
# ---------------------------------------------------------------------------


def test_an_overlay_replaces_depends_on_wholesale_and_can_withdraw_it(tmp_path: Path):
    """Substituting a producer is what a deployment overlay is for.

    Union would make an upstream the deployment satisfies another way impossible
    to remove; truthiness would silently restore the base's list — the bug the
    merge layer exists to prevent.

    @verifies REQ-0006
    """
    base = GovernanceResolver.from_dict(
        {"defaults": {}, "sources": {}, "depends_on": [{"dataset": "datasets.raw.a"}]}
    ).config

    unstated = GovernanceResolver.from_dict({"defaults": {}, "sources": {}}).config
    replaced = GovernanceResolver.from_dict(
        {"defaults": {}, "sources": {}, "depends_on": [{"dataset": "datasets.raw.b"}]}
    ).config
    withdrawn = GovernanceResolver.from_dict(
        {"defaults": {}, "sources": {}, "depends_on": []}
    ).config

    assert [d.dataset for d in merge_configs(base, unstated).depends_on] == [
        "datasets.raw.a"
    ]
    assert [d.dataset for d in merge_configs(base, replaced).depends_on] == [
        "datasets.raw.b"
    ]
    assert merge_configs(base, withdrawn).depends_on == []


# ---------------------------------------------------------------------------
# Activity
# ---------------------------------------------------------------------------


def test_active_defaults_true_and_an_inactive_producer_is_reported(tmp_path: Path):
    """A consumer reading from a paused pipeline reads whatever was last written.

    @verifies REQ-0006
    """
    write_app(
        tmp_path,
        "retired",
        """
        active: false
        defaults: {}
        sources:
          datasets.ds_dev_gold.old: {}
        """,
    )
    write_app(
        tmp_path,
        "live",
        """
        defaults: {}
        depends_on:
          - dataset: datasets.*_gold.old
        sources: {}
        """,
    )

    graph = graph_of(tmp_path)
    by_name = {p.name: p for p in graph.pipelines}

    assert by_name["retired"].active is False
    assert by_name["live"].active is True
    assert INACTIVE_PRODUCER in kinds(graph)


def test_active_is_a_known_root_key():
    assert validate({"active": False, "defaults": {}, "sources": {}}) == []
    assert "<root>: activ" in validate({"activ": False, "defaults": {}, "sources": {}})


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------


def schedules(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "schedules.yaml"
    path.write_text(textwrap.dedent(body).lstrip(), encoding="utf-8")
    return path


def test_minute_expansion_covers_the_forms_a_deployment_uses():
    assert _minutes("15") == {15}
    assert _minutes("*/15") == {0, 15, 30, 45}
    assert _minutes("2,12,22") == {2, 12, 22}
    assert _minutes("3-58/5") == {3, 8, 13, 18, 23, 28, 33, 38, 43, 48, 53, 58}
    # Not standard cron, but Prefect reads it as "from 20, every 15".
    assert _minutes("20/15") == {20, 35, 50}


def test_app_is_derived_from_a_manifest_path(tmp_path: Path):
    path = schedules(
        tmp_path,
        """
        flows:
          - name: om-wind
            path: /pipelines/apps/om/flows/pipeline_wind.py
            schedule:
              cron: "0 0,4,8,12,16,20 * * *"
          - name: manual
            path: /pipelines/apps/om/flows/adhoc.py
        """,
    )
    flows = load_flows(path)
    assert [(f.name, f.app, f.cron) for f in flows] == [
        ("om-wind", "om", "0 0,4,8,12,16,20 * * *")
    ]


def build_pair(tmp_path: Path, producer_cron: str, consumer_cron: str, extra: str = ""):
    write_app(
        tmp_path,
        "producer",
        "defaults: {}\ndepends_on: []\nsources:\n  datasets.ds_dev_gold.x: {}\n",
    )
    write_app(
        tmp_path,
        "consumer",
        """
        defaults: {}
        depends_on:
          - dataset: datasets.*_gold.x
        sources: {}
        """,
    )
    path = schedules(
        tmp_path,
        f"""
        flows:
          - name: producer
            app: producer
            schedule: {{cron: "{producer_cron}"}}
          - name: consumer
            app: consumer
            schedule: {{cron: "{consumer_cron}"}}
        {extra}
        """,
    )
    graph = graph_of(tmp_path)
    return check_schedules(graph, load_flows(path))


def test_a_consumer_scheduled_before_its_producer_is_reported(tmp_path: Path):
    """The live case: `weather` at :15 reading `mt`, which runs at :27.

    It succeeds every hour on the previous hour's data, so nothing reports it.

    @verifies REQ-0006
    """
    found = build_pair(tmp_path, producer_cron="27 * * * *", consumer_cron="15 * * * *")
    assert [f.kind for f in found] == [SCHEDULE_INVERSION]


def test_the_same_minute_is_reported_as_a_collision(tmp_path: Path):
    found = build_pair(tmp_path, producer_cron="0 2 * * *", consumer_cron="0 2 * * *")
    assert [f.kind for f in found] == [SCHEDULE_COLLISION]

    overlapping = build_pair(
        tmp_path, producer_cron="2,12,22 * * * *", consumer_cron="7,22,37 * * * *"
    )
    assert [f.kind for f in overlapping] == [SCHEDULE_COLLISION]


def test_a_correctly_ordered_pair_reports_nothing(tmp_path: Path):
    found = build_pair(tmp_path, producer_cron="5 * * * *", consumer_cron="15 * * * *")
    assert found == []


def test_different_periods_are_not_compared_on_minutes(tmp_path: Path):
    """Reasoning about an offset between hourly and daily needs the run duration,
    which is in no file. Only an identical expression is reported."""
    found = build_pair(tmp_path, producer_cron="0 6 * * *", consumer_cron="15 * * * *")
    assert found == []


def test_a_multi_flow_app_downgrades_the_finding_to_advisory(tmp_path: Path):
    """`demo3` deploys a five-minute meters flow and a monthly batch flow, and only
    the batch writes what `grid` reads. Comparing the wrong pair is arithmetically
    true and means nothing, so it must not be indistinguishable from a real one.

    @verifies REQ-0006
    """
    found = build_pair(
        tmp_path,
        producer_cron="0 2 * * *",
        consumer_cron="0 2 * * *",
        extra='  - {name: producer-batch, app: producer, schedule: {cron: "0 1 1 * *"}}',
    )
    assert SCHEDULE_COLLISION not in [f.kind for f in found]
    assert SCHEDULE_UNVERIFIED in [f.kind for f in found]


def test_an_active_pipeline_nothing_runs_is_reported(tmp_path: Path):
    write_app(tmp_path, "orphan", "defaults: {}\ndepends_on: []\nsources: {}\n")
    write_app(
        tmp_path, "paused", "active: false\ndefaults: {}\ndepends_on: []\nsources: {}\n"
    )
    path = schedules(tmp_path, "flows: []\n")

    found = check_schedules(graph_of(tmp_path), load_flows(path))

    assert [(f.kind, f.pipeline) for f in found] == [(NOT_DEPLOYED, "orphan")]


def test_run_order_is_the_tiers_flattened(tmp_path: Path):
    write_app(
        tmp_path, "a", "defaults: {}\ndepends_on: []\nsources:\n  datasets.ds_dev_gold.a: {}\n"
    )
    write_app(
        tmp_path,
        "b",
        """
        defaults: {}
        depends_on:
          - dataset: datasets.*_gold.a
        sources: {}
        """,
    )
    graph = graph_of(tmp_path)
    assert graph.run_order() == ["a", "b"]
