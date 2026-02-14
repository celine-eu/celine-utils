# celine/pipelines/lineage/dbt.py
import json, os
import typing as t

from sqlalchemy.engine import Engine
from sqlalchemy import text

from openlineage.client.event_v2 import InputDataset, OutputDataset
from openlineage.client.generated.schema_dataset import (
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
)
from openlineage.client.generated.data_quality_assertions_dataset import (
    DataQualityAssertionsDatasetFacet,
    Assertion as DqAssertion,
)

from celine.utils.pipelines.utils import get_namespace
from celine.utils.common.logger import get_logger
from celine.utils.pipelines.governance import GovernanceResolver
from celine.utils.pipelines.lineage.facets.governance import GovernanceDatasetFacet

from openlineage.client.generated.data_quality_assertions_dataset import (
    DataQualityAssertionsDatasetFacet,
    Assertion as DqAssertion,
)


class DbtLineage:
    def __init__(
        self,
        project_dir: str,
        app_name: str,
        engine: Engine | None = None,
        governance_resolver: GovernanceResolver | None = None,
    ):
        self.project_dir = project_dir
        self.app_name = app_name
        self.logger = get_logger(__name__)
        self.engine = engine  # optional, to fetch schema metadata
        self.governance_resolver = (
            governance_resolver
            or GovernanceResolver.auto_discover(
                app_name=app_name,
                project_dir=project_dir,
            )
        )

    # ---------------- Helpers ----------------
    def _dataset_key(self, node: dict) -> str:
        db = node.get("database")
        sch = node.get("schema")
        name = node.get("alias") or node.get("name") or node.get("identifier")
        return f"{db}.{sch}.{name}"

    def _fetch_columns_from_db(
        self, schema: str, name: str
    ) -> list[SchemaDatasetFacetFields]:
        """Query warehouse for column metadata if YAML docs are missing."""
        if not self.engine:
            self.logger.warning(f"Engine not available")
            return []
        try:
            with self.engine.connect() as conn:
                sql = text(
                    """
                    select column_name, data_type
                    from information_schema.columns
                    where table_schema = :schema
                      and table_name = :name
                    order by ordinal_position
                """
                )
                rows = conn.execute(sql, {"schema": schema, "name": name}).fetchall()
            return [
                SchemaDatasetFacetFields(name=row[0], type=row[1], description=None)
                for row in rows
            ]
        except Exception as e:
            self.logger.warning(f"Failed to introspect {schema}.{name}: {e}")
            return []

    def _build_schema_fields(self, node: dict) -> list[SchemaDatasetFacetFields]:
        """Prefer warehouse introspection; can be extended to use dbt docs."""
        schema_name = node.get("schema")
        table_name = node.get("alias") or node.get("name")
        fields: list[SchemaDatasetFacetFields] = []
        if schema_name and table_name:
            fields = self._fetch_columns_from_db(schema_name, table_name)
        return fields

    def _build_governance_facet(
        self, key: str, node: dict | None = None
    ) -> GovernanceDatasetFacet | None:
        """
        Compose governance from resolver (governance.yaml) and optional dbt meta.governance override.
        """
        if not self.governance_resolver:
            base_rule = None
        else:
            base_rule = self.governance_resolver.resolve(key)

        # dbt meta override
        meta_rule = None
        if node:
            meta = node.get("meta") or {}
            g = meta.get("governance") or {}
            if g:
                from celine.utils.pipelines.governance import (
                    GovernanceRule,
                    GovernanceOwner,
                )

                owners_raw = g.get("ownership") or []
                owners = [
                    (
                        GovernanceOwner(**o)
                        if isinstance(o, dict)
                        else GovernanceOwner(name=str(o))
                    )
                    for o in owners_raw
                ]
                meta_rule = GovernanceRule(
                    license=g.get("license"),
                    attribution=g.get("attribution"),
                    ownership=owners,
                    access_level=g.get("access_level"),
                    access_requirements=g.get("access_requirements"),
                    classification=g.get("classification"),
                    tags=g.get("tags") or [],
                    retention_days=g.get("retention_days"),
                    documentation_url=g.get("documentation_url"),
                    source_system=g.get("source_system"),
                    user_filter_column=g.get("user_filter_column"),  # NEW
                    extra={},
                )

        from celine.utils.pipelines.governance import GovernanceResolver as _GR

        if base_rule and meta_rule:
            rule = _GR._merge(base_rule, meta_rule)  # reuse merge logic
        else:
            rule = meta_rule or base_rule

        if not rule:
            return None

        owners = [o.name for o in rule.ownership] if rule.ownership else None
        tags = rule.tags or None

        if (
            not rule.license
            and not owners
            and not rule.access_level
            and not rule.access_requirements
            and not rule.classification
            and not tags
            and rule.retention_days is None
            and not rule.documentation_url
            and not rule.source_system
            and not rule.title
            and not rule.description
            and not rule.user_filter_column  # NEW: include in empty check
        ):
            return None

        return GovernanceDatasetFacet(
            title=rule.title,
            description=rule.description,
            license=rule.license,
            attribution=rule.attribution,
            owners=owners,
            accessLevel=rule.access_level,
            accessRequirements=rule.access_requirements,
            classification=rule.classification,
            tags=tags,
            retentionDays=rule.retention_days,
            documentationUrl=rule.documentation_url,
            sourceSystem=rule.source_system,
            userFilterColumn=rule.user_filter_column,  # NEW
        )

    def _build_assertion(self, test_node: dict, result: dict) -> DqAssertion:
        tm = test_node.get("test_metadata") or {}
        test_name = tm.get("name") or test_node.get("name") or "dbt_test"
        kwargs = tm.get("kwargs") or {}
        col = kwargs.get("column_name") or kwargs.get("field")
        status = (result.get("status") or "").lower()
        return DqAssertion(
            assertion=f"dbt:{test_name}",
            success=(status == "pass"),
            column=col,
        )

    def _index_tests_by_dataset(
        self, manifest: dict, results: dict
    ) -> dict[str, list[DqAssertion]]:
        idx: dict[str, list[DqAssertion]] = {}
        for r in results.get("results", []):
            test_id = r.get("unique_id")
            test_node = manifest.get("nodes", {}).get(test_id)
            if not test_node or test_node.get("resource_type") != "test":
                continue

            attached = test_node.get("attached_node")
            if not attached:
                deps = test_node.get("depends_on", {}).get("nodes", [])
                attached = deps[0] if deps else None
            if not attached:
                continue

            model_node = manifest.get("nodes", {}).get(attached) or manifest.get(
                "sources", {}
            ).get(attached)
            if not model_node:
                continue

            key = self._dataset_key(model_node)
            assertion = self._build_assertion(test_node, r)
            idx.setdefault(key, []).append(assertion)

        return idx

    # ---------------- Main ----------------
    def collect_inputs_outputs(
        self, tag: str
    ) -> tuple[list[InputDataset], list[OutputDataset]]:
        target_dir = os.path.join(self.project_dir, "target")
        manifest_path = os.path.join(target_dir, "manifest.json")
        results_path = os.path.join(target_dir, "run_results.json")

        if not os.path.exists(manifest_path) or not os.path.exists(results_path):
            self.logger.warning("Missing dbt artifacts, skipping lineage")
            return [], []

        manifest = json.load(open(manifest_path))
        results = json.load(open(results_path))

        tests_by_dataset = self._index_tests_by_dataset(manifest, results)
        executed_nodes = {r["unique_id"]: r for r in results.get("results", [])}
        inputs, outputs = [], []
        namespace = get_namespace(self.app_name)

        for node_id, node in manifest.get("nodes", {}).items():
            if node_id not in executed_nodes or node["resource_type"] != "model":
                continue

            key = self._dataset_key(node)
            fields = self._build_schema_fields(node)

            facets: dict[str, t.Any] = {"schema": SchemaDatasetFacet(fields=fields)}
            gov_facet = self._build_governance_facet(key, node)
            if gov_facet:
                facets["governance"] = gov_facet

            if key in tests_by_dataset:
                facets["dataQualityAssertions"] = DataQualityAssertionsDatasetFacet(
                    assertions=tests_by_dataset[key]
                )

            outputs.append(OutputDataset(namespace=namespace, name=key, facets=facets))

            # Inputs: upstream deps
            for dep in node.get("depends_on", {}).get("nodes", []):
                dep_node = manifest["nodes"].get(dep) or manifest.get(
                    "sources", {}
                ).get(dep)
                if not dep_node:
                    continue
                dep_key = self._dataset_key(dep_node)
                in_facets: dict[str, t.Any] = {}
                gov_in = self._build_governance_facet(dep_key, dep_node)
                if gov_in:
                    in_facets["governance"] = gov_in
                inputs.append(
                    InputDataset(
                        namespace=namespace,
                        name=dep_key,
                        facets=in_facets,
                    )
                )

        if tag == "test":
            existing_keys = {d.name for d in outputs}
            for key, assertions in tests_by_dataset.items():
                if key in existing_keys:
                    continue
                db, sch, name = key.split(".", 2)
                fields = self._fetch_columns_from_db(sch, name)
                facets: dict[str, t.Any] = {
                    "schema": SchemaDatasetFacet(fields=fields),
                    "dataQualityAssertions": DataQualityAssertionsDatasetFacet(
                        assertions=assertions
                    ),
                }
                gov_facet = self._build_governance_facet(key)
                if gov_facet:
                    facets["governance"] = gov_facet
                outputs.append(
                    OutputDataset(namespace=namespace, name=key, facets=facets)
                )

        self.logger.debug(
            f"dbt lineage collected {len(inputs)} inputs and {len(outputs)} outputs"
        )
        return inputs, outputs
