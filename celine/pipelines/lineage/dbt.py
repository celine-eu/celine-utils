import json, os
from openlineage.client.event_v2 import InputDataset, OutputDataset
from openlineage.client.generated.schema_dataset import (
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
)
from celine.pipelines.utils import get_namespace
from celine.common.logger import get_logger

from openlineage.client.generated.data_quality_assertions_dataset import (
    DataQualityAssertionsDatasetFacet,
)

from sqlalchemy.engine import Engine
from sqlalchemy import text
import typing as t

from openlineage.client.generated.data_quality_assertions_dataset import (
    DataQualityAssertionsDatasetFacet,
    Assertion as DqAssertion,
)


class DbtLineage:
    def __init__(self, project_dir: str, app_name: str, engine: Engine | None = None):
        self.project_dir = project_dir
        self.app_name = app_name
        self.logger = get_logger(__name__)
        self.engine = engine  # optional, to fetch schema metadata

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
            return []
        try:
            with self.engine.connect() as conn:
                sql = text(
                    f"""
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
        """Prefer documented columns, fall back to warehouse introspection."""
        fields = [
            SchemaDatasetFacetFields(
                name=col, type="unknown", description=meta.get("description")
            )
            for col, meta in (node.get("columns") or {}).items()
        ]

        schema_name = node.get("schema")
        table_name = node.get("alias") or node.get("name")
        if not fields and (schema_name and table_name):
            fields = self._fetch_columns_from_db(schema_name, table_name)
        return fields

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
            if key in tests_by_dataset:
                facets["dataQualityAssertions"] = DataQualityAssertionsDatasetFacet(
                    assertions=tests_by_dataset[key]
                )

            outputs.append(OutputDataset(namespace=namespace, name=key, facets=facets))

            for dep in node.get("depends_on", {}).get("nodes", []):
                dep_node = manifest["nodes"].get(dep) or manifest.get(
                    "sources", {}
                ).get(dep)
                if not dep_node:
                    continue
                dep_key = self._dataset_key(dep_node)
                inputs.append(InputDataset(namespace=namespace, name=dep_key))

        if tag == "test":
            existing_keys = {d.name for d in outputs}
            for key, assertions in tests_by_dataset.items():
                if key in existing_keys:
                    continue
                # build schema only once with helper
                db, sch, name = key.split(".", 2)
                fields = self._fetch_columns_from_db(sch, name)
                facets: dict[str, t.Any] = {
                    "schema": SchemaDatasetFacet(fields=fields),
                    "dataQualityAssertions": DataQualityAssertionsDatasetFacet(
                        assertions=assertions
                    ),
                }
                outputs.append(
                    OutputDataset(namespace=namespace, name=key, facets=facets)
                )

        self.logger.debug(
            f"dbt lineage collected {len(inputs)} inputs and {len(outputs)} outputs"
        )
        return inputs, outputs
