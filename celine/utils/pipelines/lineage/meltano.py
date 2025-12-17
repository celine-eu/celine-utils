# celine/pipelines/lineage/meltano.py
import os, json, yaml
from typing import Any

from openlineage.client.event_v2 import InputDataset, OutputDataset
from openlineage.client.generated.schema_dataset import (
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
)

from celine.utils.common.logger import get_logger
from celine.utils.pipelines.pipeline_config import PipelineConfig
from celine.utils.pipelines.utils import get_namespace, expand_envs
from celine.utils.pipelines.governance import GovernanceResolver
from celine.utils.pipelines.lineage.facets.governance import GovernanceDatasetFacet


class MeltanoLineage:
    """Helper to discover Meltano lineage (datasets in/out)."""

    def __init__(
        self,
        cfg: PipelineConfig,
        project_root: str | None = None,
        config_path: str = "meltano.yml",
        run_dir: str = ".meltano/run",
        governance_resolver: GovernanceResolver | None = None,
    ):
        self.logger = get_logger(__name__)
        self.cfg = cfg
        self.project_root = project_root or f"./apps/{self.cfg.app_name}/meltano"
        self.config_path = config_path
        self.run_dir = run_dir
        self.config = self._load_meltano_config()
        # NEW: governance resolver
        self.governance_resolver = (
            governance_resolver
            or GovernanceResolver.auto_discover(
                app_name=self.cfg.app_name,
                project_dir=self.project_root,
            )
        )

    # ---------- Helpers ----------
    def _load_meltano_config(self) -> dict[str, Any]:
        project_root = self.project_root
        path = os.path.join(project_root, self.config_path)
        self.logger.debug(f"Loading Meltano config from {path}")
        try:
            with open(path) as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            self.logger.warning(
                f"No Meltano config found at {path}, skipping lineage discovery."
            )
            return {}
        except Exception as e:
            self.logger.error(f"Failed to load Meltano config at {path}: {e}")
            return {}

    def _build_schema_facet(self, s: dict) -> SchemaDatasetFacet:
        schema_props: dict[str, Any] = s.get("schema", {}).get("properties", {}) or {}
        fields = [
            SchemaDatasetFacetFields(
                name=col,
                type=str(info.get("type", "unknown")),
                description=None,
            )
            for col, info in schema_props.items()
        ]
        return SchemaDatasetFacet(fields=fields)

    def _build_governance_facet(
        self, dataset_name: str
    ) -> GovernanceDatasetFacet | None:
        if not self.governance_resolver:
            return None
        rule = self.governance_resolver.resolve(dataset_name)
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
        ):
            return None

        return GovernanceDatasetFacet(
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
        )

    def collect_inputs_outputs(
        self, job_name: str
    ) -> tuple[list[InputDataset], list[OutputDataset]]:
        """Collect dataset info from Meltano taps/loaders config."""

        active_taps = []
        active_targets = []

        for job in self.config.get("jobs", []):
            if job.get("name") != job_name:
                continue

            for task in job.get("tasks", []):
                parts = str(task).split(" ")
                for part in parts:
                    if part.startswith("tap-"):
                        active_taps.append(part)
                    if part.startswith("target-"):
                        active_targets.append(part)

        inputs: list[InputDataset] = []
        outputs: list[OutputDataset] = []

        run_root = os.path.join(self.project_root, self.run_dir)
        if not os.path.isdir(run_root):
            self.logger.warning(f"No run directory {run_root}")
            return inputs, outputs

        namespace = get_namespace(self.cfg.app_name)

        for plugin in os.listdir(run_root):
            if not plugin.startswith("tap-"):
                continue
            props_file = os.path.join(run_root, plugin, "tap.properties.json")
            if not os.path.exists(props_file):
                self.logger.warning(f"Cannot find {props_file}")
                continue

            try:
                with open(props_file) as f:
                    props = json.load(f)
            except Exception as e:
                self.logger.warning(f"Failed to parse {props_file}: {e}")
                continue

            for s in props.get("streams", []):

                schema_facet = self._build_schema_facet(s)

                # Input dataset name (Singer convention)
                tap_name = plugin
                stream = s["stream"]
                input_name = f"singer.{plugin}.{s['tap_stream_id']}"

                facets_in: dict[str, Any] = {"schema": schema_facet}
                gov_in = self._build_governance_facet(input_name)
                if gov_in:
                    facets_in["governance"] = gov_in

                if tap_name in active_taps:
                    self.logger.debug(f"Append input {input_name}")
                    inputs.append(
                        InputDataset(
                            namespace=namespace,
                            name=input_name,
                            facets=facets_in,
                        )
                    )
                else:
                    self.logger.debug(
                        f"Skipping {tap_name} not part of active taps {','.join(active_taps)}"
                    )

                # Try to resolve outputs for all loaders
                for loader in self.config.get("plugins", {}).get("loaders", []):
                    loader_name = loader.get("name")

                    if loader_name not in active_targets:
                        continue

                    target_run_dir = os.path.join(run_root, f"{loader_name}")
                    if not os.path.exists(target_run_dir):
                        self.logger.debug(
                            f"Skip target {loader_name}: run path does not exists at {target_run_dir}"
                        )
                        continue

                    cfg = loader.get("config", {}) or {}

                    if loader_name == "target-postgres":
                        db = expand_envs(cfg.get("database", "postgres"))
                        schema = expand_envs(cfg.get("default_target_schema"))

                        if not schema:
                            schema = stream
                            self.logger.warning(
                                f"Loader {loader_name} has no default_target_schema; "
                                f"using derived schema '{schema}' for stream {stream}"
                            )

                        output_name = f"{db}.{schema}.{stream}"
                        self.logger.debug(f"Append output {output_name}")

                        facets_out: dict[str, Any] = {"schema": schema_facet}
                        gov_out = self._build_governance_facet(output_name)
                        if gov_out:
                            facets_out["governance"] = gov_out

                        outputs.append(
                            OutputDataset(
                                namespace=namespace,
                                name=output_name,
                                facets=facets_out,
                            )
                        )
                    else:
                        self.logger.warning(
                            f"Unsupported loader {loader_name} - no output lineage emitted for stream {stream}"
                        )

        self.logger.debug(
            f"Collected {len(inputs)} inputs and {len(outputs)} outputs for job={job_name}"
        )
        return inputs, outputs
