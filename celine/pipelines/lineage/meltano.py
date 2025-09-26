import os, json, yaml
from typing import Any
from celine.common.logger import get_logger
from celine.pipelines.pipeline_config import PipelineConfig
from openlineage.client.generated.schema_dataset import (
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
)
from openlineage.client.event_v2 import InputDataset, OutputDataset


class MeltanoLineage:
    """Helper to discover Meltano lineage (datasets in/out)."""

    def __init__(
        self,
        cfg: PipelineConfig,
        project_root: str | None = None,
        config_path: str = "meltano.yml",
        run_dir: str = ".meltano/run",
    ):
        self.logger = get_logger(__name__)
        self.cfg = cfg
        self.project_root = project_root or f"./apps/{self.cfg.app_name}/meltano"
        self.config_path = config_path
        self.run_dir = run_dir
        self.config = self._load_meltano_config()

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

    def collect_inputs_outputs(
        self, job_name: str
    ) -> tuple[list[InputDataset], list[OutputDataset]]:
        """Collect dataset info from Meltano taps/loaders config."""

        inputs: list[InputDataset] = []
        outputs: list[OutputDataset] = []

        run_root = os.path.join(self.project_root, self.run_dir)
        if not os.path.isdir(run_root):
            self.logger.warning(f"No run directory {run_root}")
            return inputs, outputs

        namespace_raw = f"celine.raw.{self.cfg.app_name}"

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
                schema_props: dict[str, Any] = (
                    s.get("schema", {}).get("properties", {}) or {}
                )

                fields = [
                    SchemaDatasetFacetFields(
                        name=col,
                        type=str(info.get("type", "unknown")),
                        description=None,
                    )
                    for col, info in schema_props.items()
                ]
                schema_facet = SchemaDatasetFacet(fields=fields)

                # Input: Singer stream
                inputs.append(
                    InputDataset(
                        namespace=f"singer://{plugin}",
                        name=s["tap_stream_id"],
                        facets={"schema": schema_facet},
                    )
                )

                # Output: raw landed table
                outputs.append(
                    OutputDataset(
                        namespace=namespace_raw,
                        name=s["stream"],
                        facets={"schema": schema_facet},
                    )
                )

        self.logger.debug(
            f"Collected {len(inputs)} inputs and {len(outputs)} outputs for job={job_name}"
        )
        return inputs, outputs
