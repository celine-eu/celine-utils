import os, json, yaml
from typing import Any
from celine.common.logger import get_logger

logger = get_logger(__name__)


def make_dataset(namespace: str, name: str, schema=None, stats=None):
    facets = {}
    if schema:
        facets["schema"] = {"fields": list(schema.keys())}
    if stats:
        facets["outputStatistics"] = stats
    return {"namespace": namespace, "name": name, "facets": facets}


class MeltanoLineage:
    """Helper to discover Meltano lineage (datasets in/out)."""

    def __init__(
        self, cfg, config_path: str = "meltano.yml", run_dir: str = ".meltano/run"
    ):
        self.cfg = cfg
        self.config_path = config_path
        self.run_dir = run_dir
        self.config = self._load_meltano_config()

    # ---------- Helpers ----------
    def _load_meltano_config(self) -> dict[str, Any]:
        project_root = self.cfg.meltano_project_root or "."
        path = os.path.join(project_root, self.config_path)
        logger.debug(f"Loading Meltano config from {path}")
        try:
            with open(path) as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            logger.warning(
                f"No Meltano config found at {path}, skipping lineage discovery."
            )
            return {}
        except Exception as e:
            logger.error(f"Failed to load Meltano config at {path}: {e}")
            return {}

    def _collect_inputs_outputs(self, job_name: str):
        """Collect dataset info from Meltano taps/loaders config."""
        if not self.config:
            return [], []

        tap_name = job_name.split(":")[1].split("-to-")[0]
        inputs, outputs = [], []

        # Inputs from tap properties JSON
        props_file = os.path.join(self.run_dir, tap_name, "tap.properties.json")
        if os.path.exists(props_file):
            try:
                with open(props_file) as f:
                    props = json.load(f)
                for s in props.get("streams", []):
                    schema_props = s.get("schema", {}).get("properties", {})
                    inputs.append(
                        make_dataset(
                            namespace=f"celine.raw.{self.cfg.app_name}",
                            name=s["tap_stream_id"],
                            schema=schema_props,
                        )
                    )
            except Exception as e:
                logger.warning(f"Failed to parse {props_file}: {e}")

        # Outputs from schema_mapping in meltano.yml
        loaders = self.config.get("plugins", {}).get("loaders", [])
        for loader in loaders:
            if loader["name"].startswith("target-postgres"):
                schema_map = loader.get("schema_mapping", {})
                for tap, mapping in schema_map.items():
                    if tap == tap_name:
                        for _, tgt_schema in mapping.items():
                            outputs.append(
                                make_dataset(
                                    namespace=f"celine.raw.{self.cfg.app_name}",
                                    name=f"{tgt_schema}.{tap_name}",
                                )
                            )

        logger.debug(
            f"Collected {len(inputs)} inputs and {len(outputs)} outputs for job={job_name}"
        )
        return inputs, outputs
