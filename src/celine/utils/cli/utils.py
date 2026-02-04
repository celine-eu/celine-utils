import json
import sys


def load_json_config(file_path: str):
    """Load JSON configuration from file or stdin"""
    if file_path == "-":
        return json.load(sys.stdin)
    else:
        with open(file_path, "r") as f:
            return json.load(f)
