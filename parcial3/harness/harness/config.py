import copy
import os

import yaml

from . import registry

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_yaml(path):
    with open(path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_config(path=None) -> dict:
    if registry.CONFIG_OVERRIDE is not None:
        return copy.deepcopy(registry.CONFIG_OVERRIDE)
    path = path or os.path.join(ROOT, "config.yaml")
    if not os.path.exists(path):
        raise FileNotFoundError(f"{path} not found. Copy config.example.yaml to config.yaml and fill it in.")
    return _load_yaml(path)


def load_thresholds(path=None) -> dict:
    if registry.THRESHOLDS_OVERRIDE is not None:
        return copy.deepcopy(registry.THRESHOLDS_OVERRIDE)
    return _load_yaml(path or os.path.join(ROOT, "thresholds.yaml"))


def load_pricing(path=None) -> dict:
    return _load_yaml(path or os.path.join(ROOT, "pricing.yaml"))
