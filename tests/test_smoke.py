"""Smoke test: the workspace packages resolve from the shared virtual environment."""

import common
import inventory_worker
import order_api


def test_workspace_packages_are_importable() -> None:
    assert common.__name__ == "common"
    assert order_api.__name__ == "order_api"
    assert inventory_worker.__name__ == "inventory_worker"
