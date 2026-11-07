"""M4: alert dispatch must not resurrect zombie workers or callbacks."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from hyperdata_terminal.data_layer.alerts import AlertManager


def test_alert_manager_has_no_disabled_dispatch_callbacks():
    for name in (
        "INTEL_INTERVAL",
        "_intel_loop",
        "_check_liquidation",
        "_check_smart_money",
        "_check_hlp_zscore",
        "_should_alert",
    ):
        assert not hasattr(AlertManager, name), name


@pytest.mark.asyncio
async def test_alert_lifecycle_registers_no_hub_callbacks_and_starts_no_task():
    manager = AlertManager()
    hub = SimpleNamespace(on_liquidation=Mock(), on_signal=Mock())

    manager.attach(hub)
    await manager.start()
    try:
        assert not hasattr(manager, "_intel_task")
        hub.on_liquidation.assert_not_called()
        hub.on_signal.assert_not_called()
    finally:
        await manager.stop()


@pytest.mark.asyncio
async def test_stop_is_safe_without_start():
    manager = AlertManager()

    await manager.stop()
    await manager.stop()
