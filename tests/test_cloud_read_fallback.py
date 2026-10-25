"""Tests for the opt-in Cloud-Read-Fallback after local poll failures."""
import time
from unittest.mock import Mock

import pytest

from app.config import settings
from app.core.gateway_manager import GatewayManager
from app.models.gateway import Gateway, GatewayStatus, PowerwallData


def _manager(monkeypatch, *, bound=True, cloud=True, failover=True, flag=True):
    gm = GatewayManager()
    gm._poll_interval = 4
    gw = Gateway(id="v1r", name="v1r", host="10.0.0.2")
    gm.gateways["v1r"] = gw
    gm.cache["v1r"] = GatewayStatus(gateway=gw, online=False)
    gm.connections["v1r"] = Mock()
    gm._cloud_control = Mock(cloudmode=cloud, fleetapi=False, mode="cloud") if cloud else Mock(
        cloudmode=False, fleetapi=False, mode="local"
    )
    gm._cloud_control_gateway_id = "v1r" if bound else None
    gm._next_poll_time["v1r"] = 0
    gm._consecutive_failures["v1r"] = 0
    gm._record_timeseries_sample = Mock()
    gm._record_signal_sample = Mock()
    gm._schedule_mqtt_publish = Mock()
    monkeypatch.setattr(settings, "cloud_read_fallback", flag)
    monkeypatch.setattr(settings, "failover", failover)
    return gm


def _data(source="cloud"):
    return PowerwallData(
        aggregates={
            "solar": {"instant_power": 1000},
            "site": {"instant_power": 0},
            "battery": {"instant_power": 0},
            "load": {"instant_power": 1000},
        },
        soe=50.0,
        timestamp=time.time(),
    )


def _install_fetch(gm, *, local_data=None, cloud_data=None, calls=None):
    async def fake_fetch(gateway_id, pw, *, cloud_fallback=False):
        calls.append((pw is gm._cloud_control, cloud_fallback))
        if pw is gm._cloud_control:
            return cloud_data
        if local_data is None:
            raise RuntimeError("local down")
        return local_data

    gm._fetch_gateway_data = fake_fetch


@pytest.mark.asyncio
async def test_cloud_fallback_triggers_on_third_local_failure(monkeypatch):
    gm = _manager(monkeypatch)
    calls = []
    _install_fetch(gm, cloud_data=_data(), calls=calls)

    await gm._poll_gateway("v1r")
    assert calls == [(False, False)]
    assert gm._cloud_read_state.get("v1r") is None

    gm._next_poll_time["v1r"] = 0
    await gm._poll_gateway("v1r")
    assert calls[-1] == (False, False)
    assert gm._cloud_read_state.get("v1r") is None

    gm._next_poll_time["v1r"] = 0
    await gm._poll_gateway("v1r")
    assert calls[-1] == (True, True)
    status = gm.cache["v1r"]
    assert status.online is True
    assert status.data_source == "cloud"
    assert status.local_online is False
    assert status.data is not None
    assert gm._cloud_read_state["v1r"]["active"] is True
    assert gm._next_poll_time["v1r"] >= time.time() + 3


@pytest.mark.asyncio
async def test_cloud_fallback_off_never_uses_cloud(monkeypatch):
    gm = _manager(monkeypatch, flag=False)
    calls = []
    _install_fetch(gm, cloud_data=_data(), calls=calls)

    for _ in range(3):
        gm._next_poll_time["v1r"] = 0
        await gm._poll_gateway("v1r")

    assert gm.cache["v1r"].online is False
    assert all(not is_cloud for is_cloud, _ in calls)


@pytest.mark.asyncio
async def test_cloud_fallback_does_not_trigger_on_first_failure(monkeypatch):
    gm = _manager(monkeypatch)
    calls = []
    _install_fetch(gm, cloud_data=_data(), calls=calls)
    await gm._poll_gateway("v1r")
    assert not any(is_cloud for is_cloud, _ in calls)
    assert gm.cache["v1r"].online is False


@pytest.mark.asyncio
async def test_cloud_fallback_failure_keeps_offline_and_records_error(monkeypatch):
    gm = _manager(monkeypatch)
    calls = []
    _install_fetch(gm, cloud_data=None, calls=calls)

    for _ in range(3):
        gm._next_poll_time["v1r"] = 0
        await gm._poll_gateway("v1r")

    assert gm.cache["v1r"].online is False
    state = gm._cloud_read_state["v1r"]
    assert state["active"] is True
    assert state["failures"] == 1
    assert "aggregates" in state["last_error"]


@pytest.mark.asyncio
async def test_local_recovery_clears_cloud_state(monkeypatch):
    gm = _manager(monkeypatch)
    gm._cloud_read_state["v1r"] = gm._new_cloud_read_state()
    gm._cloud_read_state["v1r"]["active"] = True
    calls = []
    _install_fetch(gm, local_data=_data(), calls=calls)

    await gm._poll_gateway("v1r")

    assert gm.cache["v1r"].data_source == "local"
    assert gm.cache["v1r"].local_online is True
    assert "v1r" not in gm._cloud_read_state
    assert all(not is_cloud for is_cloud, _ in calls)


@pytest.mark.asyncio
async def test_cloud_fallback_requires_bound_site(monkeypatch):
    gm = _manager(monkeypatch, bound=False)
    calls = []
    _install_fetch(gm, cloud_data=_data(), calls=calls)

    for _ in range(3):
        gm._next_poll_time["v1r"] = 0
        await gm._poll_gateway("v1r")

    assert gm.cache["v1r"].online is False
    assert all(not is_cloud for is_cloud, _ in calls)


@pytest.mark.asyncio
async def test_cloud_fallback_requires_cloud_client_and_failover(monkeypatch):
    gm = _manager(monkeypatch, cloud=False)
    assert gm._cloud_read_fallback_allowed("v1r") is False

    gm = _manager(monkeypatch, failover=False)
    assert gm._cloud_read_fallback_allowed("v1r") is False


@pytest.mark.asyncio
async def test_health_reports_degraded_for_cloud_fallback(monkeypatch):
    import app.main as main

    gm = _manager(monkeypatch)
    gm.cache["v1r"] = GatewayStatus(
        gateway=gm.gateways["v1r"],
        data=_data(),
        online=True,
        data_source="cloud",
        local_online=False,
    )
    gm._cloud_read_state["v1r"] = {
        "active": True,
        "since": time.time() - 5,
        "last_success": time.time(),
        "failures": 0,
        "last_error": None,
    }
    monkeypatch.setattr(main, "gateway_manager", gm)

    result = await main.health_check()

    assert result["status"] == "degraded"
    assert result["gateway_details"][0]["data_source"] == "cloud"
    assert result["gateway_details"][0]["local_online"] is False
    assert result["cloud_read_fallback"]["v1r"]["active"] is True


def test_cloud_read_fallback_defaults_off():
    assert settings.cloud_read_fallback is False


def test_cloud_read_state_snapshot_is_serializable(monkeypatch):
    gm = _manager(monkeypatch)
    gm._cloud_read_state["v1r"] = {
        "active": True,
        "since": time.time() - 5,
        "last_success": time.time(),
        "failures": 0,
        "last_error": None,
    }
    snapshot = gm.get_cloud_read_state("v1r")
    assert snapshot["active"] is True
    assert snapshot["duration_seconds"] >= 4
    assert snapshot["enabled"] is True
