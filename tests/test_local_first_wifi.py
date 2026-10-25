"""Tests for PW_LOCAL_FIRST_WIFI (signed WiFi-v1r after LAN failover)."""
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock, Mock

import pytest
from fastapi.testclient import TestClient

from app.config import GatewayConfig, settings
from app.core.gateway_manager import gateway_manager
from app.main import app
from app.models.gateway import Gateway, GatewayStatus, PowerwallData

_TOKEN = "local-first-wifi-test-secret"


def _gateway(gid="v1r", **kwargs):
    base = {"id": gid, "name": gid, "online": True, "host": "10.0.0.2"}
    base.update(kwargs)
    return Gateway(**base)


def _status(gateway, tedapi_mode, online=True):
    data = PowerwallData(
        tedapi_mode=tedapi_mode,
        mode="self_consumption",
        reserve=20.0,
        grid_charging=True,
        grid_export="pv_only",
        grid_status="UP",
    )
    return GatewayStatus(
        gateway=gateway,
        data=data,
        online=online,
        last_updated=time.time(),
    )


def _connection(lan_failed=True):
    conn = Mock()
    conn.tedapi_mode = "v1r"
    conn.tedapi = Mock()
    conn.tedapi.lan_failed = lan_failed
    conn.tedapi.v1r = True
    conn.tedapi.wifi_v1r_transport = Mock()
    conn.wifi_v1r_ready = Mock(return_value=True)
    return conn


@pytest.fixture
def gm(monkeypatch):
    monkeypatch.setattr(gateway_manager, "gateways", {})
    monkeypatch.setattr(gateway_manager, "cache", {})
    gw = _gateway("v1r")
    gateway_manager.gateways["v1r"] = gw
    gateway_manager.cache["v1r"] = _status(gw, "v1r")
    conn = _connection()
    monkeypatch.setattr(gateway_manager, "connections", {"v1r": conn})
    monkeypatch.setattr(
        gateway_manager, "local_control", AsyncMock(return_value={"ok": True})
    )
    monkeypatch.setattr(
        gateway_manager, "cloud_control", AsyncMock(return_value={"ok": True})
    )
    monkeypatch.setattr(
        gateway_manager, "wifi_v1r_preflight", AsyncMock(return_value=True)
    )
    monkeypatch.setattr(settings, "local_first", False)
    monkeypatch.setattr(settings, "local_first_wifi", True)
    monkeypatch.setattr(settings, "failover", True)
    return gateway_manager


@pytest.fixture
def control_client(monkeypatch):
    monkeypatch.setattr(settings, "control_secret", _TOKEN)
    return TestClient(app)


def test_local_first_wifi_defaults_to_off():
    assert settings.local_first_wifi is False


@pytest.mark.asyncio
async def test_wifi_branch_takes_write_after_probe(gm):
    label, result = await gm.control_write("v1r", "set_reserve", 20, timeout=10.0)
    assert label == "local v1r (wifi)"
    assert result == {"ok": True}
    gm.local_control.assert_awaited_once()
    gm.cloud_control.assert_not_awaited()


@pytest.mark.asyncio
async def test_wifi_branch_preflight_none_declines(gm):
    gm.wifi_v1r_preflight.return_value = None
    label, result = await gm.control_write("v1r", "set_reserve", 20, timeout=10.0)
    assert (label, result) == (None, None)
    gm.local_control.assert_not_awaited()


@pytest.mark.asyncio
async def test_wifi_branch_preflight_false_declines(gm):
    gm.wifi_v1r_preflight.return_value = False
    label, result = await gm.control_write("v1r", "set_reserve", 20, timeout=10.0)
    assert (label, result) == (None, None)
    gm.local_control.assert_not_awaited()


@pytest.mark.asyncio
async def test_wifi_branch_requires_lan_failed(gm):
    gm.connections["v1r"].tedapi.lan_failed = False
    label, result = await gm.control_write("v1r", "set_reserve", 20, timeout=10.0)
    assert (label, result) == (None, None)
    gm.wifi_v1r_preflight.assert_not_awaited()


@pytest.mark.asyncio
async def test_wifi_branch_ignores_offline_status(gm):
    gm.cache["v1r"].online = False
    label, result = await gm.control_write("v1r", "set_reserve", 20, timeout=10.0)
    assert label == "local v1r (wifi)"
    assert result == {"ok": True}
    gm.local_control.assert_awaited_once()


@pytest.mark.asyncio
async def test_wifi_branch_requires_connection(gm, monkeypatch):
    monkeypatch.setattr(gm, "connections", {})
    label, result = await gm.control_write("v1r", "set_reserve", 20, timeout=10.0)
    assert (label, result) == (None, None)
    gm.local_control.assert_not_awaited()


@pytest.mark.asyncio
async def test_wifi_flag_off_keeps_161_lan_failed_behavior(gm, monkeypatch):
    monkeypatch.setattr(settings, "local_first", True)
    monkeypatch.setattr(settings, "local_first_wifi", False)
    label, result = await gm.control_write("v1r", "set_reserve", 20, timeout=10.0)
    assert (label, result) == (None, None)
    gm.wifi_v1r_preflight.assert_not_awaited()
    gm.local_control.assert_not_awaited()


def test_http_reserve_uses_wifi_branch(control_client, gm):
    response = control_client.post(
        "/control/reserve",
        json={"value": 20},
        headers={"Authorization": _TOKEN},
    )
    assert response.status_code == 200
    gm.local_control.assert_awaited_once()
    gm.cloud_control.assert_not_awaited()


def test_http_islanding_wifi_preflight_false_is_503(control_client, gm):
    gm.wifi_v1r_preflight.return_value = False
    response = control_client.post(
        "/control/islanding",
        json={"value": "off_grid", "confirm": True},
        headers={"Authorization": _TOKEN},
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "WiFi v1r unavailable for islanding"
    gm.local_control.assert_not_awaited()


def test_http_islanding_wifi_preflight_true_dispatches(control_client, gm):
    gm.local_control.return_value = {"result": 1}
    response = control_client.post(
        "/control/islanding",
        json={"value": "off_grid", "confirm": True},
        headers={"Authorization": _TOKEN},
    )
    assert response.status_code == 200
    gm.local_control.assert_awaited_once()
    assert gm.local_control.await_args.args == ("v1r", "go_off_grid")
    assert gm.local_control.await_args.kwargs == {"timeout": 10.0, "confirm": True}


def test_http_max_backup_wifi_preflight_false_is_503(control_client, gm):
    gm.wifi_v1r_preflight.return_value = False
    response = control_client.post(
        "/control/max_backup",
        json={"value": 60},
        headers={"Authorization": _TOKEN},
    )
    assert response.status_code == 503
    assert response.json()["detail"] == "WiFi v1r unavailable for max backup"
    gm.local_control.assert_not_awaited()


def test_http_max_backup_wifi_preflight_true_dispatches(control_client, gm):
    gm.local_control.return_value = {"ok": True}
    response = control_client.post(
        "/control/max_backup",
        json={"value": 60},
        headers={"Authorization": _TOKEN},
    )
    assert response.status_code == 200
    gm.local_control.assert_awaited_once()
    assert gm.local_control.await_args.args == ("v1r", "schedule_max_backup", 60)


def test_stats_reports_local_first_wifi(control_client, gm):
    assert control_client.get("/stats").json()["config"]["PW_LOCAL_FIRST_WIFI"] is True


@pytest.mark.asyncio
async def test_wifi_v1r_preflight_direct(gm, monkeypatch):
    executor = ThreadPoolExecutor(max_workers=1)
    monkeypatch.setattr(gm, "_executor", executor)
    real = type(gm).wifi_v1r_preflight
    try:
        assert await real(gm, "v1r") is True
        gm.connections["v1r"].wifi_v1r_ready.return_value = False
        assert await real(gm, "v1r") is False
        gm.connections["v1r"].tedapi.lan_failed = False
        assert await real(gm, "v1r") is None
    finally:
        executor.shutdown(wait=True)


@pytest.mark.asyncio
async def test_wifi_v1r_preflight_not_applicable(gm, monkeypatch):
    real = type(gm).wifi_v1r_preflight
    monkeypatch.setattr(settings, "local_first_wifi", False)
    assert await real(gm, "v1r") is None
    monkeypatch.setattr(settings, "local_first_wifi", True)
    monkeypatch.setattr(settings, "failover", False)
    assert await real(gm, "v1r") is None
    monkeypatch.setattr(settings, "failover", True)
    gm.connections["v1r"].tedapi.wifi_v1r_transport = None
    assert await real(gm, "v1r") is False
    gm.connections["v1r"].tedapi.wifi_v1r_transport = Mock()
    gm.connections["v1r"].wifi_v1r_ready = None
    assert await real(gm, "v1r") is False
    gm.connections["v1r"].tedapi.v1r = False
    assert await real(gm, "v1r") is None


def test_gateway_can_v1r_write_covers_wifi_transport(gm, monkeypatch):
    from app.mqtt import publisher

    monkeypatch.setattr(publisher, "_gateway_is_v1r", lambda *args: False)
    assert publisher._gateway_can_v1r_write(gm, "v1r") is True
    assert publisher._write_path(gm, "v1r") == "local v1r"
    monkeypatch.setattr(settings, "local_first_wifi", False)
    assert publisher._gateway_can_v1r_write(gm, "v1r") is False
    assert publisher._write_path(gm, "v1r") is None


def test_local_connection_kwargs_passes_wifi_flag(monkeypatch):
    monkeypatch.setattr(settings, "local_first_wifi", True)
    config = GatewayConfig(
        id="gw",
        name="GW",
        host="10.42.1.40",
        gw_pwd="ABCDE12345",
        rsa_key_path="/tmp/tedapi_rsa_private.pem",
        wifi_host="192.168.91.1",
    )
    kwargs = gateway_manager._local_connection_kwargs(config)
    assert kwargs["wifi_v1r_fallback"] is True
    monkeypatch.setattr(settings, "local_first_wifi", False)
    kwargs = gateway_manager._local_connection_kwargs(config)
    assert "wifi_v1r_fallback" not in kwargs
