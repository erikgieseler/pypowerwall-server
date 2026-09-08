"""Tests for MQTT control command handling and grid capability gating.

No broker needed: a fake aiomqtt-style client drives
``MqttPublisher._handle_mqtt_commands`` directly.
"""
import pytest
from types import SimpleNamespace
from unittest.mock import Mock

from app.config import settings
from app.core.gateway_manager import gateway_manager
from app.models.gateway import Gateway
from app.mqtt.ha_discovery import build_discovery_payloads
from app.mqtt.publisher import mqtt_publisher


class _FakeMessages:
    def __init__(self, msgs):
        self._msgs = msgs

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def __aiter__(self):
        async def gen():
            for m in self._msgs:
                yield m
        return gen()


class _FakeClient:
    def __init__(self, msgs):
        self._msgs = msgs
        self.subscribed = []

    async def subscribe(self, topic):
        self.subscribed.append(topic)

    def messages(self):
        return _FakeMessages(self._msgs)


def _msg(topic, payload):
    return SimpleNamespace(topic=SimpleNamespace(value=topic), payload=payload)


def _track_calls():
    """Patch cloud/local control with recorders; returns (calls, restore)."""
    calls = []

    async def fake_cloud(method, *args, **kwargs):
        calls.append((method, args))
        return {"ok": True}

    async def fake_local(gid, method, *args, **kwargs):
        calls.append(("local:" + gid + ":" + method, args))
        return {"ok": True}

    gateway_manager.cloud_control = fake_cloud  # type: ignore[method-assign]
    gateway_manager.local_control = fake_local  # type: ignore[method-assign]

    def restore():
        for attr in ("cloud_control", "local_control"):
            if attr in gateway_manager.__dict__:
                delattr(gateway_manager, attr)

    return calls, restore


@pytest.mark.asyncio
async def test_mqtt_commands_dispatch_all_four(monkeypatch):
    """Valid reserve/mode/grid commands reach cloud control with parsed values."""
    monkeypatch.setattr(settings, "control_secret", "test-secret")
    old_shutdown = mqtt_publisher._shutdown
    mqtt_publisher._shutdown = False
    gateway_manager.gateways["default"] = Gateway(id="default", name="Default")
    gateway_manager._cloud_control = Mock()
    gateway_manager._cloud_control_configured = True
    calls, restore = _track_calls()
    try:
        pre = settings.mqtt_topic_prefix.rstrip("/")
        msgs = [
            _msg(pre + "/default/reserve/set", b"42"),
            _msg(pre + "/default/mode/set", b"backup"),
            _msg(pre + "/default/grid_charging/set", b"ON"),
            _msg(pre + "/default/grid_export/set", b"never"),
        ]
        await mqtt_publisher._handle_mqtt_commands(_FakeClient(msgs))
        assert ("set_reserve", (42,)) in calls
        assert ("set_mode", ("backup",)) in calls
        assert ("set_grid_charging", (True,)) in calls
        assert ("set_grid_export", ("never",)) in calls
        assert len(calls) == 4
    finally:
        restore()
        gateway_manager._cloud_control = None
        mqtt_publisher._shutdown = old_shutdown


@pytest.mark.asyncio
async def test_mqtt_commands_reject_invalid(monkeypatch):
    """Malformed payloads are skipped without any control call."""
    monkeypatch.setattr(settings, "control_secret", "test-secret")
    old_shutdown = mqtt_publisher._shutdown
    mqtt_publisher._shutdown = False
    gateway_manager.gateways["default"] = Gateway(id="default", name="Default")
    gateway_manager._cloud_control = Mock()
    gateway_manager._cloud_control_configured = True
    calls, restore = _track_calls()
    try:
        pre = settings.mqtt_topic_prefix.rstrip("/")
        msgs = [
            _msg(pre + "/default/reserve/set", b"abc"),
            _msg(pre + "/default/reserve/set", b"20.5"),
            _msg(pre + "/default/reserve/set", b"150"),
            _msg(pre + "/default/mode/set", b"bogus"),
            _msg(pre + "/default/grid_charging/set", b"maybe"),
            _msg(pre + "/default/grid_export/set", b"everything"),
            _msg(pre + "/default/bogus/set", b"x"),
            _msg(pre + "/default/reserve", b"10"),
            _msg("other/reserve/set", b"10"),
        ]
        await mqtt_publisher._handle_mqtt_commands(_FakeClient(msgs))
        assert calls == []
    finally:
        restore()
        gateway_manager._cloud_control = None
        mqtt_publisher._shutdown = old_shutdown


@pytest.mark.asyncio
async def test_mqtt_commands_need_control_enabled(monkeypatch):
    """Without control secret, commands are ignored."""
    monkeypatch.setattr(settings, "control_secret", None)
    old_shutdown = mqtt_publisher._shutdown
    mqtt_publisher._shutdown = False
    gateway_manager.gateways["default"] = Gateway(id="default", name="Default")
    gateway_manager._cloud_control = Mock()
    calls, restore = _track_calls()
    try:
        pre = settings.mqtt_topic_prefix.rstrip("/")
        await mqtt_publisher._handle_mqtt_commands(
            _FakeClient([_msg(pre + "/default/reserve/set", b"10")])
        )
        assert calls == []
    finally:
        restore()
        gateway_manager._cloud_control = None
        mqtt_publisher._shutdown = old_shutdown


@pytest.mark.asyncio
async def test_mqtt_commands_unknown_gateway_falls_back_to_default(monkeypatch):
    """Unknown gateway id falls back to 'default' for local control."""
    monkeypatch.setattr(settings, "control_secret", "test-secret")
    old_shutdown = mqtt_publisher._shutdown
    mqtt_publisher._shutdown = False
    gateway_manager.gateways["default"] = Gateway(
        id="default", name="Default", host="192.168.91.1", gw_pwd="secret"
    )
    gateway_manager._cloud_control = None
    calls, restore = _track_calls()
    try:
        pre = settings.mqtt_topic_prefix.rstrip("/")
        await mqtt_publisher._handle_mqtt_commands(
            _FakeClient([_msg(pre + "/nope/mode/set", b"autonomous")])
        )
        assert ("local:default:set_mode", ("autonomous",)) in calls
    finally:
        restore()
        mqtt_publisher._shutdown = old_shutdown


@pytest.mark.asyncio
async def test_mqtt_commands_skip_incapable_gateway(monkeypatch):
    """Basic LAN without cloud exposes no grid control: grid command skipped,
    reserve/mode still pass through local control."""
    monkeypatch.setattr(settings, "control_secret", "test-secret")
    old_shutdown = mqtt_publisher._shutdown
    mqtt_publisher._shutdown = False
    gateway_manager.gateways["b"] = Gateway(
        id="b", name="B", host="10.0.0.2", basic_lan=True
    )
    gateway_manager._cloud_control = None
    calls, restore = _track_calls()
    try:
        pre = settings.mqtt_topic_prefix.rstrip("/")
        await mqtt_publisher._handle_mqtt_commands(
            _FakeClient([
                _msg(pre + "/b/grid_export/set", b"never"),
                _msg(pre + "/b/mode/set", b"backup"),
            ])
        )
        assert ("local:b:set_mode", ("backup",)) in calls
        assert not any("grid_export" in c[0] for c in calls)
    finally:
        restore()
        mqtt_publisher._shutdown = old_shutdown


def test_gateway_grid_capable_hybrid():
    """Hybrid cloud configured covers every gateway."""
    gateway_manager._cloud_control_configured = True
    gateway_manager.gateways["x"] = Gateway(id="x", name="X", basic_lan=True)
    try:
        assert gateway_manager.gateway_grid_capable("x") is True
    finally:
        gateway_manager._cloud_control_configured = False


def test_gateway_grid_capable_local_tedapi():
    """Local TEDAPI/v1r gateway: reads work, capability true."""
    gw = Gateway(id="t", name="T", host="192.168.91.1", gw_pwd="secret")
    gateway_manager.gateways["t"] = gw
    try:
        assert gateway_manager.gateway_grid_capable("t") is True
    finally:
        del gateway_manager.gateways["t"]


def test_gateway_grid_capable_basic_lan_only():
    """Basic LAN without cloud exposes no grid control."""
    gw = Gateway(id="b", name="B", host="10.0.0.2", basic_lan=True)
    gateway_manager.gateways["b"] = gw
    try:
        assert gateway_manager.gateway_grid_capable("b") is False
    finally:
        del gateway_manager.gateways["b"]


def test_gateway_grid_capable_unknown():
    assert gateway_manager.gateway_grid_capable("nope") is False


def test_discovery_grid_entities_gated(monkeypatch):
    """Grid entities only with control enabled AND grid capability."""
    monkeypatch.setattr(settings, "control_secret", "test-secret")

    def topics(**kw):
        return [t for t, _ in build_discovery_payloads(
            gateway_id="home", gateway_name="Home",
            topic_prefix="pypowerwall", ha_prefix="homeassistant", **kw)]

    full = topics(grid_available=True)
    assert any("/switch/" in t and "grid_charging" in t for t in full)
    assert any("/select/" in t and "grid_export" in t for t in full)
    # reserve/mode stay regardless of grid capability
    assert any("reserve_control" in t for t in full)
    assert any("mode_control" in t for t in full)

    limited = topics(grid_available=False)
    assert not any("grid_charging_control" in t for t in limited)
    assert not any("grid_export_control" in t for t in limited)
    assert any("reserve_control" in t for t in limited)
    assert any("mode_control" in t for t in limited)


def test_discovery_no_control_entities_without_secret(monkeypatch):
    """Without control secret, no writable entities are advertised."""
    monkeypatch.setattr(settings, "control_secret", None)
    topics = [t for t, _ in build_discovery_payloads(
        gateway_id="home", gateway_name="Home",
        topic_prefix="pypowerwall", ha_prefix="homeassistant")]
    assert not any("reserve_control" in t for t in topics)
    assert not any("mode_control" in t for t in topics)
    assert not any("grid_charging_control" in t for t in topics)
    assert not any("grid_export_control" in t for t in topics)
