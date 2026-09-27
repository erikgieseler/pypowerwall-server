"""
Tests for MQTT HA controls autodiscovery (broker-trust, no token in payload).

Covers:
- build_discovery_payloads(controls_enabled=True) adds 6 control entities
- Controls are absent when MQTT_CONTROLS_ENABLED=no or PW_CONTROL_SECRET missing
- Controls require broker auth (MQTT_USERNAME + MQTT_PASSWORD)
- Control topics are publishable via _control_message_loop with validation
- Reserve rejects booleans (bool is an int subclass)
- Islanding is rejected unless the gateway is PW3 v1r
- Discovery re-fires when v1r capability arrives late (cold start)
- Cloud-mode gateways are driven on their own connection first
"""
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.mqtt.ha_discovery import build_discovery_payloads
from app.mqtt.publisher import MqttPublisher
from app.models.gateway import Gateway, GatewayStatus, PowerwallData


def test_controls_disabled_no_extra_entities():
    results = build_discovery_payloads(
        gateway_id="home",
        gateway_name="Home",
        topic_prefix="pypowerwall",
        ha_prefix="homeassistant",
        controls_enabled=False,
    )
    assert len(results) == 23


def test_controls_enabled_adds_six_entities():
    # Without PW3 v1r: 4 controls (no islanding)
    results = build_discovery_payloads(
        gateway_id="home",
        gateway_name="Home",
        topic_prefix="pypowerwall",
        ha_prefix="homeassistant",
        controls_enabled=True,
        is_pv3_v1r=False,
    )
    assert len(results) == 27
    topics = {t for t, _ in results}
    assert "homeassistant/number/pypowerwall_home_reserve_control/config" in topics
    assert "homeassistant/select/pypowerwall_home_mode_control/config" in topics
    assert "homeassistant/switch/pypowerwall_home_grid_charging_control/config" in topics
    assert "homeassistant/select/pypowerwall_home_grid_export_control/config" in topics
    assert "homeassistant/button/pypowerwall_home_go_off_grid/config" not in topics

    # With PW3 v1r: +2 islanding buttons
    results_v1r = build_discovery_payloads(
        gateway_id="home",
        gateway_name="Home",
        topic_prefix="pypowerwall",
        ha_prefix="homeassistant",
        controls_enabled=True,
        is_pv3_v1r=True,
    )
    assert len(results_v1r) == 29
    topics_v1r = {t for t, _ in results_v1r}
    assert "homeassistant/button/pypowerwall_home_go_off_grid/config" in topics_v1r
    assert "homeassistant/button/pypowerwall_home_reconnect_grid/config" in topics_v1r


def test_reserve_control_payload():
    results = {t: json.loads(p) for t, p in build_discovery_payloads("home", "Home", "pypowerwall", "homeassistant", controls_enabled=True)}
    payload = results["homeassistant/number/pypowerwall_home_reserve_control/config"]
    assert payload["command_topic"] == "pypowerwall/home/control/reserve/set"
    assert payload["state_topic"] == "pypowerwall/home/reserve"
    assert payload["min"] == 0
    assert payload["max"] == 100
    assert payload["step"] == 1


@pytest.mark.asyncio
async def test_control_message_reserve_valid(monkeypatch):
    from app.config import settings
    from app.core.gateway_manager import gateway_manager

    monkeypatch.setattr(settings, "mqtt_host", "localhost")
    monkeypatch.setattr(settings, "mqtt_topic_prefix", "pypowerwall")
    monkeypatch.setattr(settings, "mqtt_controls_enabled", True)
    monkeypatch.setattr(settings, "control_secret", "secret")

    gw = Gateway(id="home", name="Home", host="1.1.1.1", online=True)
    gateway_manager.gateways["home"] = gw
    gateway_manager._cloud_control = None

    mock_local = AsyncMock(return_value={"ok": True})
    monkeypatch.setattr(gateway_manager, "local_control", mock_local)
    monkeypatch.setattr(gateway_manager, "cloud_control", AsyncMock(return_value=None))

    pub = MqttPublisher()

    class FakeMsg:
        def __init__(self, topic, payload, retain=False):
            self.topic = MagicMock()
            self.topic.value = topic
            self.payload = payload.encode()
            self.retain = retain

    class FakeClient:
        def __init__(self):
            self._queue = asyncio.Queue()

        async def subscribe(self, topic, qos=1):
            pass

        @property
        def messages(self):
            return self

        def __aiter__(self):
            return self

        async def __anext__(self):
            return await self._queue.get()

        async def put(self, msg):
            await self._queue.put(msg)

    client = FakeClient()
    task = asyncio.create_task(pub._control_message_loop(client))
    await asyncio.sleep(0.05)
    await client.put(FakeMsg("pypowerwall/home/control/reserve/set", json.dumps({"value": 50})))
    await asyncio.sleep(0.2)
    mock_local.assert_called_once()
    assert mock_local.call_args[0][1] == "set_reserve"

    # Invalid reserve should not call
    mock_local.reset_mock()
    await client.put(FakeMsg("pypowerwall/home/control/reserve/set", json.dumps({"value": 150})))
    await asyncio.sleep(0.2)
    mock_local.assert_not_called()

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    del gateway_manager.gateways["home"]


@pytest.mark.asyncio
async def test_control_message_invalid_json_ignored(monkeypatch):
    from app.config import settings
    from app.core.gateway_manager import gateway_manager

    monkeypatch.setattr(settings, "mqtt_host", "localhost")
    monkeypatch.setattr(settings, "mqtt_topic_prefix", "pypowerwall")

    pub = MqttPublisher()

    class FakeMsg:
        def __init__(self, topic, payload):
            self.topic = MagicMock()
            self.topic.value = topic
            self.payload = payload.encode()
            self.retain = False

    class FakeClient:
        def __init__(self):
            self._queue = asyncio.Queue()

        async def subscribe(self, topic, qos=1):
            pass

        @property
        def messages(self):
            return self

        def __aiter__(self):
            return self

        async def __anext__(self):
            return await self._queue.get()

        async def put(self, msg):
            await self._queue.put(msg)

    client = FakeClient()
    task = asyncio.create_task(pub._control_message_loop(client))
    await asyncio.sleep(0.05)
    # malformed JSON should be ignored, not crash
    await client.put(FakeMsg("pypowerwall/home/control/mode/set", "not json"))
    await asyncio.sleep(0.2)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_mqtt_controls_require_broker_auth(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "mqtt_host", "localhost")
    monkeypatch.setattr(settings, "mqtt_controls_enabled", True)
    monkeypatch.setattr(settings, "control_secret", "secret")
    # No broker user/password -> controls unavailable (open broker)
    monkeypatch.setattr(settings, "mqtt_username", None)
    monkeypatch.setattr(settings, "mqtt_password", None)
    assert settings.mqtt_controls_available is False
    # Username alone is not enough (ACLs are enforced per user+password)
    monkeypatch.setattr(settings, "mqtt_username", "user")
    assert settings.mqtt_controls_available is False
    monkeypatch.setattr(settings, "mqtt_password", "pass")
    assert settings.mqtt_controls_available is True


class _Msg:
    def __init__(self, topic, payload, retain=False):
        self.topic = MagicMock()
        self.topic.value = topic
        self.payload = payload.encode()
        self.retain = retain


class _Client:
    def __init__(self):
        self._queue = asyncio.Queue()

    async def subscribe(self, topic, qos=1):
        pass

    @property
    def messages(self):
        return self

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self._queue.get()

    async def put(self, msg):
        await self._queue.put(msg)


async def _run_control_messages(monkeypatch, gateway_id, gw_kwargs, messages,
                                cloud_result=None, hybrid=None):
    """Feed control messages through the loop; return (mock_local, mock_cloud)."""
    from app.core.gateway_manager import gateway_manager

    gw = Gateway(id=gateway_id, name=gateway_id, online=True, **gw_kwargs)
    gateway_manager.gateways[gateway_id] = gw
    mock_local = AsyncMock(return_value={"ok": True})
    mock_cloud = AsyncMock(return_value=cloud_result)
    monkeypatch.setattr(gateway_manager, "local_control", mock_local)
    monkeypatch.setattr(gateway_manager, "cloud_control", mock_cloud)
    if hybrid is None:
        hybrid = cloud_result is not None
    monkeypatch.setattr(gateway_manager, "_cloud_control",
                        object() if hybrid else None)

    pub = MqttPublisher()
    client = _Client()
    task = asyncio.create_task(pub._control_message_loop(client))
    await asyncio.sleep(0.05)
    for topic, payload in messages:
        await client.put(_Msg(topic, payload))
    await asyncio.sleep(0.3)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    finally:
        del gateway_manager.gateways[gateway_id]
    return mock_local, mock_cloud


@pytest.mark.asyncio
async def test_control_message_reserve_rejects_bool(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "mqtt_topic_prefix", "pypowerwall")

    # True/False must not pass as 1/0 (bool subclasses int)
    for bad in (True, False):
        mock_local, _ = await _run_control_messages(
            monkeypatch, "home", {"host": "1.1.1.1"},
            [("pypowerwall/home/control/reserve/set", json.dumps({"value": bad}))],
        )
        mock_local.assert_not_called()


@pytest.mark.asyncio
async def test_control_message_islanding_needs_v1r(monkeypatch):
    from app.config import settings
    from app.core.gateway_manager import gateway_manager

    monkeypatch.setattr(settings, "mqtt_topic_prefix", "pypowerwall")
    payload = json.dumps({"action": "off_grid", "confirm": True})

    # Plain TEDAPI gateway (no RSA key): islanding rejected
    mock_local, _ = await _run_control_messages(
        monkeypatch, "plain", {"host": "1.1.1.1"},
        [("pypowerwall/plain/control/islanding/set", payload)],
    )
    mock_local.assert_not_called()

    # RSA gateway but hardware unknown (cold start, pw3 None): rejected
    mock_local, _ = await _run_control_messages(
        monkeypatch, "cold", {"host": "1.1.1.1", "rsa_key_configured": True},
        [("pypowerwall/cold/control/islanding/set", payload)],
    )
    mock_local.assert_not_called()

    # RSA + confirmed PW3 hardware: routed to go_off_grid
    status = GatewayStatus(
        gateway=Gateway(id="v1r", name="V1R", host="1.1.1.1",
                        rsa_key_configured=True, online=True),
        data=PowerwallData(pw3=True), online=True, last_updated=1.0,
    )
    monkeypatch.setattr(gateway_manager, "get_gateway", lambda gid: status)
    mock_local, _ = await _run_control_messages(
        monkeypatch, "v1r", {"host": "1.1.1.1", "rsa_key_configured": True},
        [("pypowerwall/v1r/control/islanding/set", payload)],
    )
    mock_local.assert_called_once()
    assert mock_local.call_args[0][1] == "go_off_grid"


def test_discovery_signature_tracks_v1r():
    from app.mqtt.ha_discovery import discovery_signature

    off = discovery_signature(None, None, controls_enabled=True, is_pv3_v1r=False)
    on = discovery_signature(None, None, controls_enabled=True, is_pv3_v1r=True)
    assert ("controls", False) in off
    assert ("controls", True) in on
    assert not on <= off  # capability flip re-fires discovery
    # Controls disabled: no tracking key (unchanged legacy behavior)
    assert discovery_signature(None, None) == frozenset()


@pytest.mark.asyncio
async def test_discovery_resent_when_v1r_arrives_late(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "mqtt_host", "localhost")
    monkeypatch.setattr(settings, "mqtt_username", "user")
    monkeypatch.setattr(settings, "mqtt_password", "pass")
    monkeypatch.setattr(settings, "mqtt_controls_enabled", True)
    monkeypatch.setattr(settings, "control_secret", "secret")
    monkeypatch.setattr(settings, "mqtt_ha_discovery", True)
    monkeypatch.setattr(settings, "mqtt_topic_prefix", "pypowerwall")
    monkeypatch.setattr(settings, "mqtt_ha_prefix", "homeassistant")
    monkeypatch.setattr(settings, "mqtt_qos", 1)
    monkeypatch.setattr(settings, "mqtt_retain", True)

    pub = MqttPublisher()
    pub._client = AsyncMock()
    pub._connected = True
    topics = []

    async def fake_safe(self, topic, payload, retain, qos):
        topics.append(topic)

    monkeypatch.setattr(MqttPublisher, "_safe_publish", fake_safe)

    def make_status(rsa, pw3):
        gw = Gateway(id="gw", name="GW", host="1.1.1.1",
                     rsa_key_configured=rsa, online=True)
        return GatewayStatus(gateway=gw, data=PowerwallData(pw3=pw3),
                             online=True, last_updated=1.0)

    await pub.publish_gateway("gw", make_status(False, None))
    first = len([t for t in topics if "homeassistant" in t])
    assert first > 0

    # Same capability: no re-send
    topics.clear()
    await pub.publish_gateway("gw", make_status(False, None))
    assert [t for t in topics if "homeassistant" in t] == []

    # Hardware resolves to PW3 v1r: discovery re-fires with buttons
    topics.clear()
    await pub.publish_gateway("gw", make_status(True, True))
    resent = [t for t in topics if "homeassistant" in t]
    assert len(resent) > 0
    assert any("go_off_grid" in t for t in resent)


@pytest.mark.asyncio
async def test_cloud_gateway_uses_own_connection(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "mqtt_topic_prefix", "pypowerwall")

    # Hybrid cloud present, but a cloud_mode gateway must be driven on its
    # own connection (shared cloud_control cannot target a gateway)
    mock_local, mock_cloud = await _run_control_messages(
        monkeypatch, "remote",
        {"cloud_mode": True, "email": "user@example.com"},
        [("pypowerwall/remote/control/reserve/set", json.dumps({"value": 20}))],
        cloud_result={"ok": True},
    )
    mock_local.assert_called_once()
    assert mock_local.call_args[0][0] == "remote"
    assert mock_local.call_args[0][1] == "set_reserve"
    mock_cloud.assert_not_called()

    # TEDAPI gateway with hybrid cloud keeps cloud-first + local fallback
    mock_local, mock_cloud = await _run_control_messages(
        monkeypatch, "home", {"host": "1.1.1.1"},
        [("pypowerwall/home/control/mode/set", json.dumps({"value": "backup"}))],
        cloud_result=None, hybrid=True,
    )
    mock_cloud.assert_called_once()
    mock_local.assert_called_once()
    assert mock_local.call_args[0][0] == "home"
