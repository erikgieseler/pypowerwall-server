"""
Tests for MQTT HA controls autodiscovery (broker-trust, no token in payload).

Covers:
- build_discovery_payloads(controls_enabled=True) adds 6 control entities
- Controls are absent when MQTT_CONTROLS_ENABLED=no or PW_CONTROL_SECRET missing
- Control topics are publishable via _control_message_loop with validation
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
    # Without PW3 v1r: 4 controls (no islanding) → 23+4=27
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

    # With PW3 v1r: +2 islanding buttons → 23+6=29
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
