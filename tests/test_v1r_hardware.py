"""Regression for v1r hardware label (copilot review on #112)."""
import pytest
from unittest.mock import Mock

from app.core.gateway_manager import gateway_manager
from app.models.gateway import Gateway


@pytest.mark.asyncio
async def test_v1r_cold_start_pw3_stays_null():
    """v1r without tedapi_config must leave pw3 as None (generic TEDAPI v1r)."""
    gw = Gateway(
        id="v1r-cold",
        name="V1R Cold",
        host="10.0.0.1",
        gw_pwd="x",
        rsa_key_path="/tmp/key",
        rsa_key_configured=True,
    )
    gateway_manager.gateways["v1r-cold"] = gw
    mock_pw = Mock()
    mock_pw.poll.return_value = {
        "site": {"instant_power": 0},
        "solar": {"instant_power": 0},
        "battery": {"instant_power": 0},
        "load": {"instant_power": 0},
    }
    mock_pw.level.return_value = 50.0
    mock_pw.tedapi = Mock()
    mock_pw.tedapi.pw3 = True
    mock_pw.tedapi.get_config.return_value = None
    mock_pw.tedapi.get_status.return_value = {}
    mock_pw.grid_status.return_value = "UP"
    mock_pw.get_mode.return_value = None
    mock_pw.get_reserve.return_value = None
    mock_pw.get_grid_charging.return_value = None
    mock_pw.get_grid_export.return_value = None
    mock_pw.system_status.return_value = {}
    mock_pw.vitals.return_value = {}
    mock_pw.strings.return_value = {}
    mock_pw.freq.return_value = 60.0
    mock_pw.status.return_value = "Running"
    mock_pw.version.return_value = "1.0"
    mock_pw.din.return_value = "D"
    mock_pw.uptime.return_value = "0h"
    mock_pw.site_name.return_value = "S"
    mock_pw.temps.return_value = {}
    mock_pw.alerts.return_value = []
    gateway_manager.connections["v1r-cold"] = mock_pw
    data = await gateway_manager._fetch_gateway_data("v1r-cold", mock_pw)
    assert data.pw3 is None


@pytest.mark.asyncio
async def test_non_v1r_preserves_transport_pw3_when_config_missing():
    """Non-v1r must keep transport pw3 flag even when config missing."""
    gw = Gateway(
        id="tedapi-plain",
        name="TEDAPI Plain",
        host="192.168.91.1",
        gw_pwd="x",
        rsa_key_configured=False,
    )
    gateway_manager.gateways["tedapi-plain"] = gw
    mock_pw = Mock()
    mock_pw.poll.return_value = {
        "site": {"instant_power": 0},
        "solar": {"instant_power": 0},
        "battery": {"instant_power": 0},
        "load": {"instant_power": 0},
    }
    mock_pw.level.return_value = 50.0
    mock_pw.tedapi = Mock()
    mock_pw.tedapi.pw3 = True
    mock_pw.tedapi.get_config.return_value = None
    mock_pw.grid_status.return_value = "UP"
    mock_pw.get_mode.return_value = None
    mock_pw.get_reserve.return_value = None
    mock_pw.get_grid_charging.return_value = None
    mock_pw.get_grid_export.return_value = None
    mock_pw.system_status.return_value = {}
    mock_pw.vitals.return_value = {}
    mock_pw.strings.return_value = {}
    mock_pw.freq.return_value = 60.0
    mock_pw.status.return_value = "Running"
    mock_pw.version.return_value = "1.0"
    mock_pw.din.return_value = "D"
    mock_pw.uptime.return_value = "0h"
    mock_pw.site_name.return_value = "S"
    mock_pw.temps.return_value = {}
    mock_pw.alerts.return_value = []
    gateway_manager.connections["tedapi-plain"] = mock_pw
    data = await gateway_manager._fetch_gateway_data("tedapi-plain", mock_pw)
    assert data.pw3 is True


@pytest.mark.asyncio
async def test_v1r_hardware_pw3_via_type_substring():
    """v1r with battery_blocks type containing Powerwall3 must be PW3."""
    gw = Gateway(
        id="v1r-pw3",
        name="V1R PW3",
        host="10.0.0.2",
        gw_pwd="x",
        rsa_key_path="/tmp/key",
        rsa_key_configured=True,
    )
    gateway_manager.gateways["v1r-pw3"] = gw
    mock_pw = Mock()
    mock_pw.poll.return_value = {
        "site": {"instant_power": 0},
        "solar": {"instant_power": 0},
        "battery": {"instant_power": 0},
        "load": {"instant_power": 0},
    }
    mock_pw.level.return_value = 50.0
    mock_pw.tedapi = Mock()
    mock_pw.tedapi.pw3 = False
    mock_pw.tedapi.get_config.return_value = {
        "battery_blocks": [{"type": "Powerwall3Follower"}]
    }
    mock_pw.grid_status.return_value = "UP"
    mock_pw.get_mode.return_value = None
    mock_pw.get_reserve.return_value = None
    mock_pw.get_grid_charging.return_value = None
    mock_pw.get_grid_export.return_value = None
    mock_pw.system_status.return_value = {}
    mock_pw.vitals.return_value = {}
    mock_pw.strings.return_value = {}
    mock_pw.freq.return_value = 60.0
    mock_pw.status.return_value = "Running"
    mock_pw.version.return_value = "1.0"
    mock_pw.din.return_value = "D"
    mock_pw.uptime.return_value = "0h"
    mock_pw.site_name.return_value = "S"
    mock_pw.temps.return_value = {}
    mock_pw.alerts.return_value = []
    gateway_manager.connections["v1r-pw3"] = mock_pw
    data = await gateway_manager._fetch_gateway_data("v1r-pw3", mock_pw)
    assert data.pw3 is True


@pytest.mark.asyncio
async def test_v1r_hardware_pw3_via_part_number():
    """v1r with partNumber 1707000 must be PW3 (fallback like pwModel)."""
    gw = Gateway(
        id="v1r-pn",
        name="V1R PN",
        host="10.0.0.3",
        gw_pwd="x",
        rsa_key_path="/tmp/key",
        rsa_key_configured=True,
    )
    gateway_manager.gateways["v1r-pn"] = gw
    mock_pw = Mock()
    mock_pw.poll.return_value = {
        "site": {"instant_power": 0},
        "solar": {"instant_power": 0},
        "battery": {"instant_power": 0},
        "load": {"instant_power": 0},
    }
    mock_pw.level.return_value = 50.0
    mock_pw.tedapi = Mock()
    mock_pw.tedapi.pw3 = False
    mock_pw.tedapi.get_config.return_value = {
        "battery_blocks": [{"type": "ACPW", "PackagePartNumber": "1707000-00-A"}]
    }
    mock_pw.grid_status.return_value = "UP"
    mock_pw.get_mode.return_value = None
    mock_pw.get_reserve.return_value = None
    mock_pw.get_grid_charging.return_value = None
    mock_pw.get_grid_export.return_value = None
    mock_pw.system_status.return_value = {}
    mock_pw.vitals.return_value = {}
    mock_pw.strings.return_value = {}
    mock_pw.freq.return_value = 60.0
    mock_pw.status.return_value = "Running"
    mock_pw.version.return_value = "1.0"
    mock_pw.din.return_value = "D"
    mock_pw.uptime.return_value = "0h"
    mock_pw.site_name.return_value = "S"
    mock_pw.temps.return_value = {}
    mock_pw.alerts.return_value = []
    gateway_manager.connections["v1r-pn"] = mock_pw
    data = await gateway_manager._fetch_gateway_data("v1r-pn", mock_pw)
    assert data.pw3 is True


@pytest.mark.asyncio
async def test_v1r_hardware_pw2_via_acpw_type():
    """v1r with ACPW type and no PW3 partNumber must be PW2 (False)."""
    gw = Gateway(
        id="v1r-pw2",
        name="V1R PW2",
        host="10.0.0.4",
        gw_pwd="x",
        rsa_key_path="/tmp/key",
        rsa_key_configured=True,
    )
    gateway_manager.gateways["v1r-pw2"] = gw
    mock_pw = Mock()
    mock_pw.poll.return_value = {
        "site": {"instant_power": 0},
        "solar": {"instant_power": 0},
        "battery": {"instant_power": 0},
        "load": {"instant_power": 0},
    }
    mock_pw.level.return_value = 50.0
    mock_pw.tedapi = Mock()
    mock_pw.tedapi.pw3 = True
    mock_pw.tedapi.get_config.return_value = {
        "battery_blocks": [{"type": "ACPW", "PackagePartNumber": "1114932-00-A"}]
    }
    mock_pw.grid_status.return_value = "UP"
    mock_pw.get_mode.return_value = None
    mock_pw.get_reserve.return_value = None
    mock_pw.get_grid_charging.return_value = None
    mock_pw.get_grid_export.return_value = None
    mock_pw.system_status.return_value = {}
    mock_pw.vitals.return_value = {}
    mock_pw.strings.return_value = {}
    mock_pw.freq.return_value = 60.0
    mock_pw.status.return_value = "Running"
    mock_pw.version.return_value = "1.0"
    mock_pw.din.return_value = "D"
    mock_pw.uptime.return_value = "0h"
    mock_pw.site_name.return_value = "S"
    mock_pw.temps.return_value = {}
    mock_pw.alerts.return_value = []
    gateway_manager.connections["v1r-pw2"] = mock_pw
    data = await gateway_manager._fetch_gateway_data("v1r-pw2", mock_pw)
    assert data.pw3 is False
