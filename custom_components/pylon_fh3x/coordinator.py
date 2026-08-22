"""DataUpdateCoordinator for Pylontech Force H3X."""
import asyncio
import logging
import struct
from datetime import timedelta

from pymodbus.client import AsyncModbusTcpClient
from pymodbus.exceptions import ModbusException

from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from .const import DOMAIN, DEFAULT_SCAN_INTERVAL

_LOGGER = logging.getLogger(__name__)

# =========================================================
# Modbus register decoding helpers
# =========================================================
def get_16bit_uint(regs, idx):
    return regs[idx]

def get_16bit_int(regs, idx):
    return struct.unpack('>h', struct.pack('>H', regs[idx]))[0]

def get_32bit_int(regs, idx):
    return struct.unpack('>i', struct.pack('>HH', regs[idx], regs[idx+1]))[0]

def get_32bit_float(regs, idx):
    return struct.unpack('>f', struct.pack('>HH', regs[idx], regs[idx+1]))[0]


async def _modbus_read(client, address, count, target_id):
    
    try:
        return await client.read_holding_registers(address=address, count=count, slave=target_id)
    except TypeError:
        pass
    try:
        return await client.read_holding_registers(address=address, count=count, unit=target_id)
    except TypeError:
        pass
    return await client.read_holding_registers(address=address, count=count, device_id=target_id)


class PylontechCoordinator(DataUpdateCoordinator):
    """Coordinate Modbus reads and writes for the inverter."""

    def __init__(self, hass: HomeAssistant, host: str, port: int) -> None:
        self.client = AsyncModbusTcpClient(host=host, port=port, timeout=5)
        self.host = host
        
        super().__init__(
            hass,
            _LOGGER,
            name=DOMAIN,
            update_interval=timedelta(seconds=DEFAULT_SCAN_INTERVAL),
        )

    async def safe_read(self, address, count, slave):
        
        # Space requests to avoid overwhelming the inverter's Modbus interface.
        await asyncio.sleep(0.1) 
        res = await _modbus_read(self.client, address, count, slave)
        if res.isError():
            _LOGGER.warning("error while reading adress %s (Slave %s): %s", address, slave, res)
            return None
        return res.registers

    async def _async_update_data(self):
        """Fetch data from the inverter via Modbus."""
        try:
            if not self.client.connected:
                await self.client.connect()

            data = {}

            
            # AC and grid power (registers 30100-30101 and 30108-30109).
            r_ac = await self.safe_read(30100, 2, 2)
            if r_ac: data["ac_total_power"] = get_32bit_int(r_ac, 0)
            
            r_grid = await self.safe_read(30108, 2, 2)
            if r_grid: data["grid_total_power"] = get_32bit_int(r_grid, 0)

            # Derive load power from inverter AC power and grid power.
            if "ac_total_power" in data and "grid_total_power" in data:
                data["load_power"] = data["ac_total_power"] + data["grid_total_power"]


            # Inverter status (register 30115).
            r_status = await self.safe_read(30115, 1, 2)
            if r_status: data["inverter_status"] = get_16bit_uint(r_status, 0)

            # PV voltage and current (registers 30119-30124).
            r_pv = await self.safe_read(30119, 6, 2)
            if r_pv:
                data["pv1_voltage"] = get_16bit_uint(r_pv, 0) * 0.1
                data["pv1_current"] = get_16bit_uint(r_pv, 1) * 0.1
                data["pv2_voltage"] = get_16bit_uint(r_pv, 2) * 0.1
                data["pv2_current"] = get_16bit_uint(r_pv, 3) * 0.1
                data["pv3_voltage"] = get_16bit_uint(r_pv, 4) * 0.1
                data["pv3_current"] = get_16bit_uint(r_pv, 5) * 0.1
            
            # Derive power for each PV input from its voltage and current.
            if r_pv:
                data["pv1_power"] = data["pv1_voltage"] * data["pv1_current"]
                data["pv2_power"] = data["pv2_voltage"] * data["pv2_current"]
                data["pv3_power"] = data["pv3_voltage"] * data["pv3_current"]


            # Total PV power and energy (registers 30127-30130).
            r_pv_tot = await self.safe_read(30127, 4, 2)
            if r_pv_tot:
                data["pv_total_power"] = get_32bit_int(r_pv_tot, 0)
                data["pv_total_energy"] = get_32bit_float(r_pv_tot, 2)

            # Grid phase voltages and AC frequency (registers 30131-30140).
            r_grid_v = await self.safe_read(30131, 10, 2)
            if r_grid_v:
                data["grid_voltage_r"] = get_16bit_uint(r_grid_v, 0) * 0.1
                data["grid_voltage_s"] = get_16bit_uint(r_grid_v, 2) * 0.1
                data["grid_voltage_t"] = get_16bit_uint(r_grid_v, 4) * 0.1
                data["ac_frequency"] = get_16bit_uint(r_grid_v, 9) * 0.01

            # Inverter and heatsink temperatures (registers 30146-30147).
            r_temp = await self.safe_read(30146, 2, 2)
            if r_temp: 
                data["inverter_temperature"] = get_16bit_int(r_temp, 0) * 0.1
                data["heatsink_temperature"] = get_16bit_int(r_temp, 1) * 0.1


            # Cumulative grid import and export energy (registers 30156-30159).
            r_grid_e = await self.safe_read(30156, 4, 2)
            if r_grid_e:
                data["total_grid_import"] = get_32bit_float(r_grid_e, 0)
                data["total_grid_export"] = get_32bit_float(r_grid_e, 2)


            r_active = await self.safe_read(40400, 3, 2)
            if r_active:
                # 40400: Active power control mode (U16)
                data["active_power_control_mode"] = get_16bit_uint(r_active, 0)
                # 40401: Meter limit power (S32, 2 registers)
                data["meter_export_power_max"] = get_32bit_int(r_active, 1)


            BATTERY_STATUS_MAP = {
                            0: "Sleep",
                            1: "Charging",
                            2: "Discharging",
                            3: "Idle",
                            4: "Standby",
                            5: "Run",
                            6: "Fault",
                            7: "Offline",
                        }
            
            # Battery status, power, voltage, and current (registers 30161-30165).
            r_batt = await self.safe_read(30161, 5, 2)
            if r_batt:

                raw_status = get_16bit_uint(r_batt, 0)
                data["battery_status"] = BATTERY_STATUS_MAP.get(raw_status, f"Unknown ({raw_status})")

                data["battery_power"] = get_32bit_int(r_batt, 1)
                data["battery_voltage"] = get_16bit_uint(r_batt, 3) * 0.1
                data["battery_current"] = get_16bit_int(r_batt, 4) * 0.1

            # EPS load power (registers 30172-30173).
            r_load = await self.safe_read(30172, 2, 2)
            if r_load: data["eps_power"] = get_32bit_int(r_load, 0)



            # Cumulative battery charge and discharge energy (registers 30174-30177).
            r_batt_e = await self.safe_read(30174, 4, 2)
            if r_batt_e:
                data["total_battery_charge"] = get_32bit_float(r_batt_e, 0)
                data["total_battery_discharge"] = get_32bit_float(r_batt_e, 2)

            # Battery state of charge (register 30182).
            r_soc = await self.safe_read(30182, 4, 2)
            if r_soc:
                data["battery_soc"] = get_16bit_uint(r_soc, 0)


            # =========================================================
            # Slave 2 (inverter): EMS settings
            # =========================================================
            r_ems = await self.safe_read(40901, 7, 2)
            if r_ems:
                # Register 40901 is a signed 16-bit value.
                data["charge_discharge_power"] = get_16bit_int(r_ems, 0)
                
                # The remaining values are unsigned 16-bit integers.
                data["charge_limit_soc"] = get_16bit_uint(r_ems, 1) #40902
                data["discharge_limit_soc"] = get_16bit_uint(r_ems, 2) #40903
                data["ems_mode"] = str(get_16bit_uint(r_ems, 6)) #40907


            # Heat-pump control (register 40848).
            r_hp = await self.safe_read(40848, 1, 2)
            if r_hp:
                data["heat_pump"] = get_16bit_uint(r_hp, 0)
                

            # Charge and discharge period enable flags.
            r_p1 = await self.safe_read(40908, 1, 2)
            if r_p1:
                data["period_1"] = get_16bit_uint(r_p1, 0)

            r_p2 = await self.safe_read(40914, 1, 2)
            if r_p2:
                data["period_2"] = get_16bit_uint(r_p2, 0)

            r_p3 = await self.safe_read(40920, 1, 2)
            if r_p3:
                data["period_3"] = get_16bit_uint(r_p3, 0)

            r_p4 = await self.safe_read(40926, 1, 2)
            if r_p4:
                data["period_4"] = get_16bit_uint(r_p4, 0)


            # =========================================================
            # Slave 1: battery management system (BMS)
            # =========================================================
            
            # BMS voltage (register 5123 / 0x1403).
            r_bms_v = await self.safe_read(5123, 1, 1)
            if r_bms_v: data["bms_voltage"] = get_16bit_uint(r_bms_v, 0) * 0.1

            # BMS temperature, state of charge, and cycle count (registers 5126-5128).
            r_bms_t = await self.safe_read(5126, 3, 1)
            if r_bms_t:
                data["bms_temperature"] = get_16bit_int(r_bms_t, 0) * 0.1
                data["bms_soc"] = get_16bit_uint(r_bms_t, 1)
                data["bms_cycles"] = get_16bit_uint(r_bms_t, 2)

            # Highest and lowest cell voltage (registers 5136-5137).
            r_bms_cv = await self.safe_read(5136, 2, 1)
            if r_bms_cv:
                data["bms_cell_voltage_max"] = get_16bit_uint(r_bms_cv, 0) * 0.001
                data["bms_cell_voltage_min"] = get_16bit_uint(r_bms_cv, 1) * 0.001

            # BMS state of health (register 5152 / 0x1420).
            r_bms_soh = await self.safe_read(5152, 1, 1)
            if r_bms_soh: data["bms_soh"] = get_16bit_uint(r_bms_soh, 0)




            
            if not data:
                raise UpdateFailed("No data received out of inverter.")

            return data

        except ModbusException as err:
            raise UpdateFailed(f"error with modbus communication: {err}")
        except Exception as err:
            raise UpdateFailed(f"unexpected error: {err}")

    async def async_write_register(self, address: int, value: int, slave: int = 2) -> bool:
        """Write a signed or unsigned 16-bit value to a Modbus register."""
        try:
            if not self.client.connected:
                await self.client.connect()

            if value < 0:
                value = value & 0xFFFF

            # Support the slave-ID keyword used by multiple pymodbus versions.
            try:
                res = await self.client.write_register(address=address, value=value, slave=slave)
            except TypeError:
                try:
                    res = await self.client.write_register(address=address, value=value, unit=slave)
                except TypeError:
                    res = await self.client.write_register(address=address, value=value, device_id=slave)

            if res.isError():
                _LOGGER.error("error whil writing to register %s: %s", address, res)
                return False

            
            await self.async_request_refresh()
            return True

        except Exception as err:
            _LOGGER.error("Unexpected error whil writing to: %s", err)
            return False
        
    async def async_write_register_32bit(self, address: int, value: int, slave: int = 2) -> bool:
        """Write a 32-bit signed value (S32) as two consecutive 16-bit registers."""
        try:
            if not self.client.connected:
                await self.client.connect()

            # Split the signed value into two big-endian 16-bit registers.
            packed = struct.pack('>i', value)
            high, low = struct.unpack('>HH', packed)

            try:
                res = await self.client.write_registers(address=address, values=[high, low], slave=slave)
            except TypeError:
                try:
                    res = await self.client.write_registers(address=address, values=[high, low], unit=slave)
                except TypeError:
                    res = await self.client.write_registers(address=address, values=[high, low], device_id=slave)

            if res.isError():
                _LOGGER.error("Error writing 32-bit register %s: %s", address, res)
                return False

            await self.async_request_refresh()
            return True

        except Exception as err:
            _LOGGER.error("Unexpected error writing 32-bit register: %s", err)
            return False
