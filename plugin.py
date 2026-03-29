# Marstek Venus A plugin for Domoticz, developed using Basic Python Plugin Framework as provided by GizMoCuz for Domoticz
#
# author WillemD61
# version 1.0.0
#   * initial release
# version 1.0.1
#   * fixed the processing of device type 248
# version 1.0.2
#   * replaced most types 248/1 with 243/29 => supply Watts and Domoticz calculates kWh (new install required)
#   * improved error handling in validation of input parameters for mode switch
# version 1.0.3
#   * added UPS mode, even though not in Open API specification, it works (power specification has no effect)
#   * remove passive-mode power and countdown devices because no effect
#   * negative power values allowed
#   * more clear devices names
#   * additional debug level in startup parameters
# version 1.0.4
#   * one P1 meter device added to hold all three values total_power, input_energy and output_energy (3 separate devices left alive, can be disable if desired)
#   * extended timeout handling, after 3 full cycle failures of all 6 data retrieval commands, an email will be sent.
#   * note: please also reinstall the venus_api_v2.py file for inclusion of the UPS mode
# version 1.0.5
#   * corrected the validation (sign) of power settings when setting manual mode
#   * corrected the P1 processing when devices enables/disabled
# version 1.0.6
#   * make the device name prefix customisable during startup (for multi system/plugin setups)
#   * temporarily change the multiplier for pv1_power to solve a bug in Open Api where pv1_power is reported a factor 10 too high.
#   * handle multiplier for kWh and P1 devices
# version 1.0.7
#   * adapted the validation limit for P1 meter
# version 1.0.8.
#   * moved the initiation of the api client to onStart
#   * adapt the id sequencing in venus_v2_api.py, both measures might be needed for Venus E, to be tested
# version 2.0
#   * adapted to use the API from https://github.com/jaapp/ha-marstek-local-api
#   * only for the device read functionality. MODE UPDATE NOT YET ADAPTED !!
#
# This plugin re-uses the UDP API library developed by Ivan Kablar for his MQTT bridge (https://github.com/IvanKablar/marstek-venus-bridge)
# The library was extended to cover all elements from the specification and was made more responsive and reliable.
#
# Please make sure the Open API feature has been enabled via the Marstek mobile app
#
# Reference is made to the Marstek Open API specification version rev. 1.0
#
# Modifications done to the venus_api.py library of Ivan Kablar:
# 1) Added the Masrtek.GetDevice function for device discovery (par 2.2.2 and 3.1.1)
# 2) Added both the Wifi and Bluetooth Getstatus functions (par 3.2.1. and 3.3.1)
# 3) Added the PV GetStatus function (par 3.5.1)
# 4) Changed the buffer size for the data reception.
# 5) Remove fixed period 0 for manual mode configuration
# Also the test_api.py program was extended to include the above in the tests.
#
# So the venus_api_v2 library now covers the full specification of Marstek Open API and can be used in any python program.
#
# Even though the functions are now present in the API library, the current version of this plugin does NOT (!!!) do the following:
#  1) implement the marstek.GetDevice UDP discovery to find Marstek devices on the network (par. 2.2.2 and 3.1.1). Instead, the Marstek device
#     to be used has to be specified manually in the configuration parameters of this plugin.
#  2) implement the Wifi.GetStatus (par 3.2.1) to configure or obtain Wifi info
#  3) implement the BLE.GetStatus (par 3.3.1) to obtain Bluetooth info
#  4) configuration of up to 10 periods for manual operating mode. For now it will handle one single period.
#
# It does implement the following:
#  1) Get Battery, PV, ES (Energy System) and EM (Energy Meter) status info (par. 3.4, 3.5, 3.6.1 and 3.7.1)
#  2) Get current Eenergy System operating mode (par 3.6.3)
#  3) Change Energy System operating mode (auto, AI, manual, passive as shown in par 3.6.2)
#       note the config of periods for manual mode needs to be further developed in future version of this plugin.
#  4) Create all required Domoticz devices and load received data onto the devices.
#  5) Send an alert when an error is received (if configured)
#  6) Show data received in the domoticz log for debugging/monitoring (if configured)
#
# This plugin was not tested in a multi-system environment. Only one Marstek Venus A was available for testing.
#
# Observations on the Marstek Open Api specification:
# 1) The specification includes reference to ID and SRC, maybe for multi-system environments, but that is not clear.
# 2) par 3.2.1 : the wifi response also includes a wifi_mac field
# 3) par 3.5.1 : the pv response also includes a pv_state field and reports all fields for each  of the PV connections (4x)
# 4) par 3.6.3 : the response depends on the mode. For auto (=self-consumption) the energy meter mode fields are also includes but
#                often with all values=0. For AI the energy meter mode fields are included with actual values. Note also that the UPS mode
#                in the APP is reported as a manual mode. (in UPS mode backup-power is switched on)
# 5) par 3.7.1 : the response also includes total input energy and output energy of the P1 meter.
# 6) the specifation does not mention UPS mode but since it it possible in the App, it was tested and it works
#
# Some duplications are present when looking at all responses (soc 3x, ongrid and offgrid power 2x, EM data depending on mode 2x

"""
<plugin key="MarstekOpenAPI" name="Marstek Open API" author="WillemD61" version="1.0.0" >
    <description>
        <h2>Marstek Open API plugin</h2><br/>
        This plugin uses the API for Marstek battery systems to get the values of various parameters<br/>
        and then load these values onto Domoticz devices. Devices will be created if they don't exists already.<br/><br/>
        Note the Open API feature needs to be enabled in the Marstek app first.<br/>
        Configuration options...
    </description>
    <params>
        <param field="Address" label="Marstek IP Address" width="200px" required="true"/>
        <param field="Port" label="Marstek Port" width="100px" required="true" default="30000"/>
        <param field="Mode1" label="Polling Interval" width="150px">
            <options>
                <option label="30 seconds" value="30" /> # maximum domoticz heartbeat time is 30 seconds
                <option label="1 minute" value="60" default="true" />
                <option label="2 minutes" value="120" />
                <option label="3 minutes" value="180" />
                <option label="4 minutes" value="240" />
                <option label="5 minutes" value="300" />
            </options>
        </param>
        <param field="Mode2" label="Alerts On" width="150px">
            <options>
                <option label="Yes" value="Yes" default="true" />
                <option label="No" value="No" />
            </options>
        </param>
        <param field="Mode3" label="Show data in log" width="150px">
            <options>
                <option label="Yes" value="Yes" />
                <option label="No" value="No" default="true" />
            </options>
        </param>
        <param field="Mode4" label="Max output W configured" width="150px" required="true">
        </param>
        <param field="Mode5" label="More debug info" width="150px" required="true">
            <options>
                <option label="Yes" value="Yes" />
                <option label="No" value="No" default="true" />
            </options>
        </param>
        <param field="Mode6" label="Device name prefix" width="150px">
        </param>
    </params>
</plugin>
"""

from __future__ import annotations

import DomoticzEx as Domoticz
import json,requests   # make sure these are available in your system environment
import time
from datetime import datetime
from requests.exceptions import Timeout
import urllib.parse

import asyncio, threading
from dataclasses import dataclass, field
from datetime import timedelta
import importlib.util
import sys
from pathlib import Path
from typing import Any, Callable

def load_module_from_file(module_name: str, file_path: Path):
    """Load a Python module directly from a file path."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module

# Get paths to integration modules
integration_path = Path(__file__).parent.parent / "Marstek-Venus-plugin"
# Load integration modules in dependency order
const = load_module_from_file("const",integration_path / "const.py")
api_module = load_module_from_file("api",integration_path / "api.py")

# Extract integration components we need
MarstekUDPClient = api_module.MarstekUDPClient
DEFAULT_PORT = const.DEFAULT_PORT
DEVICE_MODEL_VENUS_D = const.DEVICE_MODEL_VENUS_D
DEVICE_MODEL_VENUS_A = const.DEVICE_MODEL_VENUS_A
MODE_AUTO = const.MODE_AUTO
MODE_AI = const.MODE_AI
MODE_MANUAL = const.MODE_MANUAL
MODE_PASSIVE = const.MODE_PASSIVE
MODE_UPS = const.MODE_UPS
WEEKDAY_MAP = const.WEEKDAY_MAP
MAX_SCHEDULE_SLOTS = const.MAX_SCHEDULE_SLOTS

def _days_to_week_set(days: list[str]) -> int:
    """Convert list of day names to week_set bitmap."""
    return sum(WEEKDAY_MAP[day] for day in days)


# A dictionary to list all parameters that can be retrieved from Marstek and to define the Domoticz devices to hold them.
# currently only english names are provided, can be extended with other languages later

# Dictionary structure is as follows: Property (from API spec) : [ Unit, Type, Subtype, Switchtype, OptionsList{}, Multiplier, Name, Source ],

DEVSLIST={
# response Bat.GetStatus
    "soc"             : [1,  243,  6, 0, {}, 1   ,"Battery SOC","BAT"], # duplicate ? (soc, bat_soc)
    "charg_flag"      : [2,  244, 73, 0, {}, 1   ,"Charge permission","BAT"],
    "dischrg_flag"    : [3,  244, 73, 0, {}, 1   ,"Discharge permission","BAT"],
    "bat_temp"        : [4,   80,  5, 0, {}, 10   ,"Battery temperature","BAT"],
    "bat_capacity"    : [5,  113,  0, 0, {}, 1   ,"Remaining Capacity","BAT"],
    "rated_capacity"  : [6,  113,  0, 0, {}, 1   ,"Rated Capacity","BAT"],
# response PV.GetStatus
    "pv1_power"       : [7,  243, 29, 0, {'EnergyMeterMode': '1'}, 0.1   ,"PV1 power","PV"], # 4 groups, although not in specification ver. 1.0
    "pv1_voltage"     : [8,  243,  8, 0, {}, 1   ,"PV1 voltage","PV"],
    "pv1_current"     : [9,  243, 23, 0, {}, 1   ,"PV1 current","PV"],
    "pv1_state"       : [10, 244, 73, 0, {}, 1   ,"PV1 state","PV"], # pv_state not in specification ver. 1.0
    "pv2_power"       : [11, 243, 29, 0, {'EnergyMeterMode': '1'}, 1   ,"PV2 power","PV"],
    "pv2_voltage"     : [12, 243,  8, 0, {}, 1   ,"PV2 voltage","PV"],
    "pv2_current"     : [13, 243, 23, 0, {}, 1   ,"PV2 current","PV"],
    "pv2_state"       : [14, 244, 73, 0, {}, 1   ,"PV2 state","PV"],
    "pv3_power"       : [15, 243, 29, 0, {'EnergyMeterMode': '1'}, 1   ,"PV3 power","PV"],
    "pv3_voltage"     : [16, 243,  8, 0, {}, 1   ,"PV3 voltage","PV"],
    "pv3_current"     : [17, 243, 23, 0, {}, 1   ,"PV3 current","PV"],
    "pv3_state"       : [18, 244, 73, 0, {}, 1   ,"PV3 state","PV"],
    "pv4_power"       : [19, 243, 29, 0, {'EnergyMeterMode': '1'}, 1   ,"PV4 power","PV"],
    "pv4_voltage"     : [20, 243,  8, 0, {}, 1   ,"PV4 voltage","PV"],
    "pv4_current"     : [21, 243, 23, 0, {}, 1   ,"PV4 current","PV"],
    "pv4_state"       : [22, 244, 73, 0, {}, 1   ,"PV4 state","PV"],
# response ES.GetMode
    "mode"            : [23, 243, 19, 0, {}, 1   ,"ESM mode","ESM"],
    "ongrid_power"    : [24, 243, 29, 0, {'EnergyMeterMode': '1'}, 1   ,"ESM on-grid power","ESM"], # duplicate ?
    "offgrid_power"   : [25, 243, 29, 0, {'EnergyMeterMode': '1'}, 1   ,"ESM off-grid power","ESM"], # duplicate ?
    "bat_soc"         : [26, 243,  6, 0, {}, 1   ,"ESM Battery Soc","ESM"], # duplicate ?
# note in case of auto or AI mode the response of Es.GetMode also includes EM.GetStatus data
# reponse ES.GetStatus
    "es_bat_soc"      : [27, 243,  6, 0, {}, 1   ,"ESS Total SOC","ESS"],  # duplicate ? note es_ added to name to create unique key
    "bat_cap"         : [28, 113,  0, 0, {}, 1   ,"ESS Rated capacity","ESS"], # duplicate value but still unique name (other is bat_capacity)
    "pv_power"        : [29, 243, 29, 0, {'EnergyMeterMode': '1'}, 1   ,"ESS PV charging power","ESS"],
    "es_ongrid_power" : [30, 243, 29, 0, {'EnergyMeterMode': '1'}, 1   ,"ESS on-grid power","ESS"], # duplicate ? note es_ added to name to create unique key
    "es_offgrid_power": [31, 243, 29, 0, {'EnergyMeterMode': '1'}, 1   ,"ESS off-grid power","ESS"], # duplicate ? note es_ added to name to create unique key
#    "bat_power"      : "ES battery power W"], # not present in ES.getStatus response, although in specification ver 1.0
    "total_pv_energy"          : [32, 113,  0, 0, {}, 1   ,"ESS PV energy generated","ESS"],
    "total_grid_output_energy" : [33, 113,  0, 0, {}, 1   ,"ESS Battery output energy","ESS"],
    "total_grid_input_energy"  : [34, 113,  0, 0, {}, 1   ,"ESS Battery input energy","ESS"],
    "total_load_energy"        : [35, 113,  0, 0, {}, 1   ,"ESS Off-grid energy used","ESS"],
# response EM.GetStatus
    "ct_state"        : [36, 244, 73, 0, {}, 1,  "P1 CT state","EMS"],
    "a_power"         : [37, 243, 29, 0, {'EnergyMeterMode': '1'}, 1   ,"P1 Phase A power","EMS"],
    "b_power"         : [38, 243, 29, 0, {'EnergyMeterMode': '1'}, 1   ,"P1 Phase B power","EMS"],
    "c_power"         : [39, 243, 29, 0, {'EnergyMeterMode': '1'}, 1   ,"P1 Phase C power","EMS"],
    "total_power"     : [40, 243, 29, 0, {'EnergyMeterMode': '1'}, 1   ,"P1 A+B+C power","EMS"], # 3 devices can be disabled. A P1 meter device (51) has been added to hold all 3.
    "input_energy"    : [41, 113,  0, 0, {}, 0.1 ,"P1 input from grid","EMS"], # in response, although not in specification ver 1.0
    "output_energy"   : [42, 113,  0, 0, {}, 0.1 ,"P1 output to grid","EMS"], # in response, although not in specification ver 1.0
# device for holding one single manual mode setting
    "time_period"     : [43, 243, 19, 0, {}, 1   ,"Manual Mode periodnr","MM"],
    "start_time"      : [44, 243, 19, 0, {}, 1   ,"Manual Mode starttime","MM"],
    "end_time"        : [45, 243, 19, 0, {}, 1   ,"Manual Mode endtime","MM"],
    "week_set"        : [46, 243, 19, 0, {}, 1   ,"Manual Mode weekdays","MM"],
    "mm_power"        : [47, 248,  1, 0, {}, 1   ,"Manual Mode power","MM"], # note mm_ added to create unique key
# device for holding passive mode power and countdown
# removed in version 1.0.3 because it was determined that these fields do not have an effect
#    "pm_power"        : [48, 248,  1, 0, {}, 1   ,"Passive Mode power","PM"], # note pm_ added to create unique key
#    "countdown"       : [49, 243, 19, 0, {}, 1   ,"Passive Mode countdown s","PM"],
# device to activate mode switch
# do not change name, used on onCommand code below
    "select Marstek mode"     : [50, 244, 62, 18, {"LevelActions":"|||||","LevelNames":"|AutoSelf|AI|Manual|Passive|UPS","LevelOffHidden":"true","SelectorStyle":"0"}, 1 ,"Select Marstek mode","SM"],
    "P1 meter"   : [51, 250,  1, 0, {}, 1 ,"P1 meter","EMS"], # new P1 device to hold EMS total_power, input_energy and output_energy
} # end of dictionary

class MarstekPlugin:
    enabled = False
    def __init__(self):
        return

#    def start_async_loop(self):
#        self.loop = asyncio.new_event_loop()
#        self.loop_thread = threading.Thread(
#            target=self.loop.run_forever,
#            daemon=True
#        )
#        self.loop_thread.start()
    def start_async_loop(self):
        self.loop = asyncio.new_event_loop()

        def init_loop():
            asyncio.set_event_loop(self.loop)
            self.api_lock = asyncio.Lock()
            time.sleep(0.5)
            self.loop.run_forever()

        self.loop_thread = threading.Thread(
            target=init_loop,
            daemon=True
        )
        self.loop_thread.start()


    def onStart(self):
        global debug
        Domoticz.Log("onStart called with parameters")
        for elem in Parameters:
            Domoticz.Log(str(elem)+" "+str(Parameters[elem]))
        self.IPAddress=str(Parameters["Address"])
        self.Port=int(Parameters["Port"])
        self.PollingInterval=int(Parameters["Mode1"])
        if self.PollingInterval<=30: # heartbeat is max 30 seconds, so >30 seconds requires skipping action on heartbeat
            Domoticz.Heartbeat(self.PollingInterval)
            self.heartbeatWaits=0
        else:
            Domoticz.Heartbeat(30)
            self.heartbeatWaits=int(self.PollingInterval/30 - 1)
        self.heartbeatCounter=0
        self.notificationsOn=(Parameters["Mode2"]=="Yes")
        self.emailAlertSent=False
        self.showDataLog=(Parameters["Mode3"]=="Yes")
        self.maxOutputPower=int(Parameters["Mode4"])
        debug=(Parameters["Mode5"]=="Yes")
        self.namePrefix=str(Parameters["Mode6"])
        self.Hwid=Parameters['HardwareID']
        self.lastDataRecvdTime=time.time()
        Domoticz.Log("Checking Marstek device type")
        self.client = MarstekUDPClient(host=self.IPAddress,port=self.Port)
        self.start_async_loop()
        future = asyncio.run_coroutine_threadsafe(
            self._get_device_type(self.IPAddress),
            self.loop
        )
        try:
            future.result(timeout=30)   # wait here
        except Exception as e:
            Domoticz.Error(f"Device discovery failed: {e}")




    def createDevices(self):
        # cycle through device list and create any non-existing devices when the plugin/domoticz is started
        for Dev in DEVSLIST:
            Unit=DEVSLIST[Dev][0]
            DeviceID="{:04x}{:04x}".format(self.Hwid,Unit)
            Type=DEVSLIST[Dev][1]
            Subtype=DEVSLIST[Dev][2]
            Switchtype=DEVSLIST[Dev][3]
            Options=DEVSLIST[Dev][4]
            if Unit==50:
                self.levelNames=Options["LevelNames"]
                self.levelNamesList=[part.strip() for part in self.levelNames.split('|') if part.strip()]
            Name=self.namePrefix+DEVSLIST[Dev][6]
            if Unit<7 or Unit>22 or self.deviceType==DEVICE_MODEL_VENUS_D or self.deviceType==DEVICE_MODEL_VENUS_A:
                # devices 7 to 22 for PV data only created for model A and D
                if DeviceID not in Devices:
                    Domoticz.Status(f"Creating device for Field {Dev} ...")
                    if ((Type==243) and (Subtype==29)):
                        # below code puts an initial svalue on the kwh device and then changes the type to "computed". This is to work around a BUG in Domoticz for computed kwh devices. See issue 6194 on Github.
                        Domoticz.Unit(DeviceID=DeviceID,Unit=Unit, Name=Name, Type=Type, Subtype=Subtype, Switchtype=Switchtype, Options={}, Used=1).Create()
                        Devices[DeviceID].Units[Unit].sValue="0;0"
                        Devices[DeviceID].Units[Unit].Update()
                        Devices[DeviceID].Units[Unit].Options=Options
                        Devices[DeviceID].Units[Unit].Update(UpdateOptions=True)
                    else:
                        Domoticz.Unit(DeviceID=DeviceID,Unit=Unit, Name=Name, Type=Type, Subtype=Subtype, Switchtype=Switchtype, Options=Options, Used=1).Create()
                Domoticz.Log("DEVSLIST "+str(DEVSLIST[Dev][0])+DEVSLIST[Dev][6])


    async def _get_device_type(self, IPAddress):
        self.deviceType=None
        async with self.api_lock:

            try:
                if not self.client._connected:
                    await self.client.connect()
            except Exception as e:
                Domoticz.Error(f"error on connect attempt : {e}")
            await asyncio.sleep(3.0)
            deviceFound=False
            try:
                deviceinfo = await self._retry_command(
                        lambda: self.client.get_device_info(),
                        "get device"
                )
                Domoticz.Log("device info"+str(deviceinfo))
                if not deviceinfo:
                    Domoticz.Error("Error: Marstek device not found")
                else:
                    deviceFound=True
                    self.lastDataRecvdTime=time.time()
                    self.deviceType=deviceinfo[1]['device']
                    if debug: Domoticz.Log("Found selected device for  IP adress, type is :"+self.deviceType)
                    self.createDevices()
            except Exception as e:
                Domoticz.Error(f"error on discovering devices : {e}")
            finally:
                if not deviceFound: 
                    Domoticz.Error("No Marstek device found on IPAddress: "+str(IPAddress))
                    self.deviceType="Venus"
                await asyncio.sleep(1)

    def onStop(self):
        Domoticz.Log("onStop called")

        try:
            if self.loop and self.loop.is_running():

                async def shutdown():
                    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)

                    try:
                        await self.client.disconnect()
                    except:
                        pass

                future = asyncio.run_coroutine_threadsafe(shutdown(), self.loop)
                try:
                    future.result(timeout=5)
                except Exception as e:
                    Domoticz.Error(f"Shutdown error: {e}")

                self.loop.call_soon_threadsafe(self.loop.stop)
                self.loop_thread.join(timeout=5)

        except Exception as e:
            Domoticz.Error(f"Error in onStop: {e}")


    def onConnect(self, Connection, Status, Description):
        Domoticz.Log("onConnect called")

    def onMessage(self, Connection, Data):
        Domoticz.Log("onMessage called")

    def onCommand(self, DeviceID, Unit, Command, Level, Color):
        # used when a mode is selected using the selector switch in Domoticz
        if debug: Domoticz.Log("onCommand called for Device " + str(DeviceID) + " Unit " + str(Unit) + ": Parameter '" + str(Command) + "', Level: " + str(Level))
        modeSelectorUnit=DEVSLIST["select Marstek mode"][0]
        expectedDeviceID="{:04x}{:04x}".format(self.Hwid,modeSelectorUnit)
        if str(Command)=="Set Level" and DeviceID==expectedDeviceID: # it is a mode change initiated using the selector switch
            self.current_task=asyncio.run_coroutine_threadsafe(
                self._handle_command_async(DeviceID, Unit, Level),
                self.loop
            )

    async def _retry_command(self, func, name, max_attempts=3):
        result=None
        for attempt in range(1, max_attempts + 1):
            try:
                result = await func()
                if result:
                    self.lastDataRecvdTime=time.time()
                    return True,result
                Domoticz.Error(f"{name} failed (attempt {attempt}), retrying...")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                Domoticz.Error(f"{name} error on attempt {attempt}: {e}")
                await asyncio.sleep(1)
            await asyncio.sleep(1)
        return False,result


    async def _handle_command_async(self, DeviceID, Unit, Level):
        async with self.api_lock:
            try:
                if not self.client._connected:
                    await self.client.connect()
            except Exception as e:
                Domoticz.Error(f"error on connect attempt : {e}")
            await asyncio.sleep(1.0)

            try:
                if Level==10: # auto mode (=self consumption)
                    success=False
                    config = {"mode": MODE_AUTO, "auto_cfg": {"enable": 1}}
                    success,response = await self._retry_command(
                        lambda: self.client.set_es_mode(config),
                        "Auto mode"
                    )
                    if success: Domoticz.Log("Succesfully changed to auto mode (=self consumption mode).")
                elif Level == 20:
                    success=False
                    config = {"mode": MODE_AI, "ai_cfg": {"enable": 1}}
                    success,response = await self._retry_command(
                        lambda: self.client.set_es_mode(config),
                        "AI mode"
                    )
                    if success: Domoticz.Log("Succesfully changed to AI optimisation mode.")
                elif Level == 30:
                    success=False
                    timeperiodUnit=DEVSLIST["time_period"][0]
                    starttimeUnit=DEVSLIST["start_time"][0]
                    endtimeUnit=DEVSLIST["end_time"][0]
                    weekdayUnit=DEVSLIST["week_set"][0]
                    mmpowerUnit=DEVSLIST["mm_power"][0]
                    timeperiod=Devices["{:04x}{:04x}".format(self.Hwid,timeperiodUnit)].Units[timeperiodUnit].sValue
                    starttime=Devices["{:04x}{:04x}".format(self.Hwid,starttimeUnit)].Units[starttimeUnit].sValue
                    endtime=Devices["{:04x}{:04x}".format(self.Hwid,endtimeUnit)].Units[endtimeUnit].sValue
                    weekday=Devices["{:04x}{:04x}".format(self.Hwid,weekdayUnit)].Units[weekdayUnit].sValue
                    mmpower=Devices["{:04x}{:04x}".format(self.Hwid,mmpowerUnit)].Units[mmpowerUnit].sValue
                    if timeperiod>="0" and timeperiod<="9":
                        try:
                            startHr = int(starttime[0:2])
                            startMm = int(starttime[3:5])
                        except ValueError:
                            Domoticz.Error("No valid starttime set for manual mode, using 00:00 instead")
                            startHr, startMm = 0, 0
                        try:
                            endHr = int(endtime[0:2])
                            endMm = int(endtime[3:5])
                        except ValueError:
                            Domoticz.Error("No valid endtime set for manual mode, using 23:59 instead")
                            endHr, endMm = 23, 59
                        if (startHr>=0 and startHr<=23 and startMm>=0 and startMm<=59) and (endHr>=0 and endHr<=23 and endMm>=0 and endMm<=59):
                            if (startHr*60+startMm)<(endHr*60+endMm):
                                starttimestring=f"{startHr:02d}:{startMm:02d}"
                                endtimestring=f"{endHr:02d}:{endMm:02d}"
                                weekdayValid=True
                                weekdayvalue=0
                                bitvalue=64
                                # should be string of 7 x 0 or 1, indicating on/off of weekday starting with Sunday, to match the APP
                                # note the value passed in the API is low to high bit, starting with Monday
                                for dayCharacter in weekday:
                                    if (dayCharacter!="0" and dayCharacter!="1") or len(weekday)!=7:
                                        weekdayValid=False
                                    else:
                                        weekdayvalue+=bitvalue*int(dayCharacter)
                                    if bitvalue==64:
                                        bitvalue=1
                                    else:
                                        bitvalue=bitvalue*2
                                if not weekdayValid:
                                    Domoticz.Error("Weekday string not valid, using 1111111 = all days instead")
                                    bitvalue=127 # all days default
                                try:
                                    mmpower=int(mmpower)
                                except ValueError:
                                    mmpower=0
                                    Domoticz.Error("No valid mmpower set for manual mode")
                                # positive is discharge, negative is charge
                                if mmpower<=self.maxOutputPower and mmpower>=-1200 and mmpower!=0:
                                    # all validation done
                                    enable=1 # assuming period should be active
                                    config = { "mode": MODE_MANUAL,
                                                "manual_cfg": {
                                                    "time_num": int(timeperiod),
                                                    "start_time": starttimestring,
                                                    "end_time": endtimestring,
                                                    "week_set": weekdayvalue,
                                                    "power": mmpower,
                                                    "enable": enable,
                                                },
                                    }
                                    if debug: Domoticz.Log("Manual mode config"+str(config))
                                    success,response = await self._retry_command(
                                        lambda: self.client.set_es_mode(config),
                                        "manual mode"
                                    )
                                    #success=client.set_manual_mode(mmpower,int(timeperiod),starttimestring,endtimestring,weekdayvalue,enable)
                                    if success:
                                        Domoticz.Log("Succesfully changed to manual mode."+str(mmpower))
                                    else:
                                        Domoticz.Error("Change to manual mode failed")
                                else:
                                    Domoticz.Error("Error: power settings not valid for manual mode.")
                            else:
                                Domoticz.Error("Error: start time must be before end time for manual mode")
                        else:
                            Domoticz.Error("No valid start or end time set for manual mode")
                    else:
                        Domoticz.Error("No valid timeperiod set for manual mode")
                elif Level == 40:
                    success=False
                    # power and ct time don't seem to have an effect, but cannot be 0, so random fixed values used
                    config = { "mode": MODE_PASSIVE,
                                "passive_cfg": {
                                    "power" : 100,
                                    "cd_time" : 300
                                }
                    }
                    success,response = await self._retry_command(
                        lambda: self.client.set_es_mode(config),
                        "Passive mode"
                    )
                    if success: Domoticz.Log("Succesfully changed to passive mode.")
                elif Level == 50:
                    success=False
                    # power set to 0 because other values don't seem to have an effect
                    config = {
                        "mode": MODE_UPS,
                        "ups_cfg": {
                            "power": 0,
                            "enable": 1
                            }
                    }
                    success,response = await self._retry_command(
                        lambda: self.client.set_es_mode(config),
                        "UPS mode"
                    )
                    if success: Domoticz.Log("Succesfully changed to UPS mode.")
                else:
                    return

                if success:
                    if debug: Domoticz.Log(f"Updating switch device to reflect changed mode.")
                    Devices[DeviceID].Units[Unit].sValue = str(Level)
                    Devices[DeviceID].Units[Unit].Update()
                else:
                    levelName=self.levelNamesList[int(Level/10-1)]
                    Domoticz.Error(f"Change of mode to {levelName} failed")

            except asyncio.CancelledError:
                Domoticz.Error("Command cancelled")
                raise
            except Exception as e:
                Domoticz.Error(f"Unexpected error in command handling: {e}")


    def onNotification(self, Name, Subject, Text, Status, Priority, Sound, ImageFile):
        Domoticz.Log("Notification: " + Name + "," + Subject + "," + Text + "," + Status + "," + str(Priority) + "," + Sound + "," + ImageFile)

    def onDisconnect(self, Connection):
        Domoticz.Log("onDisconnect called")


    def onHeartbeat(self):
        self.heartbeatCounter += 1

        if debug:
            Domoticz.Log("onHeartbeat called, hearbeatCounter is "+str(self.heartbeatCounter))

        # Handle polling interval > 30 seconds
        if self.heartbeatWaits > 0:
            if self.heartbeatCounter <= self.heartbeatWaits:
                # skip heartbeat (do nothing)  in line with requested polling interval
                return

        # time for next polling
        if self.api_lock.locked():
            # time for next data collection but previous still busy
            
            if debug:
                Domoticz.Log("Time for next poll but previous cycle still running")
                Domoticz.Log("Time since last data received : "+str(int(time.time() - self.lastDataRecvdTime)))
            if time.time() - self.lastDataRecvdTime > self.PollingInterval:
                Domoticz.Error("No data during full polling interval time , restarting client ")
                asyncio.run_coroutine_threadsafe(
                    self.reset_client(),
                    self.loop
                )
            Domoticz.Log("Waiting another heartbeat ....")
            return

        else:
            # time for next polling, no lock in place, is normal process
            if time.time() - self.lastDataRecvdTime > self.PollingInterval:
                Domoticz.Log("No lock in place but data received long ago.")
                # not locked, but also no data received for long time
                # first do a reset
                asyncio.run_coroutine_threadsafe(
                    self.reset_client(),
                    self.loop
                )
                time.sleep(1)
                # send alert if not already done
                if self.notificationsOn and self.emailAlertSent==False:
                    Domoticz.Log("Sending email alert....")
                    subject = "ATTENTION: Venus communication error, check connection and Open API setting."
                    messageBody = "Problem for " + self.deviceType + " at " + self.IPAddress
                    url = "http://127.0.0.1:8080/json.htm?type=command&param=sendnotification"
                    url += "&subject=\'" + subject + "\'"
                    url += "&body=\'" + messageBody + "\'"
                    Domoticz.Log("url : "+url)
                    sendemail = requests.get(url)
                    self.emailAlertSent=True
            else:
                # data received recently        
                # check whether previous alert was sent
                Domoticz.Log("No lock in place and data received recently.")
                if self.notificationsOn and self.emailAlertSent==True:
                    Domoticz.Log("Sending email alert....")
                    subject = "PROBLEM SOLVED: Venus communication restored."
                    messageBody = "Problem for " + self.deviceType + " at " + self.IPAddress
                    url = "http://127.0.0.1:8080/json.htm?type=command&param=sendnotification"
                    url += "&subject=\'" + subject + "\'"
                    url += "&body=\'" + messageBody + "\'"
                    Domoticz.Log("url : "+url)
                    sendemail = requests.get(url)
                    self.emailAlertSent=False

            # now continue polling
            Domoticz.Log("Continue polling..")
            self.current_task=asyncio.run_coroutine_threadsafe(
                self.getVenusData(),
                self.loop
            )
            self.heartbeatCounter = 0


    def processValues(self, source, response):
        if self.showDataLog: Domoticz.Log(response)
        if debug: Domoticz.Log(response)
        for Dev in response:
            # do not process ID or the energy meter data received from getmode command in certain modes
            if (Dev!="id" and source!="ESM") or (source=="ESM" and (Dev=="mode" or Dev=="ongrid_power" or Dev=="offgrid_power" or Dev=="bat_soc")) :

                if source=="ESS": # handle the duplicate ESS field names, also received in other commands
                    if (Dev=="bat_soc" or Dev=="ongrid_power" or Dev=="offgrid_power"):
                        DevName="es_"+Dev
                    else:
                        DevName=Dev
                else:
                    DevName=Dev

                # first check whether any unexpected/new fields are received, avoid key errors
                if DEVSLIST.get(DevName)==None:
                    Domoticz.Error("Unexpected/new field received, source : "+source+" field "+DevName)
                    Domoticz.Error("API might have changed. Needs to be investigated.")
                else:

                    type=DEVSLIST[DevName][1]
                    subtype=DEVSLIST[DevName][2]
                    Unit=DEVSLIST[DevName][0]
                    DeviceID="{:04x}{:04x}".format(self.Hwid,Unit)

                    #if debug: Domoticz.Log("processing values "+source+" "+DevName+" "+str(response[Dev]))

                    if (Devices[DeviceID].Units[Unit].Used==1) : # only process active devices
                        if ((type==80) or # temperature device
                           (type==113) or # counter device
                           ((type==243) and (subtype==6)) or # percentage device
                           ((type==243) and (subtype==8)) or # percentage device
                           ((type==243) and (subtype==23)) or # percentage device
                           ((type==243) and (subtype==31)) # custom device
                              ):
                            multiplier=DEVSLIST[DevName][5]
                            if multiplier==1:
                                fieldValue=round(float(multiplier*response[Dev]),0)
                            else:
                                fieldValue=round(float(multiplier*response[Dev]),1)
                            Devices[DeviceID].Units[Unit].nValue=int(fieldValue)
                            Devices[DeviceID].Units[Unit].sValue=str(int(fieldValue))
                            Devices[DeviceID].Units[Unit].Update()
                        if ((type==243) and (subtype==19)): # text device
                            fieldValue=response[Dev]
                            Devices[DeviceID].Units[Unit].nValue=0
                            fieldText=str(fieldValue)
                            Devices[DeviceID].Units[Unit].sValue=fieldText
                            Devices[DeviceID].Units[Unit].Update()
                        if ((type==243) and (subtype==29)): # kwh device, instant+counter
                            multiplier=DEVSLIST[DevName][5]
                            fieldValue=round(float(multiplier*response[Dev]),0)
                            if fieldValue>=-20000 and fieldValue<20000 : # only "reasonable" values will be processed, not 655xx
                                Devices[DeviceID].Units[Unit].nValue=0
                                Devices[DeviceID].Units[Unit].sValue=str(fieldValue)+";1" # supply actual watts , kwh are calculated by Domoticz.
                                Devices[DeviceID].Units[Unit].Update()
                        if (type==244) : # switch device
                            fieldValue=response[Dev]
                            if fieldValue==True:
                                fieldValue=1
                            else:
                                fieldValue=0
                            Devices[DeviceID].Units[Unit].nValue=fieldValue
                            fieldText=str(fieldValue)
                            Devices[DeviceID].Units[Unit].sValue=fieldText
                            Devices[DeviceID].Units[Unit].Update()
                        if (type==248): # kW device
                            multiplier=DEVSLIST[DevName][5]
                            fieldValue=round(float(multiplier*response[Dev]),0)
                            Devices[DeviceID].Units[Unit].nValue=int(fieldValue)
                            fieldText=str(fieldValue)
                            Devices[DeviceID].Units[Unit].sValue=fieldText
                            Devices[DeviceID].Units[Unit].Update()

                        if DevName=="mode":
                            # mode switch will follow mode status received
                            modeSelectorUnit=DEVSLIST["select Marstek mode"][0]
                            modeswitchDeviceID="{:04x}{:04x}".format(self.Hwid,modeSelectorUnit)
                            fieldValue=response[Dev]
                            if fieldValue=="Auto":
                                Level=10
                            elif fieldValue=="AI":
                                Level=20
                            elif fieldValue=="Manual":
                                Level=30
                            elif fieldValue=="Passive":
                                Level=40
                            elif fieldValue=="UPS":
                                Level=50
                            Devices[modeswitchDeviceID].Units[modeSelectorUnit].sValue=str(Level)
                            Devices[modeswitchDeviceID].Units[modeSelectorUnit].Update()

                    # combine 3 EMS values onto one P1 device
                    if source=="EMS":
                        if DevName=="total_power":
                            self.saveTotalPower=int(response[Dev])
                        if DevName=="input_energy":
                            self.saveInputEnergy=int(int(response[Dev])/10)
                        if DevName=="output_energy":
                            self.saveOutputEnergy=int(int(response[Dev])/10)
                            # this is last value of 3, so now it can be processed
                            Unit=51 # fixed nr !!!
                            DeviceID="{:04x}{:04x}".format(self.Hwid,Unit)
                            Devices[DeviceID].Units[Unit].Refresh()
                            if (Devices[DeviceID].Units[Unit].Used==1) : # only process if P1 is an active device
                                if debug: Domoticz.Log("Updating P1 meter "+str(self.saveTotalPower)+" "+str(self.saveInputEnergy)+" "+str(self.saveOutputEnergy))
                                if self.saveTotalPower>=0:
                                    svalueString=str(self.saveInputEnergy)+";0;"+str(self.saveOutputEnergy)+";0;"+str(self.saveTotalPower)+";0"
                                else:
                                    svalueString=str(self.saveInputEnergy)+";0;"+str(self.saveOutputEnergy)+";0;0;"+str(-1*self.saveTotalPower)
                                if debug: Domoticz.Log(svalueString)
                                Devices[DeviceID].Units[Unit].sValue=svalueString
                                Devices[DeviceID].Units[Unit].nValue=0
                                Devices[DeviceID].Units[Unit].Update()

            #else:
                #if debug: Domoticz.Log("not processing values "+source+" "+Dev+" "+str(response[Dev]))


    async def getVenusData(self):
        if debug: Domoticz.Log("Marstek Plugin getVenusData called")

        self.Hwid=Parameters['HardwareID']
        #async with self.api_lock:
        if self.api_lock.locked():
            if debug: Domoticz.Log("Still locked, returning from getVenusData")
            return
        await self.api_lock.acquire()
        try:

            try:
                if not self.client._connected:
                    await self.client.connect()
            except Exception as e:
                Domoticz.Error(f"error on connect attempt : {e}")
            await asyncio.sleep(1.0)

            await asyncio.sleep(1.0)
            try:
                if debug: Domoticz.Log("trying get battery status ")
                responseBS=await self.client.get_battery_status()
                if debug: Domoticz.Log("battery status data received: "+str(responseBS))
                if responseBS is not None:
                    self.lastDataRecvdTime=time.time()
                    self.processValues("BAT",responseBS)
            except asyncio.CancelledError:
                raise  # NEVER swallow this
            except TimeoutError:
                Domoticz.Error(f"get battery status timeout. trying next ...")
            except Exception as e:
                Domoticz.Error(f"get battery status failed: {e}")

            await asyncio.sleep(1.0)
            try:
                if debug: Domoticz.Log("trying get em status ")
                responseEM=await self.client.get_em_status()
                if debug: Domoticz.Log("em status data received: "+str(responseEM))
                if responseEM is not None:
                    self.lastDataRecvdTime=time.time()
                    self.processValues("EMS",responseEM)
            except asyncio.CancelledError:
                raise  # NEVER swallow this
            except TimeoutError:
                Domoticz.Error(f"get em status timeout.  trying next ...")
            except Exception as e:
                Domoticz.Error(f"get em status failed: {e}")

            await asyncio.sleep(1.0)
            try:
                if debug: Domoticz.Log("trying get es status ")
                responseES=await self.client.get_es_status()
                if debug: Domoticz.Log("es status data received: "+str(responseES))
                if responseES is not None:
                    self.lastDataRecvdTime=time.time()
                    self.processValues("ESS",responseES)
            except asyncio.CancelledError:
                raise  # NEVER swallow this
            except TimeoutError:
                Domoticz.Error(f"get es status timeout. trying next ...")
            except Exception as e:
                Domoticz.Error(f"get es status failed: {e}")

            await asyncio.sleep(1.0)
            try:
                if debug: Domoticz.Log("trying get es mode ")
                responseESM=await self.client.get_es_mode()
                if debug: Domoticz.Log("get mode data received: "+str(responseESM))
                if responseESM is not None:
                    self.lastDataRecvdTime=time.time()
                    self.processValues("ESM",responseESM)
            except asyncio.CancelledError:
                raise  # NEVER swallow this
            except TimeoutError:
                Domoticz.Error(f"get es mode timeout. trying next ...")
            except Exception as e:
                Domoticz.Error(f"get es mode failed: {e}")

            await asyncio.sleep(1.0)
            if self.deviceType==DEVICE_MODEL_VENUS_D or self.deviceType==DEVICE_MODEL_VENUS_A:
                try:
                    if debug: Domoticz.Log("trying get pv status ")
                    responsePV=await self.client.get_pv_status()
                    if debug: Domoticz.Log("pv status data received: "+str(responsePV))
                    if responsePV is not None:
                        self.lastDataRecvdTime=time.time()
                        self.processValues("PV",responsePV)
                except asyncio.CancelledError:
                    raise  # NEVER swallow this
                except TimeoutError:
                    Domoticz.Error(f"get pv status timeout. trying next ...")
                except Exception as e:
                    Domoticz.Error(f"get pv status failed: {e}")

        except TimeoutError:
            # never activated because already handled?
            Domoticz.Error("Timeout on getting Marstek Venus data from "+self.deviceType+" at "+self.IPAddress+" Check connection and/or Open API setting in App.")
        
        except asyncio.CancelledError:
            Domoticz.Error("Cancellation on getting Marstek Venus data from "+self.deviceType+" at "+self.IPAddress+"Check connection and/or Open API setting in App (unless plugin was stopped manually).")

        except Exception as e:
            Domoticz.Error(f"Errors in getting Marstek Venus data from "+self.deviceType+" at "+self.IPAddress+". Check results.")

        finally:
            if self.api_lock.locked():
                self.api_lock.release()

    async def reset_client(self):
        try:
            Domoticz.Error("Resetting Marstek client connection")

            try:
                for task in asyncio.all_tasks():
                    if task is not asyncio.current_task():
                        task.cancel()
            except Exception as e:
                Domoticz.Error(f"Task cancel error: {e}")

            await asyncio.sleep(0.1)

            self.api_lock = asyncio.Lock()

            try:
                await self.client.disconnect()
            except:
                pass

            await asyncio.sleep(1)
            
            self.client = MarstekUDPClient(host=self.IPAddress, port=self.Port)

            await self.client.connect()
            await asyncio.sleep(2)

        except Exception as e:
            Domoticz.Error(f"Client reset failed: {e}")



global _plugin
_plugin = MarstekPlugin()

def onStart():
    global _plugin
    _plugin.onStart()

def onStop():
    global _plugin
    _plugin.onStop()

def onConnect(Connection, Status, Description):
    global _plugin
    _plugin.onConnect(Connection, Status, Description)

def onMessage(Connection, Data):
    global _plugin
    _plugin.onMessage(Connection, Data)

def onCommand(DeviceID, Unit, Command, Level, Color):
    global _plugin
    _plugin.onCommand(DeviceID, Unit, Command, Level, Color)

def onNotification(Name, Subject, Text, Status, Priority, Sound, ImageFile):
    global _plugin
    _plugin.onNotification(Name, Subject, Text, Status, Priority, Sound, ImageFile)

def onDisconnect(Connection):
    global _plugin
    _plugin.onDisconnect(Connection)

def onHeartbeat():
    global _plugin
    _plugin.onHeartbeat()

# Generic helper functions
def DumpConfigToLog():
    for x in Parameters:
        if Parameters[x] != "":
            Domoticz.Debug( "'" + x + "':'" + str(Parameters[x]) + "'")
    Domoticz.Debug("Device count: " + str(len(Devices)))
    for DeviceName in Devices:
        Device = Devices[DeviceName]
        Domoticz.Debug("Device ID:       '" + str(Device.DeviceID) + "'")
        Domoticz.Debug("--->Unit Count:      '" + str(len(Device.Units)) + "'")
        for UnitNo in Device.Units:
            Unit = Device.Units[UnitNo]
            Domoticz.Debug("--->Unit:           " + str(UnitNo))
            Domoticz.Debug("--->Unit Name:     '" + Unit.Name + "'")
            Domoticz.Debug("--->Unit nValue:    " + str(Unit.nValue))
            Domoticz.Debug("--->Unit sValue:   '" + Unit.sValue + "'")
            Domoticz.Debug("--->Unit LastLevel: " + str(Unit.LastLevel))
    return
