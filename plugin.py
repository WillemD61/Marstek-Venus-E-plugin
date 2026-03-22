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

#from venus_api_v2 import VenusAPIClient

import asyncio, threading
from dataclasses import dataclass, field
from datetime import timedelta
import importlib.util
import json
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
MODE_AUTO = const.MODE_AUTO
MODE_AI = const.MODE_AI
MODE_MANUAL = const.MODE_MANUAL
MODE_PASSIVE = const.MODE_PASSIVE
WEEKDAY_MAP = const.WEEKDAY_MAP
MAX_SCHEDULE_SLOTS = const.MAX_SCHEDULE_SLOTS

def format_value(value: Any, unit: str = "") -> str:
    """Format value with unit for display."""
    if value is None:
        return "N/A"
    if isinstance(value, (int, float)):
        return f"{value}{unit}"
    return str(value)


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

    def start_async_loop(self):
        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(
            target=self.loop.run_forever,
            daemon=True
        )
        self.loop_thread.start()
    
    def onStart(self):
        global debug, client
        Domoticz.Log("onStart called with parameters")
        for elem in Parameters:
            Domoticz.Log(str(elem)+" "+str(Parameters[elem]))
        self.IPAddress=str(Parameters["Address"])
        self.Port=int(Parameters["Port"])
        if int(Parameters["Mode1"])<=30: # heartbeat is max 30 seconds, so >30 seconds requires skipping action on heartbeat
            Domoticz.Heartbeat(int(Parameters["Mode1"]))
            self.heartbeatWaits=0
        else:
            Domoticz.Heartbeat(30)
            self.heartbeatWaits=int(int(Parameters["Mode1"])/30 - 1)
        self.notificationsOn=(Parameters["Mode2"]=="Yes")
        self.emailAlertSent=False
        self.failedCycleCount=0
        self.showDataLog=(Parameters["Mode3"]=="Yes")
        self.maxOutputPower=int(Parameters["Mode4"])
        debug=(Parameters["Mode5"]=="Yes")
        self.namePrefix=str(Parameters["Mode6"])
        self.heartbeatCounter=0
        self.stillbusy=False
        self.Hwid=Parameters['HardwareID']
        # cycle through device list and create any non-existing devices when the plugin/domoticz is started
        for Dev in DEVSLIST:
            Unit=DEVSLIST[Dev][0]
            DeviceID="{:04x}{:04x}".format(self.Hwid,Unit)
            Type=DEVSLIST[Dev][1]
            Subtype=DEVSLIST[Dev][2]
            Switchtype=DEVSLIST[Dev][3]
            Options=DEVSLIST[Dev][4]
            Name=self.namePrefix+DEVSLIST[Dev][6]
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
        for Dev in DEVSLIST:
            Domoticz.Log("DEVSLIST "+str(DEVSLIST[Dev][0])+DEVSLIST[Dev][6])
        #client = VenusAPIClient(ip=self.IPAddress, port=self.Port, timeout=5)
        client = MarstekUDPClient(host=self.IPAddress,port=self.Port)
        self.start_async_loop()

    def onStop(self):
        Domoticz.Log("onStop called")
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.loop_thread.join()

    def onConnect(self, Connection, Status, Description):
        Domoticz.Log("onConnect called")

    def onMessage(self, Connection, Data):
        Domoticz.Log("onMessage called")

    def onCommand(self, DeviceID, Unit, Command, Level, Color):
        # used when a mode is selected using the selector switch in Domoticz
        if debug: Domoticz.Log("onCommand called for Device " + str(DeviceID) + " Unit " + str(Unit) + ": Parameter '" + str(Command) + "', Level: " + str(Level))
        modeSelectorUnit=DEVSLIST["select Marstek mode"][0]
        expectedDeviceID="{:04x}{:04x}".format(self.Hwid,modeSelectorUnit)
        maxNrOfAttempts=3
        nrAttemptsDone=0
        try:
            if str(Command)=="Set Level" and DeviceID==expectedDeviceID: # it is a mode change initiated using the selector switch
                client = VenusAPIClient(ip=self.IPAddress, port=self.Port, timeout=5)
                if Level==10: # auto mode (=self consumption)
                    success=client.set_auto_mode()
                    while not success and nrAttemptsDone<maxNrOfAttempts:
                        Domoticz.Error("Change to auto mode (=self consumption mode) failed, retrying ...")
                        success=client.set_auto_mode()
                        nrAttemptsDone+=1
                    if success:
                        if debug: Domoticz.Log("Succesfully changed to auto mode (=self consumption mode).")
                        Devices[DeviceID].Units[Unit].sValue=str(Level)
                        Devices[DeviceID].Units[Unit].Update()
                    else:
                        Domoticz.Error("Change to auto mode (=self consumption mode) failed.")
                elif Level==20: # AI mode
                    success=client.set_ai_mode()
                    while not success and nrAttemptsDone<maxNrOfAttempts:
                        Domoticz.Error("Change to AI optimisation mode failed, retrying ...")
                        success=client.set_ai_mode()
                        nrAttemptsDone+=1
                    if success:
                        Domoticz.Log("Succesfully changed to AI optimisation mode.")
                        Devices[DeviceID].Units[Unit].sValue=str(Level)
                        Devices[DeviceID].Units[Unit].Update()
                    else:
                        Domoticz.Error("Change to AI optimisation mode failed.")
                elif Level==30: # manual mode
                    # check and build parameters. the following devices should contain config data
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
                    if int(timeperiod)>=0 and int(timeperiod)<=9:
                        startHr=int(starttime[0:2])
                        startMm=int(starttime[3:5])
                        endHr=int(endtime[0:2])
                        endMm=int(endtime[3:5])
                        if (startHr>=0 and startHr<=23 and startMm>=0 and startMm<=59) and (endHr>=0 and endHr<=23 and endMm>=0 and endMm<=59):
                            if (startHr*60+startMm)<(endHr*60+endMm):
                                starttimestring=starttime[0:2]+":"+starttime[3:5] # make sure separator is ":"
                                endtimestring=endtime[0:2]+":"+endtime[3:5] # make sure separator is ":"
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
                                if weekdayValid:
                                    mmpower=int(mmpower)
                                    # positive is discharge, negative is charge
                                    if mmpower<=self.maxOutputPower and mmpower>=-1200:
                                        # all validation done
                                        enable=1 # assuming period should be active
                                        success=client.set_manual_mode(mmpower,int(timeperiod),starttimestring,endtimestring,weekdayvalue,enable)
                                        while not success and nrAttemptsDone<maxNrOfAttempts:
                                            Domoticz.Error("Change to manual mode failed, retrying ...")
                                            success=client.set_manual_mode(mmpower,int(timeperiod),starttimestring,endtimestring,weekdayvalue,enable)
                                            nrAttemptsDone+=1
                                        if success:
                                            Domoticz.Log("Succesfully changed to manual mode."+str(mmpower))
                                            Devices[DeviceID].Units[Unit].sValue=str(Level)
                                            Devices[DeviceID].Units[Unit].Update()
                                        else:
                                            Domoticz.Error("Change to manual mode failed")
                                    else:
                                        Domoticz.Error("Error: power settings not valid for manual mode.")
                                else:
                                    Domoticz.Error("Error: weekday settings not valid for manual mode, must be 7x 0/1")
                            else:
                                Domoticz.Error("Error: start time must be before end time for manual mode")
                        else:
                            Domoticz.Error("No valid start or end time set for manual mode")
                    else:
                        Domoticz.Error("No valid timeperiod set for manual mode")
                elif Level==40: # passive mode
                    # check and build parameters for passive mode, note: removed because they did not have an effect
                    #pmpowerUnit=DEVSLIST["pm_power"][0]
                    #countdownUnit=DEVSLIST["countdown"][0]
                    #pmpower=Devices["{:04x}{:04x}".format(self.Hwid,pmpowerUnit)].Units[pmpowerUnit].sValue
                    #countdown=Devices["{:04x}{:04x}".format(self.Hwid,countdownUnit)].Units[countdownUnit].sValue
                    #pmpower=int(pmpower)
                    #countdown=int(countdown)
                    pmpower=0
                    countdown=0 # note both power and countdown are required but don't seem to have an effect
                    if pmpower<=self.maxOutputPower and pmpower>=-1200:
                        # all validation done
                        # note power and countdown are required but do not seem to have an effect
                        success=client.set_passive_mode(pmpower,countdown)
                        while not success and nrAttemptsDone<maxNrOfAttempts:
                            Domoticz.Error("Change to passive mode failed, retrying ...")
                            success=client.set_passive_mode(pmpower,countdown)
                            nrAttemptsDone+=1
                        if success:
                            Domoticz.Log("Succesfully changed to passive mode.")
                            Devices[DeviceID].Units[Unit].sValue=str(Level)
                            Devices[DeviceID].Units[Unit].Update()
                        else:
                            Domoticz.Error("Change to passive mode failed")
                    else:
                        Domoticz.Error("No valid power setting for passive mode")
                elif Level==50: # UPS
                    # check and build parameters for UPS mode
                    # note power is required but does not seem to have an effect, 0 used
                    upower=0
                    success=client.set_ups_mode(upower)
                    while not success and nrAttemptsDone<maxNrOfAttempts:
                        Domoticz.Error("Change to UPS mode failed, retrying ...")
                        success=client.set_ups_mode(upower)
                        nrAttemptsDone+=1
                    if success:
                        Domoticz.Log("Succesfully changed to UPS mode.")
                        Devices[DeviceID].Units[Unit].sValue=str(Level)
                        Devices[DeviceID].Units[Unit].Update()
                    else:
                        Domoticz.Error("Change to UPS mode failed")
            else:
                if debug: Domoticz.Log("Command "+str(Command)+" DeviceID "+DeviceID+" ExpectedID "+expectedDeviceID)
        except ValueError:
            Domoticz.Error("Change of mode failed, please check format of input parameters for conversion to integer.")
        except:
            Domoticz.Error("Change of mode failed, an unexpected error occurred.")


    def onNotification(self, Name, Subject, Text, Status, Priority, Sound, ImageFile):
        Domoticz.Log("Notification: " + Name + "," + Subject + "," + Text + "," + Status + "," + str(Priority) + "," + Sound + "," + ImageFile)

    def onDisconnect(self, Connection):
        Domoticz.Log("onDisconnect called")

    def onHeartbeat(self):
        self.heartbeatCounter+=1
        if debug: Domoticz.Log("onHeartbeat called")
        if self.stillbusy and self.heartbeatCounter<2*self.heartbeatWaits: # max 2 polling intervals total wait
            if debug: Domoticz.Log("Skipping another heartbeat - data collection still busy")
            return
        else:
            if self.stillbusy:
                # it all takes to long .. restart polling
                self.stillbusy=False
                self.heartbeatCounter=0
            else:    
                # skip one or more heartbeats if polling interval > 30 seconds
                if self.heartbeatWaits==self.heartbeatCounter-1:
                    self.stillbusy=True
                    asyncio.run_coroutine_threadsafe(
                        self.getVenusData(),
                        self.loop
                    )
                    self.heartbeatCounter=0
  

    def processValues(self, source, response):
        if self.showDataLog: Domoticz.Log(response)
        if debug: Domoticz.Log(response)
        for Dev in response:
            Domoticz.Log("Processing Dev "+Dev)
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

                    if debug: Domoticz.Log("processing values "+source+" "+DevName+" "+str(response[Dev]))

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
                    Domoticz.Log("single devices updated, now checking for P1")
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

            else:
                if debug: Domoticz.Log("not processing values "+source+" "+Dev+" "+str(response[Dev]))
            Domoticz.Log("processing values complete for "+source)        



    async def getVenusData(self):
        if debug: Domoticz.Log("Marstek Plugin getVenusData called")
        self.Hwid=Parameters['HardwareID']
        try:
            try:
                await client.connect()
                #client.host = self.IPAddress
                self.someResponseReceived=False
                #client = VenusAPIClient(ip=self.IPAddress, port=self.Port, timeout=5)
            except Exception as e:
                Domoticz.Error(f"connect failed: {e}")

            try:
                if debug: Domoticz.Log("trying bat status data ")
                responseBS=await client.get_battery_status()
                if debug: Domoticz.Log("battery status data received: "+str(responseBS))
                if responseBS is not None:
                    self.someResponseReceived=True
                    self.processValues("BAT",responseBS)
            except asyncio.CancelledError:
                raise  # NEVER swallow this
            except Exception as e:
                Domoticz.Error(f"BAT failed: {e}")         

            try:
                if debug: Domoticz.Log("trying em status data ")
                responseEM=await client.get_em_status()
                if debug: Domoticz.Log("em status data received: "+str(responseEM))
                if responseEM is not None:
                    self.someResponseReceived=True
                    self.processValues("EMS",responseEM)
            except asyncio.CancelledError:
                raise  # NEVER swallow this
            except Exception as e:
                Domoticz.Error(f"EMS failed: {e}")   

            try:
                if debug: Domoticz.Log("trying es status data ")
                responseES=await client.get_es_status()
                if debug: Domoticz.Log("es status data received: "+str(responseES))
                if responseES is not None:
                    self.someResponseReceived=True
                    self.processValues("ESS",responseES)
            except asyncio.CancelledError:
                raise  # NEVER swallow this                
            except Exception as e:
                Domoticz.Error(f"ESS failed: {e}")                

            try:
                if debug: Domoticz.Log("trying es mode data ")
                responseESM=await client.get_es_mode()
                if debug: Domoticz.Log("get mode data received: "+str(responseESM))
                if responseESM is not None:
                    self.someResponseReceived=True
                    self.processValues("ESM",responseESM)
            except asyncio.CancelledError:
                raise  # NEVER swallow this                
            except Exception as e:
                Domoticz.Error(f"ESM failed: {e}")

            try:   
                if debug: Domoticz.Log("trying pv status data ") 
                responsePV=await client.get_pv_status()
                if debug: Domoticz.Log("pv status data received: "+str(responsePV))
                if responsePV is not None:
                    self.someResponseReceived=True
                    self.processValues("PV",responsePV)
            except asyncio.CancelledError:
                raise  # NEVER swallow this                
            except Exception as e:
                Domoticz.Error(f"PV failed: {e}")                

            if self.emailAlertSent==True and self.someResponseReceived==True:
                if debug: Domoticz.Log("Communication restored. Data was received again during getVenusData cycle")
                self.emailAlertSent=False
                self.failedCycleCount=0
                sendemail=requests.get("http://127.0.0.1:8080/json.htm?type=command&param=sendnotification&subject='Venus comms working again'&body='Problem solved'")

            if self.someResponseReceived==False:
                Domoticz.Error("No data received during complete cycle. Cycle nr "+str(self.failedCycleCount+1))
                raise TimeoutError
            return True

        except TimeoutError:
            Domoticz.Error("Timeout on getting Marstek Venus data. Check connection and/or Open API setting in App.")
            self.failedCycleCount+=1
            if self.notificationsOn and self.emailAlertSent==False and self.failedCycleCount>=3:
                # sending email after 3 full cycle failures of all 6 retrieval commands (usually due to Open API disabled)
                Domoticz.Log("Sending email alert....")
                sendemail=requests.get("http://127.0.0.1:8080/json.htm?type=command&param=sendnotification&subject='ATTENTION: Venus communication timeout, check connection and Open API setting'&body='Please check'")
                self.emailAlertSent=True
            return False

        except asyncio.CancelledError:
            Domoticz.Error("Cancellation error on getting Marstek Venus data. Check connection and/or Open API setting in App.")
            self.failedCycleCount+=1
            return False
            
        except Exception as e:
            Domoticz.Error(f"Errors in getting Marstek Venus data. Check results. {e}")
            self.failedCycleCount+=1
            if self.notificationsOn and self.emailAlertSent==False:
                Domoticz.Log("Sending email alert....")
                sendemail=requests.get("http://127.0.0.1:8080/json.htm?type=command&param=sendnotification&subject='ATTENTION: Venus communication data error'&body='Please check the log and solve the error.'")
                self.emailAlertSent=True
            return False

        finally:
            await client.disconnect()
            self.stillbusy = False

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
