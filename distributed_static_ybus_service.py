# Copyright (c) 2023, Battelle Memorial Institute All rights reserved.
# Battelle Memorial Institute (hereinafter Battelle) hereby grants permission to any person or entity
# lawfully obtaining a copy of this software and associated documentation files (hereinafter the
# Software) to redistribute and use the Software in source and binary forms, with or without modification.
# Such person or entity may use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
# the Software, and may permit others to do so, subject to the following conditions:
# Redistributions of source code must retain the above copyright notice, this list of conditions and the
# following disclaimers.
# Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
# the following disclaimer in the documentation and/or other materials provided with the distribution.
# Other than as used herein, neither the name Battelle Memorial Institute or Battelle may be used in any
# form whatsoever without the express written consent of Battelle.
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
# BATTELLE OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY,
# OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
# GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
# OF THE POSSIBILITY OF SUCH DAMAGE.
# General disclaimer for use with OSS licenses
#
# This material was prepared as an account of work sponsored by an agency of the United States Government.
# Neither the United States Government nor the United States Department of Energy, nor Battelle, nor any
# of their employees, nor any jurisdiction or organization that has cooperated in the development of these
# materials, makes any warranty, express or implied, or assumes any legal liability or responsibility for
# the accuracy, completeness, or usefulness or any information, apparatus, product, software, or process
# disclosed, or represents that its use would not infringe privately owned rights.
#
# Reference herein to any specific commercial product, process, or service by trade name, trademark, manufacturer,
# or otherwise does not necessarily constitute or imply its endorsement, recommendation, or favoring by the United
# States Government or any agency thereof, or Battelle Memorial Institute. The views and opinions of authors expressed
# herein do not necessarily state or reflect those of the United States Government or any agency thereof.
#
# PACIFIC NORTHWEST NATIONAL LABORATORY operated by BATTELLE for the
# UNITED STATES DEPARTMENT OF ENERGY under Contract DE-AC05-76RL01830
#-------------------------------------------------------------------------------

from argparse import ArgumentParser
import json
import logging
import os
from pathlib import Path
import time
from typing import Dict, Optional

from cimgraph.data_profile import CIM_PROFILE
import gridappsd.field_interface.agents.agents as agents_mod
from gridappsd.field_interface.agents import FeederAgent, SwitchAreaAgent, SecondaryAreaAgent
from gridappsd.field_interface.interfaces import FieldMessageBus, MessageBusDefinition

import ybus_utils as utils

#TODO: query gridappsd-python for correct cim_profile instead of hardcoding it.
cim_profile = CIM_PROFILE.RC4_2021.value
agents_mod.set_cim_profile(cim_profile, iec61970_301=7)
# cim_profile = CIM_PROFILE.CIMHUB_2023.value
# agents_mod.set_cim_profile(cim_profile, iec61970_301=8)
cim = agents_mod.cim
logging.basicConfig(format='%(asctime)s::%(levelname)s::%(name)s::%(filename)s::%(lineno)d::%(message)s',
                    filename='DistributedYBus.log',
                    filemode='w',
                    level=logging.INFO,
                    encoding='utf-8')
logger = logging.getLogger(__name__)


class FeederAgentLevelStaticYbusService(FeederAgent):

    def __init__(self,
                 upstream_message_bus_def: MessageBusDefinition,
                 downstream_message_bus_def: MessageBusDefinition,
                 service_config: Dict,
                 feeder_dict: Optional[Dict] = None,
                 simulation_id: Optional[str] = None):
        super().__init__(upstream_message_bus_def, downstream_message_bus_def, service_config, feeder_dict,
                         simulation_id)
        self.isYbusInitialized = False
        self.isServiceInitialized = False
        if self.feeder_area is not None:
            self.isServiceInitialized = True

    def updateYbusService(self):
        if self.feeder_area is not None:
            utils.initializeCimProfile(self.feeder_area)
            self.ybus = utils.calculateYbus(self.feeder_area)
            self.isYbusInitialized = True
            logger.info(f"The Ybus for feederStaticYbusService in area id {self.feeder_area.container.mRID} is:\n"
                        f"{json.dumps(self.ybus, indent=4, sort_keys=True, cls=utils.ComplexEncoder)}")
        else:
            logger.error(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s feeder_area None. The service "
                         "is malformed.")
            raise RuntimeError(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s feeder_area None. The "
                               "service is malformed.")

    def on_request(self, message_bus: FieldMessageBus, headers: Dict, message: Dict):
        if message.get("requestType", "") == "LocalYbus":
            if not self.isYbusInitialized:
                self.updateYbusService()
            message_bus.send(headers.get('reply-to'), self.ybus)
        if message.get("requestType", "") == "is_initialized":
            response = {"is_initialized": self.isServiceInitialized}
            message_bus.send(headers.get('reply-to'), response)


class SwitchAreaAgentLevelStaticYbusService(SwitchAreaAgent):

    def __init__(self,
                 upstream_message_bus_def: MessageBusDefinition,
                 downstream_message_bus_def: MessageBusDefinition,
                 service_config: Dict,
                 switch_area_dict: Optional[Dict] = None,
                 simulation_id: Optional[str] = None):
        super().__init__(upstream_message_bus_def, downstream_message_bus_def, service_config, switch_area_dict,
                         simulation_id)
        self.isYbusInitialized = False
        self.isServiceInitialized = False
        if self.switch_area is not None:
            self.isServiceInitialized = True

    def updateYbusService(self):
        if self.switch_area is not None:
            utils.initializeCimProfile(self.switch_area)
            self.ybus = utils.calculateYbus(self.switch_area)
            self.isYbusInitialized = True
            logger.info(f"The Ybus for SwitchAreaYbusService in area id {self.switch_area.container.mRID} is:\n"
                        f"{json.dumps(self.ybus, indent=4, sort_keys=True, cls=utils.ComplexEncoder)}")
        else:
            logger.error(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s switch_area None. The service "
                         "is malformed.")
            raise RuntimeError(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s switch_area None. The "
                               "service is malformed.")

    def on_request(self, message_bus: FieldMessageBus, headers: Dict, message: Dict):
        if message.get("requestType", "") == "LocalYbus":
            if not self.isYbusInitialized:
                self.updateYbusService()
            message_bus.send(headers.get('reply-to'), self.ybus)
        if message.get("requestType", "") == "is_initialized":
            response = {"is_initialized": self.isServiceInitialized}
            message_bus.send(headers.get('reply-to'), response)


class SecondaryAreaAgentLevelStaticYbusService(SecondaryAreaAgent):

    def __init__(self,
                 upstream_message_bus_def: MessageBusDefinition,
                 downstream_message_bus_def: MessageBusDefinition,
                 service_config: Dict,
                 secondary_area_dict: Optional[Dict] = None,
                 simulation_id: Optional[str] = None):
        super().__init__(upstream_message_bus_def, downstream_message_bus_def, service_config, secondary_area_dict,
                         simulation_id)
        self.isYbusInitialized = False
        self.isServiceInitialized = False
        if self.secondary_area is not None:
            self.isServiceInitialized = True

    def updateYbusService(self):
        if self.secondary_area is not None:
            utils.initializeCimProfile(self.secondary_area)
            self.ybus = utils.calculateYbus(self.secondary_area)
            self.isYbusInitialized = True
            logger.info(f"The Ybus for SecondaryAreaYbusService in area id {self.secondary_area.container.mRID} is:\n"
                        f"{json.dumps(self.ybus, indent=4, sort_keys=True, cls=utils.ComplexEncoder)}")
        else:
            logger.error(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s secondary_area None. The "
                         "service is malformed.")
            raise RuntimeError(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s secondary_area None. The "
                               "service is malformed.")

    def on_request(self, message_bus: FieldMessageBus, headers: Dict, message: Dict):
        if message.get("requestType", "") == "localYbus":
            if not self.isYbusInitialized:
                self.updateYbusService()
            message_bus.send(headers.get('reply-to'), self.ybus)
        if message.get("requestType", "") == "is_initialized":
            response = {"is_initialized": self.isServiceInitialized}
            message_bus.send(headers.get('reply-to'), response)


def getMessageBusDefinition(areaId: str) -> MessageBusDefinition:
    if not isinstance(areaId, str):
        raise TypeError(f"area id is not a string type.\ntype: {type(areaId)}")
    connectionArgs = {
        "GRIDAPPSD_ADDRESS": os.environ.get('GRIDAPPSD_ADDRESS', "tcp://gridappsd:61613"),
        "GRIDAPPSD_USER": os.environ.get('GRIDAPPSD_USER'),
        "GRIDAPPSD_PASSWORD": os.environ.get('GRIDAPPSD_PASSWORD'),
        "GRIDAPPSD_APPLICATION_ID": os.environ.get('GRIDAPPSD_APPLICATION_ID')
    }
    bus = MessageBusDefinition(id=areaId,
                               is_ot_bus=True,
                               connection_type="GRIDAPPSD_TYPE_GRIDAPPSD",
                               conneciton_args=connectionArgs)
    return bus


def main():
    parser = ArgumentParser()
    serviceConfigHelpStr = "Variable keyword arguments that provide user defined distributed area agent " \
                           "configuration files. Valid keywords are as follows:SYSTEM_BUS_CONFIG_FILE=<full " \
                           "path of the system bus configuration file>. FEEDER_BUS_CONFIG_FILE=<full path of the " \
                           "feeder bus configuration file>. SWITCH_BUS_CONFIG_FILE=<full path to the " \
                           "directory containing the switch bus configuration file(s)>. " \
                           "SECONDARY_BUS_CONFIG_FILE=<full path to the directory containing the secondary bus " \
                           "configuration file(s)>. MODEL_MRID=<The desired model mrid to automatically start a " \
                           "ybus service for all the distributed areas for that model id>. This variable should " \
                           "not be specified in combination with SYSTEM_BUS_CONFIG_FILE, FEEDER_BUS_CONFIG_FILE, " \
                           "SWITCH_BUS_CONFIG_FILE_DIR, and SECONDARY_BUS_CONFIG_FILE_DIR, as it will override those " \
                           "values. The directory needs to contain at least one of the following folder names: " \
                           "feeder_level, secondary_level, switch_level, and/or system_level."
    parser.add_argument("service_configurations", nargs="+", help=serviceConfigHelpStr)
    args = parser.parse_args()
    validKeywords = [
        "MODEL_MRID", "SYSTEM_BUS_CONFIG_FILE", "FEEDER_BUS_CONFIG_FILE", "SWITCH_BUS_CONFIG_FILE",
        "SECONDARY_BUS_CONFIG_FILE"
    ]
    mainArgs = {}
    for arg in args.service_configurations:
        argSplit = arg.split("=", maxsplit=1)
        if argSplit[0] in validKeywords:
            mainArgs[argSplit[0]] = argSplit[1]
        else:
            logger.error(f"Invalid keyword argument, {argSplit[0]}, was given by the user. Valid keywords are "
                         f"{validKeywords}.")
            raise RuntimeError(f"Invalid keyword argument, {argSplit[0]}, was given by the user. Valid keywords are "
                               f"{validKeywords}.")
    if len(mainArgs) == 0:
        logger.error(f"No arguments were provided by the user. Valid keywords are {validKeywords}.")
        raise RuntimeError(f"No arguments were provided by the user. Valid keywords are {validKeywords}.")
    modelMrid = mainArgs.get("MODEL_MRID")
    systemMessageBusConfigFile = mainArgs.get("SYSTEM_BUS_CONFIG_FILE")
    feederMessageBusConfigFile = mainArgs.get("FEEDER_BUS_CONFIG_FILE")
    switchAreaMessageBusConfigFile = mainArgs.get("SWITCH_BUS_CONFIG_FILE")
    secondaryAreaMessageBusConfigFile = mainArgs.get("SECONDARY_BUS_CONFIG_FILE")
    if not isinstance(systemMessageBusConfigFile, str) and systemMessageBusConfigFile is not None:
        errorStr = f"system_bus_config_file isn't a str type.\ntype: {type(systemMessageBusConfigFile)}"
        logger.error(errorStr)
        raise TypeError(errorStr)
    if not isinstance(feederMessageBusConfigFile, str) and feederMessageBusConfigFile is not None:
        errorStr = f"feeder_bus_config_file isn't a str type.\ntype: {type(feederMessageBusConfigFile)}"
        logger.error(errorStr)
        raise TypeError(errorStr)
    if not isinstance(switchAreaMessageBusConfigFile, str) and switchAreaMessageBusConfigFile is not None:
        errorStr = f"switch_bus_config_file isn't a str type.\ntype: {type(switchAreaMessageBusConfigFile)}"
        logger.error(errorStr)
        raise TypeError(errorStr)
    if not isinstance(secondaryAreaMessageBusConfigFile, str) and secondaryAreaMessageBusConfigFile is not None:
        errorStr = f"secondary_bus_config_file isn't a str type.\ntype: {type(secondaryAreaMessageBusConfigFile)}"
        logger.error(errorStr)
        raise TypeError(errorStr)
    if not isinstance(modelMrid, str) and modelMrid is not None:
        errorStr = f"model_mrid is not a string.\ntype: {type(modelMrid)}"
        logger.error(errorStr)
        raise TypeError(errorStr)
    serviceMetadata = {
        "app_id": "distributed_static_ybus_service",
        "description": "This is a GridAPPS-D distributed static ybus service agent."
    }
    servicesAreRunning = False
    runningYbusServiceInfo = []
    runningServiceInstances = []
    feederMessageBusDef = None
    switchAreaMessageBusDef = None
    if modelMrid is None:
        if feederMessageBusConfigFile is not None and systemMessageBusConfigFile is not None:
            systemMessageBusDef = MessageBusDefinition.load(systemMessageBusConfigFile)
            feederMessageBusDef = MessageBusDefinition.load(feederMessageBusConfigFile)
            logger.info(f"Creating Feeder Area Ybus Service for area id: {feederMessageBusDef.id}")
            feederYbusService = FeederAgentLevelStaticYbusService(systemMessageBusDef, feederMessageBusDef,
                                                                  serviceMetadata)
            runningYbusServiceInfo.append(f"{type(feederYbusService).__name__}:{feederMessageBusDef.id}")
            runningServiceInstances.append(feederYbusService)
        if switchAreaMessageBusConfigFile is not None and feederMessageBusConfigFile is not None:
            if feederMessageBusDef is None:
                feederMessageBusDef = MessageBusDefinition.load(feederMessageBusConfigFile)
            switchAreaMessageBusDef = MessageBusDefinition.load(switchAreaMessageBusConfigFile)
            logger.info(f"Creating Switch Area Ybus Service for area id: {switchAreaMessageBusDef.id}")
            switchAreaService = SwitchAreaAgentLevelStaticYbusService(feederMessageBusDef, switchAreaMessageBusDef,
                                                                      serviceMetadata)
            runningYbusServiceInfo.append(f"{type(switchAreaService).__name__}:{switchAreaMessageBusDef.id}")
            runningServiceInstances.append(switchAreaService)
        if secondaryAreaMessageBusConfigFile is not None and switchAreaMessageBusConfigFile is not None:
            if switchAreaMessageBusDef is None:
                switchAreaMessageBusDef = MessageBusDefinition.load(switchAreaMessageBusConfigFile)
            secondaryAreaMessageBusDef = MessageBusDefinition.load(secondaryAreaMessageBusConfigFile)
            logger.info(f"Creating Secondary Area Ybus Service for area id: {secondaryAreaMessageBusDef.id}")
            secondaryAreaService = SecondaryAreaAgentLevelStaticYbusService(switchAreaMessageBusDef,
                                                                            secondaryAreaMessageBusDef, serviceMetadata)
            if len(secondaryAreaService.agent_area_dict['addressable_equipment']) == 0 and \
                    len(secondaryAreaService.agent_area_dict['unaddressable_equipment']) == 0:
                secondaryAreaService.disconnect()
            else:
                runningYbusServiceInfo.append(f"{type(secondaryAreaService).__name__}:"
                                              f"{secondaryAreaMessageBusDef.id}")
                runningServiceInstances.append(secondaryAreaService)
    else:
        systemMessageBusDef = getMessageBusDefinition(modelMrid)
        feederMessageBusDef = getMessageBusDefinition(modelMrid)
        logger.info(f"Creating Feeder Area Ybus Service for area id: {feederMessageBusDef.id}")
        feederYbusService = FeederAgentLevelStaticYbusService(systemMessageBusDef, feederMessageBusDef, serviceMetadata)
        #feederYbusService.updateYbusService()
        runningYbusServiceInfo.append(f"{type(feederYbusService).__name__}:{feederMessageBusDef.id}")
        runningServiceInstances.append(feederYbusService)
        for switchArea in feederYbusService.agent_area_dict.get('switch_areas', []):
            switchAreaId = switchArea.get('message_bus_id')
            if switchAreaId is not None:
                switchAreaMessageBusDef = getMessageBusDefinition(switchAreaId)
                logger.info(f"Creating Switch Area Ybus Service for area id: {switchAreaMessageBusDef.id}")
                switchAreaService = SwitchAreaAgentLevelStaticYbusService(feederMessageBusDef, switchAreaMessageBusDef,
                                                                          serviceMetadata)
                #switchAreaService.updateYbusService()
                runningYbusServiceInfo.append(f"{type(switchAreaService).__name__}:{switchAreaMessageBusDef.id}")
                runningServiceInstances.append(switchAreaService)
                for secondaryArea in switchArea.get('secondary_areas', []):
                    secondaryAreaId = secondaryArea.get('message_bus_id')
                    if secondaryAreaId is not None:
                        secondaryAreaMessageBusDef = getMessageBusDefinition(secondaryAreaId)
                        logger.info(
                            f"Creating Secondary Area Ybus Service for area id: {secondaryAreaMessageBusDef.id}")
                        secondaryAreaService = SecondaryAreaAgentLevelStaticYbusService(
                            switchAreaMessageBusDef, secondaryAreaMessageBusDef, serviceMetadata)
                        if len(secondaryAreaService.agent_area_dict['addressable_equipment']) == 0 and \
                                len(secondaryAreaService.agent_area_dict['unaddressable_equipment']) == 0:
                            secondaryAreaService.disconnect()
                        else:
                            #secondaryAreaService.updateYbusService()
                            runningYbusServiceInfo.append(f"{type(secondaryAreaService).__name__}:"
                                                          f"{secondaryAreaMessageBusDef.id}")
                            runningServiceInstances.append(secondaryAreaService)
    if len(runningServiceInstances) > 0:
        servicesAreRunning = True
        print("Ybus services are running!")
    while servicesAreRunning:
        try:
            for service in runningServiceInstances:
                # TODO: check to see if service instance is still running
                servicesAreRunning = True
        except KeyboardInterrupt:
            exitStr = "Manually exiting distributed static Ybus service for the following areas:"
            for area in runningYbusServiceInfo:
                exitStr += f"\n{area}"
            logger.info(exitStr)
            break


if __name__ == "__main__":
    main()
