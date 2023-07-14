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
import copy
import json
import logging
import math
import os
import time
from typing import Dict, List, Optional

from cimgraph.data_profile import CIM_PROFILE
from cimgraph.loaders import ConnectionParameters
from cimgraph.loaders.gridappsd import GridappsdConnection
from cimgraph.models import DistributedModel, SwitchArea, SecondaryArea
import gridappsd.field_interface.agents.agents as agents_mod
from gridappsd.field_interface.agents import CoordinatingAgent, FeederAgent, SwitchAreaAgent, SecondaryAreaAgent
from gridappsd.field_interface.context import LocalContext
from gridappsd.field_interface.gridappsd_field_bus import GridAPPSDMessageBus
from gridappsd.field_interface.interfaces import FieldMessageBus, MessageBusDefinition
import numpy as np
import yaml

import dsys.ybus_utils as utils

#TODO: query gridappsd-python for correct cim_profile instead of hardcoding it.
cim_profile = CIM_PROFILE.RC4_2021.value
agents_mod.set_cim_profile(cim_profile)
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
        super().__init__(upstream_message_bus_def, downstream_message_bus_def, service_config,
                         feeder_dict, simulation_id)
        self.isYbusInitialized = False

    def testYbusQueries(self):
        if self.feeder_area is not None:
            if not self.isYbusInitialized:
                utils.initializeCimProfile(self.feeder_area)
            rv = utils.perLengthPhaseImpedanceLineConfigs(self.feeder_area)
            rv = utils.perLengthPhaseImpedanceLineNames(self.feeder_area)
            rv = utils.perLengthSequenceImpedanceLineConfigs(self.feeder_area)
            rv = utils.perLengthSequenceImpedanceLineNames(self.feeder_area)
            rv = utils.acLineSegmentLineNames(self.feeder_area)
            rv = utils.wireInfoSpacing(self.feeder_area)
            rv = utils.wireInfoOverhead(self.feeder_area)
            rv = utils.wireInfoConcentricNeutral(self.feeder_area)
            rv = utils.wireInfoTapeShield(self.feeder_area)
            rv = utils.wireInfoLineNames(self.feeder_area)
            rv = utils.powerTransformerEndXfmrImpedances(self.feeder_area)
            rv = utils.powerTransformerEndXfmrNames(self.feeder_area)
            rv = utils.transformerTankXfmrRated(self.feeder_area)
            rv = utils.transformerTankXfmrSct(self.feeder_area)
            rv = utils.transformerTankXfmrNames(self.feeder_area)
            rv = utils.switchingEquipmentSwitchNames(self.feeder_area)
            rv = utils.shuntElementCapNames(self.feeder_area)
            rv = utils.transformerTankXfmrNlt(self.feeder_area)
            rv = utils.powerTransformerEndXfmrAdmittances(self.feeder_area)
        else:
            logger.error(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s feeder_area None. The service is malformed.")
            raise RuntimeError(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s feeder_area None. The service is malformed.")

    def updateYbusService(self):
        if self.feeder_area is not None:
            utils.initializeCimProfile(self.feeder_area)
            self.ybus = utils.calculateYbus(self.feeder_area)
            self.isYbusInitialized = True
            logger.info(
                f"The Ybus for feederStaticYbusService in area id {self.feeder_area.feeder.mRID} is:\n{json.dumps(self.ybus, indent=4, sort_keys=True, cls=utils.ComplexEncoder)}"
            )
        else:
            logger.error(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s feeder_area None. The service is malformed.")
            raise RuntimeError(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s feeder_area None. The service is malformed.")
        

    def on_request(self, message_bus: FieldMessageBus, headers: Dict, message: Dict):
        if message.get("requestType", "") == "LocalYbus":
            if not self.isYbusInitialized:
                self.updateYbusService()
            message_bus.send(headers.get('reply-to'), self.ybus)


class SwitchAreaAgentLevelStaticYbusService(SwitchAreaAgent):

    def __init__(self,
                 upstream_message_bus_def: MessageBusDefinition,
                 downstream_message_bus_def: MessageBusDefinition,
                 service_config: Dict,
                 switch_area_dict: Optional[Dict] = None,
                 simulation_id: Optional[str] = None):
        super().__init__(upstream_message_bus_def, downstream_message_bus_def, service_config,
                         switch_area_dict, simulation_id)
        self.isYbusInitialized = False

    def testYbusQueries(self):
        if self.switch_area is not None:
            if not self.isYbusInitialized:
                utils.initializeCimProfile(self.switch_area)
            rv = utils.perLengthPhaseImpedanceLineConfigs(self.switch_area)
            rv = utils.perLengthPhaseImpedanceLineNames(self.switch_area)
            rv = utils.perLengthSequenceImpedanceLineConfigs(self.switch_area)
            rv = utils.perLengthSequenceImpedanceLineNames(self.switch_area)
            rv = utils.acLineSegmentLineNames(self.switch_area)
            rv = utils.wireInfoSpacing(self.switch_area)
            rv = utils.wireInfoOverhead(self.switch_area)
            rv = utils.wireInfoConcentricNeutral(self.switch_area)
            rv = utils.wireInfoTapeShield(self.switch_area)
            rv = utils.wireInfoLineNames(self.switch_area)
            rv = utils.powerTransformerEndXfmrImpedances(self.switch_area)
            rv = utils.powerTransformerEndXfmrNames(self.switch_area)
            rv = utils.transformerTankXfmrRated(self.switch_area)
            rv = utils.transformerTankXfmrSct(self.switch_area)
            rv = utils.transformerTankXfmrNames(self.switch_area)
            rv = utils.switchingEquipmentSwitchNames(self.switch_area)
            rv = utils.shuntElementCapNames(self.switch_area)
            rv = utils.transformerTankXfmrNlt(self.switch_area)
            rv = utils.powerTransformerEndXfmrAdmittances(self.switch_area)
        else:
            logger.error(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s switch_area None. The service is malformed.")
            raise RuntimeError(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s switch_area None. The service is malformed.")

    def updateYbusService(self):
        if self.switch_area is not None:
            utils.initializeCimProfile(self.switch_area)
            self.ybus = utils.calculateYbus(self.switch_area)
            self.isYbusInitialized = True
            logger.info(
                f"The Ybus for SwitchAreaYbusService in area id {self.switch_area.area_id} is:\n{json.dumps(self.ybus, indent=4, sort_keys=True, cls=utils.ComplexEncoder)}"
            )
        else:
            logger.error(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s switch_area None. The service is malformed.")
            raise RuntimeError(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s switch_area None. The service is malformed.")

    def on_request(self, message_bus: FieldMessageBus, headers: Dict, message: Dict):
        if message.get("requestType", "") == "LocalYbus":
            if not self.isYbusInitialized:
                self.updateYbusService()
            message_bus.send(headers.get('reply-to'), self.ybus)


class SecondaryAreaAgentLevelStaticYbusService(SecondaryAreaAgent):

    def __init__(self,
                 upstream_message_bus_def: MessageBusDefinition,
                 downstream_message_bus_def: MessageBusDefinition,
                 service_config: Dict,
                 secondary_area_dict: Optional[Dict] = None,
                 simulation_id: Optional[str] = None):
        super().__init__(upstream_message_bus_def, downstream_message_bus_def, service_config,
                         secondary_area_dict, simulation_id)
        self.isYbusInitialized = False

    def testYbusQueries(self):
        if self.secondary_area is not None:
            if not self.isYbusInitialized:
                utils.initializeCimProfile(self.secondary_area)
            rv = utils.perLengthPhaseImpedanceLineConfigs(self.secondary_area)
            rv = utils.perLengthPhaseImpedanceLineNames(self.secondary_area)
            rv = utils.perLengthSequenceImpedanceLineConfigs(self.secondary_area)
            rv = utils.perLengthSequenceImpedanceLineNames(self.secondary_area)
            rv = utils.acLineSegmentLineNames(self.secondary_area)
            rv = utils.wireInfoSpacing(self.secondary_area)
            rv = utils.wireInfoOverhead(self.secondary_area)
            rv = utils.wireInfoConcentricNeutral(self.secondary_area)
            rv = utils.wireInfoTapeShield(self.secondary_area)
            rv = utils.wireInfoLineNames(self.secondary_area)
            rv = utils.powerTransformerEndXfmrImpedances(self.secondary_area)
            rv = utils.powerTransformerEndXfmrNames(self.secondary_area)
            rv = utils.transformerTankXfmrRated(self.secondary_area)
            rv = utils.transformerTankXfmrSct(self.secondary_area)
            rv = utils.transformerTankXfmrNames(self.secondary_area)
            rv = utils.switchingEquipmentSwitchNames(self.secondary_area)
            rv = utils.shuntElementCapNames(self.secondary_area)
            rv = utils.transformerTankXfmrNlt(self.secondary_area)
            rv = utils.powerTransformerEndXfmrAdmittances(self.secondary_area)
        else:
            logger.error(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s secondary_area None. The service is malformed.")
            raise RuntimeError(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s secondary_area None. The service is malformed.")

    def updateYbusService(self):
        if self.secondary_area is not None:
            utils.initializeCimProfile(self.secondary_area)
            self.ybus = utils.calculateYbus(self.secondary_area)
            self.isYbusInitialized = True
            logger.info(
                f"The Ybus for SecondaryAreaYbusService in area id {self.secondary_area.area_id} is:\n{json.dumps(self.ybus, indent=4, sort_keys=True, cls=utils.ComplexEncoder)}"
            )
        else:
            logger.error(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s secondary_area None. The service is malformed.")
            raise RuntimeError(f"{type(self).__name__}:{self.downstream_message_bus_def.id}'s secondary_area None. The service is malformed.")

    def on_request(self, message_bus: FieldMessageBus, headers: Dict, message: Dict):
        if message.get("requestType", "") == "LocalYbus":
            if not self.isYbusInitialized:
                self.updateYbusService()
            message_bus.send(headers.get('reply-to'), self.ybus)


def main(**kwargs):
    systemMessageBusConfigFile = kwargs.get("system_bus_config_file")
    feederMessageBusConfigFile = kwargs.get("feeder_bus_config_file")
    switchAreaMessageBusConfigFiles = kwargs.get("switch_bus_config_files")
    secondaryAreaMessageBusConfigFiles = kwargs.get("secondary_bus_config_files")
    simulationId = kwargs.get("simulation_id")
    if (not isinstance(systemMessageBusConfigFile, str) or not systemMessageBusConfigFile) and systemMessageBusConfigFile is not None:
        raise ValueError(
            f"systemMessageBusConfigFile is an invalid type or an empty string.\nsystemMessageBusConfigFile = {systemMessageBusConfigFile}"
        )
    if (not isinstance(feederMessageBusConfigFile, str) or not feederMessageBusConfigFile) and feederMessageBusConfigFile is not None:
        raise ValueError(
            f"feederMessageBusConfigFile is an invalid type or an empty string.\nfeederMessageBusConfigFile = {feederMessageBusConfigFile}"
        )
    if not isinstance(switchAreaMessageBusConfigFiles, list):
        raise ValueError(
            f"switchAreaMessageBusConfigFiles is an invalid type or an empty string.\nswitchAreaMessageBusConfigFiles = {json.dumps(switchAreaMessageBusConfigFiles, indent = 4)}"
        )
    else:
        for configFile in switchAreaMessageBusConfigFiles:
            if not isinstance(configFile, str) or not configFile:
                raise ValueError(
                    f"The contents of switchAreaMessageBusConfigFiles are not all strings or contain an empty string.\nswitchAreaMessageBusConfigFiles = {json.dumps(switchAreaMessageBusConfigFiles, indent = 4)}"
                )
    if not isinstance(secondaryAreaMessageBusConfigFiles, list):
        raise ValueError(
            f"secondaryAreaMessageBusConfigFiles is an invalid type or an empty string.\nsecondaryAreaMessageBusConfigFiles = {json.dumps(secondaryAreaMessageBusConfigFiles, indent = 4)}"
        )
    else:
        for configFile in secondaryAreaMessageBusConfigFiles:
            if not isinstance(configFile, str) or not configFile:
                raise ValueError(
                    f"The contents of secondaryAreaMessageBusConfigFiles are not all strings or contain an empty string.\nsecondaryAreaMessageBusConfigFiles = {json.dumps(secondaryAreaMessageBusConfigFiles, indent = 4)}"
                )
    serviceMetadata = {
        "app_id": "static_ybus_service",
        "description": "This is a GridAPPS-D distributed static ybus service agent."
    }
    runningYbusServiceInfo = []
    runningServiceInstances = []
    systemMessageBusDef = None
    feederMessageBusDef = None
    switchAreaMessageBusDefs = {}
    if feederMessageBusConfigFile is not None and systemMessageBusConfigFile is not None:
        systemMessageBusDef = MessageBusDefinition.load(systemMessageBusConfigFile)
        feederMessageBusDef = MessageBusDefinition.load(feederMessageBusConfigFile)
        feederYbusService = FeederAgentLevelStaticYbusService(systemMessageBusDef, feederMessageBusDef,
                                                            serviceMetadata, None, simulationId)
        feederYbusService.connect()
        runningYbusServiceInfo.append(
            f"{type(feederYbusService).__name__}:{feederMessageBusDef.id}")
        runningServiceInstances.append(feederYbusService)
#        feederYbusService.testYbusQueries()
        feederYbusService.updateYbusService()
    if len(switchAreaMessageBusConfigFiles) > 0 and feederMessageBusConfigFile is not None:
        if feederMessageBusDef is None:
            feederMessageBusDef = MessageBusDefinition.load(feederMessageBusConfigFile)
        for switchConfigFile in switchAreaMessageBusConfigFiles:
                switchAreaMessageBusDef = MessageBusDefinition.load(
                    switchConfigFile)
                switchAreaMessageBusDefs[switchAreaMessageBusDef.id] = switchAreaMessageBusDef
                switchAreaService = SwitchAreaAgentLevelStaticYbusService(
                    feederMessageBusDef, switchAreaMessageBusDef, serviceMetadata, None,
                    simulationId)
                switchAreaService.connect()
                runningYbusServiceInfo.append(
                    f"{type(switchAreaService).__name__}:{switchAreaMessageBusDef.id}")
                runningServiceInstances.append(switchAreaService)
#                switchAreaService.testYbusQueries()
                switchAreaService.updateYbusService()
    if len(secondaryAreaMessageBusConfigFiles) > 0 and len(switchAreaMessageBusConfigFiles) > 0:
        if len(switchAreaMessageBusDefs) == 0:
            for switchConfigFile in switchAreaMessageBusConfigFiles:
                switchAreaMessageBusDef = MessageBusDefinition.load(switchConfigFile)
                switchAreaMessageBusDefs[switchAreaMessageBusDef.id] = switchAreaMessageBusDef
        for secondaryConfigFile in secondaryAreaMessageBusConfigFiles:
                secondaryAreaMessageBusDef = MessageBusDefinition.load(secondaryConfigFile)
                secondaryAreaIdSplit = secondaryAreaMessageBusDef.id.split(".")
                switchAreaId = secondaryAreaIdSplit[0] + "." + secondaryAreaIdSplit[1]
                switchAreaMessageBusDef = switchAreaMessageBusDefs.get(switchAreaId)
                if switchAreaMessageBusDef is None:
                    logger.error(f"There is a missing upstream switch bus configuration for area id:{switchAreaId}")
                    raise RuntimeError(f"There is a missing upstream switch bus configuration for area id:{switchAreaId}")
                else:
                    secondaryAreaService = SecondaryAreaAgentLevelStaticYbusService(
                        switchAreaMessageBusDef, secondaryAreaMessageBusDef, serviceMetadata,
                        None, simulationId)
                    secondaryAreaService.connect()
                    runningYbusServiceInfo.append(
                        f"{type(secondaryAreaService).__name__}:{secondaryAreaMessageBusDef.id}"
                    )
                    runningServiceInstances.append(secondaryAreaService)
#                    secondaryAreaService.testYbusQueries()
                    secondaryAreaService.updateYbusService()
    servicesAreRunning = True
    print("Ybus services are running!")
    while servicesAreRunning:
        try:
            for service in runningServiceInstances:
                #TODO: check to see if service instance is still running
                servicesAreRunning = True
        except KeyboardInterrupt:
            exitStr = "Manually exiting distributed static Ybus service for the following areas:"
            for area in runningYbusServiceInfo:
                exitStr += f"\n{area}"
            logger.info(exitStr)
            break


if __name__ == "__main__":
    parser = ArgumentParser()
    serviceConfigHelpStr = "Variable keyword arguments that provide user defined distributed area agent configuration files."
    serviceConfigHelpStr += "\nValid keywords are as follows:"
    serviceConfigHelpStr += "\n\tSYSTEM_BUS_CONFIG_FILE=<full path of the system bus configuration file>"
    serviceConfigHelpStr += "\n\tFEEDER_BUS_CONFIG_FILE=<full path of the feeder bus configuration file>"
    serviceConfigHelpStr += "\n\tSWITCH_BUS_CONFIG_FILE_DIR=<full path to the directory containing the switch bus configuration file(s)>"
    serviceConfigHelpStr += "\n\tSECONDARY_BUS_CONFIG_FILE_DIR=<full path to the directory containing the secondary bus configuration file(s)>"
    serviceConfigHelpStr += "\n\tSIMULATION_ID=<simulation id>"
    parser.add_argument("service_configurations", nargs="+", help=serviceConfigHelpStr)
    args = parser.parse_args()
    switchBusConfigFiles = []
    secondaryBusConfigFiles = []
    validKeywords = ["SYSTEM_BUS_CONFIG_FILE", "FEEDER_BUS_CONFIG_FILE", "SWITCH_BUS_CONFIG_FILE_DIR", "SECONDARY_BUS_CONFIG_FILE_DIR","SIMULATION_ID"]
    mainArgs = {}
    for arg in args.service_configurations:
        argSplit = arg.split("=", maxsplit=1)
        if argSplit[0] in validKeywords:
            mainArgs[argSplit[0]] = argSplit[1]
        else:
            logger.error(f"Invalid keyword argument, {argSplit[0]}, was given by the user. Valid keywords are {validKeywords}.")
            raise RuntimeError(f"Invalid keyword argument, {argSplit[0]}, was given by the user. Valid keywords are {validKeywords}.")
    if len(mainArgs) == 0:
        logger.error(f"No arguments were provided by the user. Valid keywords are {validKeywords}.")
        raise RuntimeError(f"No arguments were provided by the user. Valid keywords are {validKeywords}.")
    simID = mainArgs.get("SIMULATION_ID")
    systemBusConfigFile = mainArgs.get("SYSTEM_BUS_CONFIG_FILE")
    feederBusConfigFile = mainArgs.get("FEEDER_BUS_CONFIG_FILE")
    switchBusConfigFileDir = mainArgs.get("SWITCH_BUS_CONFIG_FILE_DIR")
    secondaryBusConfigFileDir = mainArgs.get("SECONDARY_BUS_CONFIG_FILE_DIR")
    if switchBusConfigFileDir is not None:
        for file in os.listdir(switchBusConfigFileDir):
            if file.endswith(".yml"):
                switchBusConfigFiles.append(os.path.join(switchBusConfigFileDir, file))
    if secondaryBusConfigFileDir is not None:
        for file in os.listdir(secondaryBusConfigFileDir):
            if file.endswith(".yml"):
                secondaryBusConfigFiles.append(
                    os.path.join(secondaryBusConfigFileDir, file))
    main(system_bus_config_file=systemBusConfigFile, 
         feeder_bus_config_file=feederBusConfigFile, 
         switch_bus_config_files=switchBusConfigFiles, 
         secondary_bus_config_files=secondaryBusConfigFiles, 
         simulation_id=simID)
