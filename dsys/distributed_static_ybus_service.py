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
                    filename='DistributedYBus.log'
                    filemode='w'
                    level=logging.DEBUG)
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

    def updateYbusService(self):
        utils.initializeCimProfile(self.feeder_area)
        self.ybus = utils.calculateYbus(self.feeder_area)
        self.isYbusInitialized = True
        logger.debug(
            f"The Ybus for feederStaticYbusService in area id {self.feeder_area.feeder.mRID} is:\n{json.dumps(self.ybus, indent=4, sort_keys=True, cls=utils.ComplexEncoder)}"
        )

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

    def updateYbusService(self):
        utils.initializeCimProfile(self.switch_area)
        self.ybus = utils.calculateYbus(self.switch_area)
        self.isYbusInitialized = True
        logger.debug(
            f"The Ybus for SwitchAreaYbusService in area id {self.switch_area.area_id} is:\n{json.dumps(self.ybus, indent=4, sort_keys=True, cls=utils.ComplexEncoder)}"
        )

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

    def updateYbusService(self):
        utils.initializeCimProfile(self.secondary_area)
        self.ybus = utils.calculateYbus(self.secondary_area)
        self.isYbusInitialized = True
        logger.debug(
            f"The Ybus for SecondaryAreaYbusService in area id {self.secondary_area.area_id} is:\n{json.dumps(self.ybus, indent=4, sort_keys=True, cls=utils.ComplexEncoder)}"
        )

    def on_request(self, message_bus: FieldMessageBus, headers: Dict, message: Dict):
        if message.get("requestType", "") == "LocalYbus":
            if not self.isYbusInitialized:
                self.updateYbusService()
            message_bus.send(headers.get('reply-to'), self.ybus)


def main(systemMessageBusConfigFile: str,
         feederMessageBusConfigFile: str,
         switchAreaMessageBusConfigFiles: List[str],
         secondaryAreaMessageBusConfigFiles: List[str],
         simulationId: str = ""):
    if not isinstance(systemMessageBusConfigFile, str) or not systemMessageBusConfigFile:
        raise ValueError(
            f"systemMessageBusConfigFile is an invalid type or an empty string.\nsystemMessageBusConfigFile = {systemMessageBusConfigFile}"
        )
    if not isinstance(feederMessageBusConfigFile, str) or not feederMessageBusConfigFile:
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
    systemMessageBusDef = MessageBusDefinition.load(systemMessageBusConfigFile)
    feederMessageBusDef = MessageBusDefinition.load(feederMessageBusConfigFile)
    feederYbusService = FeederAgentLevelStaticYbusService(systemMessageBusDef, feederMessageBusDef,
                                                          serviceMetadata, None, simulationId)
    feederYbusService.connect()
    runningYbusServiceInfo.append(
        f"{type(feederYbusService).__name__}:{feederYbusService.feeder_area.feeder.mRID}")
    runningServiceInstances.append(feederYbusService)
    #    feederYbusService.testYbusQueries()
    feederYbusService.updateYbusService()

    switchAreaMessageBusIds = {}
    secondaryAreaMessageBusIds = {}
    for i in switchAreaMessageBusConfigFiles:
        with open(i, 'r') as configFile:
            config = yaml.load(configFile, Loader=yaml.FullLoader)
            switchAreaMessageBusIds[config.get("connections", {}).get("id")] = i
    for i in secondaryAreaMessageBusConfigFiles:
        with open(i, 'r') as configFile:
            config = yaml.load(configFile, Loader=yaml.FullLoader)
            secondaryAreaMessageBusIds[config.get("connections", {}).get("id")] = i
    for switchArea in feederYbusService.agent_area_dict.get("switch_areas", {}):    # type: ignore
        if switchArea.get("message_bus_id", "") in switchAreaMessageBusIds.keys():
            switchAreaMessageBusDef = MessageBusDefinition.load(
                switchAreaMessageBusIds.get(switchArea["message_bus_id"]))
            switchAreaService = SwitchAreaAgentLevelStaticYbusService(
                feederMessageBusDef, switchAreaMessageBusDef, serviceMetadata, switchArea,
                simulationId)
            switchAreaService.connect()
            runningYbusServiceInfo.append(
                f"{type(switchAreaService).__name__}:{switchAreaService.switch_area.area_id}")
            runningServiceInstances.append(switchAreaService)
            #            switchAreaService.testYbusQueries()
            switchAreaService.updateYbusService()
            for secondaryArea in switchArea.get("secondary_areas", {}):
                if secondaryArea["message_bus_id"] in secondaryAreaMessageBusIds.keys():
                    secondaryAreaMessageBus = MessageBusDefinition.load(
                        secondaryAreaMessageBusIds.get(secondaryArea["message_bus_id"]))
                    secondaryAreaService = SecondaryAreaAgentLevelStaticYbusService(
                        switchAreaMessageBusDef, secondaryAreaMessageBus, serviceMetadata,
                        secondaryArea, simulationId)
                    secondaryAreaService.connect()
                    runningYbusServiceInfo.append(
                        f"{type(secondaryAreaService).__name__}:{secondaryAreaService.secondary_area.area_id}"
                    )
                    runningServiceInstances.append(secondaryAreaService)

                    #                    secondaryAreaService.testYbusQueries()
                    secondaryAreaService.updateYbusService()
                else:
                    raise ValueError(
                        f"No secondary area message bus configuration file was given for secondary area: {secondaryArea['message_bus_id']}"
                    )
        else:
            raise ValueError(
                f"No switch area message bus configuration file was given for switch area: {switchArea['message_bus_id']}"
            )
    servicesAreRunning = True
    while servicesAreRunning:
        try:
            for service in runningServiceInstance:
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
    parser.add_argument("-s", "--simulation_id", help="The simulation id to operate with.")
    parser.add_argument("system_bus_config_file",
                        help="The full path to the system bus configuration file.")
    parser.add_argument("feeder_bus_config_file",
                        help="The full path to the feeder bus configuration file.")
    parser.add_argument("switch_bus_config_files_dir",
                        help="The directory containing the switch bus configuration file.")
    parser.add_argument("secondary_bus_config_files_dir",
                        help="The directory containing the switch bus configuration file.")
    args = parser.parse_args()
    simID = ""
    switchBusConfigFiles = []
    secondaryBusConfigFiles = []
    if args.simulation_id is not None:
        simID = args.simulation_id
    if args.switch_bus_config_files_dir is not None:
        for file in os.listdir(args.switch_bus_config_files_dir):
            if file.endswith(".yml"):
                switchBusConfigFiles.append(os.path.join(args.switch_bus_config_files_dir, file))
    if args.secondary_bus_config_files_dir is not None:
        for file in os.listdir(args.secondary_bus_config_files_dir):
            if file.endswith(".yml"):
                secondaryBusConfigFiles.append(
                    os.path.join(args.secondary_bus_config_files_dir, file))
    main(args.system_bus_config_file, args.feeder_bus_config_file, switchBusConfigFiles,
         secondaryBusConfigFiles, simID)
    for h in logger.handlers:
        h.close()
        logger.removeHandler(h)
