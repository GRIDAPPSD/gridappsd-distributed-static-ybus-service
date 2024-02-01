import json
from pathlib import Path
from typing import Dict

from gridappsd.field_interface.agents import CoordinatingAgent, FeederAgent, SecondaryAreaAgent, SwitchAreaAgent
from gridappsd.field_interface.context import LocalContext
from gridappsd.field_interface.interfaces import MessageBusDefinition
import gridappsd.topics as t


class SampleCoordinatingAgent(CoordinatingAgent):

    def __init__(self, system_message_bus_def: MessageBusDefinition, simulation_id: str = None):
        super().__init__(None, system_message_bus_def, simulation_id)


class SampleFeederAgent(FeederAgent):

    def __init__(self,
                 upstream_message_bus_def: MessageBusDefinition,
                 downstream_message_bus_def: MessageBusDefinition,
                 agent_config,
                 feeder_dict: Dict = None,
                 simulation_id: str = None):

        super().__init__(upstream_message_bus_def, downstream_message_bus_def, agent_config, feeder_dict, simulation_id)
        #find Distributed Static Ybus Service
        areaAgents = LocalContext.get_agents(self.downstream_message_bus)
        ybusServiceId = None
        self.staticYbus
        for agentId, agentDetails in areaAgents.items():
            if agentDetails.get("app_id", "") == "static_ybus_service":
                ybusServiceId = agentId
                break
        #get Ybus for my area
        if ybusServiceId is not None:
            request_queue = t.field_agent_request_queue(self.downstream_message_bus.id, ybusServiceId)
            response = self.downstream_message_bus.get_response(request_queue, {'requestType': 'LocalYbus'})
            self.ybus = response
