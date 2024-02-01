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

# This material was prepared as an account of work sponsored by an agency of the United States Government.
# Neither the United States Government nor the United States Department of Energy, nor Battelle, nor any
# of their employees, nor any jurisdiction or organization that has cooperated in the development of these
# materials, makes any warranty, express or implied, or assumes any legal liability or responsibility for
# the accuracy, completeness, or usefulness or any information, apparatus, product, software, or process
# disclosed, or represents that its use would not infringe privately owned rights.

# Reference herein to any specific commercial product, process, or service by trade name, trademark, manufacturer,
# or otherwise does not necessarily constitute or imply its endorsement, recommendation, or favoring by the United
# States Government or any agency thereof, or Battelle Memorial Institute. The views and opinions of authors expressed
# herein do not necessarily state or reflect those of the United States Government or any agency thereof.

# PACIFIC NORTHWEST NATIONAL LABORATORY operated by BATTELLE for the
# UNITED STATES DEPARTMENT OF ENERGY under Contract DE-AC05-76RL01830
# -------------------------------------------------------------------------------

import copy
import json
import logging
import math
from typing import Dict, List

from cimgraph.data_profile import CIM_PROFILE
from cimgraph.models import DistributedArea
import cimgraph.utils as cimUtils
import gridappsd.field_interface.agents.agents as agents_mod
import numpy as np

# TODO: query gridappsd-python for correct cim_profile instead of hardcoding it.
cim_profile = CIM_PROFILE.RC4_2021.value
agents_mod.set_cim_profile(cim_profile)
cim = agents_mod.cim
logger = logging.getLogger(__name__)


class ComplexEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, complex):
            return [obj.real, obj.imag]
        elif isinstance(obj, np.ndarray):
            dims = obj.shape
            rv = []
            for r in range(dims[0]):
                rl = []
                for c in range(dims[1]):
                    rl.append(obj[r, c])
                rv.append(rl)
            return rv
        else:
            return json.JSONEncoder.default(self, obj)


def initializeCimProfile(distributedArea: DistributedArea):
    cimUtils.get_all_data(distributedArea)
    # distributedArea.get_all_edges(cim.ACLineSegment)
    # distributedArea.get_all_edges(cim.TransformerTank)
    # distributedArea.get_all_edges(cim.PowerTransformer)
    # distributedArea.get_all_edges(cim.LoadBreakSwitch)
    # distributedArea.get_all_edges(cim.Recloser)
    # distributedArea.get_all_edges(cim.Breaker)
    # distributedArea.get_all_edges(cim.Fuse)
    # distributedArea.get_all_edges(cim.Sectionaliser)
    # distributedArea.get_all_edges(cim.Jumper)
    # distributedArea.get_all_edges(cim.Disconnector)
    # distributedArea.get_all_edges(cim.GroundDisconnector)
    # distributedArea.get_all_edges(cim.LinearShuntCompensator)
    # distributedArea.get_all_edges(cim.PerLengthPhaseImpedance)
    # distributedArea.get_all_edges(cim.PerLengthSequenceImpedance)
    # distributedArea.get_all_edges(cim.ACLineSegmentPhase)
    # distributedArea.get_all_edges(cim.WireSpacingInfo)
    # distributedArea.get_all_edges(cim.PowerTransformerEnd)
    # distributedArea.get_all_edges(cim.TransformerEnd)
    # distributedArea.get_all_edges(cim.TransformerMeshImpedance)
    # distributedArea.get_all_edges(cim.TransformerTankInfo)
    # distributedArea.get_all_edges(cim.TransformerTankEnd)
    # distributedArea.get_all_edges(cim.SwitchPhase)
    # distributedArea.get_all_edges(cim.LinearShuntCompensatorPhase)
    # distributedArea.get_all_edges(cim.Terminal)
    # distributedArea.get_all_edges(cim.BaseVoltage)
    # distributedArea.get_all_edges(cim.WirePosition)
    # distributedArea.get_all_edges(cim.OverheadWireInfo)
    # distributedArea.get_all_edges(cim.ConcentricNeutralCableInfo)
    # distributedArea.get_all_edges(cim.TapeShieldCableInfo)
    # distributedArea.get_all_edges(cim.TransformerEndInfo)
    # distributedArea.get_all_edges(cim.TransformerCoreAdmittance)
    # distributedArea.get_all_edges(cim.PhaseImpedanceData)
    # distributedArea.get_all_edges(cim.ShortCircuitTest)
    # distributedArea.get_all_edges(cim.NoLoadTest)


def perLengthPhaseImpedanceLineConfigs(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    acLineSegments = distributedArea.graph.get(cim.ACLineSegment, {}).keys()
    perLengthImpedances = distributedArea.graph.get(cim.PerLengthPhaseImpedance, {})
    desiredInfo = {}
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing perLengthPhaseImpedanceLineConfigs query for distributed area: {distributedAreaID}")
    for perLengthImpedance in perLengthImpedances.values():
        configIsValid = False
        for line in perLengthImpedance.ACLineSegments:
            if line.mRID in acLineSegments:
                configIsValid = True
                break
        if configIsValid:
            for phaseImpedanceData in perLengthImpedance.PhaseImpedanceData:    # type: ignore
                desiredInfo["line_config"] = {"value": perLengthImpedance.name}    # type: ignore
                desiredInfo["count"] = {"value": perLengthImpedance.conductorCount}    # type: ignore
                desiredInfo["r_ohm_per_m"] = {"value": phaseImpedanceData.r}
                desiredInfo["x_ohm_per_m"] = {"value": phaseImpedanceData.x}
                desiredInfo["row"] = {"value": phaseImpedanceData.row}
                desiredInfo["col"] = {"value": phaseImpedanceData.column}
                dictContainsNone = False
                nullAttributes = []
                for i, v in desiredInfo.items():
                    if v["value"] is None:
                        dictContainsNone = True
                        nullAttributes.append(i)
                if len(nullAttributes) > 0:
                    logger.debug(
                        f"perLengthPhaseImpedanceLineConfigs():PerLengthImpedance:{perLengthImpedance.name} contained \
                        the following Null attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                    nullAttributes.clear()
                if len(desiredInfo) > 0 and not dictContainsNone:
                    rv.append(copy.deepcopy(desiredInfo))
                desiredInfo.clear()
    logger.debug(
        f'perLengthPhaseImpdenaceLineConfigs for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def perLengthPhaseImpedanceLineNames(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    acLineSegments = distributedArea.graph.get(cim.ACLineSegment, {})
    desiredInfo = {}
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing perLengthPhaseImpedanceLineNames query for distributed area: {distributedAreaID}")
    for line in acLineSegments.values():
        if isinstance(line.PerLengthImpedance, cim.PerLengthPhaseImpedance):    # type: ignore
            for acLineSegmentPhase in line.ACLineSegmentPhases:    # type: ignore
                desiredInfo["line_name"] = {"value": line.name}    # type: ignore
                desiredInfo["length"] = {"value": line.length}    # type: ignore
                desiredInfo["line_config"] = {"value": line.PerLengthImpedance.name}    # type: ignore
                for terminal in line.Terminals:    # type: ignore
                    if int(terminal.sequenceNumber) == 1:
                        desiredInfo["bus1"] = {"value": terminal.ConnectivityNode.name}
                    elif int(terminal.sequenceNumber) == 2:
                        desiredInfo["bus2"] = {"value": terminal.ConnectivityNode.name}
                desiredInfo["phase"] = {"value": acLineSegmentPhase.phase[0]}
                dictContainsNone = False
                nullAttributes = []
                for i, v in desiredInfo.items():
                    if v["value"] is None:
                        dictContainsNone = True
                        nullAttributes.append(i)
                if len(nullAttributes) > 0:
                    logger.debug(
                        f"perLengthPhaseImpedanceLineNames():ACLineSegment:{line.name} contained the following Null \
                        attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                    nullAttributes.clear()
                if len(desiredInfo) > 0 and not dictContainsNone:
                    rv.append(copy.deepcopy(desiredInfo))
                desiredInfo.clear()
    logger.debug(
        f'perLengthPhaseImpedanceLineNames for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def perLengthSequenceImpedanceLineConfigs(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing perLengthSequenceImpedanceLineConfigs query for distributed area: {distributedAreaID}")
    acLineSegments = distributedArea.graph.get(cim.ACLineSegment, {}).keys()
    perLengthSequenceImpedances = distributedArea.graph.get(cim.PerLengthSequenceImpedance, {})
    desiredInfo = {}
    for sequenceImpedance in perLengthSequenceImpedances.values():
        sequenceImpedanceIsValid = False
        for line in sequenceImpedance.ACLineSegments:
            if line.mRID in acLineSegments:
                sequenceImpedanceIsValid = True
                break
        if sequenceImpedanceIsValid:    # type: ignore
            desiredInfo["line_config"] = {"value": sequenceImpedance.name}    # type: ignore
            desiredInfo["r1_ohm_per_m"] = {"value": sequenceImpedance.r}    # type: ignore
            desiredInfo["x1_ohm_per_m"] = {"value": sequenceImpedance.x}    # type: ignore
            desiredInfo["r0_ohm_per_m"] = {"value": sequenceImpedance.r0}    # type: ignore
            desiredInfo["x0_ohm_per_m"] = {"value": sequenceImpedance.x0}    # type: ignore
            dictContainsNone = False
            nullAttributes = []
            for i, v in desiredInfo.items():
                if v["value"] is None:
                    dictContainsNone = True
                    nullAttributes.append(i)
            if len(nullAttributes) > 0:
                logger.debug(
                    f"perLengthSequenceImpedanceLineConfigs():perLenghtSequenceImpedance:{sequenceImpedance.name} \
                    contained the following Null attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                nullAttributes.clear()
            if len(desiredInfo) > 0 and not dictContainsNone:
                rv.append(copy.deepcopy(desiredInfo))
            desiredInfo.clear()
    logger.debug(f'perLengthSequenceImpedanceLineConfigs for {distributedAreaID} returns: \
        {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def perLengthSequenceImpedanceLineNames(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing perLengthSequenceImpedanceLineNames query for distributed area: {distributedAreaID}")
    acLineSegments = distributedArea.graph.get(cim.ACLineSegment, {})
    desiredInfo = {}
    for line in acLineSegments.values():
        if isinstance(line.PerLengthImpedance, cim.PerLengthSequenceImpedance):    # type: ignore
            desiredInfo["line_name"] = {"value": line.name}    # type: ignore
            desiredInfo["length"] = {"value": line.length}    # type: ignore
            desiredInfo["line_config"] = {"value": line.PerLengthImpedance.name}    # type: ignore
            for terminal in line.Terminals:    # type: ignore
                if int(terminal.sequenceNumber) == 1:
                    desiredInfo["bus1"] = {"value": terminal.ConnectivityNode.name}
                elif int(terminal.sequenceNumber) == 2:
                    desiredInfo["bus2"] = {"value": terminal.ConnectivityNode.name}
            dictContainsNone = False
            nullAttributes = []
            for i, v in desiredInfo.items():
                if v["value"] is None:
                    dictContainsNone = True
                    nullAttributes.append(i)
            if len(nullAttributes) > 0:
                logger.debug(
                    f"perLengthSequenceImpedanceLineNames():ACLineSegment:{line.name} contained the following Null \
                    attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                nullAttributes.clear()
            if len(desiredInfo) > 0 and not dictContainsNone:
                rv.append(copy.deepcopy(desiredInfo))
            desiredInfo.clear()
    logger.debug(
        f'perLengthSequenceImpedanceLineNames for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}'
    )
    return rv


def acLineSegmentLineNames(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing acLineSegmentLineNames query for distributed area: {distributedAreaID}")
    acLineSegments = distributedArea.graph.get(cim.ACLineSegment, {})
    desiredInfo = {}
    for line in acLineSegments.values():
        desiredInfo["line_name"] = {"value": line.name}    # type: ignore
        desiredInfo["length"] = {"value": line.length}    # type: ignore
        for terminal in line.Terminals:    # type: ignore
            if int(terminal.sequenceNumber) == 1:
                desiredInfo["bus1"] = {"value": terminal.ConnectivityNode.name}
            elif int(terminal.sequenceNumber) == 2:
                desiredInfo["bus2"] = {"value": terminal.ConnectivityNode.name}
        desiredInfo["r1_Ohm"] = {"value": line.r}    # type: ignore
        desiredInfo["x1_Ohm"] = {"value": line.x}    # type: ignore
        desiredInfo["r0_Ohm"] = {"value": line.r0}    # type: ignore
        desiredInfo["x0_Ohm"] = {"value": line.x0}    # type: ignore
        dictContainsNone = False
        nullAttributes = []
        for i, v in desiredInfo.items():
            if v["value"] is None:
                dictContainsNone = True
                nullAttributes.append(i)
        if len(nullAttributes) > 0:
            logger.debug(
                f"acLineSegmentLineNames():ACLineSegment:{line.name} contained the following Null attributes:\n\
                {json.dumps(nullAttributes, indent=4, sort_keys=True)}")
            nullAttributes.clear()
        if len(desiredInfo) > 0 and not dictContainsNone:
            rv.append(copy.deepcopy(desiredInfo))
        desiredInfo.clear()
    logger.debug(f'acLineSegmentLineNames for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def wireInfoSpacing(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing wireInfoSpacing query for distributed area: {distributedAreaID}")
    acLineSegments = distributedArea.graph.get(cim.ACLineSegment, {})
    wireSpacingInfos = distributedArea.graph.get(cim.WireSpacingInfo, {})
    desiredInfo = {}
    for wireSpacingInfo in wireSpacingInfos.values():
        infoIsValid = False
        for line in wireSpacingInfo.ACLineSegments:
            if line.mRID in acLineSegments.keys():
                infoIsValid = True
                break
        if infoIsValid:
            for p in wireSpacingInfo.WirePositions:    # type: ignore
                desiredInfo["wire_spacing_info"] = {"value": wireSpacingInfo.name}    # type: ignore
                desiredInfo["cable"] = {"value": wireSpacingInfo.isCable}    # type: ignore
                desiredInfo["seq"] = {"value": p.sequenceNumber}
                desiredInfo["xCoord"] = {"value": p.xCoord}
                desiredInfo["yCoord"] = {"value": p.yCoord}
                dictContainsNone = False
                nullAttributes = []
                for i, v in desiredInfo.items():
                    if v["value"] is None:
                        dictContainsNone = True
                        nullAttributes.append(i)
                if len(nullAttributes) > 0:
                    logger.debug(
                        f"wireInfoSpacing():WireSpacingInfo:{wireSpacingInfo.name} contained the following Null \
                        attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                    nullAttributes.clear()
                if len(desiredInfo) > 0 and not dictContainsNone:
                    rv.append(copy.deepcopy(desiredInfo))
                desiredInfo.clear()
    logger.debug(f'wireInfoSpacing for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def wireInfoOverhead(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing wireInfoOverhead query for distributed area: {distributedAreaID}")
    acLineSegments = distributedArea.graph.get(cim.ACLineSegment, {})
    overheadWireInfos = distributedArea.graph.get(cim.OverheadWireInfo, {})
    desiredInfo = {}
    for overheadWireInfo in overheadWireInfos.values():
        infoIsValid = False
        for line in acLineSegments.values():
            for segmentPhase in line.ACLineSegmentPhases:
                if isinstance(segmentPhase.WireInfo, cim.OverheadWireInfo):
                    if overheadWireInfo.mRID == segmentPhase.WireInfo.mRID:
                        infoIsValid = True
                        break
            if infoIsValid:
                break
        if infoIsValid:
            desiredInfo["wire_cn_ts"] = {"value": overheadWireInfo.name}
            desiredInfo["gmr"] = {"value": overheadWireInfo.gmr}
            desiredInfo["r25"] = {"value": overheadWireInfo.rAC25}
            dictContainsNone = False
            nullAttributes = []
            for i, v in desiredInfo.items():
                if v["value"] is None:
                    dictContainsNone = True
                    nullAttributes.append(i)
            if len(nullAttributes) > 0:
                logger.debug(f"wireInfoOverhead():ACLineSegment:{line.name} contained the following Null attributes:\n\
                    {json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                nullAttributes.clear()
            if len(desiredInfo) > 0 and not dictContainsNone:
                rv.append(copy.deepcopy(desiredInfo))
            desiredInfo.clear()
    logger.debug(f'wireInfoOverhead for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def wireInfoConcentricNeutral(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing wireInfoConcentricNeutral query for distributed area: {distributedAreaID}")
    acLineSegments = distributedArea.graph.get(cim.ACLineSegment, {})
    concentricNeutralWireInfos = distributedArea.graph.get(cim.ConcentricNeutralCableInfo, {})
    desiredInfo = {}
    for concentricNeutralWireInfo in concentricNeutralWireInfos.values():
        infoIsValid = False
        for line in acLineSegments.values():
            for segmentPhase in line.ACLineSegmentPhases:
                if isinstance(segmentPhase.WireInfo, cim.ConcentricNeutralCableInfo):
                    if concentricNeutralWireInfo.mRID == segmentPhase.WireInfo.mRID:
                        infoIsValid = True
                        break
            if infoIsValid:
                break
        if infoIsValid:
            desiredInfo["wire_cn_ts"] = {"value": concentricNeutralWireInfo.name}
            desiredInfo["gmr"] = {"value": concentricNeutralWireInfo.gmr}
            desiredInfo["r25"] = {"value": concentricNeutralWireInfo.rAC25}
            desiredInfo["diameter_jacket"] = {"value": concentricNeutralWireInfo.diameterOverJacket}
            desiredInfo["strand_count"] = {"value": concentricNeutralWireInfo.neutralStrandCount}
            desiredInfo["strand_radius"] = {"value": concentricNeutralWireInfo.neutralStrandRadius}
            desiredInfo["strand_gmr"] = {"value": concentricNeutralWireInfo.neutralStrandGmr}
            desiredInfo["strand_rdc"] = {"value": concentricNeutralWireInfo.neutralStrandRDC20}
            dictContainsNone = False
            nullAttributes = []
            for i, v in desiredInfo.items():
                if v["value"] is None:
                    dictContainsNone = True
                    nullAttributes.append(i)
            if len(nullAttributes) > 0:
                logger.debug(f"wireInfoConcentricNeutral():ConcentricNeutralCableInfo:{concentricNeutralWireInfo.name} \
                    contained the following Null attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                nullAttributes.clear()
            if len(desiredInfo) > 0 and not dictContainsNone:
                rv.append(copy.deepcopy(desiredInfo))
            desiredInfo.clear()
    logger.debug(f'wireInfoConcentricNeutral for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def wireInfoTapeShield(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing wireInfoTapeShield query for distributed area: {distributedAreaID}")
    acLineSegments = distributedArea.graph.get(cim.ACLineSegment, {})
    tapeShieldWireInfos = distributedArea.graph.get(cim.TapeShieldCableInfo, {})
    desiredInfo = {}
    for tapeShieldWireInfo in tapeShieldWireInfos.values():
        infoIsValid = False
        for line in acLineSegments.values():
            for segmentPhase in line.ACLineSegmentPhases:
                if isinstance(segmentPhase.WireInfo, cim.TapeShieldCableInfo):
                    if tapeShieldWireInfo.mRID == segmentPhase.WireInfo.mRID:
                        infoIsValid = True
                        break
            if infoIsValid:
                break
        if infoIsValid:    # type: ignore
            desiredInfo["wire_cn_ts"] = {"value": tapeShieldWireInfo.name}
            desiredInfo["gmr"] = {"value": tapeShieldWireInfo.gmr}
            desiredInfo["r25"] = {"value": tapeShieldWireInfo.rAC25}
            desiredInfo["diameter_screen"] = {"value": tapeShieldWireInfo.diameterOverScreen}
            desiredInfo["tapethickness"] = {"value": tapeShieldWireInfo.tapeThickness}
            dictContainsNone = False
            nullAttributes = []
            for i, v in desiredInfo.items():
                if v["value"] is None:
                    dictContainsNone = True
                    nullAttributes.append(i)
            if len(nullAttributes) > 0:
                logger.debug(
                    f"wireInfoTapeShield():TapeShieldCableInfo:{tapeShieldWireInfo.name} contained the following Null \
                    attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                nullAttributes.clear()
            if len(desiredInfo) > 0 and not dictContainsNone:
                rv.append(copy.deepcopy(desiredInfo))
            desiredInfo.clear()
    logger.debug(f'wireInfoTapeShield for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def wireInfoLineNames(distributedArea) -> List[Dict]:
    rv = []
    rvDict = {}
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing wireInfoLineNames query for distributed area: {distributedAreaID}")
    acLineSegments = distributedArea.graph.get(cim.ACLineSegment, {})
    desiredInfo = {}
    for line in acLineSegments.values():
        for acLineSegmentPhase in line.ACLineSegmentPhases:    # type: ignore
            desiredInfo["line_name"] = {"value": line.name}    # type: ignore
            desiredInfo["length"] = {"value": line.length}    # type: ignore
            for terminal in line.Terminals:    # type: ignore
                if int(terminal.sequenceNumber) == 1:
                    desiredInfo["bus1"] = {"value": terminal.ConnectivityNode.name}
                elif int(terminal.sequenceNumber) == 2:
                    desiredInfo["bus2"] = {"value": terminal.ConnectivityNode.name}
            if isinstance(line.WireSpacingInfo, cim.WireSpacingInfo):
                desiredInfo["wire_spacing_info"] = {"value": line.WireSpacingInfo.name}    # type: ignore
            else:
                desiredInfo["wire_spacing_info"] = {"value": None}    # type: ignore
            if isinstance(acLineSegmentPhase.WireInfo, cim.WireInfo):
                desiredInfo["wire_cn_ts"] = {"value": acLineSegmentPhase.WireInfo.name}
            else:
                desiredInfo["wire_cn_ts"] = {"value": None}
            desiredInfo["phase"] = {"value": acLineSegmentPhase.phase[0]}
            desiredInfo["wireinfo"] = {"value": type(acLineSegmentPhase.WireInfo).__name__}
            dictContainsNone = False
            nullAttributes = []
            for i, v in desiredInfo.items():
                if v["value"] is None:
                    dictContainsNone = True
                    nullAttributes.append(i)
            if len(nullAttributes) > 0:
                logger.debug(f"wireInfoLineNames():ACLineSegment:{line.name} contained the following Null attributes:\n\
                    {json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                nullAttributes.clear()
            if len(desiredInfo) > 0 and not dictContainsNone:
                if desiredInfo["line_name"]["value"] not in rvDict.keys():
                    rvDict[desiredInfo["line_name"]["value"]] = {
                        "A": None,
                        "B": None,
                        "C": None,
                        "s1": None,
                        "s2": None,
                        "N": None
                    }
                rvDict[desiredInfo["line_name"]["value"]][desiredInfo["phase"]["value"]] = copy.deepcopy(desiredInfo)
                desiredInfo.clear()
    for k in rvDict.keys():
        for phs in ["A", "B", "C", "s1", "s2", "N"]:
            if rvDict[k][phs] is not None:
                rv.append(copy.deepcopy(rvDict[k][phs]))
    logger.debug(f'wireInfoLineNames for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def powerTransformerEndXfmrImpedances(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing powerTransformerEndXfmrImpedances query for distributed area: {distributedAreaID}")
    powerTransformers = distributedArea.graph.get(cim.PowerTransformer, {})
    desiredInfo = {}
    transformerMeshImpedances = distributedArea.graph.get(cim.TransformerMeshImpedance, {})
    for powerTransformer in powerTransformers.values():
        transformerEnds = []
        for transformerEnd in powerTransformer.PowerTransformerEnd:
            transformerEnds.append(transformerEnd.mRID)
        for transformerMeshImpedance in transformerMeshImpedances.values():
            if transformerMeshImpedance.FromTransformerEnd.mRID in transformerEnds:    # type: ignore
                for toTransformerEnd in transformerMeshImpedance.ToTransformerEnd:    # type: ignore
                    if toTransformerEnd.mRID in transformerEnds:    # type: ignore
                        desiredInfo["xfmr_name"] = {"value": powerTransformer.name}    # type: ignore
                        desiredInfo["mesh_x_ohm"] = {"value": transformerMeshImpedance.x}    # type: ignore
                        dictContainsNone = False
                        nullAttributes = []
                        for i, v in desiredInfo.items():
                            if v["value"] is None:
                                dictContainsNone = True
                                nullAttributes.append(i)
                        if len(nullAttributes) > 0:
                            logger.debug(
                                f"powerTransformerEndXfmrImpedances():PowerTransformer:{powerTransformer.name} \
                                contained the following Null attributes:\n\
                                {json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                            nullAttributes.clear()
                        if len(desiredInfo) > 0 and not dictContainsNone:
                            rv.append(copy.deepcopy(desiredInfo))
                        desiredInfo.clear()
    logger.debug(
        f'powerTransformerEndXfmrImpedances for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def powerTransformerEndXfmrNames(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing powerTransformerEndXfmrNames query for distributed area: {distributedAreaID}")
    powerTransformers = distributedArea.graph.get(cim.PowerTransformer, {})
    desiredInfo = {}
    for powerTransformer in powerTransformers.values():
        for powerTransformerEnd in powerTransformer.PowerTransformerEnd:    # type: ignore
            desiredInfo["xfmr_name"] = {"value": powerTransformer.name}    # type: ignore
            if isinstance(powerTransformerEnd.connectionKind, list):
                desiredInfo["connection"] = {"value": powerTransformerEnd.connectionKind[0]}
            else:
                desiredInfo["connection"] = {"value": None}
            desiredInfo["end_number"] = {"value": powerTransformerEnd.endNumber}
            if isinstance(powerTransformerEnd.Terminal, cim.Terminal) and isinstance(
                    powerTransformerEnd.Terminal.ConnectivityNode, cim.ConnectivityNode):
                desiredInfo["bus"] = {"value": powerTransformerEnd.Terminal.ConnectivityNode.name}
            else:
                desiredInfo["bus"] = {"value": None}
            desiredInfo["ratedS"] = {"value": powerTransformerEnd.ratedS}
            desiredInfo["ratedU"] = {"value": powerTransformerEnd.ratedU}
            desiredInfo["r_ohm"] = {"value": powerTransformerEnd.r}
            dictContainsNone = False
            nullAttributes = []
            for i, v in desiredInfo.items():
                if v["value"] is None:
                    dictContainsNone = True
                    nullAttributes.append(i)
            if len(nullAttributes) > 0:
                logger.debug(
                    f"powerTransformerEndXfmrNames():PowerTransformer:{powerTransformer.name} contained the following \
                    Null attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                nullAttributes.clear()
            if len(desiredInfo) > 0 and not dictContainsNone:
                rv.append(copy.deepcopy(desiredInfo))
            desiredInfo.clear()
    logger.debug(
        f'powerTransformerEndXfmrNames for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def transformerTankXfmrRated(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing transformerTankXfmrRated query for distributed area: {distributedAreaID}")
    transformerTanks = distributedArea.graph.get(cim.TransformerTank, {})
    desiredInfo = {}
    for transformerTank in transformerTanks.values():
        transformerTankInfo = transformerTank.TransformerTankInfo    # type: ignore
        for transformerEndInfo in transformerTankInfo.TransformerEndInfos:
            desiredInfo["xfmr_name"] = {"value": transformerTank.name}    # type: ignore
            desiredInfo["connection"] = {"value": transformerEndInfo.connectionKind[0]}
            desiredInfo["enum"] = {"value": transformerEndInfo.endNumber}
            desiredInfo["ratedS"] = {"value": transformerEndInfo.ratedS}
            desiredInfo["ratedU"] = {"value": transformerEndInfo.ratedU}
            desiredInfo["r_ohm"] = {"value": transformerEndInfo.r}
            dictContainsNone = False
            nullAttributes = []
            for i, v in desiredInfo.items():
                if v["value"] is None:
                    dictContainsNone = True
                    nullAttributes.append(i)
            if len(nullAttributes) > 0:
                logger.debug(
                    f"transformerTankXfmrRated():TransformerTank:{transformerTank.name} contained the following Null \
                    attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                nullAttributes.clear()
            if len(desiredInfo) > 0 and not dictContainsNone:
                rv.append(copy.deepcopy(desiredInfo))
            desiredInfo.clear()
    logger.debug(f'transformerTankXfmrRated for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def transformerTankXfmrSct(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing transformerTankXfmrSct query for distributed area: {distributedAreaID}")
    transformerTanks = distributedArea.graph.get(cim.TransformerTank, {})
    desiredInfo = {}
    for transformerTank in transformerTanks.values():
        transformerTankInfo = transformerTank.TransformerTankInfo    # type: ignore
        for transformerEndInfo in transformerTankInfo.TransformerEndInfos:
            for shortCircuitTest in transformerEndInfo.EnergisedEndShortCircuitTests:
                for groundedEnd in shortCircuitTest.GroundedEnds:
                    desiredInfo["xfmr_name"] = {"value": transformerTank.name}    # type: ignore
                    desiredInfo["enum"] = {"value": shortCircuitTest.EnergisedEnd.endNumber}
                    desiredInfo["gnum"] = {"value": groundedEnd.endNumber}
                    desiredInfo["leakage_z"] = {"value": shortCircuitTest.leakageImpedance}
                    dictContainsNone = False
                    nullAttributes = []
                    for i, v in desiredInfo.items():
                        if v["value"] is None:
                            dictContainsNone = True
                            nullAttributes.append(i)
                    if len(nullAttributes) > 0:
                        logger.debug(
                            f"transformerTankXfmrSct():TransformerTank:{transformerTank.name} contained the following \
                            Null attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                        nullAttributes.clear()
                    if len(desiredInfo) > 0 and not dictContainsNone:
                        if desiredInfo not in rv:
                            rv.append(copy.deepcopy(desiredInfo))
                    desiredInfo.clear()
            for shortCircuitTest in transformerEndInfo.GroundedEndShortCircuitTests:
                for groundedEnd in shortCircuitTest.GroundedEnds:
                    desiredInfo["xfmr_name"] = {"value": transformerTank.name}    # type: ignore
                    desiredInfo["enum"] = {"value": shortCircuitTest.EnergisedEnd.endNumber}
                    desiredInfo["gnum"] = {"value": groundedEnd.endNumber}
                    desiredInfo["leakage_z"] = {"value": shortCircuitTest.leakageImpedance}
                    dictContainsNone = False
                    nullAttributes = []
                    for i, v in desiredInfo.items():
                        if v["value"] is None:
                            dictContainsNone = True
                            nullAttributes.append(i)
                    if len(nullAttributes) > 0:
                        logger.debug(
                            f"transformerTankXfmrSct():TransformerTank:{transformerTank.name} contained the following \
                            Null attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                        nullAttributes.clear()
                    if len(desiredInfo) > 0 and not dictContainsNone:
                        if desiredInfo not in rv:
                            rv.append(copy.deepcopy(desiredInfo))
                    desiredInfo.clear()
    logger.debug(f'transformerTankXfmrSct for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def transformerTankXfmrNames(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing transformerTankXfmrNames query for distributed area: {distributedAreaID}")
    transformerTanks = distributedArea.graph.get(cim.TransformerTank, {})
    desiredInfo = {}
    for transformerTank in transformerTanks.values():
        for transformerTankEnd in transformerTank.TransformerTankEnds:    # type: ignore
            desiredInfo["xfmr_name"] = {"value": transformerTank.name}    # type: ignore
            desiredInfo["enum"] = {"value": transformerTankEnd.endNumber}
            desiredInfo["bus"] = {"value": transformerTankEnd.Terminal.ConnectivityNode.name}
            desiredInfo["baseV"] = {"value": transformerTankEnd.BaseVoltage.nominalVoltage}
            desiredInfo["phase"] = {"value": transformerTankEnd.phases[0]}
            dictContainsNone = False
            nullAttributes = []
            for i, v in desiredInfo.items():
                if v["value"] is None:
                    dictContainsNone = True
                    nullAttributes.append(i)
            if len(nullAttributes) > 0:
                logger.debug(
                    f"transformerTankXfmrNames():TransformerTank:{transformerTank.name} contained the following Null \
                        attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                nullAttributes.clear()
            if len(desiredInfo) > 0 and not dictContainsNone:
                rv.append(copy.deepcopy(desiredInfo))
            desiredInfo.clear()
    logger.debug(f'transformerTankXfmrNames for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def switchingEquipmentSwitchNames(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing switchingEquipmentSwitchNames query for distributed area: {distributedAreaID}")
    loadBreakSwitches = distributedArea.graph.get(cim.LoadBreakSwitch, {})
    reclosers = distributedArea.graph.get(cim.Recloser, {})
    breakers = distributedArea.graph.get(cim.Breaker, {})
    fuses = distributedArea.graph.get(cim.Fuse, {})
    sectionalisers = distributedArea.graph.get(cim.Sectionaliser, {})
    jumpers = distributedArea.graph.get(cim.Jumper, {})
    disconnectors = distributedArea.graph.get(cim.Disconnector, {})
    groundDisconnectors = distributedArea.graph.get(cim.GroundDisconnector, {})
    switchEquipment = {
        **loadBreakSwitches,
        **reclosers,
        **breakers,
        **fuses,
        **sectionalisers,
        **jumpers,
        **disconnectors,
        **groundDisconnectors
    }
    desiredInfo = {}
    for switchEq in switchEquipment.values():
        if len(switchEq.SwitchPhase) > 0:
            for switchPhase in switchEq.SwitchPhase:    # type: ignore
                desiredInfo["sw_name"] = {"value": switchEq.name}    # type: ignore
                desiredInfo["is_Open"] = {"value": switchEq.open}    # type: ignore
                for terminal in switchEq.Terminals:    # type: ignore
                    if int(terminal.sequenceNumber) == 1:
                        desiredInfo["bus1"] = {"value": terminal.ConnectivityNode.name}
                    if int(terminal.sequenceNumber) == 2:
                        desiredInfo["bus2"] = {"value": terminal.ConnectivityNode.name}
                if switchPhase.phaseSide1 is not None:
                    desiredInfo["phases_side1"] = {"value": switchPhase.phaseSide1[0]}
                else:
                    desiredInfo["phases_side1"] = {"value": None}
                dictContainsNone = False
                nullAttributes = []
                for i, v in desiredInfo.items():
                    if v["value"] is None:
                        dictContainsNone = True
                        nullAttributes.append(i)
                if len(nullAttributes) > 0:
                    logger.debug(f"switchingEquipmentSwitchNames():Switch:{switchEq.name} contained the following Null \
                        attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                    nullAttributes.clear()
                if len(desiredInfo) > 0 and not dictContainsNone:
                    rv.append(copy.deepcopy(desiredInfo))
                desiredInfo.clear()
        else:
            desiredInfo["sw_name"] = {"value": switchEq.name}    # type: ignore
            desiredInfo["is_Open"] = {"value": switchEq.open}    # type: ignore
            for terminal in switchEq.Terminals:    # type: ignore
                if int(terminal.sequenceNumber) == 1:
                    desiredInfo["bus1"] = {"value": terminal.ConnectivityNode.name}
                if int(terminal.sequenceNumber) == 2:
                    desiredInfo["bus2"] = {"value": terminal.ConnectivityNode.name}
            desiredInfo["phases_side1"] = {"value": ''}
            dictContainsNone = False
            nullAttributes = []
            for i, v in desiredInfo.items():
                if v["value"] is None:
                    dictContainsNone = True
                    nullAttributes.append(i)
            if len(nullAttributes) > 0:
                logger.debug(
                    f"switchingEquipmentSwitchNames():Switch:{switchEq.name} contained the following Null attributes:\n\
                    {json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                nullAttributes.clear()
            if len(desiredInfo) > 0 and not dictContainsNone:
                rv.append(copy.deepcopy(desiredInfo))
            desiredInfo.clear()
    logger.debug(
        f'switchingEquipmentSwitchNames for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def shuntElementCapNames(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing shuntElementCapNames query for distributed area: {distributedAreaID}")
    linearShuntCompensators = distributedArea.graph.get(cim.LinearShuntCompensator, {})
    desiredInfo = {}
    for linearShuntCompensator in linearShuntCompensators.values():
        if len(linearShuntCompensator.ShuntCompensatorPhase) > 0:
            for shuntCompensatorPhase in linearShuntCompensator.ShuntCompensatorPhase:    # type: ignore
                desiredInfo["cap_name"] = {"value": linearShuntCompensator.name}    # type: ignore
                desiredInfo["b_per_section"] = {"value": linearShuntCompensator.bPerSection}    # type: ignore
                for terminal in linearShuntCompensator.Terminals:
                    if int(terminal.sequenceNumber) == 1:
                        desiredInfo["bus"] = {"value": terminal.ConnectivityNode.name}    # type: ignore
                    else:
                        desiredInfo["bus"] = {"value": None}
                if shuntCompensatorPhase.phase is not None:
                    desiredInfo["phase"] = {"value": shuntCompensatorPhase.phase[0]}
                dictContainsNone = False
                nullAttributes = []
                for i, v in desiredInfo.items():
                    if v["value"] is None:
                        dictContainsNone = True
                        nullAttributes.append(i)
                if len(nullAttributes) > 0:
                    logger.debug(
                        f"shuntElementCapNames():Capacitor:{linearShuntCompensator.name} contained the following Null \
                        attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                    nullAttributes.clear()
                if len(desiredInfo) > 0 and not dictContainsNone:
                    rv.append(copy.deepcopy(desiredInfo))
                desiredInfo.clear()
        else:
            desiredInfo["cap_name"] = {"value": linearShuntCompensator.name}    # type: ignore
            desiredInfo["b_per_section"] = {"value": linearShuntCompensator.bPerSection}    # type: ignore
            for terminal in linearShuntCompensator.Terminals:
                if int(terminal.sequenceNumber) == 1:
                    desiredInfo["bus"] = {"value": terminal.ConnectivityNode.name}    # type: ignore
                else:
                    desiredInfo["bus"] = {"value": None}
            dictContainsNone = False
            nullAttributes = []
            for i, v in desiredInfo.items():
                if v["value"] is None:
                    dictContainsNone = True
                    nullAttributes.append(i)
            if len(nullAttributes) > 0:
                logger.debug(
                    f"shuntElementCapNames():Capacitor:{linearShuntCompensator.name} contained the following Null \
                    attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                nullAttributes.clear()
            if len(desiredInfo) > 0 and not dictContainsNone:
                rv.append(copy.deepcopy(desiredInfo))
            desiredInfo.clear()
    logger.debug(f'shuntElementCapNames for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def transformerTankXfmrNlt(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing transformerTankXfmrNlt query for distributed area: {distributedAreaID}")
    powerTransformers = distributedArea.graph.get(cim.PowerTransformer, {})
    transformerTanks = distributedArea.graph.get(cim.TransformerTank, {})
    desiredInfo = {}
    for transformerTank in transformerTanks.values():
        transformerTankInfo = transformerTank.TransformerTankInfo    # type: ignore
        for transformerEndInfo in transformerTankInfo.TransformerEndInfos:
            for noLoadTest in transformerEndInfo.EnergisedEndNoLoadTests:
                desiredInfo["xfmr_name"] = {"value": transformerTank.name}    # type: ignore
                desiredInfo["noloadloss_kW"] = {"value": noLoadTest.loss}
                desiredInfo["i_exciting"] = {"value": noLoadTest.excitingCurrent}
                dictContainsNone = False
                nullAttributes = []
                for i, v in desiredInfo.items():
                    if v["value"] is None:
                        dictContainsNone = True
                        nullAttributes.append(i)
                if len(nullAttributes) > 0:
                    logger.debug(
                        f"transformerTankXfmrNlt():TransformerTank:{transformerTank.name} contained the following Null \
                        attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                    nullAttributes.clear()
                if len(desiredInfo) > 0 and not dictContainsNone:
                    rv.append(copy.deepcopy(desiredInfo))
                desiredInfo.clear()
    logger.debug(f'transformerTankXfmrNlt for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def powerTransformerEndXfmrAdmittances(distributedArea: DistributedArea) -> List[Dict]:
    rv = []
    distributedAreaID = distributedArea.container.mRID
    logger.debug(f"performing powerTransformerEndXfmrAdmittances query for distributed area: {distributedAreaID}")
    powerTransformers = distributedArea.graph.get(cim.PowerTransformer, {})
    desiredInfo = {}
    for powerTransformer in powerTransformers.values():
        for powerTransformerEnd in powerTransformer.PowerTransformerEnd:    # type: ignore
            if powerTransformerEnd.CoreAdmittance is not None:
                desiredInfo["xfmr_name"] = {"value": powerTransformer.name}    # type: ignore
                desiredInfo["b_S"] = {"value": powerTransformerEnd.CoreAdmittance.b}
                desiredInfo["g_S"] = {"value": powerTransformerEnd.CoreAdmittance.g}
                dictContainsNone = False
                nullAttributes = []
                for i, v in desiredInfo.items():
                    if v["value"] is None:
                        dictContainsNone = True
                        nullAttributes.append(i)
                if len(nullAttributes) > 0:
                    logger.debug(
                        f"powerTransformerEndXfmrAdmittances():PowerTransformer:{powerTransformer.name} contained the \
                        following Null attributes:\n{json.dumps(nullAttributes, indent=4, sort_keys=True)}")
                    nullAttributes.clear()
                if len(desiredInfo) > 0 and not dictContainsNone:
                    rv.append(copy.deepcopy(desiredInfo))
                desiredInfo.clear()
    logger.debug(
        f'powerTransformerEndXfmrAdmittances for {distributedAreaID} returns: {json.dumps(rv,indent=4,sort_keys=True)}')
    return rv


def fillYbusUnique(bus1: str, bus2: str, Yval: float, Ybus: Dict):
    if Yval == 0j:
        return
    if bus1 not in Ybus:
        Ybus[bus1] = {}
    if bus2 not in Ybus:
        Ybus[bus2] = {}
    if bus2 in Ybus[bus1]:
        logger.warn(f'Unexpected existing value found for Ybus[{bus1}][{bus2}] when filling model value')
    Ybus[bus1][bus2] = Ybus[bus2][bus1] = Yval


def fillYbusAdd(bus1: str, bus2: str, Yval: float, Ybus: Dict):
    if Yval == 0j:
        return
    if bus1 not in Ybus:
        Ybus[bus1] = {}
    if bus2 not in Ybus:
        Ybus[bus2] = {}
    if bus2 in Ybus[bus1]:
        Ybus[bus1][bus2] += Yval
        Ybus[bus2][bus1] = Ybus[bus1][bus2]
    else:
        Ybus[bus1][bus2] = Ybus[bus2][bus1] = Yval


def fillYbusUniqueUpperLines(bus1: str, bus2: str, Yval: float, Ybus: Dict):
    if Yval == 0j:
        return
    if bus1 not in Ybus:
        Ybus[bus1] = {}
    if bus2 not in Ybus:
        Ybus[bus2] = {}
    if bus2 in Ybus[bus1]:
        logger.warn('Unexpected existing value found for Ybus[{bus1}][{bus2}] when filling line model value')
    # extract the node and phase from bus1 and bus2
    node1, phase1 = bus1.split('.')
    node2, phase2 = bus2.split('.')
    bus3 = node1 + '.' + phase2
    bus4 = node2 + '.' + phase1
    if bus3 not in Ybus:
        Ybus[bus3] = {}
    if bus4 not in Ybus:
        Ybus[bus4] = {}
    Ybus[bus1][bus2] = Ybus[bus2][bus1] = Ybus[bus3][bus4] = Ybus[bus4][bus3] = Yval


def fillYbusNoSwapLines(bus1: str, bus2: str, Yval: float, Ybus: Dict):
    fillYbusUnique(bus2, bus1, Yval, Ybus)
    fillYbusAdd(bus1, bus1, -Yval, Ybus)
    fillYbusAdd(bus2, bus2, -Yval, Ybus)


def fillYbusSwapLines(bus1: str, bus2: str, Yval: float, Ybus: Dict):
    fillYbusUniqueUpperLines(bus2, bus1, Yval, Ybus)
    # extract the node and phase from bus1 and bus2
    node1, phase1 = bus1.split('.')
    node2, phase2 = bus2.split('.')
    # mix-and-match nodes and phases for filling Ybus
    fillYbusAdd(bus1, node1 + '.' + phase2, -Yval, Ybus)
    fillYbusAdd(node2 + '.' + phase1, bus2, -Yval, Ybus)


def fillYbusPerLengthPhaseImpedanceLines(distributedArea, Ybus: Dict):
    bindings = perLengthPhaseImpedanceLineConfigs(distributedArea)
    if len(bindings) == 0:
        return
    Zabc = {}
    for obj in bindings:
        line_config = obj['line_config']['value']
        count = int(obj['count']['value'])
        row = int(obj['row']['value'])
        col = int(obj['col']['value'])
        r_ohm_per_m = float(obj['r_ohm_per_m']['value'])
        x_ohm_per_m = float(obj['x_ohm_per_m']['value'])
        if line_config not in Zabc:
            if count == 1:
                Zabc[line_config] = np.zeros((1, 1), dtype=complex)
            elif count == 2:
                Zabc[line_config] = np.zeros((2, 2), dtype=complex)
            elif count == 3:
                Zabc[line_config] = np.zeros((3, 3), dtype=complex)
        Zabc[line_config][row - 1, col - 1] = complex(r_ohm_per_m, x_ohm_per_m)
        if row != col:
            Zabc[line_config][col - 1, row - 1] = complex(r_ohm_per_m, x_ohm_per_m)
    bindings = perLengthPhaseImpedanceLineNames(distributedArea)
    if len(bindings) == 0:
        return
    bindingsSorted = sorted(bindings, key=lambda d: (d["line_name"]["value"], d["phase"]["value"]))
    # map line_name query phase values to nodelist indexes
    ybusPhaseIdx = {'A': '.1', 'B': '.2', 'C': '.3', 's1': '.1', 's2': '.2'}
    last_name = ''
    for obj in bindingsSorted:
        line_name = obj['line_name']['value']
        bus1 = obj['bus1']['value'].upper()
        bus2 = obj['bus2']['value'].upper()
        length = float(obj['length']['value'])
        line_config = obj['line_config']['value']
        phase = obj['phase']['value']
        if line_name != last_name and line_config in Zabc:
            last_name = line_name
            line_idx = 0
            # multiply by scalar length
            lenZabc = Zabc[line_config] * length
            # invert the matrix
            invZabc = np.linalg.inv(lenZabc)
            # test if the inverse * original = identity
            # identityTest = np.dot(lenZabc, invZabc)
            # logger.debug('identity test for ' + line_name + ': ' + str(identityTest))
            # negate the matrix and assign it to Ycomp
            Ycomp = invZabc * -1
        # we now have the negated inverted matrix for comparison
        line_idx += 1
        if Ycomp.size == 1:
            fillYbusNoSwapLines(bus1 + ybusPhaseIdx[phase], bus2 + ybusPhaseIdx[phase], Ycomp[0, 0], Ybus)
        elif Ycomp.size == 4:
            if line_idx == 1:
                pair_i0b1 = bus1 + ybusPhaseIdx[phase]
                pair_i0b2 = bus2 + ybusPhaseIdx[phase]
            else:
                pair_i1b1 = bus1 + ybusPhaseIdx[phase]
                pair_i1b2 = bus2 + ybusPhaseIdx[phase]
                fillYbusNoSwapLines(pair_i0b1, pair_i0b2, Ycomp[0, 0], Ybus)
                fillYbusSwapLines(pair_i1b1, pair_i0b2, Ycomp[1, 0], Ybus)
                fillYbusNoSwapLines(pair_i1b1, pair_i1b2, Ycomp[1, 1], Ybus)
        elif Ycomp.size == 9:
            if line_idx == 1:
                pair_i0b1 = bus1 + ybusPhaseIdx[phase]
                pair_i0b2 = bus2 + ybusPhaseIdx[phase]
            elif line_idx == 2:
                pair_i1b1 = bus1 + ybusPhaseIdx[phase]
                pair_i1b2 = bus2 + ybusPhaseIdx[phase]
            else:
                pair_i2b1 = bus1 + ybusPhaseIdx[phase]
                pair_i2b2 = bus2 + ybusPhaseIdx[phase]
                fillYbusNoSwapLines(pair_i0b1, pair_i0b2, Ycomp[0, 0], Ybus)
                fillYbusSwapLines(pair_i1b1, pair_i0b2, Ycomp[1, 0], Ybus)
                fillYbusNoSwapLines(pair_i1b1, pair_i1b2, Ycomp[1, 1], Ybus)
                fillYbusSwapLines(pair_i2b1, pair_i0b2, Ycomp[2, 0], Ybus)
                fillYbusSwapLines(pair_i2b1, pair_i1b2, Ycomp[2, 1], Ybus)
                fillYbusNoSwapLines(pair_i2b1, pair_i2b2, Ycomp[2, 2], Ybus)


def fillYbusPerLengthSequenceImpedanceLines(distributedArea: DistributedArea, Ybus: Dict):
    bindings = perLengthSequenceImpedanceLineConfigs(distributedArea)
    if len(bindings) == 0:
        return
    Zabc = {}
    for obj in bindings:
        line_config = obj['line_config']['value']
        r1 = float(obj['r1_ohm_per_m']['value'])
        x1 = float(obj['x1_ohm_per_m']['value'])
        r0 = float(obj['r0_ohm_per_m']['value'])
        x0 = float(obj['x0_ohm_per_m']['value'])
        Zs = complex((r0 + 2.0 * r1) / 3.0, (x0 + 2.0 * x1) / 3.0)
        Zm = complex((r0 - r1) / 3.0, (x0 - x1) / 3.0)
        Zabc[line_config] = np.array([(Zs, Zm, Zm), (Zm, Zs, Zm), (Zm, Zm, Zs)], dtype=complex)
    bindings = perLengthSequenceImpedanceLineNames(distributedArea)
    if len(bindings) == 0:
        return
    for obj in bindings:
        line_name = obj['line_name']['value']
        bus1 = obj['bus1']['value'].upper()
        bus2 = obj['bus2']['value'].upper()
        length = float(obj['length']['value'])
        line_config = obj['line_config']['value']
        # multiply by scalar length
        lenZabc = Zabc[line_config] * length
        # invert the matrix
        invZabc = np.linalg.inv(lenZabc)
        # test if the inverse * original = identity
        # identityTest = np.dot(lenZabc, invZabc)
        # logger.debug('identity test for ' + line_name + ': ' + str(identityTest))
        # negate the matrix and assign it to Ycomp
        Ycomp = invZabc * -1
        fillYbusNoSwapLines(bus1 + '.1', bus2 + '.1', Ycomp[0, 0], Ybus)
        fillYbusSwapLines(bus1 + '.2', bus2 + '.1', Ycomp[1, 0], Ybus)
        fillYbusNoSwapLines(bus1 + '.2', bus2 + '.2', Ycomp[1, 1], Ybus)
        fillYbusSwapLines(bus1 + '.3', bus2 + '.1', Ycomp[2, 0], Ybus)
        fillYbusSwapLines(bus1 + '.3', bus2 + '.2', Ycomp[2, 1], Ybus)
        fillYbusNoSwapLines(bus1 + '.3', bus2 + '.3', Ycomp[2, 2], Ybus)


def fillYbusACLineSegmentLines(distributedArea: DistributedArea, Ybus: Dict):
    bindings = acLineSegmentLineNames(distributedArea)
    if len(bindings) == 0:
        return
    for obj in bindings:
        # logger.debug(obj.keys())
        line_name = obj['line_name']['value']
        bus1 = obj['bus1']['value'].upper()
        bus2 = obj['bus2']['value'].upper()
        length = float(obj['length']['value'])
        r1 = float(obj['r1_Ohm']['value'])
        x1 = float(obj['x1_Ohm']['value'])
        r0 = float(obj['r0_Ohm']['value'])
        x0 = float(obj['x0_Ohm']['value'])
        Zs = complex((r0 + 2.0 * r1) / 3.0, (x0 + 2.0 * x1) / 3.0)
        Zm = complex((r0 - r1) / 3.0, (x0 - x1) / 3.0)
        Zabc = np.array([(Zs, Zm, Zm), (Zm, Zs, Zm), (Zm, Zm, Zs)], dtype=complex)
        # multiply by scalar length
        lenZabc = Zabc * length
        # lenZabc = Zabc * length * 3.3 # Kludge to get arount units issue (ft vs. m)
        # invert the matrix
        invZabc = np.linalg.inv(lenZabc)
        # test if the inverse * original = identity
        # identityTest = np.dot(lenZabc, invZabc)
        # logger.debug('identity test for ' + line_name + ': ' + str(identityTest))
        # negate the matrix and assign it to Ycomp
        Ycomp = invZabc * -1
        fillYbusNoSwapLines(bus1 + '.1', bus2 + '.1', Ycomp[0, 0], Ybus)
        fillYbusSwapLines(bus1 + '.2', bus2 + '.1', Ycomp[1, 0], Ybus)
        fillYbusNoSwapLines(bus1 + '.2', bus2 + '.2', Ycomp[1, 1], Ybus)
        fillYbusSwapLines(bus1 + '.3', bus2 + '.1', Ycomp[2, 0], Ybus)
        fillYbusSwapLines(bus1 + '.3', bus2 + '.2', Ycomp[2, 1], Ybus)
        fillYbusNoSwapLines(bus1 + '.3', bus2 + '.3', Ycomp[2, 2], Ybus)


def CN_dist_R(dim, i, j, wire_spacing_info, wire_cn_ts, XCoord, YCoord, CN_strand_count, CN_strand_rdc, CN_strand_gmr,
              CN_strand_radius, CN_diameter_jacket):
    dist = (CN_diameter_jacket[wire_cn_ts] - CN_strand_radius[wire_cn_ts] * 2.0) / 2.0
    return dist


def CN_dist_D(dim, i, j, wire_spacing_info, wire_cn_ts, XCoord, YCoord, CN_strand_count, CN_strand_rdc, CN_strand_gmr,
              CN_strand_radius, CN_diameter_jacket):
    ii, jj = CN_dist_ij[dim][i][j]
    dist = math.sqrt(
        math.pow(XCoord[wire_spacing_info][ii] - XCoord[wire_spacing_info][jj], 2) +
        math.pow(YCoord[wire_spacing_info][ii] - YCoord[wire_spacing_info][jj], 2))
    return dist


def CN_dist_DR(dim, i, j, wire_spacing_info, wire_cn_ts, XCoord, YCoord, CN_strand_count, CN_strand_rdc, CN_strand_gmr,
               CN_strand_radius, CN_diameter_jacket):
    ii, jj = CN_dist_ij[dim][i][j]
    d = math.sqrt(
        math.pow(XCoord[wire_spacing_info][ii] - XCoord[wire_spacing_info][jj], 2) +
        math.pow(YCoord[wire_spacing_info][ii] - YCoord[wire_spacing_info][jj], 2))
    k = CN_strand_count[wire_cn_ts]
    R = (CN_diameter_jacket[wire_cn_ts] - CN_strand_radius[wire_cn_ts] * 2.0) / 2.0
    dist = math.pow(math.pow(d, k) - math.pow(R, k), 1.0 / k)

    return dist


# global constants for determining Zprim values
u0 = math.pi * 4.0e-7
w = math.pi * 2.0 * 60.0
p = 100.0
f = 60.0
Rg = (u0 * w) / 8.0
X0 = (u0 * w) / (math.pi * 2.0)
Xg = X0 * math.log(658.5 * math.sqrt(p / f))

CN_dist_func = {}
CN_dist_ij = {}

# 2x2 distance function mappings
CN_dist_func[1] = {}
CN_dist_func[1][2] = {}
CN_dist_func[1][2][1] = CN_dist_R

# 4x4 distance function mappings
CN_dist_func[2] = {}
CN_dist_ij[2] = {}
CN_dist_func[2][2] = {}
CN_dist_ij[2][2] = {}
CN_dist_func[2][2][1] = CN_dist_D
CN_dist_ij[2][2][1] = (2, 1)
CN_dist_func[2][3] = {}
CN_dist_ij[2][3] = {}
CN_dist_func[2][3][1] = CN_dist_R
CN_dist_func[2][3][2] = CN_dist_DR
CN_dist_ij[2][3][2] = (2, 1)
CN_dist_func[2][4] = {}
CN_dist_ij[2][4] = {}
CN_dist_func[2][4][1] = CN_dist_DR
CN_dist_ij[2][4][1] = (2, 1)
CN_dist_func[2][4][2] = CN_dist_R
CN_dist_func[2][4][3] = CN_dist_D
CN_dist_ij[2][4][3] = (2, 1)

# 6x6 distance function mappings
CN_dist_func[3] = {}
CN_dist_ij[3] = {}
CN_dist_func[3][2] = {}
CN_dist_ij[3][2] = {}
CN_dist_func[3][2][1] = CN_dist_D
CN_dist_ij[3][2][1] = (2, 1)
CN_dist_func[3][3] = {}
CN_dist_ij[3][3] = {}
CN_dist_func[3][3][1] = CN_dist_D
CN_dist_ij[3][3][1] = (3, 1)
CN_dist_func[3][3][2] = CN_dist_D
CN_dist_ij[3][3][2] = (3, 2)
CN_dist_func[3][4] = {}
CN_dist_ij[3][4] = {}
CN_dist_func[3][4][1] = CN_dist_R
CN_dist_func[3][4][2] = CN_dist_DR
CN_dist_ij[3][4][2] = (2, 1)
CN_dist_func[3][4][3] = CN_dist_DR
CN_dist_ij[3][4][3] = (3, 1)
CN_dist_func[3][5] = {}
CN_dist_ij[3][5] = {}
CN_dist_func[3][5][1] = CN_dist_DR
CN_dist_ij[3][5][1] = (2, 1)
CN_dist_func[3][5][2] = CN_dist_R
CN_dist_func[3][5][3] = CN_dist_DR
CN_dist_ij[3][5][3] = (3, 2)
CN_dist_func[3][5][4] = CN_dist_D
CN_dist_ij[3][5][4] = (2, 1)
CN_dist_func[3][6] = {}
CN_dist_ij[3][6] = {}
CN_dist_func[3][6][1] = CN_dist_DR
CN_dist_ij[3][6][1] = (3, 1)
CN_dist_func[3][6][2] = CN_dist_DR
CN_dist_ij[3][6][2] = (3, 2)
CN_dist_func[3][6][3] = CN_dist_R
CN_dist_func[3][6][4] = CN_dist_D
CN_dist_ij[3][6][4] = (3, 1)
CN_dist_func[3][6][5] = CN_dist_D
CN_dist_ij[3][6][5] = (3, 2)


def diagZprim(wireinfo: str, wire_cn_ts, neutralFlag, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr,
              CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen):
    if wireinfo == 'ConcentricNeutralCableInfo' and neutralFlag:
        R = (CN_diameter_jacket[wire_cn_ts] - CN_strand_radius[wire_cn_ts] * 2.0) / 2.0
        k = CN_strand_count[wire_cn_ts]
        dist = math.pow(CN_strand_gmr[wire_cn_ts] * k * math.pow(R, k - 1), 1.0 / k)
        Zprim = complex(CN_strand_rdc[wire_cn_ts] / k + Rg, X0 * math.log(1.0 / dist) + Xg)
    # this situation won't normally occur so we are just using neutralFlag to recognize the
    # row 2 diagonal for the shield calculation vs. row1 and row3 that are handled below
    elif wireinfo == 'TapeShieldCableInfo' and neutralFlag:
        T = TS_tape_thickness[wire_cn_ts]
        ds = TS_diameter_screen[wire_cn_ts] + 2.0 * T
        Rshield = 0.3183 * 2.3718e-8 / (ds * T * math.sqrt(50.0 / (100.0 - 20.0)))
        Dss = 0.5 * (ds - T)
        Zprim = complex(Rshield + Rg, X0 * math.log(1.0 / Dss) + Xg)
    else:
        Zprim = complex(R25[wire_cn_ts] + Rg, X0 * math.log(1.0 / GMR[wire_cn_ts]) + Xg)
    return Zprim


def offDiagZprim(i, j, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count,
                 CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness,
                 TS_diameter_screen):
    if wireinfo == 'OverheadWireInfo':
        dist = math.sqrt(
            math.pow(XCoord[wire_spacing_info][i] - XCoord[wire_spacing_info][j], 2) +
            math.pow(YCoord[wire_spacing_info][i] - YCoord[wire_spacing_info][j], 2))
    elif wireinfo == 'ConcentricNeutralCableInfo':
        dim = len(XCoord[wire_spacing_info])    # 1=2x2, 2=4x4, 3=6x6
        dist = CN_dist_func[dim][i][j](dim, i, j, wire_spacing_info, wire_cn_ts, XCoord, YCoord, CN_strand_count,
                                       CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket)
    elif wireinfo == 'TapeShieldCableInfo':
        # this should only be hit for i==2
        T = TS_tape_thickness[wire_cn_ts]
        ds = TS_diameter_screen[wire_cn_ts] + 2.0 * T
        dist = 0.5 * (ds - T)
    Zprim = complex(Rg, X0 * math.log(1.0 / dist) + Xg)
    return Zprim


def fillYbusWireInfoAndWireSpacingInfoLines(distributedArea: DistributedArea, Ybus: Dict):
    # WireSpacingInfo query
    bindings = wireInfoSpacing(distributedArea)
    XCoord = {}
    YCoord = {}
    for obj in bindings:
        wire_spacing_info = obj['wire_spacing_info']['value']
        cableFlag = obj['cable']['value'].upper() == 'TRUE'    # don't depend on lowercase
        seq = int(obj['seq']['value'])
        if wire_spacing_info not in XCoord.keys():
            XCoord[wire_spacing_info] = {}
        if wire_spacing_info not in YCoord.keys():
            YCoord[wire_spacing_info] = {}
        XCoord[wire_spacing_info][seq] = float(obj['xCoord']['value'])
        YCoord[wire_spacing_info][seq] = float(obj['yCoord']['value'])
    # OverheadWireInfo specific query
    bindings = wireInfoOverhead(distributedArea)
    GMR = {}
    R25 = {}
    for obj in bindings:
        wire_cn_ts = obj['wire_cn_ts']['value']
        GMR[wire_cn_ts] = float(obj['gmr']['value'])
        R25[wire_cn_ts] = float(obj['r25']['value'])
    # ConcentricNeutralCableInfo specific query
    bindings = wireInfoConcentricNeutral(distributedArea)
    CN_diameter_jacket = {}
    CN_strand_count = {}
    CN_strand_radius = {}
    CN_strand_gmr = {}
    CN_strand_rdc = {}
    for obj in bindings:
        wire_cn_ts = obj['wire_cn_ts']['value']
        GMR[wire_cn_ts] = float(obj['gmr']['value'])
        R25[wire_cn_ts] = float(obj['r25']['value'])
        CN_diameter_jacket[wire_cn_ts] = float(obj['diameter_jacket']['value'])
        CN_strand_count[wire_cn_ts] = int(obj['strand_count']['value'])
        CN_strand_radius[wire_cn_ts] = float(obj['strand_radius']['value'])
        CN_strand_gmr[wire_cn_ts] = float(obj['strand_gmr']['value'])
        CN_strand_rdc[wire_cn_ts] = float(obj['strand_rdc']['value'])
    # TapeShieldCableInfo specific query
    bindings = wireInfoTapeShield(distributedArea)
    TS_diameter_screen = {}
    TS_tape_thickness = {}
    for obj in bindings:
        wire_cn_ts = obj['wire_cn_ts']['value']
        GMR[wire_cn_ts] = float(obj['gmr']['value'])
        R25[wire_cn_ts] = float(obj['r25']['value'])
        TS_diameter_screen[wire_cn_ts] = float(obj['diameter_screen']['value'])
        TS_tape_thickness[wire_cn_ts] = float(obj['tapethickness']['value'])
    # line_names query for all types
    bindings = wireInfoLineNames(distributedArea)
    if len(bindings) == 0:
        return
    bindingsSorted = sorted(bindings, key=lambda d: (d["line_name"]["value"], d["phase"]["value"]))
    # map line_name query phase values to nodelist indexes
    ybusPhaseIdx = {'A': '.1', 'B': '.2', 'C': '.3', 'N': '.4', 's1': '.1', 's2': '.2'}
    # map between 0-base numpy array indices and 1-based formulas so everything lines up
    i1 = j1 = 0
    i2 = j2 = 1
    i3 = j3 = 2
    i4 = j4 = 3
    i5 = j5 = 4
    i6 = j6 = 5
    tape_line = None
    tape_skip = False
    phaseIdx = 0
    CN_done = False
    for obj in bindingsSorted:
        line_name = obj['line_name']['value']
        bus1 = obj['bus1']['value'].upper()
        bus2 = obj['bus2']['value'].upper()
        length = float(obj['length']['value'])
        wire_spacing_info = obj['wire_spacing_info']['value']
        phase = obj['phase']['value']
        wire_cn_ts = obj['wire_cn_ts']['value']
        wireinfo = obj['wireinfo']['value']
        # TapeShieldCableInfo is special so it needs some special processing
        # first, the wireinfo isn't always TapeShieldCableInfo so need to match on line_name instead
        # second, only a single phase is implemented so need a way to skip processing multiple phases
        if wireinfo == 'TapeShieldCableInfo' or line_name == tape_line:
            tape_line = line_name
            if tape_skip:
                continue
        else:
            tape_line = None
            tape_skip = False
        if phaseIdx == 0:
            pair_i0b1 = bus1 + ybusPhaseIdx[phase]
            pair_i0b2 = bus2 + ybusPhaseIdx[phase]
            dim = len(XCoord[wire_spacing_info])
            if wireinfo == 'OverheadWireInfo':
                if dim == 2:
                    Zprim = np.empty((2, 2), dtype=complex)
                elif dim == 3:
                    Zprim = np.empty((3, 3), dtype=complex)
                elif dim == 4:
                    Zprim = np.empty((4, 4), dtype=complex)
            elif wireinfo == 'ConcentricNeutralCableInfo':
                if dim == 1:
                    Zprim = np.empty((2, 2), dtype=complex)
                elif dim == 2:
                    Zprim = np.empty((4, 4), dtype=complex)
                elif dim == 3:
                    Zprim = np.empty((6, 6), dtype=complex)
            elif wireinfo == 'TapeShieldCableInfo':
                if dim == 2:
                    Zprim = np.empty((3, 3), dtype=complex)
                else:
                    tape_skip = True
                    continue
            # row 1
            Zprim[i1,
                  j1] = diagZprim(wireinfo, wire_cn_ts, False, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr,
                                  CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
            if wireinfo == 'ConcentricNeutralCableInfo' and dim == 1:
                CN_done = True
                # row 2
                Zprim[i2, j1] = Zprim[i1, j2] = offDiagZprim(2, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i2, j2] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc,
                                          CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness,
                                          TS_diameter_screen)
            elif wireinfo == 'TapeShieldCableInfo':
                # row 2
                Zprim[i2, j1] = Zprim[i1, j2] = offDiagZprim(2, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                # neutralFlag is passed as True as a flag indicating to use the 2nd row shield calculation
                Zprim[i2, j2] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc,
                                          CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness,
                                          TS_diameter_screen)
        elif phaseIdx == 1:
            pair_i1b1 = bus1 + ybusPhaseIdx[phase]
            pair_i1b2 = bus2 + ybusPhaseIdx[phase]
            # row 2
            if line_name != tape_line:
                Zprim[i2, j1] = Zprim[i1, j2] = offDiagZprim(2, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i2, j2] = diagZprim(wireinfo, wire_cn_ts, False, R25, GMR, CN_strand_count, CN_strand_rdc,
                                          CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness,
                                          TS_diameter_screen)
            if wireinfo == 'ConcentricNeutralCableInfo' and dim == 2:
                CN_done = True
                # row 3
                Zprim[i3, j1] = Zprim[i1, j3] = offDiagZprim(3, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i3, j2] = Zprim[i2, j3] = offDiagZprim(3, 2, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i3, j3] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc,
                                          CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness,
                                          TS_diameter_screen)
                # row 4
                Zprim[i4, j1] = Zprim[i1, j4] = offDiagZprim(4, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i4, j2] = Zprim[i2, j4] = offDiagZprim(4, 2, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i4, j3] = Zprim[i3, j4] = offDiagZprim(4, 3, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i4, j4] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc,
                                          CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness,
                                          TS_diameter_screen)
            elif line_name == tape_line:
                # row 3
                # coordinates for neutral are stored in index 2 for TapeShieldCableInfo
                Zprim[i3, j1] = Zprim[i1, j3] = Zprim[i3, j2] = Zprim[i2, j3] = offDiagZprim(
                    2, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25, GMR, CN_strand_count,
                    CN_strand_rdc, CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness,
                    TS_diameter_screen)
                Zprim[i3, j3] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc,
                                          CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness,
                                          TS_diameter_screen)
        elif phaseIdx == 2:
            pair_i2b1 = bus1 + ybusPhaseIdx[phase]
            pair_i2b2 = bus2 + ybusPhaseIdx[phase]
            # row 3
            Zprim[i3,
                  j1] = Zprim[i1,
                              j3] = offDiagZprim(3, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25,
                                                 GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius,
                                                 CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
            Zprim[i3,
                  j2] = Zprim[i2,
                              j3] = offDiagZprim(3, 2, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25,
                                                 GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius,
                                                 CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
            Zprim[i3,
                  j3] = diagZprim(wireinfo, wire_cn_ts, False, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr,
                                  CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
            if wireinfo == 'ConcentricNeutralCableInfo':
                CN_done = True
                # row 4
                Zprim[i4, j1] = Zprim[i1, j4] = offDiagZprim(4, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i4, j2] = Zprim[i2, j4] = offDiagZprim(4, 2, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i4, j3] = Zprim[i3, j4] = offDiagZprim(4, 3, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i4, j4] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc,
                                          CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness,
                                          TS_diameter_screen)
                # row 5
                Zprim[i5, j1] = Zprim[i1, j5] = offDiagZprim(5, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i5, j2] = Zprim[i2, j5] = offDiagZprim(5, 2, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i5, j3] = Zprim[i3, j5] = offDiagZprim(5, 3, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i5, j4] = Zprim[i4, j5] = offDiagZprim(5, 4, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i5, j5] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc,
                                          CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness,
                                          TS_diameter_screen)
                # row 6
                Zprim[i6, j1] = Zprim[i1, j6] = offDiagZprim(6, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i6, j2] = Zprim[i2, j6] = offDiagZprim(6, 2, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i6, j3] = Zprim[i3, j6] = offDiagZprim(6, 3, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i6, j4] = Zprim[i4, j6] = offDiagZprim(6, 4, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i6, j5] = Zprim[i5, j6] = offDiagZprim(6, 5, wireinfo, wire_spacing_info, wire_cn_ts, XCoord,
                                                             YCoord, R25, GMR, CN_strand_count, CN_strand_rdc,
                                                             CN_strand_gmr, CN_strand_radius, CN_diameter_jacket,
                                                             TS_tape_thickness, TS_diameter_screen)
                Zprim[i6, j6] = diagZprim(wireinfo, wire_cn_ts, True, R25, GMR, CN_strand_count, CN_strand_rdc,
                                          CN_strand_gmr, CN_strand_radius, CN_diameter_jacket, TS_tape_thickness,
                                          TS_diameter_screen)
        elif phaseIdx == 3:
            # this can only be phase 'N' so no need to store 'pair' values
            # row 4
            Zprim[i4,
                  j1] = Zprim[i1,
                              j4] = offDiagZprim(4, 1, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25,
                                                 GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius,
                                                 CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
            Zprim[i4,
                  j2] = Zprim[i2,
                              j4] = offDiagZprim(4, 2, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25,
                                                 GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius,
                                                 CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
            Zprim[i4,
                  j3] = Zprim[i3,
                              j4] = offDiagZprim(4, 3, wireinfo, wire_spacing_info, wire_cn_ts, XCoord, YCoord, R25,
                                                 GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr, CN_strand_radius,
                                                 CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
            Zprim[i4,
                  j4] = diagZprim(wireinfo, wire_cn_ts, phase, R25, GMR, CN_strand_count, CN_strand_rdc, CN_strand_gmr,
                                  CN_strand_radius, CN_diameter_jacket, TS_tape_thickness, TS_diameter_screen)
        # for OverheadWireInfo, take advantage that there is always a phase N
        # and it's always the last item processed for a line_name so a good way
        # to know when to trigger the Ybus comparison code
        # for ConcentricNeutralCableInfo, a flag is the easiest
        if (wireinfo == 'OverheadWireInfo' and phase == 'N') or (wireinfo == 'ConcentricNeutralCableInfo' and CN_done):
            if wireinfo == 'ConcentricNeutralCableInfo':
                # the Z-hat slicing below is based on having an 'N' phase so need to
                # account for that when it doesn't exist
                phaseIdx += 1
                CN_done = False
            # create the Z-hat matrices to then compute Zabc for Ybus comparisons
            Zij = Zprim[:phaseIdx, :phaseIdx]
            Zin = Zprim[:phaseIdx, phaseIdx:]
            Znj = Zprim[phaseIdx:, :phaseIdx]
            # Znn = Zprim[phaseIdx:,phaseIdx:]
            invZnn = np.linalg.inv(Zprim[phaseIdx:, phaseIdx:])
            # finally, compute Zabc from Z-hat matrices
            Zabc = np.subtract(Zij, np.matmul(np.matmul(Zin, invZnn), Znj))
            # multiply by scalar length
            lenZabc = Zabc * length
            # invert the matrix
            invZabc = np.linalg.inv(lenZabc)
            # test if the inverse * original = identity
            # identityTest = np.dot(lenZabc, invZabc)
            # logger.debug('identity test for ' + line_name + ': ' + str(identityTest))
            # negate the matrix and assign it to Ycomp
            Ycomp = invZabc * -1
            if Ycomp.size == 1:
                fillYbusNoSwapLines(pair_i0b1, pair_i0b2, Ycomp[0, 0], Ybus)
            elif Ycomp.size == 4:
                fillYbusNoSwapLines(pair_i0b1, pair_i0b2, Ycomp[0, 0], Ybus)
                fillYbusSwapLines(pair_i1b1, pair_i0b2, Ycomp[1, 0], Ybus)
                fillYbusNoSwapLines(pair_i1b1, pair_i1b2, Ycomp[1, 1], Ybus)
            elif Ycomp.size == 9:
                fillYbusNoSwapLines(pair_i0b1, pair_i0b2, Ycomp[0, 0], Ybus)
                fillYbusSwapLines(pair_i1b1, pair_i0b2, Ycomp[1, 0], Ybus)
                fillYbusNoSwapLines(pair_i1b1, pair_i1b2, Ycomp[1, 1], Ybus)
                fillYbusSwapLines(pair_i2b1, pair_i0b2, Ycomp[2, 0], Ybus)
                fillYbusSwapLines(pair_i2b1, pair_i1b2, Ycomp[2, 1], Ybus)
                fillYbusNoSwapLines(pair_i2b1, pair_i2b2, Ycomp[2, 2], Ybus)
            phaseIdx = 0
        else:
            phaseIdx += 1


def fillYbus6x6Xfmrs(bus1: str, bus2: str, DY_flag: bool, Ycomp: np.ndarray, Ybus: Dict):
    # fill Ybus directly from Ycomp
    # first fill the ones that are independent of DY_flag
    # either because the same bus is used or the same phase
    fillYbusAdd(bus1 + '.1', bus1 + '.1', Ycomp[0, 0], Ybus)
    fillYbusAdd(bus1 + '.2', bus1 + '.1', Ycomp[1, 0], Ybus)
    fillYbusAdd(bus1 + '.2', bus1 + '.2', Ycomp[1, 1], Ybus)
    fillYbusAdd(bus1 + '.3', bus1 + '.1', Ycomp[2, 0], Ybus)
    fillYbusAdd(bus1 + '.3', bus1 + '.2', Ycomp[2, 1], Ybus)
    fillYbusAdd(bus1 + '.3', bus1 + '.3', Ycomp[2, 2], Ybus)
    fillYbusUnique(bus2 + '.1', bus1 + '.1', Ycomp[3, 0], Ybus)
    fillYbusAdd(bus2 + '.1', bus2 + '.1', Ycomp[3, 3], Ybus)
    fillYbusUnique(bus2 + '.2', bus1 + '.2', Ycomp[4, 1], Ybus)
    fillYbusAdd(bus2 + '.2', bus2 + '.1', Ycomp[4, 3], Ybus)
    fillYbusAdd(bus2 + '.2', bus2 + '.2', Ycomp[4, 4], Ybus)
    fillYbusUnique(bus2 + '.3', bus1 + '.3', Ycomp[5, 2], Ybus)
    fillYbusAdd(bus2 + '.3', bus2 + '.1', Ycomp[5, 3], Ybus)
    fillYbusAdd(bus2 + '.3', bus2 + '.2', Ycomp[5, 4], Ybus)
    fillYbusAdd(bus2 + '.3', bus2 + '.3', Ycomp[5, 5], Ybus)
    # now fill the ones that are dependent on DY_flag, which
    # are different bus and different phase
    if DY_flag:
        fillYbusUnique(bus2 + '.1', bus1 + '.2', Ycomp[4, 0], Ybus)
        fillYbusUnique(bus2 + '.1', bus1 + '.3', Ycomp[5, 0], Ybus)
        fillYbusUnique(bus2 + '.2', bus1 + '.1', Ycomp[3, 1], Ybus)
        fillYbusUnique(bus2 + '.2', bus1 + '.3', Ycomp[5, 1], Ybus)
        fillYbusUnique(bus2 + '.3', bus1 + '.1', Ycomp[3, 2], Ybus)
        fillYbusUnique(bus2 + '.3', bus1 + '.2', Ycomp[4, 2], Ybus)
    else:
        fillYbusUnique(bus2 + '.2', bus1 + '.1', Ycomp[4, 0], Ybus)
        fillYbusUnique(bus2 + '.3', bus1 + '.1', Ycomp[5, 0], Ybus)
        fillYbusUnique(bus2 + '.1', bus1 + '.2', Ycomp[3, 1], Ybus)
        fillYbusUnique(bus2 + '.3', bus1 + '.2', Ycomp[5, 1], Ybus)
        fillYbusUnique(bus2 + '.1', bus1 + '.3', Ycomp[3, 2], Ybus)
        fillYbusUnique(bus2 + '.2', bus1 + '.3', Ycomp[4, 2], Ybus)


def fillYbusPowerTransformerEndXfmrs(distributedArea: DistributedArea, Ybus: Dict):
    bindings = powerTransformerEndXfmrImpedances(distributedArea)
    if len(bindings) == 0:
        return
    Mesh_x_ohm = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        Mesh_x_ohm[xfmr_name] = float(obj['mesh_x_ohm']['value'])
    bindings = powerTransformerEndXfmrNames(distributedArea)
    if len(bindings) == 0:
        return
    Bus = {}
    Connection = {}
    RatedS = {}
    RatedU = {}
    R_ohm = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        end_number = int(obj['end_number']['value'])
        # can't handle 3-winding transformers so issue a warning and skip
        # to the next transformer in that case
        if end_number == 3:
            bus1 = Bus[xfmr_name][1]
            bus2 = Bus[xfmr_name][2]
            bus3 = obj['bus']['value'].upper()
            logger.warn(
                f'3-winding, 3-phase PowerTransformerEnd transformers are not supported, xfmr: {xfmr_name}, bus1: \
                {bus1}, bus2: {bus2}, bus3: {bus3}')
            # need to clear out the previous dictionary entries for this
            # 3-winding transformer so it isn't processed below
            Bus.pop(xfmr_name, None)
            Connection.pop(xfmr_name, None)
            RatedS.pop(xfmr_name, None)
            RatedU.pop(xfmr_name, None)
            R_ohm.pop(xfmr_name, None)
            continue
        if xfmr_name not in Bus:
            Bus[xfmr_name] = {}
            Connection[xfmr_name] = {}
            RatedS[xfmr_name] = {}
            RatedU[xfmr_name] = {}
            R_ohm[xfmr_name] = {}
        Bus[xfmr_name][end_number] = obj['bus']['value'].upper()
        Connection[xfmr_name][end_number] = obj['connection']['value']
        RatedS[xfmr_name][end_number] = int(float(obj['ratedS']['value']))
        RatedU[xfmr_name][end_number] = int(obj['ratedU']['value'])
        R_ohm[xfmr_name][end_number] = float(obj['r_ohm']['value'])
    # initialize B upfront because it's constant
    B = np.zeros((6, 3))
    B[0, 0] = B[2, 1] = B[4, 2] = 1.0
    B[1, 0] = B[3, 1] = B[5, 2] = -1.0
    # initialize Y and D matrices, also constant, used to set A later
    Y1 = np.zeros((4, 12))
    Y1[0, 0] = Y1[1, 4] = Y1[2, 8] = Y1[3, 1] = Y1[3, 5] = Y1[3, 9] = 1.0
    Y2 = np.zeros((4, 12))
    Y2[0, 2] = Y2[1, 6] = Y2[2, 10] = Y2[3, 3] = Y2[3, 7] = Y2[3, 11] = 1.0
    D1 = np.zeros((4, 12))
    D1[0, 0] = D1[0, 9] = D1[1, 1] = D1[1, 4] = D1[2, 5] = D1[2, 8] = 1.0
    D2 = np.zeros((4, 12))
    D2[0, 2] = D2[0, 11] = D2[1, 3] = D2[1, 6] = D2[2, 7] = D2[2, 10] = 1.0
    for xfmr_name in Bus:
        # Note that division is always floating point in Python 3 even if
        # operands are integer
        zBaseP = (RatedU[xfmr_name][1] * RatedU[xfmr_name][1]) / RatedS[xfmr_name][1]
        r_ohm_pu = R_ohm[xfmr_name][1] / zBaseP
        mesh_x_ohm_pu = Mesh_x_ohm[xfmr_name] / zBaseP
        zsc_1V = complex(2.0 * r_ohm_pu, mesh_x_ohm_pu) * (3.0 / RatedS[xfmr_name][1])
        # initialize ZB
        ZB = np.zeros((3, 3), dtype=complex)
        ZB[0, 0] = ZB[1, 1] = ZB[2, 2] = zsc_1V
        # set both Vp/Vs for N and top/bottom for A
        if Connection[xfmr_name][1] == 'Y':
            Vp = RatedU[xfmr_name][1] / math.sqrt(3.0)
            top = Y1
        else:
            Vp = RatedU[xfmr_name][1]
            top = D1
        if Connection[xfmr_name][2] == 'Y':
            Vs = RatedU[xfmr_name][2] / math.sqrt(3.0)
            bottom = Y2
        else:
            Vs = RatedU[xfmr_name][2]
            bottom = D2
        # initialize N
        N = np.zeros((12, 6))
        N[0, 0] = N[4, 2] = N[8, 4] = 1.0 / Vp
        N[1, 0] = N[5, 2] = N[9, 4] = -1.0 / Vp
        N[2, 1] = N[6, 3] = N[10, 5] = 1.0 / Vs
        N[3, 1] = N[7, 3] = N[11, 5] = -1.0 / Vs
        # initialize A
        A = np.vstack((top, bottom))
        # compute Ycomp = A x N x B x inv(ZB) x B' x N' x A'
        # there are lots of ways to break this up including not at all, but
        # here's one way that keeps it from looking overly complex
        ANB = np.matmul(np.matmul(A, N), B)
        ANB_invZB = np.matmul(ANB, np.linalg.inv(ZB))
        ANB_invZB_Bp = np.matmul(ANB_invZB, np.transpose(B))
        ANB_invZB_BpNp = np.matmul(ANB_invZB_Bp, np.transpose(N))
        Ycomp = np.matmul(ANB_invZB_BpNp, np.transpose(A))
        bus1 = Bus[xfmr_name][1]
        bus2 = Bus[xfmr_name][2]
        # set special case flag that indicates if we need to swap the phases
        # for each bus to do the Ybus matching
        connect_DY_flag = Connection[xfmr_name][1] == 'D' and Connection[xfmr_name][2] == 'Y'
        # delete row and column 8 and 4 making a 6x6 matrix
        Ycomp = np.delete(Ycomp, 7, 0)
        Ycomp = np.delete(Ycomp, 7, 1)
        Ycomp = np.delete(Ycomp, 3, 0)
        Ycomp = np.delete(Ycomp, 3, 1)
        fillYbus6x6Xfmrs(bus1, bus2, connect_DY_flag, Ycomp, Ybus)


def fillYbusTransformerTankXfmrs(distributedArea: DistributedArea, Ybus: Dict):
    bindings = transformerTankXfmrRated(distributedArea)
    if len(bindings) == 0:
        return
    RatedS = {}
    RatedU = {}
    Connection = {}
    R_ohm = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        enum = int(obj['enum']['value'])
        if xfmr_name not in RatedS:
            RatedS[xfmr_name] = {}
            RatedU[xfmr_name] = {}
            Connection[xfmr_name] = {}
            R_ohm[xfmr_name] = {}
        RatedS[xfmr_name][enum] = int(float(obj['ratedS']['value']))
        RatedU[xfmr_name][enum] = int(obj['ratedU']['value'])
        Connection[xfmr_name][enum] = obj['connection']['value']
        R_ohm[xfmr_name][enum] = float(obj['r_ohm']['value'])
    bindings = transformerTankXfmrSct(distributedArea)
    Leakage_z = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        enum = int(obj['enum']['value'])
        if xfmr_name not in Leakage_z:
            Leakage_z[xfmr_name] = {}
        Leakage_z[xfmr_name][enum] = float(obj['leakage_z']['value'])
    bindings = transformerTankXfmrNames(distributedArea)
    if len(bindings) == 0:
        return
    Bus = {}
    Phase = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        enum = int(obj['enum']['value'])
        if xfmr_name not in Bus:
            Bus[xfmr_name] = {}
            Phase[xfmr_name] = {}
        Bus[xfmr_name][enum] = obj['bus']['value'].upper()
        Phase[xfmr_name][enum] = obj['phase']['value']
    # initialize different variations of B upfront and then figure out later
    # which to use for each transformer
    # 3-phase
    B = {}
    B['3p'] = np.zeros((6, 3))
    B['3p'][0, 0] = B['3p'][2, 1] = B['3p'][4, 2] = 1.0
    B['3p'][1, 0] = B['3p'][3, 1] = B['3p'][5, 2] = -1.0
    # logger.debug(B['3p'])
    # 1-phase, 2-windings
    B['2w'] = np.zeros((2, 1))
    B['2w'][0, 0] = 1.0
    B['2w'][1, 0] = -1.0
    # 1-phase, 3-windings
    B['3w'] = np.zeros((3, 2))
    B['3w'][0, 0] = B['3w'][0, 1] = B['3w'][2, 1] = 1.0
    B['3w'][1, 0] = -1.0
    # initialize Y and D matrices, also constant, used to set A for
    # 3-phase transformers
    Y1_3p = np.zeros((4, 12))
    Y1_3p[0, 0] = Y1_3p[1, 4] = Y1_3p[2, 8] = Y1_3p[3, 1] = Y1_3p[3, 5] = Y1_3p[3, 9] = 1.0
    Y2_3p = np.zeros((4, 12))
    Y2_3p[0, 2] = Y2_3p[1, 6] = Y2_3p[2, 10] = Y2_3p[3, 3] = Y2_3p[3, 7] = Y2_3p[3, 11] = 1.0
    D1_3p = np.zeros((4, 12))
    D1_3p[0, 0] = D1_3p[0, 9] = D1_3p[1, 1] = D1_3p[1, 4] = D1_3p[2, 5] = D1_3p[2, 8] = 1.0
    D2_3p = np.zeros((4, 12))
    D2_3p[0, 2] = D2_3p[0, 11] = D2_3p[1, 3] = D2_3p[1, 6] = D2_3p[2, 7] = D2_3p[2, 10] = 1.0
    # initialize A for each transformer variation
    A = {}
    A['2w'] = np.identity(4)
    A['3w'] = np.identity(6)
    A['3p_YY'] = np.vstack((Y1_3p, Y2_3p))
    A['3p_DD'] = np.vstack((D1_3p, D2_3p))
    A['3p_YD'] = np.vstack((Y1_3p, D2_3p))
    A['3p_DY'] = np.vstack((D1_3p, Y2_3p))
    # map transformer query phase values to nodelist indexes
    ybusPhaseIdx = {'A': '.1', 'B': '.2', 'C': '.3', 's1': '.1', 's2': '.2'}
    for xfmr_name in Bus:
        # determine the type of transformer to drive the computation
        if Phase[xfmr_name][1] == 'ABC':
            # 3-phase
            Bkey = '3p'
            Akey = Bkey + '_' + Connection[xfmr_name][1] + Connection[xfmr_name][2]
        elif 3 in Phase[xfmr_name]:
            # 1-phase, 3-winding
            Bkey = '3w'
            Akey = Bkey
        else:
            # 1-phase, 2-winding
            Bkey = '2w'
            Akey = Bkey
        # note that division is always floating point in Python 3 even if
        # operands are integer
        zBaseP = (RatedU[xfmr_name][1] * RatedU[xfmr_name][1]) / RatedS[xfmr_name][1]
        zBaseS = (RatedU[xfmr_name][2] * RatedU[xfmr_name][2]) / RatedS[xfmr_name][2]
        r_ohm_pu = R_ohm[xfmr_name][1] / zBaseP
        mesh_x_ohm_pu = Leakage_z[xfmr_name][1] / zBaseP
        if Bkey == '3p':
            zsc_1V = complex(2.0 * r_ohm_pu, mesh_x_ohm_pu) * (3.0 / RatedS[xfmr_name][1])
            # initialize ZB
            ZB = np.zeros((3, 3), dtype=complex)
            ZB[0, 0] = ZB[1, 1] = ZB[2, 2] = zsc_1V
            # initialize N
            if Connection[xfmr_name][1] == 'Y':
                Vp = RatedU[xfmr_name][1] / math.sqrt(3.0)
            else:
                Vp = RatedU[xfmr_name][1]
            if Connection[xfmr_name][2] == 'Y':
                Vs = RatedU[xfmr_name][2] / math.sqrt(3.0)
            else:
                Vs = RatedU[xfmr_name][2]
            N = np.zeros((12, 6))
            N[0, 0] = N[4, 2] = N[8, 4] = 1.0 / Vp
            N[1, 0] = N[5, 2] = N[9, 4] = -1.0 / Vp
            N[2, 1] = N[6, 3] = N[10, 5] = 1.0 / Vs
            N[3, 1] = N[7, 3] = N[11, 5] = -1.0 / Vs
        elif Bkey == '3w':
            zsc_1V = complex(3.0 * r_ohm_pu, mesh_x_ohm_pu) * (1.0 / RatedS[xfmr_name][1])
            zod_1V = complex(2.0 * R_ohm[xfmr_name][2], Leakage_z[xfmr_name][2]) / zBaseS * (1.0 / RatedS[xfmr_name][2])
            # initialize ZB
            ZB = np.zeros((2, 2), dtype=complex)
            ZB[0, 0] = ZB[1, 1] = zsc_1V
            ZB[1, 0] = ZB[0, 1] = 0.5 * (zsc_1V + zsc_1V - zod_1V)
            # initialize N
            Vp = RatedU[xfmr_name][1]
            Vs1 = RatedU[xfmr_name][2]
            Vs2 = RatedU[xfmr_name][3]
            N = np.zeros((6, 3))
            N[0, 0] = 1.0 / Vp
            N[1, 0] = -1.0 / Vp
            N[2, 1] = 1.0 / Vs1
            N[3, 1] = -1.0 / Vs1
            N[4, 2] = -1.0 / Vs2
            N[5, 2] = 1.0 / Vs2
        else:
            zsc_1V = complex(2.0 * r_ohm_pu, mesh_x_ohm_pu) * (1.0 / RatedS[xfmr_name][1])
            # initialize ZB
            ZB = np.zeros((1, 1), dtype=complex)
            ZB[0, 0] = zsc_1V
            # initialize N
            Vp = RatedU[xfmr_name][1]
            Vs = RatedU[xfmr_name][2]
            N = np.zeros((4, 2))
            N[0, 0] = 1.0 / Vp
            N[1, 0] = -1.0 / Vp
            N[2, 1] = 1.0 / Vs
            N[3, 1] = -1.0 / Vs
        # compute Ycomp = A x N x B x inv(ZB) x B' x N' x A'
        # there are lots of ways to break this up including not at all, but
        # here's one way that keeps it from looking overly complex
        ANB = np.matmul(np.matmul(A[Akey], N), B[Bkey])
        ANB_invZB = np.matmul(ANB, np.linalg.inv(ZB))
        ANB_invZB_Bp = np.matmul(ANB_invZB, np.transpose(B[Bkey]))
        ANB_invZB_BpNp = np.matmul(ANB_invZB_Bp, np.transpose(N))
        Ycomp = np.matmul(ANB_invZB_BpNp, np.transpose(A[Akey]))
        # do Ybus comparisons and determine overall transformer status color
        xfmrColorIdx = 0
        if Bkey == '3p':
            bus1 = Bus[xfmr_name][1]
            bus2 = Bus[xfmr_name][2]
            # delete row and column 8 and 4 making a 6x6 matrix
            Ycomp = np.delete(Ycomp, 7, 0)
            Ycomp = np.delete(Ycomp, 7, 1)
            Ycomp = np.delete(Ycomp, 3, 0)
            Ycomp = np.delete(Ycomp, 3, 1)
            fillYbus6x6Xfmrs(bus1, bus2, Akey == '3p_DY', Ycomp, Ybus)
        elif Bkey == '3w':
            bus1 = Bus[xfmr_name][1] + ybusPhaseIdx[Phase[xfmr_name][1]]
            bus2 = Bus[xfmr_name][2] + ybusPhaseIdx[Phase[xfmr_name][2]]
            bus3 = Bus[xfmr_name][3] + ybusPhaseIdx[Phase[xfmr_name][3]]
            # split phase transformers are a bit tricky, but Shiva
            # figured out how it needs to be done with reducing the
            # matrix and how the 3 buses come into it
            # delete row and column 5, 4, and 2 making a 3x3 matrix
            Ycomp = np.delete(Ycomp, 4, 0)
            Ycomp = np.delete(Ycomp, 4, 1)
            Ycomp = np.delete(Ycomp, 3, 0)
            Ycomp = np.delete(Ycomp, 3, 1)
            Ycomp = np.delete(Ycomp, 1, 0)
            Ycomp = np.delete(Ycomp, 1, 1)
            fillYbusAdd(bus1, bus1, Ycomp[0, 0], Ybus)
            fillYbusUnique(bus2, bus1, Ycomp[1, 0], Ybus)
            fillYbusAdd(bus2, bus2, Ycomp[1, 1], Ybus)
            fillYbusUnique(bus3, bus1, Ycomp[2, 0], Ybus)
            fillYbusAdd(bus3, bus2, Ycomp[2, 1], Ybus)
            fillYbusAdd(bus3, bus3, Ycomp[2, 2], Ybus)
        else:
            bus1 = Bus[xfmr_name][1] + ybusPhaseIdx[Phase[xfmr_name][1]]
            bus2 = Bus[xfmr_name][2] + ybusPhaseIdx[Phase[xfmr_name][2]]
            Yval = Ycomp[2, 0]
            # delete row and column 4 and 2 making a 2x2 matrix
            Ycomp = np.delete(Ycomp, 3, 0)
            Ycomp = np.delete(Ycomp, 3, 1)
            Ycomp = np.delete(Ycomp, 1, 0)
            Ycomp = np.delete(Ycomp, 1, 1)
            fillYbusAdd(bus1, bus1, Ycomp[0, 0], Ybus)
            fillYbusUnique(bus2, bus1, Ycomp[1, 0], Ybus)
            fillYbusAdd(bus2, bus2, Ycomp[1, 1], Ybus)


def fillYbusUniqueSwitches(bus1: str, bus2: str, Ybus: Dict):
    if bus1 not in Ybus:
        Ybus[bus1] = {}
    if bus2 not in Ybus:
        Ybus[bus2] = {}
    if bus2 in Ybus[bus1]:
        logger.warn(f'Unexpected existing value found for Ybus[{bus1}][{bus2}] when filling switching equipment value')
    Ybus[bus1][bus2] = Ybus[bus2][bus1] = complex(-500.0, 500.0)


def fillYbusAddSwitches(bus1: str, bus2: str, Ybus: Dict):
    if bus1 not in Ybus:
        Ybus[bus1] = {}
    if bus2 not in Ybus:
        Ybus[bus2] = {}
    if bus2 in Ybus[bus1]:
        Ybus[bus1][bus2] += complex(500.0, -500.0)
        Ybus[bus2][bus1] = Ybus[bus1][bus2]
    else:
        Ybus[bus1][bus2] = Ybus[bus2][bus1] = complex(500.0, -500.0)


def fillYbusNoSwapSwitches(bus1: str, bus2: str, is_Open: bool, Ybus: Dict):
    if not is_Open:
        fillYbusUniqueSwitches(bus2, bus1, Ybus)
        fillYbusAddSwitches(bus1, bus1, Ybus)
        fillYbusAddSwitches(bus2, bus2, Ybus)


def fillYbusSwitchingEquipmentSwitches(distributedArea: DistributedArea, Ybus: Dict):
    bindings = switchingEquipmentSwitchNames(distributedArea)
    if len(bindings) == 0:
        return
    # map transformer query phase values to nodelist indexes
    ybusPhaseIdx = {'A': '.1', 'B': '.2', 'C': '.3'}
    for obj in bindings:
        sw_name = obj['sw_name']['value']
        is_Open = obj['is_Open']['value'].upper() == 'TRUE'
        bus1 = obj['bus1']['value'].upper()
        bus2 = obj['bus2']['value'].upper()
        phases_side1 = obj['phases_side1']['value']
        if phases_side1 == '':
            # 3-phase switch
            fillYbusNoSwapSwitches(bus1 + '.1', bus2 + '.1', is_Open, Ybus)
            fillYbusNoSwapSwitches(bus1 + '.2', bus2 + '.2', is_Open, Ybus)
            fillYbusNoSwapSwitches(bus1 + '.3', bus2 + '.3', is_Open, Ybus)
        else:
            # 1- or 2-phase switch
            switchColorIdx = 0
            for phase in phases_side1:
                if phase in ybusPhaseIdx:
                    fillYbusNoSwapSwitches(bus1 + ybusPhaseIdx[phase], bus2 + ybusPhaseIdx[phase], is_Open, Ybus)


def fillYbusOnlyAddShunts(bus: str, Yval: complex, Ybus: Dict):
    if Yval == 0j:
        return
    if bus not in Ybus or bus not in Ybus[bus]:
        logger.warn(f'Existing value not found for Ybus[{bus}][{bus}] when adding shunt element model contribution')
        return
    Ybus[bus][bus] += Yval


def fillYbusShuntElementShunts(distributedArea: DistributedArea, Ybus: Dict):
    # map query phase values to nodelist indexes
    ybusPhaseIdx = {'A': '.1', 'B': '.2', 'C': '.3', 's1': '.1', 's2': '.2'}
    # CAPACITORS DATA STRUCTURES INITIALIZATION
    bindings = shuntElementCapNames(distributedArea)
    Cap_name = {}
    B_per_section = {}
    for obj in bindings:
        cap_name = obj['cap_name']['value']
        B_per_section[cap_name] = float(obj['b_per_section']['value'])
        bus = obj['bus']['value'].upper()
        phase = 'ABC'    # no phase specified indicates 3-phase
        if 'phase' in obj:
            phase = obj['phase']['value']
        if phase == 'ABC':    # 3-phase
            if bus + '.1' not in Cap_name:
                Cap_name[bus + '.1'] = []
                Cap_name[bus + '.2'] = []
                Cap_name[bus + '.3'] = []
            Cap_name[bus + '.1'].append(cap_name)
            Cap_name[bus + '.2'].append(cap_name)
            Cap_name[bus + '.3'].append(cap_name)
        else:    # specified phase only
            if bus + ybusPhaseIdx[phase] not in Cap_name:
                Cap_name[bus + ybusPhaseIdx[phase]] = []
            Cap_name[bus + ybusPhaseIdx[phase]].append(cap_name)
    # TRANSFORMERS DATA STRUCTURES INITIALIZATION
    bindings = transformerTankXfmrRated(distributedArea)
    # TransformerTank queries
    RatedS_tank = {}
    RatedU_tank = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        enum = int(obj['enum']['value'])
        if xfmr_name not in RatedS_tank:
            RatedS_tank[xfmr_name] = {}
            RatedU_tank[xfmr_name] = {}
        RatedS_tank[xfmr_name][enum] = int(float(obj['ratedS']['value']))
        RatedU_tank[xfmr_name][enum] = int(obj['ratedU']['value'])
    bindings = transformerTankXfmrNlt(distributedArea)
    Noloadloss = {}
    I_exciting = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        Noloadloss[xfmr_name] = float(obj['noloadloss_kW']['value'])
        I_exciting[xfmr_name] = float(obj['i_exciting']['value'])
    bindings = transformerTankXfmrNames(distributedArea)
    Xfmr_tank_name = {}
    Enum_tank = {}
    BaseV_tank = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        enum = int(obj['enum']['value'])
        bus = obj['bus']['value'].upper()
        baseV = float(obj['baseV']['value'])
        if xfmr_name not in Enum_tank:
            Enum_tank[xfmr_name] = {}
            BaseV_tank[xfmr_name] = {}
        Enum_tank[xfmr_name][bus] = enum
        BaseV_tank[xfmr_name][bus] = baseV
        phase = obj['phase']['value']
        if phase == 'ABC':
            if bus + '.1' not in Xfmr_tank_name:
                Xfmr_tank_name[bus + '.1'] = []
                Xfmr_tank_name[bus + '.2'] = []
                Xfmr_tank_name[bus + '.3'] = []
            Xfmr_tank_name[bus + '.1'].append(xfmr_name)
            Xfmr_tank_name[bus + '.2'].append(xfmr_name)
            Xfmr_tank_name[bus + '.3'].append(xfmr_name)
        else:
            if bus + ybusPhaseIdx[phase] not in Xfmr_tank_name:
                Xfmr_tank_name[bus + ybusPhaseIdx[phase]] = []
            Xfmr_tank_name[bus + ybusPhaseIdx[phase]].append(xfmr_name)
    # TransformerEnd queries
    bindings = powerTransformerEndXfmrAdmittances(distributedArea)
    B_S = {}
    G_S = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        B_S[xfmr_name] = float(obj['b_S']['value'])
        G_S[xfmr_name] = float(obj['g_S']['value'])
    bindings = powerTransformerEndXfmrNames(distributedArea)
    Xfmr_end_name = {}
    RatedU_end = {}
    Enum_end = {}
    for obj in bindings:
        xfmr_name = obj['xfmr_name']['value']
        enum = int(obj['end_number']['value'])
        bus = obj['bus']['value'].upper()
        if bus + '.1' not in Xfmr_end_name:
            Xfmr_end_name[bus + '.1'] = []
            Xfmr_end_name[bus + '.2'] = []
            Xfmr_end_name[bus + '.3'] = []
        Xfmr_end_name[bus + '.1'].append(xfmr_name)
        Xfmr_end_name[bus + '.2'].append(xfmr_name)
        Xfmr_end_name[bus + '.3'].append(xfmr_name)
        if xfmr_name not in Enum_end:
            Enum_end[xfmr_name] = {}
            RatedU_end[xfmr_name] = {}
        Enum_end[xfmr_name][bus] = enum
        RatedU_end[xfmr_name][enum] = int(obj['ratedU']['value'])
    for node in Cap_name:
        sum_shunt_imag = 0.0
        for cap in Cap_name[node]:
            # no real component contribution for capacitors
            sum_shunt_imag += B_per_section[cap]
        fillYbusOnlyAddShunts(node, complex(0.0, sum_shunt_imag), Ybus)
    for node in Xfmr_tank_name:
        sum_shunt_imag = sum_shunt_real = 0.0
        bus = node.split('.')[0]
        for xfmr in Xfmr_tank_name[node]:
            if Enum_tank[xfmr][bus] >= 2:
                ratedU_sq = RatedU_tank[xfmr][2] * RatedU_tank[xfmr][2]
                zBaseS = ratedU_sq / RatedS_tank[xfmr][2]
                sum_shunt_real += (Noloadloss[xfmr] * 1000.0) / ratedU_sq
                G_c = (Noloadloss[xfmr] * 1000.0) / ratedU_sq
                Ym = I_exciting[xfmr] / (100.0) * RatedS_tank[xfmr][2] / RatedU_tank[xfmr][2] * 1 / (
                    RatedU_tank[xfmr][2])
                try:
                    B_m = math.sqrt(Ym**2 - G_c**2)
                except:
                    B_m = Ym
                sum_shunt_imag += -B_m
        fillYbusOnlyAddShunts(node, complex(sum_shunt_real, sum_shunt_imag), Ybus)
    for node in Xfmr_end_name:
        sum_shunt_imag = sum_shunt_real = 0.0
        bus = node.split('.')[0]
        for xfmr in Xfmr_end_name[node]:
            if Enum_end[xfmr][bus] == 2:
                ratedU_ratio = RatedU_end[xfmr][1] / RatedU_end[xfmr][2]
                ratedU_sq = ratedU_ratio * ratedU_ratio
                sum_shunt_real += G_S[xfmr] * ratedU_sq
                sum_shunt_imag += -B_S[xfmr] * ratedU_sq
        fillYbusOnlyAddShunts(node, complex(sum_shunt_real, sum_shunt_imag), Ybus)


def countUniqueYbus(Ybus):
    count = 0
    for bus1 in Ybus:
        for bus2 in Ybus[bus1]:
            if bus2 != bus1:
                count += 1
    count = int(count / 2)    # halve the total for duplicated entries
    for bus1 in Ybus:
        if bus1 in Ybus[bus1]:
            count += 1
    return count


def makeYbusSerializable(Ybus: Dict):
    for bus1 in Ybus.keys():
        for bus2 in Ybus[bus1].keys():
            yVal = complex(Ybus[bus1][bus2])
            Ybus[bus1][bus2] = (yVal.real, yVal.imag)


def calculateYbus(distributedArea: DistributedArea) -> Dict:
    Ybus = {}
    areaID = distributedArea.container.mRID
    fillYbusPerLengthPhaseImpedanceLines(distributedArea, Ybus)
    logger.debug(f"Ybus for {areaID} after fillYbusPerLengthPhaseImpedanceLines is:\n\
        {json.dumps(Ybus, indent=4, sort_keys=True, cls=ComplexEncoder)}")
    fillYbusPerLengthSequenceImpedanceLines(distributedArea, Ybus)
    logger.debug(f"Ybus for {areaID} after fillYbusPerLengthSequenceImpedanceLines is:\n\
        {json.dumps(Ybus, indent=4, sort_keys=True, cls=ComplexEncoder)}")
    fillYbusACLineSegmentLines(distributedArea, Ybus)
    logger.debug(f"Ybus for {areaID} after fillYbusACLineSegmentLines is:\n\
        {json.dumps(Ybus, indent=4, sort_keys=True, cls=ComplexEncoder)}")
    fillYbusWireInfoAndWireSpacingInfoLines(distributedArea, Ybus)
    logger.debug(f"Ybus for {areaID} after fillYbusWireInfoAndWireSpacingInfoLines is:\n\
        {json.dumps(Ybus, indent=4, sort_keys=True, cls=ComplexEncoder)}")
    line_count = countUniqueYbus(Ybus)
    logger.debug(f'Line_model # entries: {line_count}')
    fillYbusPowerTransformerEndXfmrs(distributedArea, Ybus)
    logger.debug(f"Ybus for {areaID} after fillYbusPowerTransformerEndXfmrs is:\n\
        {json.dumps(Ybus, indent=4, sort_keys=True, cls=ComplexEncoder)}")
    fillYbusTransformerTankXfmrs(distributedArea, Ybus)
    logger.debug(f"Ybus for {areaID} after fillYbusTransformerTankXfmrs is:\n\
        {json.dumps(Ybus, indent=4, sort_keys=True, cls=ComplexEncoder)}")
    count = countUniqueYbus(Ybus)
    xfmr_count = count - line_count
    logger.debug(f'Power_transformer # entries: {xfmr_count}')
    fillYbusSwitchingEquipmentSwitches(distributedArea, Ybus)
    logger.debug(f"Ybus for {areaID} after fillYbusSwitchingEquipmentSwitches is:\n\
        {json.dumps(Ybus, indent=4, sort_keys=True, cls=ComplexEncoder)}")
    count = countUniqueYbus(Ybus)
    switch_count = count - line_count - xfmr_count
    logger.debug(f'Switching_equipment # entries: {switch_count}')
    fillYbusShuntElementShunts(distributedArea, Ybus)
    logger.debug(f"Ybus for {areaID} after fillYbusShuntElementShunts is:\n\
        {json.dumps(Ybus, indent=4, sort_keys=True, cls=ComplexEncoder)}")
    logger.debug('Shunt_element contributions added (no new entries)')
    makeYbusSerializable(Ybus)
    return Ybus
