from copy import deepcopy
from pathlib import Path

import yaml

fieldBusConfigBoilerPlate = {
    "connections": {
        "id": "otbus",
        "is_ot_bus": True,
        "connection_type": "CONNECTION_TYPE_GRIDAPPSD",
        "connection_args": {
            "GRIDAPPSD_ADDRESS": "tcp://gridappsd:61613",
            "GRIDAPPSD_USER": "system",
            "GRIDAPPSD_PASSWORD": "manager"
        }
    }
}
modelInfo = {
    "ieee13nodecktassets": {
        "id": "_5B816B93-7A5F-B64C-8460-47C17D6E4B0F",
        "switchAreas": 1
    },
    "ieee13nodeckt": {
        "id": "_49AD8E07-3BF9-A4E2-CB8F-C3722F837B62",
        "switchAreas": 5,
        "secondaryAreas": {
            "1": 1,
            "4": 1,
        }
    },
    "ieee123": {
        "id": "_C1C3E687-6FFD-C753-582B-632A27E28507",
        "switchAreas": 6,
        "secondaryAreas": {
            "0": 1
        }
    },
    "ieee123pv": {
        "id": "_E407CBB6-8C8D-9BC9-589C-AB83FBF0826D",
        "switchAreas": 8,
        "secondaryAreas": {
            "2": 25,
            "3": 10,
            "4": 23,
            "5": 1,
            "6": 11,
            "7": 11
        }
    }
}
rootDir = (Path(__file__).parent).parent.resolve()
for mod in modelInfo.keys():
    systemConfigDir = rootDir / "service_config_files" / mod / "system_level"
    if not systemConfigDir.is_dir():
        systemConfigDir.mkdir(parents=True)
    systemConfigFile = systemConfigDir / "system-message-bus.yml"
    with systemConfigFile.open(mode="w", encoding="utf-8") as scf:
        yaml.safe_dump(fieldBusConfigBoilerPlate, scf)
    feederConfigDir = rootDir / "service_config_files" / mod / "feeder_level"
    if not feederConfigDir.is_dir():
        feederConfigDir.mkdir(parents=True)
    feederConfigFile = feederConfigDir / "feeder-message-bus.yml"
    with feederConfigFile.open(mode="w", encoding="utf-8") as fcf:
        feederConfig = deepcopy(fieldBusConfigBoilerPlate)
        feederConfig["connections"]["id"] = modelInfo[mod]["id"]
        yaml.safe_dump(feederConfig, fcf)
    switchAreas = modelInfo[mod].get("switchAreas")
    if switchAreas is not None:
        switchConfigDir = rootDir / "service_config_files" / mod / "switch_level"
        if not switchConfigDir.is_dir():
            switchConfigDir.mkdir(parents=True)
        for i in range(switchAreas):
            switchConfigFile = switchConfigDir / f"switch_area_message_bus_{i}.yml"
            with switchConfigFile.open(mode="w", encoding="utf-8") as scf:
                switchConfig = deepcopy(fieldBusConfigBoilerPlate)
                switchConfig["connections"]["id"] = f"{modelInfo[mod]['id']}.{i}"
                yaml.safe_dump(switchConfig, scf)
    secondaryAreas = modelInfo[mod].get("secondaryAreas")
    if secondaryAreas is not None:
        secondaryConfigDir = rootDir / "service_config_files" / mod / "secondary_level"
        if not secondaryConfigDir.is_dir():
            secondaryConfigDir.mkdir(parents=True)
        for sw in secondaryAreas.keys():
            for i in range(secondaryAreas[sw]):
                secondaryConfigFile = secondaryConfigDir / f"secondary_area_message_bus_{sw}_{i}.yml"
                with secondaryConfigFile.open(mode="w", encoding="utf-8") as scf:
                    secondaryConfig = deepcopy(fieldBusConfigBoilerPlate)
                    secondaryConfig["connections"]["id"] = f"{modelInfo[mod]['id']}.{sw}.{i}"
                    yaml.safe_dump(secondaryConfig, scf)
