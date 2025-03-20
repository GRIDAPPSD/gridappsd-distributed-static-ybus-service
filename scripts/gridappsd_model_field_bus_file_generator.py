from copy import deepcopy
from pathlib import Path

import yaml

fieldBusConfigBoilerPlate = {
    "connections": {
        "id": "otbus",
        "is_ot_bus": True,
        "connection_type": "gridappsd_field_bus.field_interface.gridappsd_field_bus.GridAPPSDMessageBus",
        "connection_args": {
            "GRIDAPPSD_ADDRESS": "tcp://gridappsd:61613",
            "GRIDAPPSD_USER": "system",
            "GRIDAPPSD_PASSWORD": "manager"
        }
    }
}
modelInfo = {
    "ieee13nodecktassets": {
        "id": "5B816B93-7A5F-B64C-8460-47C17D6E4B0F",
        "switchAreas": ['b8eec813-dad8-43a2-a955-46beef5e784f']
    },
    "ieee13nodeckt": {
        "id": "49AD8E07-3BF9-A4E2-CB8F-C3722F837B62",
        "switchAreas": ['1f39b37f-2a84-4f56-b557-3a72cb840434', '2cff1e26-8f8f-42b5-b054-87f28523d303',
                        '30c3c4ce-a7b6-4f7b-b2bc-3d771ed09b2b', '4021f7cf-340c-45e3-841f-e7b5fe8b93be'],
        "secondaryAreas": ['91aac04c-8d87-4385-a3a2-b768aeaddfd2', '4396f6fc-5231-4af1-a2a6-d5b9e0836983']
    },
    "ieee123": {
        "id": "C1C3E687-6FFD-C753-582B-632A27E28507",
        "switchAreas": ['87182da3-243f-4f83-8962-e555238affd0', 'b2274e97-3d3c-4268-b7e1-12b2f5481565',
                        'f4630006-2ffd-42eb-bb4d-246abe009c5b', '0ac9247d-0778-4dc4-9148-2bb66816eda3',
                        '5b1beeaa-4bf3-4621-9894-c9b0ea55e65a', '79d5a293-17fe-40d4-9eb8-fcfd2ae90457'],
        "secondaryAreas": ['d6c70d77-17e2-49d2-b33e-552341b20941']
    },
    "ieee123pv": {
        "id": "E407CBB6-8C8D-9BC9-589C-AB83FBF0826D",
        "switchAreas": ['cfe72655-2adf-4910-a106-5c74bf6eeeda', 'd2a49a04-7ac9-49d5-8b33-9757e075053d',
                        '099fff25-3a67-45e4-a09f-03f5737ee681', '3fc6bce4-af26-4e4c-ba56-cb795137dfd6',
                        '549112bd-1d79-4f20-b38c-829bb04b17aa', '56147bbc-5139-47a9-aeb2-5c92a2180800',
                        '667ec684-e4d1-42a6-a56d-4ffc768e3081', '78ff7d1c-2e4b-42e2-a37b-908a20a43b63'],
        "secondaryAreas": ['92b002cb-c6a4-4492-80fc-6d98e5c2a085', 'a38623d4-18e6-4fdb-8c08-2a3e5b115257',
                           'd68f12c2-256c-4b0e-a9ee-accba9dd0e58', 'e99e77c6-3c9f-43f7-85b4-6b13e393bbb4',
                           'ea8bf8ae-586d-4970-ae79-430a6922f8d6', 'f4fdffc7-c1fb-4df9-9bb7-2d03d4567895',
                           'fd453761-794e-483e-ba54-b876b7cf3503', '06a87652-5c85-4a4c-abaf-d674d608b239',
                           '425f6787-1f1e-4c7d-8a48-4a18fa1ed0e6', '5063f680-b0c4-4566-864b-e1b1284c3256',
                           '510f5531-61ab-4bb1-8910-41f686b3de2a', '654f7407-0804-43b2-9feb-e5e4063a6fa6',
                           '65868b62-17ce-48cd-9a0e-78f966f701c0', '39034e1a-132d-49b0-9a20-a05d42352cbc',
                           '8eb86ce1-8de2-4b5f-8bf2-37ecfb5e2ebe', 'a4dc6287-b11c-47df-b290-5cc2098df862',
                           'c4722d84-060b-47c6-aed5-a37df291a056', 'ca33c117-349d-45f3-8a0d-740ab8b6a98b',
                           'd31fb149-abcf-407b-ae05-00d2178f5932', 'dfdf9496-97ad-4caa-8208-25aa23546241',
                           'ee2e966b-e897-44f1-a91f-3a3f24d92655', 'ef6b9afa-122e-4c02-8e09-6b03e9b2ebed',
                           'f26fbea0-30e6-433e-b89b-5d6945225e60', '0c3e20d7-21c6-4e1a-9008-ace67b2dcf9e',
                           '1e3bad10-ae30-42b4-b0d2-62954a9a5a8b', '1f8c1709-a97e-4c63-b007-aa78e0acaa2c',
                           '2ad86e8c-a4e8-4a8b-b647-b3325a84828c', '30bc7e23-fbc2-459b-8bfb-a1be1fb7fe93',
                           '467d9d54-c883-4bc1-9e6c-c8564bec7bb7', '4d90eb7d-ea37-4e8a-a137-18d250b2717e',
                           '55030ee1-a753-4c91-a81e-0801661b681c', '58d1232a-ad91-4f2d-a0c4-b0152b2d4013',
                           '5fe40c35-8144-4862-b44e-6b645630ef21', '65012c83-ef87-43ee-9c59-51ddd8fffc09',
                           '6e5c768d-0e5e-41a2-a646-0ea4d13bf66b', '7a0031aa-9d54-4673-bd7e-fef375a35af7',
                           'a877ed04-6f28-4d1f-9669-fcaa20005a21', 'aa77a7a3-db92-401b-afb0-37d713dafbbf',
                           'f08d573b-500c-486e-90a9-50e079cf43d0', 'f8569761-abcb-4484-b392-5003f6d1c611',
                           '2147d1b6-90ef-4f1e-aa6d-5d7816cebaee', '2851b843-c1eb-4da2-b582-37552ae865f4',
                           '2f380d3e-108b-4ec0-93c2-ec3c3de2b979', '458c5c7c-579e-4cf0-82db-1bd41e360097',
                           '5b029e6f-dbfa-4f07-8ae9-6b181c9b32aa', '6c9e3bda-7e71-4caf-8b98-463c3b81dff6',
                           '70c12af0-238a-42ec-835e-414d3b0684a9', '619339fe-cafa-4ad6-bb50-1fba1a4de5ef',
                           '696d07ca-f019-4f1c-bb02-9144ffcc2e6a', '69a9e814-7a00-4738-a8fd-f5d89c910681',
                           '8becb29a-cc2a-4208-8af0-170f4d87addf', '9943fac6-a14e-4313-9d22-69fc8962d6a8',
                           'a5e81405-d73c-4e6b-bd49-2804eaf89732', 'afea7fac-e9b7-4df9-93c6-152e1e2f1972',
                           'b3b3c576-5106-4668-bfd8-5065508bc0e1', 'c12a8894-c47a-46ad-9937-ca5406210a16',
                           'cf6dbefd-4eb3-460f-ac53-2a5df360d3af', 'ddeedebf-09ba-463b-99d1-ebd42411593c',
                           '036f7580-5849-459e-9422-18052e296d4c', '04af00eb-3346-4406-936d-7159b685238e',
                           '0ae9716c-c066-45e9-ba81-f104527da389', '0d5b2746-8ad2-4179-b68f-8324e12e5a41',
                           '0d7d47b0-fabd-4655-935d-11fd7449f603', '2672e4cc-0a99-4544-8595-c60d762273ef',
                           '460584e8-4261-45a6-9d2e-b852cc8975fc', '4847493c-4e6f-4983-a172-78d0bc49a730',
                           '49e95d31-c15e-4af8-9f55-f820eac418f0', '4f253ca3-5d17-49e2-a7f0-29ef9a448a83',
                           '539507a4-309f-414f-928d-7f1e41b1c9c8', '5575d113-c2cc-444e-ad41-69580edba193',
                           '71500756-4a76-4bcb-b4d2-d4728d8fdb9f', '7476c8ff-311d-47d3-a53e-d4af537987a3',
                           '860fe1f3-df24-4b70-917e-22524ce09300', 'bce63d78-b8c4-44cb-b5d6-16a260bab8a1',
                           'd762ff9f-9acf-46a9-8af6-060cf56b4112', 'df4a052a-20b5-4038-bf5d-6a01d8177664',
                           '0bcb2438-4856-4cdd-97a1-0a9adca5d735', '248b3cd9-1842-4789-b895-a4eb8670f79b',
                           '366b26db-7d73-42f2-bd86-505b3587ca1c', '3b20f571-5d81-4991-adc9-3645238ffd05',
                           '5863c659-08c3-4a50-b6e6-f654cd739898', '59f34aa9-711d-42ff-84b7-e6632edc2c6c']
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
        for sid in switchAreas:
            switchConfigFile = switchConfigDir / f"switch_area_message_bus_{sid}.yml"
            with switchConfigFile.open(mode="w", encoding="utf-8") as scf:
                switchConfig = deepcopy(fieldBusConfigBoilerPlate)
                switchConfig["connections"]["id"] = f"{sid}"
                yaml.safe_dump(switchConfig, scf)
    secondaryAreas = modelInfo[mod].get("secondaryAreas")
    if secondaryAreas is not None:
        secondaryConfigDir = rootDir / "service_config_files" / mod / "secondary_level"
        if not secondaryConfigDir.is_dir():
            secondaryConfigDir.mkdir(parents=True)
        for sid in secondaryAreas:
            secondaryConfigFile = secondaryConfigDir / f"secondary_area_message_bus_{sid}.yml"
            with secondaryConfigFile.open(mode="w", encoding="utf-8") as scf:
                secondaryConfig = deepcopy(fieldBusConfigBoilerPlate)
                secondaryConfig["connections"]["id"] = f"{sid}"
                yaml.safe_dump(secondaryConfig, scf)
