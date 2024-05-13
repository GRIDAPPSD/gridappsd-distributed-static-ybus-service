import json
from pathlib import Path
import re
from typing import Any


class GridappsdJsonEncoder(json.JSONEncoder):

    def default(self, obj: Any) -> Any:
        rv = None
        if isinstance(obj, complex):
            rvInstance = [obj.real, obj.imag]
        else:
            rv = super().default(obj)
        return rv


def main():
    ybus = {}
    ybusPath = Path("/home/vale/git/gridappsd-distributed-static-ybus-service/scripts").resolve()
    for file in ybusPath.iterdir():
        if file.suffix == ".json":
            ybusFile = file.resolve()
            with ybusFile.open(mode="r") as jf:
                ybus[ybusFile.name] = json.load(jf)
    ybusDiff = {}
    for bus1 in ybus["feederYbus.json"].keys():
        for bus2 in ybus["feederYbus.json"][bus1].keys():
            feederVal = ybus["feederYbus.json"][bus1][bus2]
            switchVal = feederVal
            for i in range(6):
                switchVal = ybus.get(f"switch{i}.json", {}).get(bus1, {}).get(bus2)
                if switchVal is not None:
                    break
            if switchVal is not None:
                if bus1 not in ybusDiff.keys():
                    ybusDiff[bus1] = {}
                if bus2 not in ybusDiff[bus1].keys():
                    ybusDiff[bus1][bus2] = {}
                diff = [(feederVal[0] - switchVal[0]), (feederVal[1] - switchVal[1])]
                if diff[0] != 0.0 or diff[1] != 0.0:
                    ybusDiff[bus1][bus2]["error"] = [(feederVal[0] - switchVal[0]), (feederVal[1] - switchVal[1])]
                    ybusDiff[bus1][bus2]["percentError"] = [(feederVal[0] - switchVal[0]) * 100.0 / feederVal[0],
                                                            (feederVal[1] - switchVal[1]) * 100.0 / feederVal[1]]
                else:
                    del ybusDiff[bus1][bus2]
    delKeys = []
    for d in ybusDiff.keys():
        if ybusDiff[d] == {}:
            delKeys.append(d)
    for i in delKeys:
        del ybusDiff[i]
    ybusDiffFile = ybusPath / "ybusError.json"
    with ybusDiffFile.open(mode="w", encoding="utf-8") as rf:
        json.dump(ybusDiff, rf, indent=4, sort_keys=True)


if __name__ == "__main__":
    main()
