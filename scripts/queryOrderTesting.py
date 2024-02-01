import json
import numpy as np
from typing import Dict, List


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


lineConfig = [{
    'line_config': {
        'value': '1'
    },
    'count': {
        'value': '3'
    },
    'r_ohm_per_m': {
        'value': '0.00028676281'
    },
    'x_ohm_per_m': {
        'value': '0.00066182246'
    },
    'row': {
        'value': '3'
    },
    'col': {
        'value': '3'
    }
}, {
    'line_config': {
        'value': '1'
    },
    'count': {
        'value': '3'
    },
    'r_ohm_per_m': {
        'value': '0.0002899318'
    },
    'x_ohm_per_m': {
        'value': '0.00065132128'
    },
    'row': {
        'value': '2'
    },
    'col': {
        'value': '2'
    }
}, {
    'line_config': {
        'value': '1'
    },
    'count': {
        'value': '3'
    },
    'r_ohm_per_m': {
        'value': '0.000095380479'
    },
    'x_ohm_per_m': {
        'value': '0.00023916577'
    },
    'row': {
        'value': '3'
    },
    'col': {
        'value': '1'
    }
}, {
    'line_config': {
        'value': '1'
    },
    'count': {
        'value': '3'
    },
    'r_ohm_per_m': {
        'value': '0.000098176647'
    },
    'x_ohm_per_m': {
        'value': '0.00026321284'
    },
    'row': {
        'value': '3'
    },
    'col': {
        'value': '2'
    }
}, {
    'line_config': {
        'value': '1'
    },
    'count': {
        'value': '3'
    },
    'r_ohm_per_m': {
        'value': '0.000096933907'
    },
    'x_ohm_per_m': {
        'value': '0.00031174193'
    },
    'row': {
        'value': '2'
    },
    'col': {
        'value': '1'
    }
}, {
    'line_config': {
        'value': '1'
    },
    'count': {
        'value': '3'
    },
    'r_ohm_per_m': {
        'value': '0.00028433946'
    },
    'x_ohm_per_m': {
        'value': '0.00066983815'
    },
    'row': {
        'value': '1'
    },
    'col': {
        'value': '1'
    }
}]
aclines = [{
    'line_name': {
        'value': 'l3'
    },
    'length': {
        'value': '91.44'
    },
    'line_config': {
        'value': '1'
    },
    'bus1': {
        'value': '1'
    },
    'bus2': {
        'value': '7'
    },
    'phase': {
        'value': 'A'
    }
}, {
    'line_name': {
        'value': 'l3'
    },
    'length': {
        'value': '91.44'
    },
    'line_config': {
        'value': '1'
    },
    'bus1': {
        'value': '1'
    },
    'bus2': {
        'value': '7'
    },
    'phase': {
        'value': 'C'
    }
}, {
    'line_name': {
        'value': 'l35'
    },
    'length': {
        'value': '91.44'
    },
    'line_config': {
        'value': '1'
    },
    'bus1': {
        'value': '5'
    },
    'bus2': {
        'value': '7'
    },
    'phase': {
        'value': 'B'
    }
}, {
    'line_name': {
        'value': 'l3'
    },
    'length': {
        'value': '91.44'
    },
    'line_config': {
        'value': '1'
    },
    'bus1': {
        'value': '1'
    },
    'bus2': {
        'value': '7'
    },
    'phase': {
        'value': 'B'
    }
}, {
    'line_name': {
        'value': 'l35'
    },
    'length': {
        'value': '91.44'
    },
    'line_config': {
        'value': '1'
    },
    'bus1': {
        'value': '5'
    },
    'bus2': {
        'value': '7'
    },
    'phase': {
        'value': 'A'
    }
}, {
    'line_name': {
        'value': 'l35'
    },
    'length': {
        'value': '91.44'
    },
    'line_config': {
        'value': '1'
    },
    'bus1': {
        'value': '5'
    },
    'bus2': {
        'value': '7'
    },
    'phase': {
        'value': 'C'
    }
}]


def fillYbusUnique(bus1: str, bus2: str, Yval: float, Ybus: Dict, debugYbusDict):
    if Yval == 0j:
        return
    if bus1 not in Ybus:
        Ybus[bus1] = {}
        debugYbusDict[bus1] = {}
    if bus2 not in Ybus:
        Ybus[bus2] = {}
        debugYbusDict[bus2] = {}
    if bus2 in Ybus[bus1]:
        logger.warn(f'Unexpected existing value found for Ybus[{bus1}][{bus2}] when filling model value')
    Ybus[bus1][bus2] = Ybus[bus2][bus1] = Yval
    debugYbusDict[bus1][bus2] = debugYbusDict[bus2][bus1] = 1


def fillYbusAdd(bus1: str, bus2: str, Yval: float, Ybus: Dict, debugYbusDict):
    if Yval == 0j:
        return
    if bus1 not in Ybus:
        Ybus[bus1] = {}
        debugYbusDict[bus1] = {}
    if bus2 not in Ybus:
        Ybus[bus2] = {}
        debugYbusDict[bus2] = {}
    if bus2 in Ybus[bus1]:
        Ybus[bus1][bus2] += Yval
        Ybus[bus2][bus1] = Ybus[bus1][bus2]
        debugYbusDict[bus1][bus2] += 1
        debugYbusDict[bus2][bus1] = debugYbusDict[bus1][bus2]
    else:
        Ybus[bus1][bus2] = Ybus[bus2][bus1] = Yval
        debugYbusDict[bus1][bus2] = debugYbusDict[bus2][bus1] = 1


def fillYbusUniqueUpperLines(bus1: str, bus2: str, Yval: float, Ybus: Dict, debugYbusDict):
    if Yval == 0j:
        return
    if bus1 not in Ybus:
        Ybus[bus1] = {}
        debugYbusDict[bus1] = {}
    if bus2 not in Ybus:
        Ybus[bus2] = {}
        debugYbusDict[bus2] = {}
    if bus2 in Ybus[bus1]:
        logger.warn('Unexpected existing value found for Ybus[{bus1}][{bus2}] when filling line model value')
    # extract the node and phase from bus1 and bus2
    node1, phase1 = bus1.split('.')
    node2, phase2 = bus2.split('.')
    bus3 = node1 + '.' + phase2
    bus4 = node2 + '.' + phase1
    if bus3 not in Ybus:
        Ybus[bus3] = {}
        debugYbusDict[bus3] = {}
    if bus4 not in Ybus:
        Ybus[bus4] = {}
        debugYbusDict[bus4] = {}
    Ybus[bus1][bus2] = Ybus[bus2][bus1] = Ybus[bus3][bus4] = Ybus[bus4][bus3] = Yval
    debugYbusDict[bus1][bus2] = debugYbusDict[bus2][bus1] = debugYbusDict[bus3][bus4] = debugYbusDict[bus4][bus3] = 1


def fillYbusNoSwapLines(bus1: str, bus2: str, Yval: float, Ybus: Dict, debugYbusDict):
    fillYbusUnique(bus2, bus1, Yval, Ybus, debugYbusDict)
    fillYbusAdd(bus1, bus1, -Yval, Ybus, debugYbusDict)
    fillYbusAdd(bus2, bus2, -Yval, Ybus, debugYbusDict)


def fillYbusSwapLines(bus1: str, bus2: str, Yval: float, Ybus: Dict, debugYbusDict):
    fillYbusUniqueUpperLines(bus2, bus1, Yval, Ybus, debugYbusDict)
    # extract the node and phase from bus1 and bus2
    node1, phase1 = bus1.split('.')
    node2, phase2 = bus2.split('.')
    # mix-and-match nodes and phases for filling Ybus
    fillYbusAdd(bus1, node1 + '.' + phase2, -Yval, Ybus, debugYbusDict)
    fillYbusAdd(node2 + '.' + phase1, bus2, -Yval, Ybus, debugYbusDict)


def main():
    Ybus = {}
    debugYbusDict = {}
    Zabc = {}
    for obj in lineConfig:
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

    ybusPhaseIdx = {'A': '.1', 'B': '.2', 'C': '.3', 's1': '.1', 's2': '.2'}
    last_name = ''
    aclinesSorted = sorted(aclines, key=lambda d: (d['line_name']['value'], d['phase']['value']))
    for obj in aclines:
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
            #identityTest = np.dot(lenZabc, invZabc)
            #logger.debug('identity test for ' + line_name + ': ' + str(identityTest))
            # negate the matrix and assign it to Ycomp
            Ycomp = invZabc * -1
        # we now have the negated inverted matrix for comparison
        line_idx += 1
        if Ycomp.size == 1:
            fillYbusNoSwapLines(bus1 + ybusPhaseIdx[phase], bus2 + ybusPhaseIdx[phase], Ycomp[0, 0], Ybus,
                                debugYbusDict)
        elif Ycomp.size == 4:
            if line_idx == 1:
                pair_i0b1 = bus1 + ybusPhaseIdx[phase]
                pair_i0b2 = bus2 + ybusPhaseIdx[phase]
            else:
                pair_i1b1 = bus1 + ybusPhaseIdx[phase]
                pair_i1b2 = bus2 + ybusPhaseIdx[phase]
                fillYbusNoSwapLines(pair_i0b1, pair_i0b2, Ycomp[0, 0], Ybus, debugYbusDict)
                fillYbusSwapLines(pair_i1b1, pair_i0b2, Ycomp[1, 0], Ybus, debugYbusDict)
                fillYbusNoSwapLines(pair_i1b1, pair_i1b2, Ycomp[1, 1], Ybus, debugYbusDict)
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
                fillYbusNoSwapLines(pair_i0b1, pair_i0b2, Ycomp[0, 0], Ybus, debugYbusDict)
                fillYbusSwapLines(pair_i1b1, pair_i0b2, Ycomp[1, 0], Ybus, debugYbusDict)
                fillYbusNoSwapLines(pair_i1b1, pair_i1b2, Ycomp[1, 1], Ybus, debugYbusDict)
                fillYbusSwapLines(pair_i2b1, pair_i0b2, Ycomp[2, 0], Ybus, debugYbusDict)
                fillYbusSwapLines(pair_i2b1, pair_i1b2, Ycomp[2, 1], Ybus, debugYbusDict)
                fillYbusNoSwapLines(pair_i2b1, pair_i2b2, Ycomp[2, 2], Ybus, debugYbusDict)
    print(json.dumps(Ybus, indent=4, sort_keys=True, cls=ComplexEncoder))


if __name__ == "__main__":
    main()
