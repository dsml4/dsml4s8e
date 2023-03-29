from dataclasses import dataclass
import json
from typing import List, Dict


@dataclass(frozen=True)
class DataKeys:
    '''
    format of key: <pipeline>.<component>.<notebook>.<data_obj_name>
    ins: list of input data keys
    outs: list of output data keys
    '''
    keys: tuple[str]


def data_key2url_name(data_key: str) -> str:
    return '_'.join(['url'] + data_key.split('.')[-2:])


class NotebookDataKeys:
    def __init__(
            self,
            ins_data_key_dag_name: Dict[str, dict],
            outs: List[str],
            op_id: str
            ):
        '''
        ins is a list of keys of input data objects
        outs is a list of local names of out data objects
        format of keys of data obj:
        <pipeline>.<component>.<netebook>.<data_obj_name>
        '''
        self.ins = DataKeys(ins_data_key_dag_name.keys())
        self.outs = DataKeys([f'{op_id}.{a}' for a in outs])

    def __str__(self):
        interface_info = {
            "ins": self.ins,
            "outs": self.outs
        }
        return json.dumps(interface_info, indent=4)
