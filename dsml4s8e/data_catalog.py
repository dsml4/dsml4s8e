from dataclasses import dataclass
import json
from typing import List


@dataclass(frozen=True)
class DataCatalogPaths:
    '''
    list of catalog data paths
    format of catalog path: <pipeline>.<component>.<notebook>.<data_obj_name>
    '''
    paths: tuple[str]


def make_data_catalog_path(op_id: str, var_name):
    return f'{op_id}.{var_name}'


class NotebookPaths:
    def __init__(
            self,
            ins_catalog_paths: List[str],
            outs_vars: List[str],
            op_id: str
            ):
        '''
        ins is a list of input data paths in catalog
        outs is a list of local names of out data objects
        format of keys of data obj:
        <pipeline>.<component>.<netebook>.<data_obj_name>
        '''
        self.ins = DataCatalogPaths(ins_catalog_paths)
        self.outs = DataCatalogPaths(
            [make_data_catalog_path(op_id, var_name) for
             var_name in outs_vars])

    def __str__(self):
        interface_info = {
            "ins": self.ins,
            "outs": self.outs
        }
        return json.dumps(interface_info, indent=4)
