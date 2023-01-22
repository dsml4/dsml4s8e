from dataclasses import dataclass, asdict
from pathlib import Path
import json
from typing import List


@dataclass(frozen=True)
class DataKeys:
    '''
    format of key: <pipeline>.<component>.<notebook>.<data_obj_name>
    ins: list of input data keys
    outs: list of output data keys
    '''
    ins: List[str]
    outs: List[str]


def _get_nb_id(
        pipeline: str,
        component: str,
        notebook: str):
    return '{}.{}.{}'.format(
        pipeline,
        component,
        notebook)


def _get_artefact_ids(nb_id: str, artefact_names: list[str]):
    return [f'{nb_id}.{a}' for a in artefact_names]


def data_key2dag_name(data_key: str) -> str:
    return '_'.join(data_key.split('.')[-2:])


class NotebookDataKeys:
    def __init__(
            self,
            ins_data_key_dag_name: dict,
            outs: List[str],
            nb_path: str
            ):
        '''
        ins is a list of keys of input data objects
        outs is a list of local names of out data objects
        format of keys of data obj:
        <pipeline>.<component>.<netebook>.<data_obj_name>
        '''
        self._ins_aliases = ins_data_key_dag_name
        path = Path(nb_path)
        parts = path.parts
        self.notebook_path = str(path)

        self.id = _get_nb_id(
            pipeline=parts[-3],
            component=parts[-2],
            notebook=path.stem
        )

        self.data_keys = DataKeys(
            ins_data_key_dag_name.keys(),
            _get_artefact_ids(self.id, outs)
            )

    def __str__(self):
        interface_info = {
            "ins": self.data_keys.ins,
            "outs": self.data_keys.outs
        }
        return json.dumps(interface_info, indent=4)

    # TODO: make abc
    def _is_valid_id(self):
        pass

    @property
    def name(self):
        return self.id.split('.')[-1]
