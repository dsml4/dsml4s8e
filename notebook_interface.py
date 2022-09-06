from dataclasses import dataclass, asdict
from pathlib import Path
import json

from . import lib_cfg


@dataclass
class Ids:
    resources: list[str]
    artefacts: list[str]


def _get_json_file(nb_name: str):
    return f'{lib_cfg.nb_interfaces_dir}/{nb_name}.json'


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


class NotebookInterface:
    def __init__(
            self,
            artefact_names: list[str],
            nb_full_name: str
            ) -> None:
        """
        In case of interactive mode nb_full_name is emty then
        is gotten from ipynbname.path()
        """
        if not nb_full_name:
            import ipynbname
            path = ipynbname.path()
        else:
            path = Path(nb_full_name)
        parts = path.parts

        self.id = _get_nb_id(
            pipeline=parts[-3],
            component=parts[-2],
            notebook=path.stem
        )

        self.idis = Ids([], _get_artefact_ids(self.id, artefact_names))
        self.path_to_interface_json = ''

    def __str__(self):
        json_file_name = _get_json_file(self.name)
        with open(json_file_name, 'r') as inf:
            interface_info = json.load(inf)
        interface_info['json_file'] = json_file_name
        return json.dumps(interface_info, indent=4)

    # TODO: make abc
    def _is_valid_id(self):
        pass

    @property
    def name(self):
        return self.id.split('.')[-1]

    def set_resource_component_link(
            self,
            nb_id: str,
            resource_names: list[str],
            ) -> None:

        for resource_id in _get_artefact_ids(nb_id, resource_names):
            if resource_id in self.idis.resources:
                print(f'worning: {resource_id} has already exist in ')
            else:
                self.idis.resources.append(resource_id)

    def serialize(self):
        """
        serialize netebook interface to create DAG subsequently
        """
        path = lib_cfg.nb_interfaces_dir
        Path(path).mkdir(parents=True, exist_ok=True)
        with open(_get_json_file(self.id), 'w') as outf:
            json.dump(asdict(self.idis), outf, indent=4)

    @staticmethod
    def deserialize(name: str, nb_full_name: str):
        with open(_get_json_file(name), 'r') as inf:
            idis = json.load(inf)
        nbi = NotebookInterface(idis['artefacts'], nb_full_name)
        nbi.idis.resources = idis['resources']
        return nbi
