from dataclasses import dataclass, asdict
from pathlib import Path
import json

from . import lib_cfg
from .project import Project


@dataclass
class Ids:
    resources: list[str]
    artefacts: list[str]


def _id_str(pipeline: str,
            component: str,
            notebook: str,
            entity: str,
            ):
    return '{}.{}.{}.{}'.format(
        pipeline,
        component,
        notebook,
        entity)


def _get_artefact_ids(artefact_names: list[str], prj: Project):
    return [_id_str(
           prj.pipeline,
           prj.component,
           prj.notebook,
           an)
           for an in artefact_names]


def _get_json_file(name: str):
    return f'{lib_cfg.nb_interfaces_dir}/{name}.json'


class NotebookInterface:
    def __init__(
            self,
            artefact_names: list[str],
            prj: Project
            ) -> None:
        self.prj = prj
        self.name = self.prj.notebook
        self.idis = Ids([], _get_artefact_ids(artefact_names, self.prj))
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

    def set_resource_componennt_link(
            self,
            pipeline_name: str,
            component_name: str,
            notebook_name: str,
            resource_names: list[str],
            ) -> None:

        for r in resource_names:
            resource_id = _id_str(pipeline_name,
                                  component_name,
                                  notebook_name,
                                  r)
            if resource_id in self.idis.resources:
                # TODO: worning
                pass
            self.idis.resources.append(resource_id)

    def serialize(self):
        """
        serialize netebook interface to create DAG
        abc
        """
        path = lib_cfg.nb_interfaces_dir
        Path(path).mkdir(parents=True, exist_ok=True)
        with open(_get_json_file(self.name), 'w') as outf:
            json.dump(asdict(self.idis), outf, indent=4)

    @staticmethod
    def deserialize(name: str, prj: Project):
        with open(_get_json_file(name), 'r') as inf:
            idis = json.load(inf)
        nbi = NotebookInterface(idis['artefacts'], prj)
        nbi.idis.resources = idis['resources']
        return nbi
