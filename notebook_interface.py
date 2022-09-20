from dataclasses import dataclass, asdict
from pathlib import Path
import json
import nbformat


@dataclass(frozen=True)
class EntityIDs:
    resources: list[str]
    artefacts: list[str]


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
            resource_names: list[str],
            artefact_names: list[str],
            nb_full_name: str
            ):

        path = Path(nb_full_name)
        parts = path.parts
        self.nb_full_name = str(path)

        self.id = _get_nb_id(
            pipeline=parts[-3],
            component=parts[-2],
            notebook=path.stem
        )

        self.entityIDs = EntityIDs(
            resource_names,
            _get_artefact_ids(self.id, artefact_names)
            )

    def __str__(self):
        interface_info = {
            "resources": self.entityIDs.resources,
            "artefacts": self.entityIDs.artefacts
        }
        return json.dumps(interface_info, indent=4)

    # TODO: make abc
    def _is_valid_id(self):
        pass

    @property
    def name(self):
        return self.id.split('.')[-1]


def exec_interface_cell(source: str, parameters) -> NotebookInterface:
    parameters = parameters
    exec_output = {}
    source = f'{source}\nexec_output["nbi"] = nbi\n'
    exec(source)
    return exec_output['nbi']


def exec_params_cell(source: str) -> dict:
    exec_output = {}
    source = f'{source}\nexec_output["p"] = parameters\n'
    exec(source)
    return exec_output['p']


def get_cell_tags(cell):
    if cell.cell_type == 'code':
        return cell.metadata.get('tags', [])
    return []


def extract_from_nb(nb_full_name: str):
    nb = nbformat.read(nb_full_name, as_version=4)
    parameters = {}
    for cell in nb.cells:
        tags = []
        if cell.cell_type == 'code':
            tags = get_cell_tags(cell)
        if 'parameters' in tags:
            parameters = exec_params_cell(cell.source)
        if parameters and 'interface' in tags:
            parameters['nb_full_name'] = nb_full_name
            nbi = exec_interface_cell(cell.source, parameters)
            return nbi, parameters
