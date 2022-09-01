from dataclasses import dataclass, make_dataclass
from pathlib import Path
import json
import git

from . import lib_cfg
from .notebook_interface import NotebookInterface
from datetime import datetime
# component serialize: notebook run parameters  


def new_run_id():
    return f"run-{datetime.now().strftime('%y-%m-%d-%H%M%S')}"


def _get_json_file(name: str):
    return f'{lib_cfg.run_parameter}/{name}.json'


@dataclass(frozen=True)
class Urls:
    resources: list[str]
    artefacts: list[str]


@dataclass(frozen=True)
class NBComponent:
    ndi: NotebookInterface
    params: dict


class Component:
    def __init__(self) -> None:
        self.nb_params = {}

    def add_nb(
            self,
            nbi: NotebookInterface,
            params: dict
            ) -> NBComponent:
        nbc = NBComponent(nbi,  params)
        self.nb_params[nbi.name] = nbc
        return nbc

    def validate(self):
        pass

    def __str__(self):
        pass
