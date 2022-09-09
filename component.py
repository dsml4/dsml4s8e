from dataclasses import dataclass
import os
import json
import git
from matplotlib.pyplot import get

from . import lib_cfg
from .notebook_interface import NotebookInterface
from .storage import StorageABC
from . import urls
from datetime import datetime
import glob
from .runner import run_id
# component serialize: notebook run parameters  


def get_last_run_id(resource_id, storage_prefix):
    # glob.glob('/home/jovyan/data/dev/pipeline_example/load/*/test/data1/*')
    return 'last_run'


def _get_json_file(name: str):
    return f'{lib_cfg.run_parameter}/{name}.json'


@dataclass(frozen=True)
class NbRunInfo:
    input: object
    output: object
    params: dict
    full_name: str
    url: str


class ComponentEnv:
    def __init__(self, env_params: dict, storage: StorageABC) -> None:
        self.run_id = env_params.get('run_id',  run_id)
        self.cdlc_stage = env_params.get('cdlc_stage', 'dev')
        self.storage = storage
        self._resources_dict = None
        self._artefacts_dict = None

    def _make_ids_urls_dict(
            self,
            ids: list[str],
            run_id: str
            ) -> dict[str: str]:
        if run_id == '':
            print(run_id)
            run_id = get_last_run_id()
        return dict(
            [(id, urls.id2url(
                id,
                self.cdlc_stage,
                self.run_id,
                self.storage.url_prefix
                ))
                for id in ids])

    def get_nb_run_info(
            self,
            nbi: NotebookInterface,
            params: dict
            ) -> NbRunInfo:
        if nbi.entityIDs.resources:
            self._resources_dict = self._make_ids_urls_dict(
                nbi.entityIDs.resources,
                un_id='')
        self._artefacts_dict = self._make_ids_urls_dict(
            nbi.entityIDs.artefacts,
            self.run_id)

        nb_url = urls.nb_id2url(
            nbi.id,
            self.cdlc_stage,
            self.run_id,
            self.storage.url_prefix
        )
        nb_run_info = NbRunInfo(
            input=urls.make_urls(self._resources_dict),
            output=urls.make_urls(self._artefacts_dict),
            params=params,
            full_name=nbi.nb_full_name,
            url=nb_url
            )
        return nb_run_info

    @property
    def cache(self):
        return f'{os.getcwd()}/cache'

    def validate(self):
        return True

    def seve_artefacts(self):
        ...

    @property
    def run_params_dir(self):
        if self.cdlc_stage == 'dev':
            rpd = f'{os.getcwd()}/run_parameters'
            os.makedirs(rpd, exist_ok=True)
            return rpd
        return ''

    def update_parameters(self):
        rpd = self.run_params_dir
        if rpd:
            with open(rpd, 'r') as infile:
                params = json.load(infile)
                print(params)
