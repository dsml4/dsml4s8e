from dataclasses import dataclass
import os
import json
import git


from .notebook_interface import NotebookInterface
from .storage import StorageABC
from . import urls
from .runner import new_run_id
from datetime import datetime
import glob
# component serialize: notebook run parameters  


def get_last_run_id(resource_id, storage_prefix):
    # glob.glob('/home/jovyan/data/dev/pipeline_example/load/*/test/data1/*')
    return 'last_run'


@dataclass(frozen=True)
class NbRunInfo:
    input: object
    output: object
    params: dict
    full_name: str
    url: str


class ComponentEnv:
    def __init__(self, env_params: dict, storage: StorageABC) -> None:
        self.run_id = env_params.get('run_id',  new_run_id())
        self.cdlc_stage = env_params.get('cdlc_stage', 'dev')
        self.storage = storage
        self._resources_dict = {}
        self._artefacts_dict = {}

    def _make_ids_urls_dict(
            self,
            ids: list[str],
            run_id: str,
            cdlc_stage: str
            ) -> dict[str: str]:
        if run_id == '':
            print(run_id)
            run_id = get_last_run_id()
        return dict(
            [(id, urls.id2url(
                id,
                cdlc_stage,
                run_id,
                self.storage.url_prefix
                ))
                for id in ids])

    def get_nb_run_info(
            self,
            nbi: NotebookInterface,
            params: dict,
            res_cdlc_stage: str = ''
            ) -> NbRunInfo:
    # resource (id, cdlc_stage) 
        if nbi.entityIDs.resources:
            if res_cdlc_stage == '':
                res_cdlc_stage = self.cdlc_stage
            self._resources_dict = self._make_ids_urls_dict(
                nbi.entityIDs.resources,
                run_id='',
                cdlc_stage=res_cdlc_stage)
        self._artefacts_dict = self._make_ids_urls_dict(
            nbi.entityIDs.artefacts,
            self.run_id,
            cdlc_stage=self.cdlc_stage)

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

    def validate(self):
        return True

    def to_json(self):
        run_info = {
            'artefacts': self._artefacts_dict,
            'resources': self._resources_dict
        }
        return json.dumps(run_info, indent=4)
