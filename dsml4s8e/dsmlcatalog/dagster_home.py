from .. import storage_catalog as sc
from ..nb_data_keys import DataKeys
from typing import List, Dict

import os
from pathlib import Path


def _data_key2url(
        key: str,
        prefix: str
        ) -> str:
    """
    format of data_key: <pipeline>.<component>.<notebook>.<name_data_opj>
    cdlc_stage is a stage of component development life cycle: dev, test, ops
    url: <prefix>/<pipeline>/<component>/<notebook>/<name_data_opj>
    """
    nb, name = key.split('.')[-2:]
    return f'{prefix}/{nb}/{name}'


class DagsterStorageCatalog(sc.StorageCatalogABC):

    def __init__(self,
                 dagster_context
                 ):
        dgd_home = os.environ['DAGSTER_HOME']
        self._prefix = f'{dgd_home}/storage/{dagster_context.run.run_id}'

    @property
    def url_prefix(self):
        return self._prefix

    def is_valid(self) -> bool:
        return True

    def get_outs_data_urls(self, data_kyes: DataKeys) -> Dict[str, str]:
        return dict(
            [(
                k,
                _data_key2url(
                    key=k,
                    prefix=self._prefix
                    )
            ) for k in data_kyes.keys]
        )
