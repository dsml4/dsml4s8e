from .. import storage_catalog as sc
from ..data_catalog import DataCatalogPaths
from typing import Dict

import os
from pathlib import Path


def _catalog_path2storage_path(
        catalog_path: str,
        prefix: str
        ) -> str:
    """
    format of catalog_path: <pipeline>.<component>.<notebook>.<name_data_opj>
    cdlc_stage is a stage of component development life cycle: dev, test, ops
    return a storage path
    fromat of a starge path: <prefix>/<pipeline>/<component>/<notebook>/<name_data_opj>
    """
    nb, name = catalog_path.split('.')[-2:]
    return f'{prefix}/{nb}/{name}'


class DagsterStorageCatalog(sc.StorageCatalogABC):

    def __init__(self,
                 dagster_context
                 ):
        dgd_home = os.environ['DAGSTER_HOME']
        self._prefix = f'{dgd_home}/storage/{dagster_context.run.run_id}'

    def is_valid(self) -> bool:
        return True

    def get_outs_data_paths(self, catalog: DataCatalogPaths) -> Dict[str, str]:
        return dict(
            [(
                cat_path,
                _catalog_path2storage_path(
                    catalog_path=cat_path,
                    prefix=self._prefix
                    )
            ) for cat_path in catalog.paths]
        )
