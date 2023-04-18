from dsml4s8e import storage_catalog as sc
from dsml4s8e.data_catalog import DataCatalogPaths
from typing import List, Dict


def _catalog_path2storage_path(
        catalog_path: str,
        run_id: str,
        prefix: str
        ) -> str:
    """
    format of catalog_path: <pipeline>.<component>.<notebook>.<name_data_opj>
    cdlc_stage is a stage of component development life cycle: dev, test, ops
    return a storage path
    format of a storagr path: <prefix>/<pipeline>/<component>/<notebook>/<name_data_opj>
    """
    p, c, nb, e = catalog_path.split('.')
    return f'{prefix}/{p}/{c}/{run_id}/{nb}/{e}'


class LoacalStorageCatalog(sc.StorageCatalogABC):

    def __init__(self,
                 prefix: str,
                 dagster_context
                 ):
        self.cdlc_stage = dagster_context.op_def.tags.get(
            'cdlc_stage',
            'dev')
        self._prefix = f'{prefix}/{self.cdlc_stage}'
        self.run_id = dagster_context.run.run_id

    @property
    def url_prefix(self):
        return self._prefix

    def is_valid(self) -> bool:
        return True

    def get_outs_data_paths(self, catalog: DataCatalogPaths) -> Dict[str, str]:
        return dict(
                [(
                    k,
                    _catalog_path2storage_path(
                        k,
                        self.run_id,
                        self.url_prefix
                        )
                 ) for k in catalog.paths]
            )
