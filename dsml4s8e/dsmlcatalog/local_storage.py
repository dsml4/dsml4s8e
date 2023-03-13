from .. import storage_catalog as sc
from ..nb_data_keys import DataKeys
from typing import List, Dict


def _data_key2url(
        key: str,
        run_id: str,
        prefix: str
        ) -> str:
    """
    format of data_key: <pipeline>.<component>.<notebook>.<name_data_opj>
    cdlc_stage is a stage of component development life cycle: dev, test, ops
    url: <prefix>/<pipeline>/<component>/<notebook>/<name_data_opj>
    """
    p, c, nb, e = key.split('.')
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

    def get_out_urls(self, data_kyes: DataKeys) -> Dict[str, str]:
        return dict(
                [(
                    k,
                    _data_key2url(
                        k,
                        self.run_id,
                        self.url_prefix
                        )
                 ) for k in data_kyes.keys]
            )

    def get_in_urls(self,
                    local_vars: Dict[str, str],
                    op_parameters_ins: Dict[str, str]
                    ) -> Dict[str, str]:
        return super().get_in_urls(local_vars, op_parameters_ins)
