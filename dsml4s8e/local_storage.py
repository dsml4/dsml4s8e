from .storage_catalog import StorageCatalogABC
from .nb_data_keys import DataKeys
from .urls import data_key2url
import pandas as pd
import os
from pathlib import Path
from typing import List, Dict


class MissedInsParameters(Exception):
    def __init__(self, missed_vars, op_parameters_ins):
        keys_str = '/n'.join(missed_vars)
        self.message = f"""
        variables with keys:
        {keys_str}
        from dict in cell 'op_parameters'
        {op_parameters_ins}
        must be declared in cell 'parameters'
        """
        super().__init__(self.message)


class LoacalStorageCatalog(StorageCatalogABC):

    def __init__(self,
                 prefix: str,
                 dagster_context
                 ):
        self._prefix = prefix
        self.run_id = dagster_context.run.run_id
        self.cdlc_stage = dagster_context.op_def.tags.get('cdlc_stage',
                                                          'dev')

    @property
    def url_prefix(self):
        return self._prefix

    def is_valid(self) -> bool:
        return True

    def get_out_urls(self, data_kyes: DataKeys) -> Dict[str, str]:
        return dict(
                [(
                    k,
                    data_key2url(
                        k,
                        self.cdlc_stage,
                        self.run_id,
                        self.url_prefix
                        )
                 ) for k in data_kyes.keys]
            )

    def get_in_urls(self,
                    local_vars: Dict[str, str],
                    op_parameters_ins: Dict[str, str]
                    ) -> Dict[str, str]:
        res = super().get_in_urls(local_vars, op_parameters_ins)
        empty_vals = [k for k, v in res.items() if not v]
        if len(empty_vals) > 0:
            raise MissedInsParameters(empty_vals, op_parameters_ins)
        return res


def write_pandas_df(df: pd.DataFrame, url: str):
    os.makedirs(url, exist_ok=True)
    entity_name = url.split('/')[-1]
    entity_name = f'{entity_name}.zip'
    url2file = os.path.join(url, entity_name)
    print(url2file)
    status = os.path.join(url, '_SUCCESS')
    print(status)
    df.to_csv(url2file)
    open(os.path.join(url, '_SUCCESS'), 'w').close()


def read_pandas_df(url: str) -> pd.DataFrame:
    full_data_file_name = os.path.join(url,  f'{Path(url).name}.zip')
    return pd.read_csv(full_data_file_name, compression='zip')
