from .storage_catalog import StorageCatalogABC
import pandas as pd
import os
from pathlib import Path
from typing import List


class LoacalStorageCatalog(StorageCatalogABC):

    def __init__(self, prefix):
        self._prefix = prefix

    @property
    def url_prefix(self):
        return self._prefix

    def is_valid(self) -> bool:
        return True

    def get_urls(self, prefix: str) -> List[str]:
        return []


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
