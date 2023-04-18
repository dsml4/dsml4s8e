import pandas as pd
import os
from pathlib import Path


def write_pandas_df(df: pd.DataFrame, path: str):
    os.makedirs(path, exist_ok=True)
    entity_name = path.split('/')[-1]
    entity_name = f'{entity_name}.zip'
    path2file = os.path.join(path, entity_name)
    print(path2file)
    status = os.path.join(path, '_SUCCESS')
    print(status)
    df.to_csv(path2file)
    open(os.path.join(path, '_SUCCESS'), 'w').close()


def read_pandas_df(path: str) -> pd.DataFrame:
    full_data_file_name = os.path.join(path,  f'{Path(path).name}.zip')
    return pd.read_csv(full_data_file_name, compression='zip')
