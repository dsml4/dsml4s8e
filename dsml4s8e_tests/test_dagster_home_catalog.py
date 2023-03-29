from .context_mock import get_context_mock

from dsml4s8e.dsmlcatalog import dagster_home
from dsml4s8e import nb_data_keys
from collections import namedtuple
import os


def test_dagster_home():
    context = get_context_mock('0000')
    dagcat = dagster_home.DagsterStorageCatalog(context)
    k = 'simple_pipeline.data_load.op_nb_1.data1'
    dk = nb_data_keys.DataKeys([k])
    res = dagcat.get_outs_data_urls(dk)
    home = os.environ['DAGSTER_HOME']
    url = f'{home}/storage/0000/op_nb_1/data1'
    assert (res[k] == url)
