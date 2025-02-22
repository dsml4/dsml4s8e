from .fake_context import get_fake_context

from dsml4s8e.dsmlcatalog import dagster_home
from dsml4s8e import data_keys
import os


def test_dagster_home():
    context = get_fake_context("0000")
    dagcat = dagster_home.DagsterStorageCatalog(context)
    k = "simple_pipeline.data_load.op_nb_1.data1"
    dk = data_keys.DataKyes([k])
    res = dagcat.make_outs_data_paths(dk)
    home = os.environ["DAGSTER_HOME"]
    path = f"{home}/storage/0000/op_nb_1/data1"
    assert res[k] == path
