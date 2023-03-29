from .context_mock import get_context_mock

from dsml4s8e import nb_interface
from dsml4s8e.dag_params import NbOpParams
from dsml4s8e.dsmlcatalog import local_storage
from dagster import Field


def test_nb_interface():
    nb_path = '/home/jovyan/work/dev/pipeline_example/data_load/dagstermill.ipynb'
    context = get_context_mock(
        run_id='0000',
        tags={'notebook_path': nb_path}
    )
    nb_op_params = NbOpParams(
                config_schema={
                    "a": Field(
                        int,
                        description='this is the paramentr description',
                        default_value=10
                        )
                },
                ins={'pipeline_example.data': 'var_name'},
                outs=['data1']
            )
    var_name = 'val'
    nb_interface_ = nb_interface.NBInterface(
        locals_=locals(),
        nb_op_params=nb_op_params,
        storage_catalog=local_storage.LoacalStorageCatalog(
            '/home/jovyan/data',
            dagster_context=context
            )
    )
    etalon_url = '/home/jovyan/data/dev/pipeline_example/data_load/0000/dagstermill/data1'
    assert (nb_interface_._outs_dict['pipeline_example.data_load.dagstermill.data1'] ==
            etalon_url)

    data_urls = nb_interface_.get_data_urls()
    assert (data_urls.outs.pipeline_example.data_load.dagstermill.data1 ==
            etalon_url)
    assert (data_urls.ins.pipeline_example.data ==
            'val')
