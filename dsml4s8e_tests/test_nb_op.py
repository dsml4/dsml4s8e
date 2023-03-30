from dsml4s8e.nb_op import (
    NbOp
)
from dsml4s8e.dsmlcatalog.local_storage import LoacalStorageCatalog
from .context_mock import get_context_mock
from typing import Dict
from dagster import Field


def test_get_op_config():
    NbOp.current_op_id = 'test.op'
    dag_op_params = NbOp(
        config_schema={
            "a": Field(
                int,
                description='nb params',
                default_value=10
                )
        }
    )
    assert (dag_op_params.config['a'] == 10)


def test_op():
    NbOp.current_op_id = 'test.op'
    url_nb_1_data1 = 'url'
    nb_path = '/home/jovyan/work/dev/pipeline_example/data_load/nb_1.ipynb'
    context = get_context_mock(
        run_id='0000',
        tags={'notebook_path': nb_path}
    )
    nb_params = NbOp(
        config_schema={
            "a": Field(
                int,
                description='nb params',
                default_value=10
                )
        },
        ins={'simple_pipeline.data_load.nb_1.data1': 'url_nb_1_data1'},
        outs=['data1']
        )
    assert (nb_params.config['a'] == 10)
    ins_data_urls: Dict[str, str] = nb_params.get_ins_data_urls(
        locals_=locals())
    assert (ins_data_urls['simple_pipeline.data_load.nb_1.data1'] ==
            'url')
    nb_params.set_locals(locals_=locals())
    assert (nb_params._id == 'pipeline_example.data_load.nb_1')
    assert (nb_params.nb_name == 'nb_1')


def test_get_data_urls():
    nb_path = '/home/jovyan/work/dev/pipeline_example/data_load/dagstermill.ipynb'
    context = get_context_mock(
        run_id='0000',
        tags={'notebook_path': nb_path}
    )
    op = NbOp(
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
    urls = op.get_data_urls(
        locals_=locals(),
        storage_catalog=LoacalStorageCatalog(
            '/home/jovyan/data',
            dagster_context=context
            )
    )
    etalon_url = '/home/jovyan/data/dev/pipeline_example/data_load/0000/dagstermill/data1'
    assert (op._outs_dict['pipeline_example.data_load.dagstermill.data1'] ==
            etalon_url)

    assert (urls.outs.pipeline_example.data_load.dagstermill.data1 ==
            etalon_url)
    assert (urls.ins.pipeline_example.data ==
            'val')
