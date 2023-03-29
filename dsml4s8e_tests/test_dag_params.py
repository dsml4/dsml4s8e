from dsml4s8e.dag_params import (
    NbOpParams
)
from .context_mock import get_context_mock
from typing import Dict
from dagster import Field


def test_get_op_config():
    NbOpParams.current_op_id = 'test.op'
    dag_op_params = NbOpParams(
        config_schema={
            "a": Field(
                int,
                description='nb params',
                default_value=10
                )
        }
    )
    assert (dag_op_params.config['a'] == 10)


def test_op_parameters():
    NbOpParams.current_op_id = 'test.op'
    url_nb_1_data1 = 'url'
    nb_path = '/home/jovyan/work/dev/pipeline_example/data_load/nb_1.ipynb'
    context = get_context_mock(
        run_id='0000',
        tags={'notebook_path': nb_path}
    )
    nb_params = NbOpParams(
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
