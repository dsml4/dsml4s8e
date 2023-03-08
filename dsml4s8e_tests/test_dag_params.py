from dsml4s8e.dag_params import get_op_config
from dagster import Field


def test_get_op_config():
    dag_op_params = {
        'config_schema': {
            "a": Field(
                int,
                description='nb params',
                default_value=10
                )
        }
    }
    op_config = get_op_config(dag_op_params)
    assert (op_config['a'] == 10)
