from .. import dag_params
from pathlib import Path
from dagster import Field
from dagster import Out, In


def test_extract_op_params_from_nb():
    test_dir = Path(__file__).parent
    nb_path = f'{test_dir}/data/nb_with_params.ipynb'
    op_params = dag_params.extract_op_params_from_nb(nb_path)

    etalon_op_params = {
        'name': "name_op_1",
        'output_notebook_name': "output_notebook_1",
        'config_schema': {
            "a": Field(
                int,
                description='nb params',
                default_value=10
                )
        },
        'description': """comp.notebook1""",
        'ins': {'pip1.comp1.nb1.data1': 'alias_data_key'},
        'outs': ['data1']
    }
    comp_keys = [
        'name',
        'output_notebook_name',
        'description'

    ]
    for k in comp_keys:
        assert (op_params[k] == etalon_op_params[k])
    # pylint: disable-next=line-too-long
    assert (
        etalon_op_params['config_schema']['a'].default_value ==
        op_params['config_schema']['a'].default_value)


def test_nb_ins2dagster_ins():
    ins = {
        'pip1.comp1.nb1.data1': 'comp1_nb1_data1'
        }
    dagster_ins = dag_params.nb_ins2dagster_ins(ins)
    assert (dagster_ins['comp1_nb1_data1'] == In(str))


def test_nb_outs2dagster_outs():
    outs = ['data1']
    nb_path = '/home/jovyan/work/dev/pipeline_example/data_load/dagstermill.ipynb'
    dagster_ins = dag_params.nb_outs2dagster_outs(outs, nb_path)
    assert (dagster_ins['dagstermill_data1'] ==
            Out(str))


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
    op_config = dag_params.get_op_config(dag_op_params)
    assert (op_config['a'] == 10)
