from dsml4s8e import op_params_from_nb
from pathlib import Path
from dagster import Field
from dagster import Out, In


def test_op_params_from_nb():
    test_dir = Path(__file__).parent
    nb_path = f'{test_dir}/notebooks/nb_with_params.ipynb'
    op_params = op_params_from_nb.dagstermill_op_params_from_nb(nb_path)

    etalon_op_params = {
        'config_schema': {
            "a": Field(
                int,
                description='nb params',
                default_value=10
                )
        },
        'description': """path: notebooks/nb_with_params.ipynb""",
        'ins': {'pip1.comp1.nb1.data1': 'alias_data_key'},
        'outs': ['data1']
    }
    comp_keys = [
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
    dagster_ins = op_params_from_nb.nb_ins2dagster_ins(ins)
    assert (dagster_ins['comp1_nb1_data1'] == In(str))


def test_nb_outs2dagster_outs():
    outs = ['data1']
    nb_id = 'pipeline_example.nb'
    dagster_ins = op_params_from_nb.nb_outs2dagster_outs(outs, nb_id)
    assert (dagster_ins['url_nb_data1'] ==
            Out(str))
