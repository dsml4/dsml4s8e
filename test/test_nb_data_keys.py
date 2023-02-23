from .. import nb_data_keys


def test_nb_data_keys():
    op_config = {'a': 4}
    nb_data_keys_ = nb_data_keys.NotebookDataKeys(
        ins_data_key_dag_name={'data_key': 'alias_data_key'},
        outs=['data1'],
        nb_path='/home/jovyan/work/dev/pipeline_example/data_load/dagstermill.ipynb'
    )
    assert (nb_data_keys_.id ==
            'pipeline_example.data_load.dagstermill')
    assert (nb_data_keys_.outs.keys[0] ==
            'pipeline_example.data_load.dagstermill.data1')
