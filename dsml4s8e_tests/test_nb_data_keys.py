from dsml4s8e import nb_data_keys


def test_nb_data_keys():
    nb_data_keys_ = nb_data_keys.NotebookDataKeys(
        ins_data_key_dag_name={'data_key': 'alias_data_key'},
        outs=['data1'],
        op_id='pipeline_example.data_load.dagstermill'
    )
    assert (nb_data_keys_.outs.keys[0] ==
            'pipeline_example.data_load.dagstermill.data1')
