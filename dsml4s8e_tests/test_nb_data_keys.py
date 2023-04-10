from dsml4s8e import data_catalog


def test_nb_data_catalog():
    nb_data_keys_ = data_catalog.NotebookPaths(
        ins_catalog_paths={'data_key': 'alias_data_key'},
        outs_vars=['data1'],
        op_id='pipeline_example.data_load.dagstermill'
    )
    assert (nb_data_keys_.outs.paths[0] ==
            'pipeline_example.data_load.dagstermill.data1')
