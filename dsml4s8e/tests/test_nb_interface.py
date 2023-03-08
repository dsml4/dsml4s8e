from collections import namedtuple
from .. import nb_interface
from .. import local_storage


def _get_contex_mock(run_id: str,
                     tags: str):

    return namedtuple(
        'DagsterContextMock',
        ['run', 'op_def'])(
            namedtuple('Run', ['run_id'])(run_id),
            namedtuple('OpDef', ['tags'])(tags)
    )


def test_nb_interface():
    nb_path = '/home/jovyan/work/dev/pipeline_example/data_load/dagstermill.ipynb'
    dagster_context = _get_contex_mock(
        run_id='0000',
        tags={'notebook_path': nb_path}
    )
    nb_interface_ = nb_interface.NBInterface(
        dagster_context=dagster_context,
        dag_op_params={
            'outs': ['data1'],
            'ins': {'pipeline_example.data': 'var_name'}
        },
        local_vars={'var_name': 'val'},
        storage_catalog=local_storage.LoacalStorageCatalog(
            '/home/jovyan/data',
            dagster_context=dagster_context
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
