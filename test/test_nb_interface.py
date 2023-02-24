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


def test_make_urls_dict():
    urls = nb_interface.make_urls_dict(
        keys=['pip1.comp1.nb1.entity1',
              'pip1.comp1.nb1.entity2'],
        run_id='0000',
        cdlc_stage='dev',
        url_prefix='/data'
    )
    assert (
        urls['pip1.comp1.nb1.entity1'] ==
        '/data/dev/pip1/comp1/0000/nb1/entity1'
        )


def test_get_ins_from_locals():
    local_vars = {
        'comp1_nb1_data1': 'data1_url'
    }
    dag_op_ins = {
        'pip1.comp1.nb1.data1': 'comp1_nb1_data1'
    }
    d = nb_interface.get_ins_key_data_obj(
        local_vars=local_vars,
        dag_op_ins=dag_op_ins
    )
    assert (d['pip1.comp1.nb1.data1'] == 'data1_url')
    assert ('comp1_nb1_data1' not in local_vars)


def test_nb_interface():
    nb_path = '/home/jovyan/work/dev/pipeline_example/data_load/dagstermill.ipynb'
    dagster_context = _get_contex_mock(
        run_id='0000',
        tags={'notebook_path': nb_path}
    )
    nb_context_ = nb_interface.NBInterface(
        dagster_context=dagster_context,
        dag_op_params={
            'outs': ['data1']
        },
        local_vars={},
        storage_catalog=local_storage.LoacalStorageCatalog(
            '/home/jovyan/data',
            dagster_context=dagster_context
            )
    )
    etalon_url = '/home/jovyan/data/dev/pipeline_example/data_load/0000/dagstermill/data1'
    assert (nb_context_._outs_dict['pipeline_example.data_load.dagstermill.data1'] ==
            etalon_url)

    data_urls = nb_context_.get_data_urls()
    assert (data_urls.outs.pipeline_example.data_load.dagstermill.data1 ==
            etalon_url)
