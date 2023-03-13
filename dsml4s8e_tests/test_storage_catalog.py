from dsml4s8e import storage_catalog
from dsml4s8e.dsmlcatalog import local_storage


def test_get_in_urls():
    local_vars = {
        'comp1_nb1_data1': 'data1_url'
    }
    dag_op_ins = {
        'pip1.comp1.nb1.data1': 'comp1_nb1_data1'
    }
    d = storage_catalog._get_in_urls(
        local_vars=local_vars,
        op_parameters_ins=dag_op_ins
    )
    assert (d['pip1.comp1.nb1.data1'] == 'data1_url')


def test_data_key2url():
    id_ = 'pip1.comp1.nb1.entity1'
    url = local_storage._data_key2url(id_, 'run_id', '/data/dev')
    assert (url == '/data/dev/pip1/comp1/run_id/nb1/entity1')
