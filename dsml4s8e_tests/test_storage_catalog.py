from dsml4s8e import storage_catalog
from dsml4s8e.dsmlcatalog import local_storage


def test_data_key2url():
    id_ = 'pip1.comp1.nb1.entity1'
    url = local_storage._data_key2url(id_, 'run_id', '/data/dev')
    assert (url == '/data/dev/pip1/comp1/run_id/nb1/entity1')
