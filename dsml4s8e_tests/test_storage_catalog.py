from dsml4s8e.dsmlcatalog import local_storage


def test_data_key2url():
    id_ = 'pip1.comp1.nb1.entity1'
    path = local_storage._catalog_path2storage_path(id_, 'run_id', '/data/dev')
    assert (path == '/data/dev/pip1/comp1/run_id/nb1/entity1')
