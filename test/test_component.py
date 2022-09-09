from .. import component
from .. import local_storage


def test_make_ids_urls_dict_():
    cenv = component.ComponentEnv(
        env_params={},
        storage=local_storage.LoacalStorage('/home/jovyan/data')
    )
    res = ['pipeline_example.load.test.data1',
           'pipeline_example.load.test.data2']
    ids_urs = cenv._make_ids_urls_dict(res, cenv.run_id)
    etalon = f'/home/jovyan/data/dev/pipeline_example/load/{cenv.run_id}/test/data1'
    assert (ids_urs['pipeline_example.load.test.data1'] == etalon)

    ids_urs = cenv._make_ids_urls_dict([], cenv.run_id)
    assert (ids_urs == {})
