from .. import component
from .. import local_storage


def test_make_ids_urls_dict_():
    cenv = component.ComponentEnv(
        env_params={},
        storage=local_storage.LoacalStorage('/home/jovyan/data')
    )
    res = ['pipeline_example.load.test.data1',
           'pipeline_example.load.test.data2']
    ids_urs = cenv._make_ids_urls_dict(res, cenv.run_id, cenv.cdlc_stage)
    etalon = f'{cenv.storage.url_prefix}/dev/pipeline_example/load/'
    etalon += f'{cenv.run_id}/test/data1'
    assert (ids_urs['pipeline_example.load.test.data1'] == etalon)
