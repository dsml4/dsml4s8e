from .. import nb_parser

from pathlib import Path


def test_get_results():
    test_dir = Path(__file__).parent
    nb_full_name = f'{test_dir}/data/parser_test.ipynb'
    res = nb_parser.get_results(nb_full_name)
    url = res['artefacts']['pipeline_example.data_load.test.data1']
    eurl = '/home/jovyan/data/dev/pipeline_example/data_load/run-22-09-19-112951/test/data1'
    assert (eurl == url)
