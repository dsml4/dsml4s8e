from dsml4s8e.notebook_interface import NotebookInterface, extract_from_nb
from dsml4s8e.runner import get_cell_tags

import nbformat
from pathlib import Path


def test_exec_interface_cell():
    test_dir = Path(__file__).parent
    nb_full_name = f'{test_dir}/data/nb_interface.ipynb'
    nbi, parameters = extract_from_nb(nb_full_name)
    assert (nbi.id == 'test.data.nb_interface')
    assert (parameters['a'] == 1)
    assert (nbi.entityIDs.artefacts[0] == 'test.data.nb_interface.data1')
