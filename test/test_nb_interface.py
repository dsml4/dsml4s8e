from dsml4s8e.notebook_interface import NotebookInterface, exec_interface_cell
from dsml4s8e.runner import get_cell_tags

import nbformat
from pathlib import Path


def test_exec_interface_cell():
    test_dir = Path(__file__).parent
    nb_full_name = f'{test_dir}/data/nb_interface.ipynb'
    nb = nbformat.read(nb_full_name, as_version=4)
    parameters = {
        'a': 1,
        'nb_full_name': nb_full_name
    }
    nbi = None
    for cell in nb.cells:
        tags = get_cell_tags(cell)
        print(tags)
        if 'interface' in tags:
            nbi = exec_interface_cell(cell.source, parameters)
            break

    assert (nbi.id == 'test.data.nb_interface')
