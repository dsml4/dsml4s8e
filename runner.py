import papermill
from datetime import datetime
import os
import nbformat
from .storage import StorageABC


run_id = f"run-{datetime.now().strftime('%y-%m-%d-%H%M%S')}"

env_params = {
    "run_id": run_id,
    "cdlc_stage": "ops"
}

papermill_params = {
    "parameters": {
    },
    "env_params": env_params
}


def is_result_cell(cell):
    return cell.cell_type == 'code' and 'results' in cell.metadata.get('tags', [])


def extract_run_results(nn: nbformat.notebooknode.NotebookNode):
    # nn = nbformat.read(nb_full_name, as_version=4)
    for cell in nn.cells:
        if is_result_cell(cell):
            output = cell['outputs']
            if output['name'] == 'stdout':
                return output['text']
    return ''


def run_notebooks(
        notebooks: list[str],
        storag: StorageABC
        ):
    component_wd = os.getcwd()
    component_cd = f'{component_wd}/cache/{run_id}'
    os.makedirs(component_cd, exist_ok=True)
    for nb in notebooks:
        nb_full_name = f'{component_wd}/{nb}.ipynb'
        out_nb_full_name = f'{component_cd}/{nb}.ipynb'
        papermill_params['parameters']['nb_full_name'] = nb_full_name
        nn = papermill.execute.execute_notebook(
            nb_full_name,
            out_nb_full_name,
            parameters=papermill_params)
        result_json = extract_run_results(nn)
        nb_url = result_json['nb_url']
        storag.sava_artefact(out_nb_full_name, nb_url)

