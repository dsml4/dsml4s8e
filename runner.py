import papermill
from datetime import datetime
import os
from .storage import StorageABC
from pathlib import Path


def new_run_id():
    return f"run-{datetime.now().strftime('%y-%m-%d-%H%M%S')}"


env_params = {
    'run_id': 'run_id',
    'cdlc_stage': 'ops'
}


papermill_params = {
    'parameters': {
    },
    'env_params': env_params
}


def is_result_cell(cell):
    return cell.cell_type == 'code' and 'results' in cell.metadata.get('tags', [])


def get_cell_tags(cell):
    if cell.cell_type == 'code':
        return cell.metadata.get('tags', [])
    return []


def make_cache_dir(component_wd: str, run_id: str):
    component_cd = f'{component_wd}/cache/{run_id}'
    os.makedirs(component_cd, exist_ok=True)
    return component_cd


def run_notebook(
        nb_path: str,
        papermill_params: dict,
        component_cd: str
        ):
    nb_name = Path(nb_path).name
    out_nb_path = f'{component_cd}/{nb_name}'
    nn = papermill.execute.execute_notebook(
        nb_path,
        out_nb_path,
        parameters=papermill_params)
    return out_nb_path
