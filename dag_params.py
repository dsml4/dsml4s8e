from dagster import Out, In
import os
from pathlib import Path
import nbformat
from dsml4s8e.nb_data_keys import (
    NotebookDataKeys,
    data_key2dag_name
    )


def uniq_name(entity_id: str):
    name = entity_id.split('.')[-1]
    return name


def exec_params_cell(source: str) -> dict:
    exec_output = {}
    source = f'{source}\nexec_output["op_params"] = dag_op_params\n'
    exec(source)
    return exec_output['op_params']


def get_cell_tags(cell):
    if cell.cell_type == 'code':
        return cell.metadata.get('tags', [])
    return []


def nb_ins2dagster_ins(nb_ins):
    return {
        dag_name: In(str)
        for dag_name in nb_ins.values()
    }


def nb_outs2dagster_outs(outs, nb_path):
    nb_data_keys = NotebookDataKeys(
        ins_data_key_dag_name={},
        outs=outs,
        nb_path=nb_path
    )
    return {
        data_key2dag_name(k): Out(str)
        for k in nb_data_keys.data_keys.outs
    }


def extract_op_params_from_nb(nb_path: str):
    nb = nbformat.read(nb_path, as_version=4)
    params = {}
    for cell in nb.cells:
        tags = []
        if cell.cell_type == 'code':
            tags = get_cell_tags(cell)
        if 'dagstermill' in tags:
            params = exec_params_cell(cell.source)
    if 'ins' in params:
        params['ins'] = nb_ins2dagster_ins(
            nb_ins=params['ins'],
        )
    if 'outs' in params:
        params['outs'] = nb_outs2dagster_outs(
            outs=params['outs'],
            nb_path=nb_path
        )
    return params


def get_op_config(dag_op_params):
    return {
        k: v.default_value for k, v in
        dag_op_params['config_schema'].items()
    }
