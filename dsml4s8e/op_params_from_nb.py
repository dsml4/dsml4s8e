from dsml4s8e.nb_data_keys import (
    NotebookDataKeys,
    data_key2url_name
    )
import dsml4s8e.nb_op as op

from dagster import Out, In
import nbformat


def uniq_name(entity_id: str):
    name = entity_id.split('.')[-1]
    return name


def get_cell_tags(cell):
    if cell.cell_type == 'code':
        return cell.metadata.get('tags', [])
    return []


def nb_ins2dagster_ins(nb_ins):
    return {
        dag_name: In(str)
        for dag_name in nb_ins.values()
    }


def nb_outs2dagster_outs(outs, nb_id):
    nb_data_keys = NotebookDataKeys(
        ins_data_key_dag_name={},
        outs=outs,
        op_id=nb_id
    )
    return {
        data_key2url_name(k): Out(str)
        for k in nb_data_keys.outs.keys
    }


def get_dagstermill_op_params(nb_path: str):
    nb = nbformat.read(nb_path, as_version=4)
    op.NbOp.current_op_id, nb_name = op.op_name_from_nb_path(nb_path)
    for cell in nb.cells:
        tags = []
        if cell.cell_type == 'code':
            tags = get_cell_tags(cell)
        if 'op_parameters' in tags:
            exec(cell.source)
            params = op.NbOp.params()
    if 'ins' in params:
        params['ins'] = nb_ins2dagster_ins(
            nb_ins=params['ins'],
        )
    if 'outs' in params:
        params['outs'] = nb_outs2dagster_outs(
            outs=params['outs'],
            nb_id=op.NbOp.current_op_id
        )
    params['notebook_path'] = nb_path
    params['name'] = nb_name
    params['output_notebook_name'] = f"out_{nb_name}"
    local_path = '/'.join(nb_path.split('/')[-2:])
    params['description'] = f"path: {local_path}"
    return params
