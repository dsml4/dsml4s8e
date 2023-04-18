from dsml4s8e.data_catalog import NotebookPaths
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
    nb_catalog_paths = NotebookPaths(
        ins_catalog_paths=[],
        outs_vars=outs,
        op_id=nb_id
    )
    return {
        op.NbOp.dotted_path2path_varname(p): Out(str)
        for p in nb_catalog_paths.outs.paths
    }


def dagstermill_op_params_from_nb(nb_path: str):
    nb = nbformat.read(nb_path, as_version=4)
    params = None
    for cell in nb.cells:
        tags = []
        if cell.cell_type == 'code':
            tags = get_cell_tags(cell)
        if 'op_parameters' in tags:
            op.NbOp.set_current_nb_path(nb_path)
            # In this cell the object of NbOp is creaded
            # in NbOp.__init__
            exec(cell.source)
            params = op.NbOp.params()
            if 'ins' in params:
                params['ins'] = nb_ins2dagster_ins(
                    nb_ins=params['ins'],
                )
            if 'outs' in params:
                params['outs'] = nb_outs2dagster_outs(
                    outs=params['outs'],
                    nb_id=op.NbOp._current_op_id
                )
            params['notebook_path'] = nb_path
            params['output_notebook_name'] = f"out_{params['name']}"
            local_path = '/'.join(nb_path.split('/')[-2:])
            params['description'] = f"path: {local_path}"
            return params
    return params
