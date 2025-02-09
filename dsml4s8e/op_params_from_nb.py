from dsml4s8e.data_keys import NotebookData, kye2pathvar_name
import dsml4s8e.nb_op as op

from dagster import Out, In
import nbformat


def uniq_name(entity_id: str):
    name = entity_id.split(".")[-1]
    return name


def get_cell_tags(cell):
    if cell.cell_type == "code":
        return cell.metadata.get("tags", [])
    return []


def nb_ins2dagster_ins(nb_ins):
    return {dag_name: In(str) for dag_name in nb_ins.values()}


def nb_outs2dagster_outs(outs, nb_id):
    nb_data = NotebookData(ins=[], out_var_names=outs, op_id=nb_id)
    return {kye2pathvar_name(k): Out(str) for k in nb_data.outs.keys}


class MissingTagsException(Exception):
    def __init__(self, nb_path, tags):
        self.tags = tags
        self.nb_path = nb_path

    def __str__(self):
        return f"Missing tags: {self.tags} in {self.nb_path}"


def dagstermill_op_params_from_nb(nb_path: str):
    nb = nbformat.read(nb_path, as_version=4)
    nb_tags = set()
    params = None
    for cell in nb.cells:
        cell_tags = get_cell_tags(cell)
        nb_tags.update(cell_tags)
        if "op_parameters" in cell_tags:
            op.NbOp.set_current_nb_path(nb_path)
            # In this cell, the object of NbOp is created,
            # and its parameters saved in NbOp._all_params
            # to be available via the static method op.NbOp.params()
            # see NbOp.__init__ and op.NbOp.params()
            exec(cell.source)
            params = op.NbOp.params()
            if "ins" in params:
                params["ins"] = nb_ins2dagster_ins(
                    nb_ins=params["ins"],
                )
            if "outs" in params:
                params["outs"] = nb_outs2dagster_outs(
                    outs=params["outs"], nb_id=op.NbOp._current_op_id
                )
            params["notebook_path"] = nb_path
            params["output_notebook_name"] = f"out_{params['name']}"
            local_path = "/".join(nb_path.split("/")[-2:])
            params["description"] = f"path: {local_path}"
    mandatory_tags = {"op_parameters", "parameters"}
    dsml_nb_tags = mandatory_tags.intersection(nb_tags)
    if dsml_nb_tags == mandatory_tags:
        return params
    raise MissingTagsException(nb_path, mandatory_tags - dsml_nb_tags)
