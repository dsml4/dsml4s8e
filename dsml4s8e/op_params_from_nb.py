from pydantic import Field
import nbformat
from dagster import Out, In
from pathlib import Path

import dsml4s8e.nb_op as op


def get_cell_tags(cell):
    if cell.cell_type == "code":
        return cell.metadata.get("tags", [])
    return []


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
            # In this cell, the object of NbOp is created,
            # and its parameters saved in NbOp._all_params
            # to be available via the static method op.NbOp.params()
            # see NbOp.__init__ and op.NbOp.params()
            exec(cell.source)
            params = op.NbOp.get_curren_params()
            if "ins" in params:
                params["ins"] = {name: In(str) for name in params["ins"]}
            if "outs" in params:
                params["outs"] = {name: Out(str) for name in params["outs"]}

            params["notebook_path"] = nb_path
            params["name"] = Path(nb_path).stem
            params["output_notebook_name"] = f"out_{params['name']}"
            local_path = "/".join(nb_path.split("/")[-2:])
            params["description"] = f"path: {local_path}"
    mandatory_tags = {"op_parameters", "parameters"}
    dsml_nb_tags = mandatory_tags.intersection(nb_tags)
    if dsml_nb_tags == mandatory_tags:
        return params
    raise MissingTagsException(nb_path, sorted(mandatory_tags - dsml_nb_tags))
