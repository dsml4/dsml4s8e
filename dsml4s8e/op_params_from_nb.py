from typing import Any
import nbformat
from dagster import Out, In
from pathlib import Path

import dsml4s8e
from dsml4s8e.nb_op import NbOp


def get_cell_tags(cell):
    if cell.cell_type == "code":
        return cell.metadata.get("tags", [])
    return []


class MissingTagsException(Exception):
    def __init__(self, nb_path, tags):
        self._tags = tags
        self._nb_path = nb_path

    def __str__(self):
        return f"Missing tags: {self._tags} in {self._nb_path}"


class CellSourceCodeError(Exception):
    def __init__(self, source):
        self._source = source

    def __str__(self):
        return f"Error in a cell tagged with the op_parameters: {self._source}"


def missing_tags(nb_tags: set) -> set:
    mandatory_tags = {"op_parameters", "parameters"}
    return mandatory_tags - mandatory_tags.intersection(nb_tags)


def create_nb_op_from_cell_source(
    source_parameters: str, source_op_parameters: str
) -> tuple[str, NbOp]:
    exec(source_parameters, globals=globals())
    exec(source_op_parameters, globals=globals())
    nb_op_var_name, nb_op = next(
        ((k, v) for k, v in globals().items() if isinstance(v, dsml4s8e.NbOp)),
        (None, None),
    )
    if isinstance(nb_op_var_name, str) and isinstance(nb_op, dsml4s8e.NbOp):
        return nb_op_var_name, nb_op
    raise CellSourceCodeError(source_op_parameters)


def define_dagstermill_op_kvargs_from_nb(
    nb_path: Path,
    tmp_src_nbs_path: Path,
) -> dict[str, Any]:
    """
    create kvargs for define_dagstermill_op:
    define_dagstermill_op(**args)
    """
    with open(nb_path, "r", encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)
    nb_tags = set()
    nb_op_var_name = ""
    kvargs = {}
    cell_parameters_source = ""
    for cell in nb.cells:
        cell_tags = get_cell_tags(cell)
        nb_tags.update(cell_tags)
        if "parameters" in cell_tags:
            cell_parameters_source = cell.source
        if "op_parameters" in cell_tags:
            if not cell_parameters_source:
                raise MissingTagsException(nb_path=nb_path, tags=["parameters"])
            nb_op_var_name, nb_op = create_nb_op_from_cell_source(
                source_parameters=cell_parameters_source,
                source_op_parameters=cell.source,
            )
            kvargs = nb_op.op_params

            if "ins" in kvargs:
                kvargs["ins"] = {name: In(str) for name in kvargs["ins"]}
            if "outs" in kvargs:
                kvargs["outs"] = {name: Out(str) for name in kvargs["outs"]}

            #  create new tmp nb in tmp dir and add the downstream cel "op.downstream"
            kvargs["name"] = nb_path.stem
            kvargs["output_notebook_name"] = f"out_{kvargs['name']}"
            local_path = "/".join(str(nb_path).split("/")[-2:])
            kvargs["description"] = f"path: {local_path}"

    if missing_tags(nb_tags=nb_tags):
        raise MissingTagsException(nb_path, missing_tags(nb_tags=nb_tags))

    if kvargs is None:
        raise TypeError("kvargs is None")

    notebook_path_arg = str(tmp_src_nbs_path / nb_path.name)
    kvargs["notebook_path"] = notebook_path_arg

    if isinstance(kvargs, dict):
        if "outs" in kvargs:
            # append cell with yield downstream data
            outs2downstream_cell = nbformat.v4.new_code_cell(
                f"{nb_op_var_name}._yield_output_paths()"
            )
            nb["cells"].append(outs2downstream_cell)
            with open(notebook_path_arg, "w", encoding="utf-8") as f:
                nbformat.write(nb, f)
            kvargs["notebook_path"] = notebook_path_arg

    return kvargs
