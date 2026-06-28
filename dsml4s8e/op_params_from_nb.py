from dataclasses import asdict
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


def define_dagstermill_op_kvargs_from_nb(
    nb_path: Path,
    tmp_src_nbs_path: Path,
) -> dict[str, any]:
    """
    create kvargs for define_dagstermill_op:
    define_dagstermill_op(**args)
    """
    with open(nb_path, "r", encoding="utf-8") as f:
        nb = nbformat.read(f, as_version=4)
    nb_tags = set()
    kvargs = None
    locals_ = {}
    nbop_var_name = ""
    for cell in nb.cells:
        cell_tags = get_cell_tags(cell)
        nb_tags.update(cell_tags)
        if "parameters" in cell_tags:
            exec(cell.source, locals=locals_)
            print(locals_)
        if "op_parameters" in cell_tags:
            # In this cell, the object of NbOp is created,
            # and its parameters saved in NbOp._all_params
            # to be available via the static method op.NbOp.params()
            # see NbOp.__init__ and op.NbOp.params()
            print("----op_parameters=")
            print(locals_)
            exec(cell.source, locals=locals_)
            print(locals_)
            nbop_var_name = next(
                (v for k, v in locals_.items() if isinstance(v, op.NbOp)), None
            )
            # append cell with yield downstream
            outs2downstream_cell = nbformat.v4.new_code_cell(
                f"{nbop_var_name}.pass_outs_to_next_steps()"
            )
            nb["cells"].append(outs2downstream_cell)

            tmp_input_notebook_path = tmp_src_nbs_path / nb_path.name
            with open(tmp_src_nbs_path / nb_path.name, "w", encoding="utf-8") as f:
                nbformat.write(nb, f)

            kvargs = op.NbOp.get_curren_params()
            op_name = Path(nb_path).stem
            if "ins" in kvargs:
                kvargs["ins"] = {name: In(str) for name in kvargs["ins"]}
            if "outs" in kvargs:
                kvargs["outs"] = {name: Out(str) for name in kvargs["outs"]}
                op.NbOp.nb_outs[op_name] = asdict(op.NbOp.current.outs)
            #  create new tmp nb in tmp dir and add the downstream cel "op.downstream"
            kvargs["notebook_path"] = str(tmp_input_notebook_path)
            kvargs["name"] = op_name
            kvargs["output_notebook_name"] = f"out_{kvargs['name']}"
            local_path = "/".join(str(nb_path).split("/")[-2:])
            kvargs["description"] = f"path: {local_path}"
    mandatory_tags = {"op_parameters", "parameters"}
    dsml_nb_tags = mandatory_tags.intersection(nb_tags)
    if dsml_nb_tags == mandatory_tags:
        return kvargs
    raise MissingTagsException(nb_path, sorted(mandatory_tags - dsml_nb_tags))
