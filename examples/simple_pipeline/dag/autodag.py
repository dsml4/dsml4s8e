from dsml4s8e.op_params_from_nb import dagstermill_op_params_from_nb
from dagstermill import define_dagstermill_op, local_output_notebook_io_manager
from dagster import (
    job,
    op,
    In
)
from pathlib import Path


def full_path(path: str) -> str:
    return str(Path(path).resolve())


def define_op(op_params):
    op = define_dagstermill_op(**op_params,
                               save_notebook_on_failure=True)
    return op


def define_branch(ops_list):
    parent_outs, _ = define_op(ops_list[0])()
    for nbp in ops_list[1:]:
        parent_outs, _ = define_op(nbp)(parent_outs)


class OpsTree:
    def __init__(self, root_dir):
        self._root_dir = root_dir

    def find_nbops():
        ...

    def build():
        ...


@job(
    name='simple_pipeline',
    tags={"cdlc_stage": "dev"},
    resource_defs={
        "output_notebook_io_manager": local_output_notebook_io_manager,
    }
)
def dagstermill_pipeline():
    ops_list = [
        dagstermill_op_params_from_nb(full_path("../data_load/nb_1.ipynb")),
        dagstermill_op_params_from_nb(full_path("../data_load/nb_2.ipynb"))
    ]
    define_branch(ops_list)
