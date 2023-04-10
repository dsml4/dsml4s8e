from .op_params_from_nb import dagstermill_op_params_from_nb
from dagstermill import define_dagstermill_op
from dagster import (
    job,
    OpDefinition
)

from pathlib import Path
from typing import (
    Tuple,
    Sequence
)


class JobOuts:
    def __init__(self):
        self.fn_results_index = {}
        self.fn_results = {}

    def add_op_outs(
            self,
            invoked_op: OpDefinition,
            fn_result):
        self.fn_results[invoked_op.name] = fn_result
        for i, p_name in enumerate(invoked_op.outs.keys()):
            self.fn_results_index[p_name] = (invoked_op.name, i)

    def get_fn_ins(
            self,
            positional_inputs: Sequence[str]):
        ins = []
        for in_p in positional_inputs:
            invoked_op_name, i = self.fn_results_index[in_p]
            ins.append(
                self.fn_results[invoked_op_name][i]
            )
        return ins


def define_job(root_path: Path,
               nbs_sequence: Tuple[str],
               save_notebook_on_failure: bool = True
               ):
    job_outs = JobOuts()
    for relative_nb_path in nbs_sequence:
        full_nb_path = root_path.joinpath(
            root_path,
            relative_nb_path
            )
        params = dagstermill_op_params_from_nb(str(full_nb_path))
        op_def: OpDefinition = define_dagstermill_op(
                **params,
                save_notebook_on_failure=save_notebook_on_failure
            )
        fn_ins = job_outs.get_fn_ins(op_def.positional_inputs)
        results = op_def(*fn_ins)
        job_outs.add_op_outs(
            op_def,
            results[:-1]
        )
