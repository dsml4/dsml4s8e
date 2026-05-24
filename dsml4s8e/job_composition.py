from pathlib import Path
from typing import Sequence
from types import MappingProxyType

from functools import cached_property

from dagster import OpDefinition, MetadataValue, Config
from dagster._core.definitions.composition import PendingNodeInvocation
from dagstermill import define_dagstermill_op
from dsml4s8e.op_params_from_nb import dagstermill_op_params_from_nb


class _JobInsOutsComposition:
    def __init__(self):
        self.op_output_name_pos: dict[str, tuple[str, int]] = {}
        self.op_outputs = {}

    def save_nb_outpusts(
        self, op_def: OpDefinition, nb_outpusts: PendingNodeInvocation
    ):
        # the last output is a handler of an output notebook
        # Papermill generates an output notebook for each nb
        # dsml4s8e don't use output notebooks as inputs of ops
        nb_outpusts = nb_outpusts[:-1]
        self.op_outputs[op_def.name] = nb_outpusts
        for pos, out_key in enumerate(op_def.outs.keys()):
            self.op_output_name_pos[out_key] = (op_def.name, pos)

    def get_op_ins_by_names(self, op_positional_inputs: Sequence[str]):
        ins = []
        for in_key in op_positional_inputs:
            op_name, pos = self.op_output_name_pos[in_key]
            ins.append(self.op_outputs[op_name][pos])
        return ins


class NbsJobComposition:
    def __init__(self, root_path: Path, nbs_sequence: tuple[str]):
        self._nbs_params_seq: list[dict[str, dict]] = []
        self._job_metadata = {}
        for relative_nb_path in nbs_sequence:
            absolute_nb_path = root_path.joinpath(root_path, relative_nb_path)
            self._nbs_params_seq.append(
                dagstermill_op_params_from_nb(str(absolute_nb_path))
            )
            self._job_metadata[relative_nb_path] = MetadataValue.notebook(
                absolute_nb_path
            )

    @property
    def metadata(self):
        return self._job_metadata

    @cached_property
    def op_config_cls(self):
        return MappingProxyType(
            {
                nb_params["name"]: nb_params["config_schema"]
                for nb_params in self._nbs_params_seq
            }
        )

    def do_compositioin(self, save_notebook_on_failure: bool = True):
        # _core/definitions/composition.py
        # function which is our DSL for constructing a dependency graph
        job_outs = _JobInsOutsComposition()
        for op_params in self._nbs_params_seq:
            op_def: OpDefinition = define_dagstermill_op(
                **op_params, save_notebook_on_failure=save_notebook_on_failure
            )
            op_ins = job_outs.get_op_ins_by_names(op_def.positional_inputs)
            nb_outpusts = op_def(*op_ins)
            if "outs" in op_params:
                job_outs.save_nb_outpusts(op_def, nb_outpusts)

    def __call__(self):
        self.do_compositioin(save_notebook_on_failure=True)

    def make_config(self, nb_name: str, **kvargs):
        nb_config = {
            "nb_0": {"a": 1},
            "nb_1": {"a": 1},
            "nb_2": {"b": 2},
        }
        {
            nb_name: self.op_config_cls[nb_name](**nb_config[nb_name])
            for nb_name in nb_config
        }
        return {nb_name: self.op_config_cls[nb_name](**kvargs)}
