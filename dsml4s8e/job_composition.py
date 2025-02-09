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
        self.op_outputs_pos = {}
        self.op_outputs = {}

    def save_nb_outpusts(
        self, op_def: OpDefinition, nb_outpusts: PendingNodeInvocation
    ):
        try:
            nb_outpusts = nb_outpusts[:-1]
        except IndexError:
            return
        self.op_outputs[op_def.name] = nb_outpusts
        for pos, out_key in enumerate(op_def.outs.keys()):
            self.op_outputs_pos[out_key] = (op_def.name, pos)

    def get_op_ins_by_names(self, op_positional_inputs: Sequence[str]):
        ins = []
        for in_key in op_positional_inputs:
            op_name, pos = self.op_outputs_pos[in_key]
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
    def ops_configs(self):
        ops_configs = {}

        for nb_params in self._nbs_params_seq:
            ops_configs[nb_params["name"]] = type(
                f"Op{nb_params['name']}Cfg",
                (Config,),
                {"__annotations__": nb_params["config_schema"]},
            )
        self._ops_configs = MappingProxyType(ops_configs)
        return self._ops_configs

    def do_compositioin(self, save_notebook_on_failure: bool = True):
        job_outs = _JobInsOutsComposition()
        for op_params in self._nbs_params_seq:
            op_def: OpDefinition = define_dagstermill_op(
                **op_params, save_notebook_on_failure=save_notebook_on_failure
            )
            op_ins = job_outs.get_op_ins_by_names(op_def.positional_inputs)
            nb_outpusts = op_def(*op_ins)
            job_outs.save_nb_outpusts(op_def, nb_outpusts)

    def cfg_map(self, nb_name: str, **kvargs):
        return {nb_name: self.ops_configs[nb_name](**kvargs)}
