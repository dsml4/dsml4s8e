from typing import Dict, Tuple
from types import SimpleNamespace
from functools import cached_property
from dataclasses import dataclass, make_dataclass, asdict
import copy

import dagster as dg
import dagstermill
from dagstermill import _load_input_parameter, DagstermillExecutionContext
from dagstermill.manager import MANAGER_FOR_NOTEBOOK_INSTANCE


def nb_path():
    import ipynbname

    print(ipynbname.path())


def make_paths(out_names, out_paths) -> dataclass:
    return make_dataclass(
        cls_name="StorageCatalog", fields=[(o, str) for o in out_names]
    )(*out_paths)


class MissedInsParameters(Exception):
    def __init__(self, missed_vars, op_parameters_ins):
        keys_str = "/n".join(missed_vars)
        vars = "/n".join([op_parameters_ins[k] for k in missed_vars])
        self.message = f"""
        variables:
        {vars}
        with keys:
        {keys_str}
        from dict in cell 'op_parameters'
        {op_parameters_ins}
        must be declared in cell 'parameters'
        """
        super().__init__(self.message)


@dataclass(frozen=True)
class NbDataCatalog:
    ins: object
    outs: object


def standalone_context(cfg: dg.Config) -> DagstermillExecutionContext:
    return dagstermill.get_context(cfg)


class NbOp:
    def _paths(self, outs: list[str], run_id: str):
        root_path = f"s3://backet/path/{run_id}/"
        return [root_path + out_name for out_name in outs]

    current = None

    def __init__(
        self,
        config_schema: type[dg.Config] = None,
        ins: list[str] = None,
        outs: list[str] = None,
    ) -> None:
        """
        ins: [data1, data2] -> to catalog
        outs: output name(variable store the path to)
        """
        self._op_params = {}
        if config_schema:
            self._op_params["config_schema"] = config_schema
        if ins:
            self._op_params["ins"] = ins
        if outs:
            self._op_params["outs"] = outs
        NbOp.current = self

    @staticmethod
    def get_curren_params() -> dict[str, dict]:
        return copy.deepcopy(NbOp.current._op_params)

    def pass_outs_to_next_steps(self):
        for output_name, storage_path in asdict(self.outs).items():
            dagstermill.yield_result(value=storage_path, output_name=output_name)

    def set_context(self, context: DagstermillExecutionContext):
        self._context = context
        if "ins" in self._op_params:
            ins = self._op_params["ins"]
            self.ins = make_paths(
                ins, self._paths(outs=ins, run_id=self._context.run_id)
            )

        if "outs" in self._op_params:
            outs = self._op_params["outs"]
            self.outs = make_paths(
                outs, self._paths(outs=outs, run_id=self._context.run_id)
            )

        self.cfg = self._context.op_config
        if isinstance(self._context.op_config, dict):
            self.cfg = SimpleNamespace(self._context.op_config)
