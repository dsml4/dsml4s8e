from functools import cached_property
from types import SimpleNamespace
from dataclasses import make_dataclass, asdict, dataclass


import dagster as dg
import dagstermill
from dagstermill import DagstermillExecutionContext


@dataclass
class StorageCatalog:
    pass


def make_storage_catalog(names: list[str], paths: list[str]) -> StorageCatalog:
    return make_dataclass(
        cls_name="StorageCatalog",
        fields=[(name, str) for name in names],
        bases=(StorageCatalog,),
    )(*paths)


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


def standalone_context(
    op_config: dg.Config | None = None,
) -> DagstermillExecutionContext:
    return dagstermill.get_context(op_config)


class NbOp:
    def create_catalog(self, names: list[str], run_id: str):
        root_path = f"s3://backet/path/{run_id}/"
        paths = [root_path + out_name for out_name in names]
        return make_storage_catalog(names=names, paths=paths)

    def __init__(
        self,
        context: DagstermillExecutionContext,
        ins: list[str] | None = None,
        outs: list[str] | None = None,
    ) -> None:
        """
        ins: [data1, data2] -> to catalog
        outs: output name(variable store the path to)
        """
        self._op_params = {}
        self.context = context
        op_config = context.op_config
        if op_config:
            self._op_params["config_schema"] = type(op_config)
        if ins:
            self._op_params["ins"] = ins
        if outs:
            self._op_params["outs"] = outs

    @property
    def op_params(self) -> dict:
        return self._op_params

    @property
    def cfg(self):
        if isinstance(self.context.op_config, dict):
            return SimpleNamespace(self.context.op_config)
        return self.context.op_config

    @cached_property
    def ins(self) -> StorageCatalog | None:
        if "ins" in self._op_params:
            return self.create_catalog(
                names=self._op_params["ins"], run_id=self.context.run_id
            )
        return None

    @cached_property
    def outs(self) -> StorageCatalog | None:
        if "outs" in self._op_params:
            return self.create_catalog(
                names=self._op_params["outs"], run_id=self.context.run_id
            )
        return None

    def _yield_output_paths(self):
        if self.outs is None:
            return
        for output_name, storage_path in asdict(self.outs).items():
            dagstermill.yield_result(value=storage_path, output_name=output_name)
