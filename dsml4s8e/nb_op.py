from functools import cached_property
from dataclasses import dataclass, make_dataclass, asdict

import dagster as dg
import dagstermill
from dagstermill import DagstermillExecutionContext


def make_storage_catalog(names: list[str], paths: list[str]):
    return make_dataclass(
        cls_name="StorageCatalog", fields=[(name, str) for name in names]
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


@dataclass(frozen=True)
class NbDataCatalog:
    ins: object
    outs: object


def standalone_context(
    op_onfig: dg.Config | None = None,
) -> DagstermillExecutionContext:
    return dagstermill.get_context(op_onfig)


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

    @cached_property
    def catalog(self) -> NbDataCatalog:
        ins = None
        outs = None
        if "ins" in self._op_params:
            ins = self.create_catalog(
                names=self._op_params["ins"], run_id=self.context.run_id
            )
        if "outs" in self._op_params:
            outs = self.create_catalog(
                names=self._op_params["outs"], run_id=self.context.run_id
            )
        return NbDataCatalog(ins=ins, outs=outs)

    def pass_outs_to_next_steps(self):
        if "outs" not in self._op_params:
            return
        for output_name, storage_path in asdict(self._op_params[""]).items():
            dagstermill.yield_result(value=storage_path, output_name=output_name)
