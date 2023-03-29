from pathlib import Path
from typing import Dict
from functools import cached_property
from dagster import Field

import dagstermill


def op_name_from_nb_path(nb_path, level_from_root=2):
    p = Path(nb_path)
    level_from_root += 1
    return '.'.join(list(p.parts[-level_from_root:-1]) + [p.stem]), p.stem


class MissedInsParameters(Exception):
    def __init__(self, missed_vars, op_parameters_ins):
        keys_str = '/n'.join(missed_vars)
        vars = '/n'.join([op_parameters_ins[k] for k in missed_vars])
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


class NbOpParams:
    current_op_id = 'from_jupyter'
    ops: Dict[str, dict] = {}

    def __init__(self,
                 config_schema: Dict[str, Field] = None,
                 ins: Dict[str, dict] = None,
                 outs: Dict[str, dict] = None,
                 path: str = None,
                 level_from_root=2
                 ) -> None:
        self._op_params = {}
        self._level_from_root = level_from_root
        if self.current_op_id == 'from_jupyter':
            import ipynbname
            try:
                path = ipynbname.path()
                self.current_op_id, self.nb_name = op_name_from_nb_path(
                    nb_path=path,
                    level_from_root=self._level_from_root
                    )
            except Exception:
                ...
        self._id = self.current_op_id
        self.ops[self._id] = self._op_params
        if config_schema:
            self._op_params['config_schema'] = config_schema
        if ins:
            self._op_params['ins'] = ins
        if outs:
            self._op_params['outs'] = outs

    def set_locals(self, locals_: Dict[str, dict]):
        self.dagster_context = locals_['context']
        if 'notebook_path' in self.dagster_context.op_def.tags:
            self._id, self.nb_name = op_name_from_nb_path(
                        nb_path=self.dagster_context.op_def.tags['notebook_path'],
                        level_from_root=self._level_from_root
                        )

    def get_ins_data_urls(self, locals_: Dict[str, dict]) -> Dict[str, str]:
        """
        op_parameters_ins = op_parameters['ins']
        op_parameters is a dict from notebook cell with tag op_parameters
        _locals = locals() # Local symbol Table
        'ins': {'key': 'nb_data1'} -> {key: _locals['nb_data1']}
        """
        ins: Dict[str, dict] = self._op_params['ins']
        urls_dict = {
            k: locals_.get(k_alias, '')
            for k, k_alias in ins.items()
        }
        empty_vals = [k for k, url in urls_dict.items() if not url]
        if len(empty_vals) > 0:
            raise MissedInsParameters(empty_vals, ins)
        return urls_dict

    def get_context(self):
        return dagstermill.get_context(op_config=self.config)

    @property
    def id(self):
        return self._id

    @property
    def op_params(self):
        return self._op_params

    @cached_property
    def config(self):
        config_schema: Dict[str, Field] = self._op_params['config_schema']
        return {
            k: v.default_value for k, v in
            config_schema.items()
        }

    @classmethod
    def params(cls):
        return cls.ops[cls.current_op_id].copy()
