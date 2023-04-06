from dsml4s8e import dotted_urls_names
from dsml4s8e.storage_catalog import StorageCatalogABC
from dsml4s8e.nb_data_keys import NotebookDataKeys

import dagstermill

from pathlib import Path
from typing import Dict, Tuple
from functools import cached_property
from dagster import Field
from dataclasses import dataclass


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


@dataclass(frozen=True)
class NbDataUrls:
    ins: object
    outs: object


class NbOp:
    _current_op_id = None
    _current_nb_path = None
    ops: Dict[str, dict] = {}

    @classmethod
    def data_key2url_name(cls, data_key: str):
        return '_'.join(['url'] + data_key.split('.')[-2:])

    @classmethod
    def set_current_nb_path(cls, nb_path: str):
        cls._current_nb_path = nb_path

    @classmethod
    def params(cls):
        return cls.ops[cls._current_op_id].copy()

    @classmethod
    def _op_name_and_id_from_nb_path(
        cls,
        nb_path: str,
        level_from_root: int
    ) -> Tuple[str, str]:
        """
        retrun op_name, op_id
        """
        p = Path(nb_path)
        level_from_root += 1
        op_name = p.stem
        op_id = '.'.join(list(p.parts[-level_from_root:-1]) + [op_name])
        return p.stem, op_id

    def __init__(self,
                 config_schema: Dict[str, Field] = None,
                 ins: Dict[str, dict] = None,
                 outs: Dict[str, dict] = None,
                 level_from_root=2,
                 ) -> None:
        self._op_params = {}
        self._level_from_root = level_from_root
        if config_schema:
            self._op_params['config_schema'] = config_schema
        if ins:
            self._op_params['ins'] = ins
        if outs:
            self._op_params['outs'] = outs
        if self._current_nb_path:
            # If the static method set_current_nb_path has been called outside.
            # This code is executed to extract op parameters from a notebook.
            # For more details look op_params_nb.dagstermill_op_params_from_nb
            (
             self._op_params['name'],
             NbOp._current_op_id
             ) = self._op_name_and_id_from_nb_path(
                nb_path=self._current_nb_path,
                level_from_root=level_from_root
            )
            NbOp.ops[NbOp._current_op_id] = self._op_params

    def _get_ins_data_urls(self, locals_: Dict[str, dict]) -> Dict[str, str]:
        """
        Create the data_urls dict with keys from ins and values from locals_
        (join by data_url_key).
        Variables with names from ins dict values must be in locals_
        else the MissedInsParameters exсeption is raised:
        'ins': {'data_url_key': 'nb_data1'} ->
               {data_url_key: _locals['nb_data1']}.
        Where locals_ is a Local symbol Table returning by
        the Build-In Python function locals().
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

    def get_data_urls(
            self,
            locals_: Dict[str, dict],
            storage_catalog: StorageCatalogABC
            ) -> NbDataUrls:
        """
        This metod calls from notebook.
        locals_ is a Local symbol Table
        returning by the Build-In Python function locals().
        """
        dagster_context = locals_['context']
        if 'notebook_path' in dagster_context.op_def.tags:
            # if a notebook is executed by dagstermill
            nb_path = dagster_context.op_def.tags['notebook_path']
        else:
            # if a notebook is executed by jupyter
            nb_path = locals_['__session__']

        self.nb_name, self._id = self._op_name_and_id_from_nb_path(
                    nb_path=nb_path,
                    level_from_root=self._level_from_root
                    )
        op_params = self._op_params
        self.nb_data_keys = NotebookDataKeys(
            ins_data_key_dag_name=op_params.get('ins', {}),
            outs=op_params.get('outs', []),
            op_id=self._id
        )
        _ins_dict = {}
        self._outs_dict = {}
        if 'ins' in op_params:
            _ins_dict = self._get_ins_data_urls(locals_)
        if 'outs' in op_params:
            self._outs_dict = storage_catalog.get_outs_data_urls(
                data_kyes=self.nb_data_keys.outs,
            )
        return NbDataUrls(
            ins=dotted_urls_names.do_dotted_urls_names(_ins_dict),
            outs=dotted_urls_names.do_dotted_urls_names(self._outs_dict)
            )

    def pass_outs_to_next_steps(self):
        out_keys = []
        for data_obj_key, url in self._outs_dict.items():
            url_name = self.data_key2url_name(data_obj_key)
            out_keys.append((f"'{data_obj_key}'",
                             f"'{url_name}'"))
            print(f"{url_name} = '{url}'")
            dagstermill.yield_result(
                url,
                output_name=url_name
             )
        print("\n(data_key: url_variable_name) to put in ins dict of next ops")
        for url_key in out_keys:
            print(f'{url_key[0]}:{url_key[1]},')

    def get_context(self):
        return dagstermill.get_context(op_config=self.config)

    @cached_property
    def config(self):
        config_schema: Dict[str, Field] = self._op_params['config_schema']
        return {
            k: v.default_value for k, v in
            config_schema.items()
        }
