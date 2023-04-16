from dsml4s8e import dotted_catalog_path
from dsml4s8e.storage_catalog import StorageCatalogABC
from dsml4s8e.data_catalog import NotebookPaths

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
class NbDataCatalog:
    ins: object
    outs: object


class NbOp:
    _current_op_id = None
    _current_nb_path = None
    ops: Dict[str, dict] = {}

    @classmethod
    def dotted_path2path_varname(cls, path: str):
        return '_'.join(['path'] + path.split('.')[-2:])

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

    def _get_ins_data_paths(self, locals_: Dict[str, dict]) -> Dict[str, str]:
        """
        Create the data_paths dict with keys from ins and values from locals_
        (join by data_path).
        Variables with names from ins dict values must be in locals_
        else the MissedInsParameters exсeption is raised:
        'ins': {'data_path': 'nb_data1'} ->
               {data_path: _locals['nb_data1']}.
        Where locals_ is a Local symbol Table returning by
        the Build-In Python function locals().
        """
        ins: Dict[str, dict] = self._op_params['ins']
        paths_dict = {
            k: locals_.get(k_alias, '')
            for k, k_alias in ins.items()
        }
        empty_vals = [k for k, path in paths_dict.items() if not path]
        if len(empty_vals) > 0:
            raise MissedInsParameters(empty_vals, ins)
        return paths_dict

    def get_catalog(
            self,
            locals_: Dict[str, dict],
            storage_catalog: StorageCatalogABC
            ) -> NbDataCatalog:
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
        self.nb_data_keys = NotebookPaths(
            ins_catalog_paths=op_params.get('ins', {}),
            outs_vars=op_params.get('outs', []),
            op_id=self._id
        )
        _ins_dict = {}
        self._outs_dict = {}
        if 'ins' in op_params:
            _ins_dict = self._get_ins_data_paths(locals_)
        if 'outs' in op_params:
            self._outs_dict = storage_catalog.get_outs_data_paths(
                catalog=self.nb_data_keys.outs,
            )
        return NbDataCatalog(
            ins=dotted_catalog_path.do_dotted_paths(_ins_dict),
            outs=dotted_catalog_path.do_dotted_paths(self._outs_dict)
            )

    def pass_outs_to_next_steps(self):
        out_paths = []

        print("Strings below can be pasted in cells with tags 'parameters' of next notebooks.\n")
        for dotted_path, storage_path in self._outs_dict.items():
            path_varname = self.dotted_path2path_varname(dotted_path)
            out_paths.append((f"'{dotted_path}'",
                             f"'{path_varname}'"))
            print(f"{path_varname} = '{storage_path}'")
            dagstermill.yield_result(
                storage_path,
                output_name=path_varname
             )
        print("\nStrings below can be pasted in code of declaration of NbOp")
        print("in cells with tags 'op_parameters' of next notebooks.\n")
        for path in out_paths:
            print(f'{path[0]}:{path[1]},')

    def get_context(self):
        return dagstermill.get_context(op_config=self.config)

    @cached_property
    def config(self):
        config_schema: Dict[str, Field] = self._op_params['config_schema']
        return {
            k: v.default_value for k, v in
            config_schema.items()
        }
