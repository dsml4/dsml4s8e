from dataclasses import dataclass


from .nb_data_keys import (
    NotebookDataKeys,
    data_key2dag_name
    )
from .storage_catalog import StorageCatalogABC
from . import urls
from datetime import datetime
import glob
# component serialize: notebook run parameters


@dataclass(frozen=True)
class NbDataUrl:
    ins: object
    outs: object


def get_ins_key_data_obj(local_vars: dict, dag_op_ins: dict) -> dict:
    return {
        k: local_vars[k_alias]
        for k, k_alias in dag_op_ins.items()
    }


def make_urls_dict(
        keys: list[str],
        run_id: str,
        cdlc_stage: str,
        url_prefix: str
        ) -> dict[str: str]:
    return dict(
        [(k, urls.data_key2url(
            k,
            cdlc_stage,
            run_id,
            url_prefix
            ))
            for k in keys])


def get_nb_path(dag_context):
    if not dag_context.op_def.tags:
        import ipynbname
        return ipynbname.path()
    else:
        return dag_context.op_def.tags['notebook_path']


class NBContext:
    def __init__(self,
                 dagster_context: dict,
                 dag_op_params: dict,
                 local_vars: dict,
                 storage_catalog: StorageCatalogABC) -> None:
        if dagster_context:
            self.path = get_nb_path(dagster_context)
            self.nb_data_keys = NotebookDataKeys(
                ins_data_key_dag_name=dag_op_params.get('ins', {}),
                outs=dag_op_params.get('outs', []),
                nb_path=self.path
            )
        self._ins_dict = {}
        self._outs_dict = {}
        if 'ins' in dag_op_params:
            self._ins_dict = get_ins_key_data_obj(
                local_vars=local_vars,
                dag_op_ins=dag_op_params['ins']
            )
        if 'outs' in dag_op_params:
            self._outs_dict = storage_catalog.get_out_urls(
                data_kyes=self.nb_data_keys.outs,
            )

    def get_data_urls(
            self
            ) -> NbDataUrl:
        return NbDataUrl(
            ins=urls.make_data_obj_urls_from_dict(self._ins_dict),
            outs=urls.make_data_obj_urls_from_dict(self._outs_dict)
            )

    def validate(self):
        return True

    def send_outs_data_urls_to_next_stage(self, yield_result):
        print('outs dagster params:')
        for data_obj_key, url in self._outs_dict.items():
            dagster_name = data_key2dag_name(data_obj_key)
            print(dagster_name)
            yield_result(
                url,
                output_name=dagster_name
             )
