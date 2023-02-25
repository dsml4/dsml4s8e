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


def get_nb_path(dag_context):
    if not dag_context.op_def.tags:
        import ipynbname
        return ipynbname.path()
    else:
        return dag_context.op_def.tags['notebook_path']


class NBInterface:
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
            self._ins_dict = storage_catalog.get_in_urls(
                local_vars=local_vars,
                op_parameters_ins=dag_op_params['ins']
            )
            for k in dag_op_params['ins'].values():
                del local_vars[k]
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

    def send_outs_data_urls_to_next_stages(self, yield_result):
        print('outs:')
        for data_obj_key, url in self._outs_dict.items():
            dagster_name = data_key2dag_name(data_obj_key)
            print(f'{dagster_name} = "{url}"')
            yield_result(
                url,
                output_name=dagster_name
             )
